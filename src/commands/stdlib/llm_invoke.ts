import { defineCommand } from "../define.js";
import { markLlmItem } from "../../llm/accounting.js";
import type { LlmChargeCost, LlmSpendLedger, LlmProvenance } from "../../llm/accounting.js";
import { formatSchemaError, prepareSchema, type SchemaValidator } from "../../util/validation.js";
import { streamOf } from "../../io/streams.js";
import {
	payloadValidator,
	responseEnvelopeValidator,
	type LlmResponseEnvelope,
	type NormalizedInvocationItem,
	normalizeArtifact,
	hashArtifact,
	computeCacheKey,
	normalizeResult,
	formatSchemaErrors,
} from "../../llm/protocol.js";
import { type SupportedProvider, resolveProvider, resolveAdapter } from "../../llm/adapters.js";
import { asCancellation, throwIfCancelled, abortable } from "../../util/abort.js";
import {
	persistOutputs,
	readReusableLlmState,
	pickReusableState,
	readCacheEntry,
	writeCacheEntry,
} from "../../llm/cache.js";
import {
	resolveModel,
	resolveEnvString,
	getFirstEnv,
	extractPrompt,
	parseJsonArray,
	parseJsonObject,
	parseOptionalNumber,
	flag,
} from "../../llm/flags.js";
import { messageOf } from "../../util/errors.js";

const DEFAULT_MAX_VALIDATION_RETRIES = 1;

/**
 * Opens a charge for a live call, and only for one that costs something. An answer that reports
 * no usage is billed nowhere however many times it is replayed, so a charge for it would sit in
 * the ledger with nothing to settle it -- and would be handed to the next caller with a real
 * call to account for, leaving that one's charge open to be billed a second time.
 */
function recordLiveCharge(ctx: any, cacheKey: string, item: NormalizedInvocationItem | undefined) {
	const cost = chargeCostOf(item);
	if (!cost) return;
	ledgerFrom(ctx)?.record(cacheKey, cost);
}

/** The cost of a live call, read from the item this module just built for it. */
function chargeCostOf(item: NormalizedInvocationItem | undefined): LlmChargeCost | undefined {
	const usage = item?.usage;
	if (!usage || typeof usage !== "object") return undefined;
	return { model: typeof item?.model === "string" ? item.model : null, usage };
}

function ledgerFrom(ctx: any): LlmSpendLedger | null {
	const ledger = ctx?.llmSpendLedger;
	if (!ledger || typeof ledger !== "object") return null;
	return typeof ledger.record === "function" && typeof ledger.claim === "function" ? ledger : null;
}

export type CommandConfig = {
	name: string;
	itemKind: string;
	stateType: string;
	cacheNamespace: string;
	defaultProvider?: SupportedProvider | null;
	description: string;
	helpTitle: string;
	helpConfig: string[];
	helpExamples: string[];
	sourceForProvider?: (provider: SupportedProvider) => string;
};

export const llmInvokeCommand = createLlmInvokeCommand({
	name: "llm.invoke",
	itemKind: "llm.invoke",
	stateType: "llm.invoke",
	cacheNamespace: "llm.invoke",
	defaultProvider: null,
	description: "Call a configured LLM adapter with typed payloads and caching",
	helpTitle: "llm.invoke — call a configured LLM adapter with caching and schema validation",
	helpConfig: [
		"Provider resolution order: --provider, LOBSTER_LLM_PROVIDER, then environment auto-detect.",
		"Built-in providers: openclaw, pi, http.",
		"OpenClaw provider uses OPENCLAW_URL (CLAWD_URL also supported) and OPENCLAW_TOKEN.",
		"Pi provider uses LOBSTER_PI_LLM_ADAPTER_URL and is intended to be supplied by a Pi extension.",
		"Generic http provider uses LOBSTER_LLM_ADAPTER_URL and optional LOBSTER_LLM_ADAPTER_TOKEN.",
	],
	helpExamples: [
		"llm.invoke --prompt 'Write summary'",
		"llm.invoke --provider openclaw --model claude-3-sonnet --prompt 'Write summary'",
		"cat artifacts.json | llm.invoke --provider pi --prompt 'Score each item'",
		"... | llm.invoke --prompt 'Plan next steps' --output-schema '{\"type\":\"object\"}'",
	],
	sourceForProvider(provider) {
		return provider;
	},
});

export function createLlmInvokeCommand(config: CommandConfig) {
	return defineCommand({
		name: config.name,
		meta: {
			description: config.description,
			argsSchema: {
				type: "object",
				properties: {
					provider: {
						type: "string",
						description: "LLM adapter provider (openclaw, pi, http). Optional if auto-detected.",
					},
					token: {
						type: "string",
						description: "Optional bearer token for providers that support it.",
					},
					prompt: { type: "string", description: "Primary prompt / instructions" },
					model: {
						type: "string",
						description: "Model identifier. Optional; adapter defaults may apply if omitted.",
					},
					"artifacts-json": { type: "string", description: "JSON array of artifacts to send" },
					"metadata-json": { type: "string", description: "JSON object of metadata to include" },
					"output-schema": { type: "string", description: "JSON schema LLM output must satisfy" },
					"schema-version": { type: "string", description: "Logical schema version for caching" },
					"max-validation-retries": {
						type: "number",
						description:
							"Extra model calls allowed after the first when schema validation fails (default 1)",
					},
					temperature: { type: "number", description: "Sampling temperature" },
					"max-output-tokens": { type: "number", description: "Max completion tokens" },
					"state-key": {
						type: "string",
						description: "Run-state key override (else LOBSTER_RUN_STATE_KEY)",
					},
					refresh: { type: "boolean", description: "Bypass run-state + cache" },
					"disable-cache": { type: "boolean", description: "Skip persistent cache" },
					_: { type: "array", items: { type: "string" } },
				},
				required: [],
			},
			sideEffects: ["calls_llm"],
			// Answers are cached and replayed, so a repeated call does not repeat the work.
			repeatable: true,
		},
		help() {
			const lines = [
				config.helpTitle,
				"",
				"Usage:",
				...config.helpExamples.map((example) => `  ${example}`),
				"",
				"Features:",
				"  - Typed payload validation before invoking the adapter.",
				"  - Run-state + file cache so resumes do not re-call the LLM.",
				"  - Optional JSON-schema enforcement, retried at most --max-validation-retries times",
				"    after the first call.",
				"",
				"Config:",
				...config.helpConfig.map((line) => `  - ${line}`),
			];
			return `${lines.join("\n")}\n`;
		},
		async run({ input, args, ctx }) {
			return runLlmInvoke({ input, args, ctx, config });
		},
	});
}

async function runLlmInvoke({
	input,
	args,
	ctx,
	config,
}: {
	input: AsyncIterable<any>;
	args: any;
	ctx: any;
	config: CommandConfig;
}) {
	const env = ctx.env ?? process.env;
	const signal: AbortSignal | undefined = ctx?.signal;
	// Run-state and cache hits return before any adapter call, so a cancelled run
	// would otherwise still finish as a success.
	throwIfCancelled(signal);
	const provider = resolveProvider(args, env, config.defaultProvider, ctx);
	const adapter = resolveAdapter({ provider, env, args, config, ctx });
	const prompt = extractPrompt(args);
	if (!prompt) throw new Error(`${config.name} requires --prompt or positional text`);

	const model = resolveModel(args, env);
	const schemaVersion = resolveEnvString(
		args["schema-version"],
		["LOBSTER_LLM_SCHEMA_VERSION", "LLM_TASK_SCHEMA_VERSION"],
		env,
		"v1",
	);
	const maxOutputTokens = parseOptionalNumber(args["max-output-tokens"]);
	const temperature = parseOptionalNumber(args.temperature);
	const providedArtifacts = parseJsonArray(
		args["artifacts-json"],
		`${config.name} --artifacts-json`,
	);
	const metadataObject = parseJsonObject(args["metadata-json"], `${config.name} --metadata-json`);
	const userOutputSchema = parseJsonObject(args["output-schema"], `${config.name} --output-schema`);
	const maxValidationRetriesRaw =
		args["max-validation-retries"] ??
		getFirstEnv(env, ["LOBSTER_LLM_VALIDATION_RETRIES", "LLM_TASK_VALIDATION_RETRIES"]);
	const maxValidationRetries = userOutputSchema
		? Math.max(
				0,
				Number.isFinite(Number(maxValidationRetriesRaw))
					? Number(maxValidationRetriesRaw)
					: DEFAULT_MAX_VALIDATION_RETRIES,
			)
		: 0;
	const disableCache = flag(args["disable-cache"]);
	const forceRefresh = flag(
		args.refresh ?? getFirstEnv(env, ["LOBSTER_LLM_FORCE_REFRESH", "LLM_TASK_FORCE_REFRESH"]),
	);
	const stateKey = String(args["state-key"] ?? env.LOBSTER_RUN_STATE_KEY ?? "").trim() || null;

	const inputArtifacts: any[] = [];
	for await (const item of input) inputArtifacts.push(item);
	// Draining pipeline input waits on the upstream step, so a timeout can fire
	// here. Re-check before the reuse lookups below can answer with a success.
	throwIfCancelled(signal);

	const normalizedArtifacts = [...inputArtifacts, ...providedArtifacts].map(normalizeArtifact);
	const artifactHashes = normalizedArtifacts.map(hashArtifact);
	const cacheKey = computeCacheKey({
		provider,
		prompt,
		model,
		schemaVersion,
		artifactHashes,
		outputSchema: userOutputSchema,
		temperature,
		maxOutputTokens,
	});

	if (stateKey && !forceRefresh) {
		const stored = await readReusableLlmState(env, stateKey, ctx.signal);
		// Reading run state is I/O of unbounded duration; a signal that aborted
		// during it must not be overtaken by the replay below.
		throwIfCancelled(signal);
		const reused = pickReusableState(stored, cacheKey, config.stateType);
		if (reused) {
			const replay: LlmProvenance = { cacheKey, replayed: true };
			return {
				output: streamOf(
					reused.items.map((item) =>
						markLlmItem({ ...item, source: "run_state", cached: true, replayed: true }, replay),
					),
				),
			};
		}
	}

	if (!disableCache && !forceRefresh) {
		const cache = await readCacheEntry(env, cacheKey, config.cacheNamespace, ctx.signal);
		throwIfCancelled(signal);
		if (cache) {
			const replay: LlmProvenance = { cacheKey, replayed: true };
			return {
				output: streamOf(
					cache.items.map((item) =>
						markLlmItem({ ...item, source: "cache", cached: true, replayed: true }, replay),
					),
				),
			};
		}
	}

	const payload: Record<string, any> = {
		prompt,
		...(model ? { model } : null),
		artifacts: normalizedArtifacts,
		artifactHashes,
	};
	if (metadataObject) payload.metadata = metadataObject;
	if (userOutputSchema) payload.outputSchema = userOutputSchema;
	if (schemaVersion) payload.schemaVersion = schemaVersion;
	if (Number.isFinite(maxOutputTokens ?? NaN)) payload.maxOutputTokens = Number(maxOutputTokens);
	if (Number.isFinite(temperature ?? NaN)) payload.temperature = Number(temperature);

	const payloadCheck = payloadValidator.validate(payload, { allErrors: true });
	if (!payloadCheck.ok) {
		throw new Error(`${config.name} payload invalid: ${formatSchemaErrors(payloadCheck.errors)}`);
	}

	let validator: SchemaValidator | null = null;
	if (userOutputSchema) {
		try {
			validator = prepareSchema(userOutputSchema);
		} catch (err) {
			throw new Error(`${config.name} output schema is invalid: ${messageOf(err)}`);
		}
	}
	let attempt = 0;
	let lastValidationErrors: string[] = [];

	while (true) {
		throwIfCancelled(signal);
		attempt += 1;
		if (attempt > 1) {
			payload.retryContext = {
				attempt,
				...(lastValidationErrors.length ? { validationErrors: lastValidationErrors } : null),
			};
		} else {
			delete payload.retryContext;
		}

		let responseEnvelope: LlmResponseEnvelope;
		try {
			ctx.signal?.throwIfAborted();
			ctx.assertInvocationCurrent?.();
			responseEnvelope = await abortable(
				adapter.invoke({
					env,
					args,
					payload,
					signal: ctx.signal,
					assertInvocationCurrent: ctx.assertInvocationCurrent,
				}),
				signal,
			);
			ctx.signal?.throwIfAborted();
		} catch (err) {
			// Cancellation is the caller's error, not an adapter failure: surface it
			// as an abort so workflow timeout and abort handling still recognizes it,
			// rather than wrapping it in a "request failed" adapter error.
			if (signal?.aborted) throw asCancellation(err, signal);
			throw new Error(`${config.name} request failed: ${messageOf(err)}`);
		}

		if (!responseEnvelopeValidator.validate(responseEnvelope).ok) {
			throw new Error(`${config.name} received invalid response envelope`);
		}

		if (responseEnvelope.ok !== true) {
			const message = responseEnvelope.error?.message ?? "llm adapter returned an error";
			throw new Error(`${config.name} remote error: ${message}`);
		}

		const normalized = normalizeResult({
			envelope: responseEnvelope,
			cacheKey,
			schemaVersion,
			artifactHashes,
			source: adapter.source,
			attempt,
			itemKind: config.itemKind,
		});
		const live: LlmProvenance = { cacheKey, replayed: false };
		for (const item of normalized) markLlmItem(item, live);
		// The provider has answered and been paid, so the charge is opened here rather than at
		// either of the returns below. An attempt the local validator rejects never reaches one
		// of them -- it goes round the loop and asks again -- and its call was as real as the one
		// that eventually satisfies the schema. Opening it here also puts it before the writes
		// that store the answer: either can fail once run state already holds a replayable copy,
		// and the retry that replays it must still find a charge to settle.
		recordLiveCharge(ctx, cacheKey, normalized[0]);

		if (!validator) {
			ctx.signal?.throwIfAborted();
			await persistOutputs({
				env,
				stateKey,
				cacheKey,
				items: normalized,
				stateType: config.stateType,
				signal: ctx.signal,
				assertInvocationCurrent: ctx.assertInvocationCurrent,
				afterStore: disableCache
					? undefined
					: () =>
							writeCacheEntry(
								env,
								cacheKey,
								normalized,
								config.cacheNamespace,
								ctx.signal,
								ctx.assertInvocationCurrent,
							),
			});
			return { output: streamOf(normalized) };
		}

		const structured = normalized[0]?.output?.data ?? null;
		const outputCheck = validator.validate(structured, { allErrors: true });
		if (outputCheck.ok) {
			ctx.signal?.throwIfAborted();
			await persistOutputs({
				env,
				stateKey,
				cacheKey,
				items: normalized,
				stateType: config.stateType,
				signal: ctx.signal,
				assertInvocationCurrent: ctx.assertInvocationCurrent,
				afterStore: disableCache
					? undefined
					: () =>
							writeCacheEntry(
								env,
								cacheKey,
								normalized,
								config.cacheNamespace,
								ctx.signal,
								ctx.assertInvocationCurrent,
							),
			});
			return { output: streamOf(normalized) };
		}

		lastValidationErrors = outputCheck.errors.map(formatSchemaError);
		if (attempt > maxValidationRetries) {
			throw new Error(
				`${config.name} output failed schema validation: ${lastValidationErrors.join("; ")}`,
			);
		}
	}
}
