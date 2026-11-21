// What `llm.invoke` sends to an adapter and accepts back: the payload and response schemas,
// the items it builds from a response, and the cache key that identifies an answer.
import { createHash } from "node:crypto";
import { stableStringify } from "../util/serialization.js";
import { formatSchemaError, prepareSchema, type SchemaError } from "../util/validation.js";
import type { SupportedProvider } from "./adapters.js";

const artifactSchema = {
	type: "object",
	properties: {
		kind: { type: "string" },
		role: { type: "string" },
		name: { type: "string" },
		mimeType: { type: "string" },
		text: { type: "string" },
		data: {},
		uri: { type: "string" },
	},
	additionalProperties: true,
};

const payloadSchema = {
	type: "object",
	properties: {
		prompt: { type: "string", minLength: 1 },
		model: { type: "string", minLength: 1 },
		artifacts: { type: "array", items: artifactSchema },
		artifactHashes: { type: "array", items: { type: "string", minLength: 10 } },
		schemaVersion: { type: "string" },
		metadata: { type: "object", additionalProperties: true },
		outputSchema: { type: "object", additionalProperties: true },
		temperature: { type: "number" },
		maxOutputTokens: { type: "number" },
		retryContext: {
			type: "object",
			properties: {
				attempt: { type: "number" },
				validationErrors: { type: "array", items: { type: "string" } },
			},
			additionalProperties: false,
		},
	},
	required: ["prompt", "artifacts", "artifactHashes"],
	additionalProperties: false,
};

const responseSchema = {
	type: "object",
	properties: {
		ok: { type: "boolean" },
		result: {
			type: "object",
			properties: {
				runId: { type: "string" },
				model: { type: "string" },
				prompt: { type: "string" },
				status: { type: "string" },
				output: {
					type: "object",
					properties: {
						text: { type: "string" },
						data: {},
						format: { type: "string" },
					},
					required: [],
					additionalProperties: true,
				},
				usage: {
					type: "object",
					properties: {
						inputTokens: { type: "number" },
						outputTokens: { type: "number" },
						totalTokens: { type: "number" },
					},
					additionalProperties: true,
				},
				warnings: { type: "array", items: { type: "string" } },
				metadata: { type: "object", additionalProperties: true },
				diagnostics: { type: "object", additionalProperties: true },
			},
			required: ["output"],
			additionalProperties: true,
		},
		error: { type: "object", additionalProperties: true },
	},
	required: ["ok"],
	additionalProperties: true,
};

export const payloadValidator = prepareSchema(payloadSchema);

export const responseEnvelopeValidator = prepareSchema(responseSchema);

export const STATE_VERSION = 1;

// Identity version of the response cache key. See computeCacheKey.
const CACHE_KEY_VERSION = 2;

export type LlmResponseEnvelope = {
	ok: boolean;
	result?: LlmResponse | null;
	error?: { message?: string } | null;
};

type LlmResponse = {
	runId?: string | null;
	model?: string | null;
	prompt?: string | null;
	status?: string | null;
	output?: {
		text?: string | null;
		data?: any;
		format?: string | null;
	} | null;
	usage?: Record<string, unknown> | null;
	warnings?: string[] | null;
	metadata?: Record<string, unknown> | null;
	diagnostics?: Record<string, unknown> | null;
};

export type NormalizedInvocationItem = {
	kind: string;
	runId: string | null;
	prompt: string | null;
	model: string | null;
	schemaVersion: string | null;
	status: string;
	cacheKey: string;
	artifactHashes: string[];
	output: { format: string | null; text: string | null; data: any };
	usage: Record<string, unknown> | null;
	metadata: Record<string, unknown> | null;
	warnings: string[] | null;
	diagnostics: Record<string, unknown> | null;
	createdAt: string;
	source: string;
	cached: boolean;
	// Set only when a stored item is re-emitted, so consumers can tell a replay from a live
	// call. `source` cannot carry that: a direct adapter's source is its provider name.
	replayed?: boolean;
	attemptCount: number;
};

export function normalizeArtifact(raw: any) {
	if (raw && typeof raw === "object" && !Array.isArray(raw)) {
		return raw;
	}
	if (typeof raw === "string") {
		return { kind: "text", text: raw };
	}
	if (typeof raw === "number" || typeof raw === "boolean") {
		return { kind: "text", text: String(raw) };
	}
	return { kind: "json", data: raw };
}

export function hashArtifact(artifact: any) {
	const stable = stableStringify(artifact);
	return createHash("sha256").update(stable).digest("hex");
}

export function computeCacheKey({
	provider,
	prompt,
	model,
	schemaVersion,
	artifactHashes,
	outputSchema,
	temperature,
	maxOutputTokens,
}: {
	provider: SupportedProvider;
	prompt: string;
	model: string;
	schemaVersion: string;
	artifactHashes: string[];
	outputSchema: any;
	temperature: number | null;
	maxOutputTokens: number | null;
}) {
	// `null` is the identity of an omitted parameter, distinct from any value a caller can
	// pass, so an omitted request can never resolve to an entry written with an explicit one.
	// The version separates this identity from the keys earlier releases wrote, where the two
	// parameters were absent from the payload and a sampled answer therefore shared a key with
	// an unsampled request. Bump it whenever the fields below change: entries under older
	// versions become unreachable, which costs one re-invocation and never a wrong replay.
	const payload = {
		cacheKeyVersion: CACHE_KEY_VERSION,
		provider,
		prompt,
		model: model || `${provider}-default`,
		schemaVersion,
		artifactHashes,
		outputSchema: outputSchema ?? null,
		// The same predicate that decides whether each parameter is sent to the adapter.
		temperature: Number.isFinite(temperature ?? NaN) ? Number(temperature) : null,
		maxOutputTokens: Number.isFinite(maxOutputTokens ?? NaN) ? Number(maxOutputTokens) : null,
	};
	return createHash("sha256").update(stableStringify(payload)).digest("hex");
}

export function normalizeResult({
	envelope,
	cacheKey,
	schemaVersion,
	artifactHashes,
	source,
	attempt,
	itemKind,
}: {
	envelope: LlmResponseEnvelope;
	cacheKey: string;
	schemaVersion: string;
	artifactHashes: string[];
	source: string;
	attempt: number;
	itemKind: string;
}): NormalizedInvocationItem[] {
	const result = envelope.result ?? {};
	const output = result.output ?? {};
	const item: NormalizedInvocationItem = {
		kind: itemKind,
		runId: (result.runId ?? null) as any,
		prompt: (result.prompt ?? null) as any,
		model: (result.model ?? null) as any,
		schemaVersion,
		status: String(result.status ?? "completed"),
		cacheKey,
		artifactHashes,
		output: {
			format: (output.format ?? (output.data ? "json" : "text")) as any,
			text: (output.text ?? null) as any,
			data: (output.data ?? null) as any,
		},
		usage: (result.usage ?? null) as any,
		metadata: (result.metadata ?? null) as any,
		warnings: (result.warnings ?? null) as any,
		diagnostics: (result.diagnostics ?? null) as any,
		createdAt: new Date().toISOString(),
		source,
		// A fresh answer from the adapter; replays from the cache or the run mark themselves.
		cached: false,
		attemptCount: attempt,
	};
	return [item];
}

export function formatSchemaErrors(errors: SchemaError[]) {
	return errors.map(formatSchemaError).join(", ");
}
