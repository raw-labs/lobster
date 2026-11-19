// The SDK's `Lobster` class: a pipeline of JavaScript stages, run, resumed and advanced as a
// run of the engine, and the conversion of tokens handed out before SDK pipelines were runs.
import { checkRunId } from "../persistence/model.js";
import {
	decisionCount,
	type Decision,
	type ResumeDecision,
	type WaitResumeOptions,
} from "../resume/options.js";
import type { SdkResult } from "./types.js";
import { createHash } from "node:crypto";
import { Readable } from "node:stream";
import { discardStream } from "../io/streams.js";
import { validateCommandInputState } from "../pipeline/input_request.js";
import { parsePipeline } from "../pipeline/parser.js";
import { runtimeErrorDetails, WaitConflictError } from "../persistence/backend.js";
import { sessionFromEnv, bindResumeGuard } from "../persistence/context.js";
import { withStoreContext } from "../persistence/execution.js";
import type { PersistenceOptions, StoreEnvironment } from "../persistence/context.js";
import { findWaitAtRevision, callerId, checkWaitReference } from "../persistence/waits.js";
import {
	decodeToken,
	decodeWorkflowResumePayload,
	workflowResumePayload,
} from "../resume/token.js";
import { isPlainObject } from "../util/objects.js";
import type { RunContext } from "../engine/context.js";
import { pipelineDefinition } from "../workflows/definition.js";
import { advanceWorkflowRun, runWorkflowFile, runIdOf } from "../engine/runner.js";
import { ResumeRefusedError, UpgradeRequiredError, InvalidAnswerError } from "../engine/errors.js";
import { type WorkflowRunResult } from "../workflows/types.js";
import { savedByOlderVersion } from "../engine/upgrade.js";
import { waitingPipelineRun, type PausedPipeline } from "../engine/legacy_pauses.js";
import { CheckpointTakenError } from "../persistence/session.js";
import { sdkInputItems, sdkPipelineText, sdkStageRegistry, type SdkStage } from "./runtime.js";
import { assertSavable } from "../util/serialization.js";

/** A token handed out by Lobster versions before SDK pipelines became runs. */
type LegacySdkToken = {
	protocolVersion: 1;
	v: 1;
	resumeAtIndex: number;
	items?: unknown[];
	prompt?: string;
	inputSchema?: unknown;
	inputSubject?: unknown;
	resumeMode?: "next_stage" | "same_stage";
	stateKey?: string;
};

export type LobsterOptions = PersistenceOptions & {
	env?: NodeJS.ProcessEnv;
	stateDir?: string;
	signal?: AbortSignal;
};

/**
 * An SDK pipeline. Every run is a run of the workflow engine: a pipeline of the stages
 * `sdk.stage.0 | sdk.stage.1 | ...`, which the instance supplies. A paused run is resumed by
 * an instance with the same stages, once per token, like any other run.
 */
export class Lobster {
	#stages: SdkStage[] = [];
	#options: LobsterOptions & { env: NodeJS.ProcessEnv };
	#meta: Record<string, unknown> | null = null;

	constructor(options: LobsterOptions = {}) {
		this.#options = {
			storeSession: options.storeSession,
			history: options.history,
			runMetadata: options.runMetadata,
			env: options.env ?? process.env,
			stateDir: options.stateDir,
			signal: options.signal,
		};
	}

	pipe(stage: SdkStage) {
		if (typeof stage !== "function" && typeof stage?.run !== "function") {
			throw new Error("Stage must be a function or have a run() method");
		}
		this.#stages.push(stage);
		return this;
	}

	meta(meta: Record<string, unknown>) {
		this.#meta = meta;
		return this;
	}

	getMeta() {
		return this.#meta;
	}

	/**
	 * Run the stages on `initialInput`: an iterable of items (a string is iterated by
	 * character), null or undefined as no items, or any other value as one item.
	 */
	async run(initialInput: unknown = []): Promise<SdkResult> {
		try {
			const input = await sdkInputItems(initialInput);
			if (!this.#stages.length) return finished({ status: "ok", output: input });
			// Refused before the run exists: input the run cannot save would leave it stuck.
			assertSavable(input, "Lobster input");
			return await this.#execute(this.#options.env, (ctx) =>
				runWorkflowFile({
					definition: pipelineDefinition(this.#pipeline(), input),
					kind: "sdk",
					sdk: { pipeline: this.#pipeline() },
					ctx,
				}),
			);
		} catch (err) {
			return failed(err);
		}
	}

	resume(token: string, options: ResumeDecision): Promise<SdkResult>;
	async resume(token: string, options: Decision = {}): Promise<SdkResult> {
		refuseSeveralDecisions(options);
		const payload = decodeToken(token);
		if (!isPlainObject(payload) || payload.protocolVersion !== 1) throw new Error("Invalid token");
		const workflow = decodeWorkflowResumePayload(payload);
		if (workflow) return this.#resumeKey(workflow.stateKey, options, this.#options.env);
		const legacy = decodeLegacyToken(payload);
		return this.#resumeKey(
			(env) => this.#convertLegacyToken(token, legacy, env),
			options,
			this.#options.env,
		);
	}

	/** Answer a wait found with `getRun`/`getWait`, checking it is still at `expectedRevision`. */
	async resumeWait(options: WaitResumeOptions): Promise<SdkResult> {
		refuseSeveralDecisions(options);
		// Like a malformed token, a malformed reference is refused before the store is opened.
		checkWaitReference(options.waitId, options.expectedRevision);
		try {
			return await withStoreContext(
				{ ...this.#options, env: stateEnv(this.#options) },
				async (ctx) => {
					const store = sessionFromEnv(ctx.env)!;
					const wait = await findWaitAtRevision(
						store,
						options.waitId,
						options.expectedRevision,
						callerId(this.#options.runMetadata),
					);
					const { waitId: _waitId, expectedRevision, ...decision } = options;
					return this.#resumeKey(
						wait.key,
						decision,
						bindResumeGuard(ctx.env, { key: wait.key, expectedRevision }),
					);
				},
			);
		} catch (err) {
			return refusedOrFailed(err);
		}
	}

	/**
	 * Continue a run of these stages that stopped without a question to answer: one whose
	 * process stopped, or one that failed (it is retried from the stage step that failed).
	 */
	async advance(runId: string): Promise<SdkResult> {
		checkRunId(runId); // Malformed input is thrown, as in resume and resumeWait.
		try {
			return await this.#execute(this.#options.env, (ctx) =>
				advanceWorkflowRun({ runId, ctx, sdk: { pipeline: this.#pipeline() } }),
			);
		} catch (err) {
			return refusedOrFailed(err);
		}
	}

	async #resumeKey(
		key: string | ((env: StoreEnvironment) => Promise<string>),
		decision: Decision,
		env: NodeJS.ProcessEnv,
	): Promise<SdkResult> {
		try {
			return await this.#execute(env, async (ctx) => {
				const stateKey = typeof key === "string" ? key : await key(ctx.env);
				return runWorkflowFile({
					resume: workflowResumePayload(stateKey),
					...decision,
					sdk: { pipeline: this.#pipeline() },
					ctx,
				});
			});
		} catch (err) {
			return refusedOrFailed(err);
		}
	}

	async #execute(
		env: NodeJS.ProcessEnv,
		task: (ctx: RunContext) => Promise<WorkflowRunResult & { runId?: string }>,
	): Promise<SdkResult> {
		return withStoreContext(
			{ ...this.#options, env: stateEnv({ ...this.#options, env }) },
			async (store) =>
				finished(
					await task({
						env: store.env!,
						mode: "sdk",
						// Nobody is at a terminal here: a question pauses the run.
						stdin: Readable.from([]),
						stdout: discardStream(),
						stderr: discardStream(),
						cwd: process.cwd(),
						signal: this.#options.signal,
						registry: sdkStageRegistry(this.#stages),
						runMetadata: this.#options.runMetadata,
					}),
				),
		);
	}

	#pipeline() {
		return sdkPipelineText(this.#stages.length);
	}

	/**
	 * Turn a pause saved by an older Lobster into a waiting run, once. Its token then
	 * resumes that run, and a token used before is refused instead of acting again.
	 */
	async #convertLegacyToken(token: string, legacy: LegacySdkToken, env: StoreEnvironment) {
		// Called inside the run's store context, which always binds a session.
		const session = sessionFromEnv(env)!;
		let key: string;
		let pause: PausedPipeline;
		let once: string | undefined;
		if (legacy.resumeMode === "same_stage") {
			// The state lives under its key; once a run owns the key, it is that run's.
			key = legacy.stateKey!;
			const record = await session.read(key);
			if (record.runId) return key;
			if (record.text === null) {
				// Still in a file an older Lobster wrote: it has to be imported first.
				if (await savedByOlderVersion(session.directory, key)) throw new UpgradeRequiredError();
				throw new ResumeRefusedError();
			}
			const state = JSON.parse(record.text);
			// Answered before, by an older Lobster.
			if (state?.type === "lobster.consumed-resume-state.v1") throw new ResumeRefusedError();
			if (
				!Number.isSafeInteger(state?.resumeAtIndex) ||
				state.resumeAtIndex < 0 ||
				!Array.isArray(state.items) ||
				state.inputSchema === undefined
			)
				throw new Error("Invalid SDK resume state");
			pause = {
				gate: "same_stage",
				resumeAtIndex: state.resumeAtIndex,
				items: state.items,
				inputSchema: state.inputSchema,
				commandInput: validateCommandInputState(state.commandInput),
			};
		} else {
			// Older tokens carried their whole state, so nothing marked them used: they are marked
			// for good together with the run they become (see createWaitingRun), and only once
			// everything that could refuse them has been checked. A marked token is never
			// converted again.
			const hash = createHash("sha256").update(token).digest("hex");
			key = `sdk_resume_${hash.slice(0, 32)}`;
			once = `sdk_token_${hash}`;
			if ((await session.read(once)).text !== null) return key;
			pause =
				legacy.inputSchema !== undefined
					? {
							gate: "input",
							resumeAtIndex: legacy.resumeAtIndex,
							prompt: legacy.prompt ?? "Input required",
							inputSchema: legacy.inputSchema,
							subject: legacy.inputSubject,
						}
					: {
							gate: "approval",
							resumeAtIndex: legacy.resumeAtIndex,
							items: legacy.items ?? [],
							prompt: legacy.prompt ?? "Approve?",
						};
		}
		// A pause continues at that stage: one asked inside a stage runs that stage again, so it
		// must exist; one asked after a stage may continue past the last.
		const lastStart = this.#stages.length - (pause.gate === "same_stage" ? 1 : 0);
		if (pause.resumeAtIndex > lastStart)
			throw new ResumeRefusedError(
				`This Lobster instance has ${this.#stages.length} stages; the paused pipeline had more`,
			);
		const text = this.#pipeline();
		const registry = sdkStageRegistry(this.#stages);
		const stages = parsePipeline(text, registry).map((stage) => stage.raw);
		const { execution, records, continuation } = waitingPipelineRun(
			key,
			pipelineDefinition(text, []),
			stages,
			pause,
		);
		await session
			.createWaitingRun({
				kind: "sdk",
				key,
				execution,
				records,
				continuation,
				once,
				// The converting host's scope, so it can see and resume the run.
				metadata: this.#options.runMetadata,
			})
			.catch((error) => {
				// Another resume converted it first: resume that run.
				if (!(error instanceof CheckpointTakenError)) throw error;
			});
		return key;
	}

	clone() {
		const cloned = new Lobster(this.#options);
		cloned.#stages = [...this.#stages];
		cloned.#meta = this.#meta ? { ...this.#meta } : null;
		return cloned;
	}
}

/**
 * More than one decision is refused up front, as core refuses it; none is left to the
 * question, whose own message says what it needs (as in tool mode).
 */
function refuseSeveralDecisions(options: Decision) {
	if (decisionCount(options) > 1)
		throw new InvalidAnswerError("resume takes only one of approved, response, or cancel");
}

function decodeLegacyToken(data: Record<string, unknown>): LegacySdkToken {
	if (data.v !== 1) throw new Error("Invalid token");
	if (
		typeof data.resumeAtIndex !== "number" ||
		!Number.isInteger(data.resumeAtIndex) ||
		data.resumeAtIndex < 0
	) {
		throw new Error("Invalid token");
	}
	if (data.items !== undefined && !Array.isArray(data.items)) throw new Error("Invalid token");
	if (
		data.resumeMode !== undefined &&
		(typeof data.resumeMode !== "string" || !["next_stage", "same_stage"].includes(data.resumeMode))
	) {
		throw new Error("Invalid token");
	}
	if (data.resumeMode === "same_stage") {
		if (typeof data.stateKey !== "string" || !/^sdk_resume_[a-z0-9-]+$/.test(data.stateKey)) {
			throw new Error("Invalid token");
		}
	} else if (data.stateKey !== undefined) {
		throw new Error("Invalid token");
	}
	if (data.commandInput !== undefined) throw new Error("Invalid token");
	return data as unknown as LegacySdkToken;
}

/**
 * A failure as the SDK reports it. A refusal (nothing ran: the token, the answer, the wait's
 * revision or this instance does not fit the paused run) is thrown; anything else is a
 * failed result.
 */
function refusedOrFailed(err: unknown): SdkResult {
	if (
		err instanceof InvalidAnswerError ||
		err instanceof ResumeRefusedError ||
		err instanceof WaitConflictError
	)
		throw err;
	return failed(err);
}

/** The SDK's result shape for a finished run step. */
function finished(output: WorkflowRunResult & { runId?: string }): SdkResult {
	const runId = output.runId ? { runId: output.runId } : {};
	if (output.status === "needs_approval")
		return {
			...runId,
			ok: true,
			status: "needs_approval",
			output: [],
			requiresApproval: output.requiresApproval!,
			requiresInput: null,
		};
	if (output.status === "needs_input")
		return {
			...runId,
			ok: true,
			status: "needs_input",
			output: [],
			requiresApproval: null,
			requiresInput: output.requiresInput!,
		};
	if (output.status === "cancelled")
		return {
			...runId,
			ok: true,
			status: "cancelled",
			output: [],
			requiresApproval: null,
			requiresInput: null,
		};
	if (output.status === "waiting")
		return failed(new Error("SDK pipelines cannot wait for timers or events"), output.runId);
	return {
		...runId,
		ok: true,
		status: "ok",
		output: output.output,
		requiresApproval: null,
		requiresInput: null,
	};
}

function failed(err: unknown, runId = runIdOf(err)): SdkResult {
	const error = runtimeErrorDetails(err);
	return {
		ok: false,
		status: "error",
		...(error.runId || runId ? { runId: error.runId ?? runId } : {}),
		output: [],
		requiresApproval: null,
		requiresInput: null,
		error,
	};
}

/** The env with the instance's `stateDir`, if it set one (a `storeSession` is bound by the store context). */
function stateEnv(options: { stateDir?: string; env: NodeJS.ProcessEnv }) {
	return options.stateDir ? { ...options.env, LOBSTER_STATE_DIR: options.stateDir } : options.env;
}
