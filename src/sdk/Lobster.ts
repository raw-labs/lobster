import type { ResumeDecision, WaitResumeOptions } from "../resume/options.js";
import type { SdkResult } from "./types.js";
import { createHash, randomUUID } from "node:crypto";
import { Writable } from "node:stream";
import { validateCommandInputState } from "../pipeline/input_request.js";
import { parsePipeline } from "../pipeline/parser.js";
import { runtimeErrorDetails } from "../persistence/backend.js";
import { sessionFromEnv, bindResumeGuard } from "../persistence/context.js";
import { withStoreContext } from "../persistence/execution.js";
import type { PersistenceOptions } from "../persistence/context.js";
import { resolveWait, callerId } from "../persistence/waits.js";
import { decodeToken } from "../resume/token.js";
import type { RunContext } from "../engine/context.js";
import { pipelineDefinition } from "../workflows/definition.js";
import {
	advanceWorkflowRun,
	runWorkflowFile,
	ResumeRefusedError,
	UpgradeRequiredError,
	WorkflowResumeArgumentError,
	runIdOf,
	type WorkflowRunResult,
} from "../engine/runner.js";
import {
	savedByOlderVersion,
	waitingPipelineRun,
	type PausedPipeline,
} from "../workflows/upgrade.js";
import { CheckpointTakenError } from "../persistence/session.js";
import { sdkInputItems, sdkPipelineText, sdkStageRegistry } from "./runtime.js";
import { stateEnv } from "./state_env.js";

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
	#stages = [];
	#options: LobsterOptions;
	#meta = null;

	constructor(options: LobsterOptions = {}) {
		this.#options = {
			storeSession: options.storeSession,
			history: options.history,
			trackRuns: options.trackRuns,
			runMetadata: options.runMetadata,
			env: options.env ?? process.env,
			stateDir: options.stateDir,
			signal: options.signal,
		};
	}

	pipe(stage) {
		if (typeof stage !== "function" && typeof stage?.run !== "function") {
			throw new Error("Stage must be a function or have a run() method");
		}
		this.#stages.push(stage);
		return this;
	}

	meta(meta) {
		this.#meta = meta;
		return this;
	}

	getMeta() {
		return this.#meta;
	}

	async run(initialInput = []): Promise<SdkResult> {
		try {
			const input = await sdkInputItems(initialInput);
			if (!this.#stages.length) return finished({ status: "ok", output: input });
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
	async resume(
		token: string,
		options: { approved?: boolean; response?: unknown; cancel?: boolean } = {},
	): Promise<SdkResult> {
		assertOneDecision(options);
		const payload = decodeToken(token) as Record<string, unknown> | null;
		if (!payload || typeof payload !== "object" || payload.protocolVersion !== 1)
			throw new Error("Invalid token");
		if (payload.kind === "workflow-file") {
			if (payload.v !== 1 || typeof payload.stateKey !== "string" || !payload.stateKey)
				throw new Error("Invalid token");
			return this.#resumeKey(payload.stateKey, options, this.#options.env);
		}
		const legacy = decodeLegacyToken(payload);
		return this.#resumeKey(
			(env) => this.#convertLegacyToken(token, legacy, env),
			options,
			this.#options.env,
		);
	}

	/** Answer a wait found with `getRun`/`getWait`, checking it is still at `expectedRevision`. */
	async resumeWait(options: WaitResumeOptions): Promise<SdkResult> {
		assertOneDecision(options);
		return withStoreContext(
			{ ...this.#options, env: stateEnv(this.#options), trackRuns: true },
			async (ctx) => {
				const store = sessionFromEnv(ctx.env);
				if (!store) throw new Error("Wait resume requires a persistence session");
				const wait = await resolveWait(
					store,
					options.waitId,
					options.expectedRevision,
					callerId(this.#options.runMetadata),
				);
				const { waitId: _waitId, expectedRevision, ...decision } = options;
				return this.#resumeKey(
					wait.key,
					decision,
					bindResumeGuard(ctx.env, { key: wait.key, expectedRevision, admitted: false }),
				);
			},
		);
	}

	/**
	 * Continue a run of these stages that stopped without a question to answer: one whose
	 * process stopped, or one that failed (it is retried from the stage step that failed).
	 */
	async advance(runId: string): Promise<SdkResult> {
		try {
			return await this.#execute(this.#options.env, (ctx) =>
				advanceWorkflowRun({ runId, ctx, sdk: { pipeline: this.#pipeline() } }),
			);
		} catch (err) {
			if (err instanceof ResumeRefusedError) throw err;
			return failed(err, runId);
		}
	}

	async #resumeKey(
		key: string | ((env: NodeJS.ProcessEnv) => Promise<string>),
		decision: { approved?: boolean; response?: unknown; cancel?: boolean },
		env: NodeJS.ProcessEnv,
	): Promise<SdkResult> {
		try {
			return await this.#execute(env, async (ctx) => {
				const stateKey = typeof key === "string" ? key : await key(ctx.env as NodeJS.ProcessEnv);
				return runWorkflowFile({
					resume: { protocolVersion: 1, v: 1, kind: "workflow-file", stateKey },
					...decision,
					sdk: { pipeline: this.#pipeline() },
					ctx,
				});
			});
		} catch (err) {
			// Nothing ran: the token, the answer or this instance does not fit the paused run.
			if (
				err instanceof WorkflowResumeArgumentError ||
				err instanceof ResumeRefusedError ||
				(err as Error)?.message === "Invalid token"
			)
				throw err;
			return failed(err);
		}
	}

	async #execute(
		env: NodeJS.ProcessEnv | undefined,
		task: (ctx: RunContext) => Promise<WorkflowRunResult & { runId?: string }>,
	): Promise<SdkResult> {
		return withStoreContext(
			{ ...this.#options, env: stateEnv({ ...this.#options, env }), trackRuns: true },
			async (store) =>
				finished(
					await task({
						env: store.env!,
						mode: "sdk",
						stdin: { isTTY: false } as unknown as NodeJS.ReadableStream,
						stdout: discard(),
						stderr: discard(),
						cwd: process.cwd(),
						signal: this.#options.signal,
						registry: sdkStageRegistry(this.#stages, { stateDir: this.#options.stateDir }),
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
	async #convertLegacyToken(token: string, legacy: LegacySdkToken, env: NodeJS.ProcessEnv) {
		const session = sessionFromEnv(env);
		if (!session) throw new Error("SDK resume requires a persistence session");
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
				commandInput: validateCommandInputState(state.commandInput) as any,
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
		if (pause.resumeAtIndex > this.#stages.length)
			throw new ResumeRefusedError(
				`This Lobster instance has ${this.#stages.length} stages; the paused pipeline had more`,
			);
		const text = this.#pipeline();
		const registry = sdkStageRegistry(this.#stages);
		const stages = parsePipeline(text, registry as any).map((stage) => ({
			name: stage.name,
			args: stage.args,
			raw: stage.raw,
		}));
		const { execution, continuation } = waitingPipelineRun(
			key,
			pipelineDefinition(text, []),
			stages,
			pause,
		);
		await session
			.createWaitingRun({ id: randomUUID(), kind: "sdk", key, execution, continuation, once })
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

function assertOneDecision(options: { approved?: boolean; response?: unknown; cancel?: boolean }) {
	const count =
		Number(typeof options.approved === "boolean") +
		Number(options.response !== undefined) +
		Number(options.cancel === true);
	if (count > 1) throw new Error("resume accepts only one of approved, response, or cancel");
	if (count === 0) throw new Error("resume requires approved, response, or cancel");
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

/** The SDK's result shape for a finished run step. */
function finished(output: Partial<WorkflowRunResult> & { runId?: string }): SdkResult {
	const runId = output.runId ? { runId: output.runId } : {};
	if (output.status === "needs_approval")
		return {
			...runId,
			ok: true,
			status: "needs_approval",
			output: [],
			requiresApproval: output.requiresApproval as any,
			requiresInput: null,
		};
	if (output.status === "needs_input")
		return {
			...runId,
			ok: true,
			status: "needs_input",
			output: [],
			requiresApproval: null,
			requiresInput: output.requiresInput as any,
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
		output: (output.output ?? []) as unknown[],
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

function discard() {
	return new Writable({
		write(_chunk, _encoding, callback) {
			callback();
		},
	});
}
