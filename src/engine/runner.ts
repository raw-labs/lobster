// The run API: start a run (a workflow file, a standalone pipeline, an SDK pipeline), answer
// a wait, advance a run that stopped, and abandon one. Each holds the run's execution lease
// while it works. Many runs at once: due.ts.
import path from "node:path";
import { withStoreContext } from "../persistence/execution.js";
import { bindRecorder, recorderFromEnv, sessionFromEnv } from "../persistence/context.js";
import { readStateJsonWithLock } from "../state/store.js";
import { loadWorkflowFile } from "../workflows/load.js";
import { dryRunWorkflow } from "../workflows/dry_run.js";
import { resolveWorkflowArgs } from "../workflows/args.js";
import { pipelineDefinition, snapshotDefinition } from "../workflows/definition.js";
import { ExecutionStore } from "./store.js";
import { Machine } from "./machine.js";
import { answer, cancelWithdrawing } from "./waits.js";
import type { RunContext } from "./context.js";
import {
	asContinuation,
	isFinished,
	keys,
	unresolvedActivities,
	type ExecutionState,
} from "./checkpoint.js";
import type { Definition } from "../workflows/definition.js";
import type { WorkflowRunResult } from "../workflows/types.js";
import { visibleTo, callerId } from "../persistence/waits.js";
import type { StoreSession } from "../persistence/session.js";
import { ExecutionRecorder } from "../persistence/session.js";
import { ResumeRefusedError, UpgradeRequiredError } from "./errors.js";
import type { WorkflowResumePayload } from "../resume/token.js";

export type WorkflowRunOptions = {
	filePath?: string;
	args?: Record<string, unknown>;
	ctx: RunContext;
	resume?: WorkflowResumePayload;
	approved?: boolean;
	response?: unknown;
	cancel?: boolean;
	/** Host execution lease; not a workflow setting. */
	leaseMs?: number;
	/** Run this definition instead of loading `filePath` (a standalone pipeline). */
	definition?: Definition;
	/** The kind recorded for a new run (default "workflow"). */
	kind?: "workflow" | "pipeline" | "sdk";
	/**
	 * Set by the Lobster SDK, whose stages are closures only its host has: the run's pipeline
	 * must name exactly these stages. Runs of kind "sdk" are refused without it.
	 */
	sdk?: { pipeline: string };
};

const SDK_RUN_ELSEWHERE =
	"This run was started by the Lobster SDK: resume it with Lobster.resume on an instance with the same stages";

/** SDK runs need their host's stages; everything else must not claim to be one. */
function assertRunnableHere(kind: string, sdk: WorkflowRunOptions["sdk"]) {
	if (kind === "sdk" && !sdk) throw new ResumeRefusedError(SDK_RUN_ELSEWHERE);
	if (kind !== "sdk" && sdk)
		throw new ResumeRefusedError("This run was not started by the Lobster SDK");
}

/**
 * Run a standalone pipeline (`lobster '...'`, `runToolRequest({ pipeline })`) as a one-step
 * workflow, so it pauses, resumes and recovers exactly like workflow files do.
 */
export async function runPipelineText(options: {
	pipeline: string;
	input?: unknown[];
	ctx: RunContext;
	leaseMs?: number;
}): Promise<WorkflowRunResult & { runId?: string }> {
	return runWorkflowFile({
		definition: pipelineDefinition(options.pipeline, options.input),
		kind: "pipeline",
		ctx: options.ctx,
		leaseMs: options.leaseMs,
	});
}

export async function runWorkflowFile(
	options: WorkflowRunOptions,
): Promise<WorkflowRunResult & { runId?: string }> {
	if (options.ctx.dryRun) {
		const workflow = await loadWorkflowFile(options.filePath!);
		return dryRunWorkflow({
			steps: workflow.steps,
			resolvedArgs: resolveWorkflowArgs(workflow.args, options.args),
			ctx: options.ctx,
		});
	}
	return ownFailure((label) =>
		withStoreContext({ ...options.ctx, trackRuns: true }, async (ctx) => {
			const session = sessionFromEnv(ctx.env)!;
			let recorder = recorderFromEnv(ctx.env);
			let runId: string;
			let fresh: Pick<ExecutionState, "definition" | "args"> | undefined;
			const stateKey = options.resume?.stateKey;
			if (options.resume) {
				runId = await resumedRunId(stateKey, ctx);
				ctx.hooks?.onResumeStateResolved?.(stateKey!);
			} else {
				// Load and validate first: a workflow that cannot start leaves no run behind.
				if (!options.filePath && !options.definition)
					throw new Error("Workflow file path required");
				const definition =
					options.definition ?? (await snapshotDefinition(options.filePath!, options.args ?? {}));
				fresh = { definition, args: resolveWorkflowArgs(definition.workflow.args, options.args) };
				recorder ??= await session.createRun(options.kind ?? "workflow", ctx.runMetadata);
				runId = recorder.runId;
			}
			const run = await findRun(session, runId, ctx);
			assertRunnableHere(run.kind, options.sdk);
			recorder ??= await session.recorderForState(stateKey!);
			const bound = { ...ctx, env: recorder ? bindRecorder(ctx.env, recorder) : ctx.env };
			return withClaimedRun(session, runId, options, label, async (store, saved) => {
				let state = saved;
				if (!state) {
					if (!fresh) throw new UpgradeRequiredError();
					state = {
						version: 1,
						revision: 0,
						...fresh,
						cwd: path.resolve(ctx.cwd ?? process.cwd()),
						activations: {},
						status: "running",
						startedAt: Date.now(),
					};
				}
				checkExecution(state, options.sdk);
				const machine = new Machine(state, store, bound);
				if (!state.revision) await machine.save();
				if (stateKey) await answer(machine, stateKey, options);
				return { ...(await machine.run()), runId };
			});
		}),
	);
}

/**
 * Continue a saved run: after a process restart, a lapsed timer or a crash. A failed run is
 * retried from the step that failed. No conversation is required.
 */
export async function advanceWorkflowRun(options: {
	runId: string;
	ctx: RunContext;
	leaseMs?: number;
	/** Set by the Lobster SDK for its own runs (see `WorkflowRunOptions.sdk`). */
	sdk?: { pipeline: string };
}): Promise<WorkflowRunResult & { runId: string }> {
	return ownFailure((label) =>
		withStoreContext({ ...options.ctx, trackRuns: true }, async (ctx) => {
			const session = sessionFromEnv(ctx.env)!;
			const run = await findRun(session, options.runId, ctx);
			if (!run.execution)
				throw new Error(
					"Run has no restartable workflow checkpoint; migration or inspection required",
				);
			assertRunnableHere(run.kind, options.sdk);
			return withClaimedRun(session, run.id, options, label, async (store, saved) => {
				const state = saved!;
				checkExecution(state, options.sdk);
				const recorder = new ExecutionRecorder(session, run.id, "", run.historyEnabled !== false);
				const machine = new Machine(state, store, { ...ctx, env: bindRecorder(ctx.env, recorder) });
				if (state.status === "failed") {
					machine.retryFailed();
					await machine.save();
				}
				return { ...(await machine.run()), runId: run.id };
			});
		}),
	);
}

/**
 * Stop a run for good: one that is blocked, was interrupted, waits for an answer, or failed
 * while commands of it were cut off. Steps that had started are recorded as abandoned
 * (their outcome stays unknown and nothing runs again), pending questions are withdrawn,
 * and the run is cancelled, so retention can clean it up. A run that ended otherwise is
 * returned as it is. Refuses while another process is executing the run.
 */
export async function abandonWorkflowRun(options: {
	runId: string;
	ctx: RunContext;
	leaseMs?: number;
}): Promise<{ runId: string; status: string; abandoned: string[] }> {
	return withStoreContext({ ...options.ctx, trackRuns: true }, async (ctx) => {
		const session = sessionFromEnv(ctx.env)!;
		const run = await findRun(session, options.runId, ctx);
		if (!run.execution) throw new Error("Run has no workflow execution to abandon");
		if (settled(run.execution))
			return { runId: run.id, status: run.execution.status, abandoned: [] };
		return withClaimedRun(
			session,
			run.id,
			options,
			(error) => error,
			async (store, saved) => {
				const state = saved!;
				// It may have finished between the first read and the claim: keep its result.
				if (settled(state)) return { runId: run.id, status: state.status, abandoned: [] };
				// A cancelled run never runs again; its error keeps what was abandoned.
				const abandoned = unresolvedActivities(state);
				await cancelWithdrawing(
					new Machine(state, store, ctx),
					"Abandoned" +
						(abandoned.length
							? `; outcome unknown for ${abandoned.map(keys.describe).join(", ")}`
							: ""),
				);
				return { runId: run.id, status: "cancelled", abandoned };
			},
		);
	});
}

/** A run abandoning would not change: finished, and with no command whose outcome is unknown. */
function settled(state: ExecutionState) {
	return isFinished(state) && !unresolvedActivities(state).length;
}

/** The run a resume token's continuation belongs to. */
async function resumedRunId(stateKey: string | undefined, ctx: RunContext) {
	if (!stateKey)
		throw new Error(
			"This resume token is from a Lobster version that kept workflow state inside the token; it cannot be resumed",
		);
	const saved = (await readStateJsonWithLock({
		env: ctx.env,
		key: stateKey,
		signal: ctx.signal,
	})) as {
		type?: string;
	} | null;
	// A token an older Lobster already used left this marker behind.
	if (!saved || saved.type === "lobster.consumed-resume-state.v1") throw new ResumeRefusedError();
	const continuation = asContinuation(saved);
	if (!continuation) throw new UpgradeRequiredError();
	return continuation.runId;
}

/** The run `runId`, if this caller may see it. */
async function findRun(session: StoreSession, runId: string, ctx: Pick<RunContext, "runMetadata">) {
	const run = await session.getRunState(runId);
	if (!run || !visibleTo(run, callerId(ctx.runMetadata))) throw new Error("Workflow run not found");
	return run;
}

/**
 * Claim the run's execution lease, run `task` with its saved checkpoint, and release the
 * lease. A failure is labelled with the run (see ownFailure).
 */
async function withClaimedRun<T>(
	session: StoreSession,
	runId: string,
	options: { leaseMs?: number },
	label: (error: unknown, runId: string) => unknown,
	task: (store: ExecutionStore, saved: ExecutionState | undefined) => Promise<T>,
): Promise<T> {
	const store = new ExecutionStore(session, runId, options.leaseMs);
	try {
		return await task(store, await store.claim());
	} catch (error) {
		throw label(error, runId);
	} finally {
		await store.close();
	}
}

/** A checkpoint this version can continue, of the SDK instance's stages if it is one. */
function checkExecution(state: ExecutionState, sdk: { pipeline: string } | undefined) {
	if (state.version !== 1) throw new Error("Unsupported workflow execution version");
	if (!sdk) return;
	const saved = String(state.definition.workflow.steps[0]?.pipeline ?? "");
	if (saved !== sdk.pipeline)
		throw new ResumeRefusedError(
			`This Lobster instance has ${stageCount(sdk.pipeline)} stages; the paused run has ${stageCount(saved)}`,
		);
}

function stageCount(pipeline: string) {
	return pipeline ? pipeline.split(" | ").length : 0;
}

/** The run each failure belongs to, set only here so a command cannot claim another run. */
const failedRuns = new WeakMap<object, string>();

/**
 * Run `task`, so that a failure names this call's run and no other: the run it labelled, or
 * none when it failed before a run existed. The same error object can come from several runs
 * (a shared abort signal's reason, say, or a run a step inside this one started), so the label
 * is set, or cleared, by each call as the error leaves it.
 */
async function ownFailure<T>(
	task: (label: (error: unknown, runId: string) => unknown) => Promise<T>,
): Promise<T> {
	let labelled = false;
	try {
		return await task((error, runId) => {
			labelled = true;
			if (error && typeof error === "object") failedRuns.set(error, runId);
			return error;
		});
	} catch (error) {
		if (!labelled && error && typeof error === "object") failedRuns.delete(error);
		throw error;
	}
}

/** The run a failure from `runWorkflowFile` or `advanceWorkflowRun` belongs to. */
export function runIdOf(error: unknown): string | undefined {
	return error && typeof error === "object" ? failedRuns.get(error) : undefined;
}
