// The run API: start a run (a workflow file, a standalone pipeline, an SDK pipeline), answer
// a wait, advance a run that stopped, and abandon one. Each holds the run's execution lease
// while it works. Many runs at once: due.ts.
import { checkPipelineSteps } from "../workflows/pipeline_check.js";
import { withStoreContext } from "../persistence/execution.js";
import { bindRecorder, recorderFromEnv, sessionFromEnv } from "../persistence/context.js";
import { dryRunWorkflow } from "../workflows/dry_run.js";
import { resolveWorkflowArgs } from "../workflows/args.js";
import { assertSavable } from "../util/serialization.js";
import { pipelineDefinition, snapshotDefinition } from "../workflows/definition.js";
import { checkLeaseMs, ExecutionStore } from "./store.js";
import { Machine } from "./machine.js";
import { answer, cancelWithdrawing, waitGone } from "./waits.js";
import { inRunDirectory, type RunContext } from "./context.js";
import {
	asContinuation,
	isFinished,
	keys,
	unresolvedActivities,
	type ExecutionState,
	type RunStatus,
} from "./checkpoint.js";
import type { Definition } from "../workflows/definition.js";
import type { WorkflowRunResult } from "../workflows/types.js";
import { visibleTo, callerId } from "../persistence/waits.js";
import { checkRunId, type RunKind } from "../persistence/model.js";
import type { StoreSession } from "../persistence/session.js";
import { ExecutionRecorder } from "../persistence/session.js";
import { ResumeRefusedError, UpgradeRequiredError } from "./errors.js";
import type { WorkflowResumePayload } from "../resume/token.js";
import { decisionCount, type Decision } from "../resume/options.js";

/** What to run: a workflow file or definition to start, or a wait to answer (`resume` and a decision). */
export type WorkflowRunOptions = Decision & {
	filePath?: string;
	args?: Record<string, unknown>;
	ctx: RunContext;
	resume?: WorkflowResumePayload;
	/** Host execution lease; not a workflow setting. */
	leaseMs?: number;
	/** Run this definition instead of loading `filePath` (a standalone pipeline). */
	definition?: Definition;
	/** The kind recorded for a new run (default "workflow"). */
	kind?: RunKind;
	/**
	 * Set by the Lobster SDK, whose stages are closures only its host has: the run's pipeline
	 * must name exactly these stages. Runs of kind "sdk" are refused without it.
	 */
	sdk?: { pipeline: string };
};

const SDK_RUN_ELSEWHERE =
	"This run was started by the Lobster SDK: continue it with Lobster.resume or Lobster.advance on an instance with the same stages";

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
	ctx: RunContext;
}): Promise<WorkflowRunResult & { runId?: string }> {
	return runWorkflowFile({
		definition: pipelineDefinition(options.pipeline),
		kind: "pipeline",
		ctx: options.ctx,
	});
}

export async function runWorkflowFile(
	options: WorkflowRunOptions,
): Promise<WorkflowRunResult & { runId?: string }> {
	// A decision answers a paused run; args or a definition start a new one. Mixed, one half
	// would be dropped without a word. (A resumed run keeps its own definition: a `filePath`
	// given with a resume names it and changes nothing.)
	if (options.resume) {
		if (options.args !== undefined || options.definition !== undefined)
			throw new Error("A resume takes no args or definition: the run keeps its own");
	} else if (decisionCount(options) > 0) {
		throw new Error("approved, response and cancel apply to a resume");
	}
	// A relative path is the run's directory's, as for every entry point.
	const filePath = options.filePath && inRunDirectory(options.ctx, options.filePath);
	if (options.ctx.dryRun) {
		// A dry run plans a workflow file; there is nothing to plan for a resume or a definition.
		if (!filePath || options.resume)
			throw new Error("A dry run applies to starting a workflow file");
		// Checked as a start checks it: the child workflows it names load, and every pipeline
		// step (theirs included) names known commands and flags.
		const definition = await snapshotDefinition(filePath, options.args ?? {});
		const args = resolveWorkflowArgs(definition.workflow.args, options.args);
		if (options.ctx.registry) checkPipelineSteps(definition, args, options.ctx.registry);
		return dryRunWorkflow({
			steps: definition.workflow.steps,
			resolvedArgs: args,
			ctx: options.ctx,
		});
	}
	return ownFailure((label) =>
		withStoreContext(options.ctx, async (ctx) => {
			const session = sessionFromEnv(ctx.env)!;
			let recorder = options.resume ? undefined : await recorderForThisRun(session, ctx.env);
			let runId: string;
			let fresh: Pick<ExecutionState, "definition" | "args"> | undefined;
			const stateKey = options.resume?.stateKey;
			if (options.resume) {
				runId = await resumedRunId(session, options.resume.stateKey, ctx);
			} else {
				// Load and validate first: a workflow that cannot start leaves no run behind.
				if (!filePath && !options.definition) throw new Error("Workflow file path required");
				const definition =
					options.definition ?? (await snapshotDefinition(filePath!, options.args ?? {}));
				fresh = { definition, args: resolveWorkflowArgs(definition.workflow.args, options.args) };
				// Refused before the run exists: args the run cannot save would leave it stuck.
				assertSavable(fresh.args, "Workflow args");
				// A workflow file's pipeline steps are checked before any of its steps acts.
				if (!options.definition && ctx.registry)
					checkPipelineSteps(definition, fresh.args, ctx.registry);
				if (!recorder) {
					// Refused before the run exists, so a refusal leaves no run behind either.
					assertRunnableHere(options.kind ?? "workflow", options.sdk);
					if (options.leaseMs !== undefined) checkLeaseMs(options.leaseMs);
					recorder = await session.createRun(options.kind ?? "workflow", ctx.runMetadata);
				}
				runId = recorder.runId;
			}
			const run = await findRun(session, runId, ctx);
			assertRunnableHere(run.kind, options.sdk);
			recorder ??= new ExecutionRecorder(session, run.id, "", run.historyEnabled !== false);
			const bound = { ...ctx, env: bindRecorder(ctx.env, recorder) };
			return withClaimedRun(session, runId, options, label, async (store, saved) => {
				let state = saved;
				if (!state) {
					if (!fresh) throw new UpgradeRequiredError();
					state = {
						version: 1,
						revision: 0,
						...fresh,
						cwd: inRunDirectory(ctx),
						activations: {},
						status: "running",
						startedAt: Date.now(),
					};
				}
				checkExecution(state, options.sdk);
				const machine = await Machine.load(state, store, bound);
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
	checkRunId(options.runId);
	// Advancing runs steps for real: a dry run is refused, never ignored.
	if (options.ctx.dryRun) throw new Error("A dry run applies to starting a workflow file");
	return ownFailure((label) =>
		withStoreContext(options.ctx, async (ctx) => {
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
				const machine = await Machine.load(state, store, {
					...ctx,
					env: bindRecorder(ctx.env, recorder),
				});
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
}): Promise<{ runId: string; status: RunStatus; abandoned: string[] }> {
	checkRunId(options.runId);
	// Abandoning cancels the run for real: a dry run is refused, never ignored.
	if (options.ctx.dryRun) throw new Error("A dry run applies to starting a workflow file");
	return withStoreContext(options.ctx, async (ctx) => {
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
					await Machine.load(state, store, ctx),
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

/**
 * The run a resume token's continuation belongs to. A token for a run this caller cannot see
 * is refused like an unknown one: it tells nothing about that run.
 */
async function resumedRunId(session: StoreSession, stateKey: string, ctx: RunContext) {
	// Continuations are written by store transitions only: a plain read sees them whole.
	const text = (await session.read(stateKey)).text;
	const saved = (text === null ? null : JSON.parse(text)) as { type?: string } | null;
	// A token an older Lobster already used left this marker behind.
	if (!saved || saved.type === "lobster.consumed-resume-state.v1")
		throw waitGone(ctx.env, stateKey);
	const continuation = asContinuation(saved);
	if (!continuation) throw new UpgradeRequiredError();
	const run = await session.getRunState(continuation.runId);
	if (!run || !visibleTo(run, callerId(ctx.runMetadata))) throw new ResumeRefusedError();
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

/**
 * The recorder of the run a host created for this call (`withExecution`), which nothing has
 * started yet. One inherited from a run in progress (a step's command starting a run of its
 * own) belongs to that run, not to this one.
 */
async function recorderForThisRun(session: StoreSession, env: RunContext["env"]) {
	const recorder = recorderFromEnv(env);
	if (!recorder) return undefined;
	const run = await session.getRunState(recorder.runId);
	return run && !run.execution ? recorder : undefined;
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
