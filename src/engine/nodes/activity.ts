// A step that runs a command: a shell command, a pipeline, or nothing (a value or an approval).
import { messageOf } from "../../util/errors.js";
import type { WorkflowStep, WorkflowStepResult } from "../../workflows/types.js";
import { parsePipeline } from "../../pipeline/parser.js";
import { getStepExecution } from "../../workflows/steps.js";
import { resolveTemplate } from "../../workflows/expressions.js";
import {
	mergeEnv,
	resolveCwd,
	resolveInputValue,
	encodeShellInput,
} from "../../workflows/values.js";
import { bindRecorder, recorderFromEnv } from "../../persistence/context.js";
import { activation, keys, type Activation, type ActivityInput } from "../checkpoint.js";
import { inRunDirectory, type RunContext } from "../context.js";
import type { Machine } from "../machine.js";
import type { Scope } from "../scope.js";
import { InterruptedActivityError, lostRun } from "../errors.js";
import { runShellCommand } from "../activities/shell.js";
import { runPipelineStep, PipelinePausedError } from "../activities/pipeline.js";
import { parseJson, createSyntheticStepResult } from "../results.js";
import { inputStep } from "./input.js";
import { pipelineApproval } from "./approval.js";

/**
 * Run the step's command once per attempt (`key` is the attempt). What it runs is resolved
 * and saved before dispatch; its result is saved with the completed marker. A pipeline that
 * paused to ask continues here once answered. After a crash, a command that may have acted
 * runs again only when the step allows it (`recovery: repeat`, nothing unrepeatable).
 */
export async function activityStep(
	machine: Machine,
	step: WorkflowStep,
	key: string,
	scope: Scope,
): Promise<WorkflowStepResult> {
	const record = activation(machine.state, keys.activity(key));
	if (record.phase === "completed") return record.result!;
	if (record.phase === "failed") throw new Error(record.error);
	if (record.phase === "waiting") await answerPipelineQuestion(machine, step, key, scope, record);
	else if (record.phase === "started") {
		if (record.activity?.input.kind !== "none" && !mayRepeat(step, record, scope.ctx))
			throw new InterruptedActivityError(key);
		record.phase = "ready";
	}
	if (record.phase !== "waiting") {
		// Counted on the step's own frame, over all its attempts and recoveries.
		const stepFrame = activation(machine.state, keys.stepOf(key));
		stepFrame.dispatches = (stepFrame.dispatches ?? 0) + 1;
		record.activity ??= { input: resolveActivityInput(step, scope), dispatch: 0 };
		record.activity.dispatch = stepFrame.dispatches;
	}
	return dispatch(machine, step, key, scope, record);
}

/** What the step runs, resolved from its templates; saved before it is dispatched. */
function resolveActivityInput(step: WorkflowStep, scope: Scope): ActivityInput {
	const execution = getStepExecution(step);
	if (execution.kind !== "shell" && execution.kind !== "pipeline" && execution.kind !== "none")
		throw new Error("Control node dispatched as activity");
	return {
		kind: execution.kind,
		...(execution.kind !== "none"
			? { text: resolveTemplate(execution.value, scope.args, scope.results) }
			: {}),
		input:
			scope.definition.standalone && scope.definition.input !== undefined
				? scope.definition.input
				: resolveInputValue(step.stdin, scope.args, scope.results),
		// Do not persist the host environment or credentials. Resolve only explicit workflow values.
		env: mergeEnv(scope.baseEnv ?? {}, scope.env, step.env, scope.args, scope.results),
		cwd: inRunDirectory(scope.ctx, resolveCwd(step.cwd ?? scope.cwd, scope.args)),
	};
}

async function dispatch(
	machine: Machine,
	step: WorkflowStep,
	key: string,
	scope: Scope,
	record: Activation,
) {
	const { input: saved, dispatch: count } = record.activity!;
	const activityId = keys.stepOf(key);
	const history = recorderFromEnv(scope.ctx.env)?.child(key);
	const historyId = await history?.start("activity", step.id);
	record.phase = "started";
	await machine.save();
	// Whether a command may have started. Until then, stopping leaves nothing to inspect: a
	// stop between the save above and the dispatch puts the step back to ready (below).
	let dispatched = false;
	try {
		scope.startTimeout?.();
		scope.ctx.signal?.throwIfAborted();
		machine.checkAuthority(scope.ctx);
		const hooks = scope.ctx.hooks;
		const ctx: RunContext = {
			...scope.ctx,
			hooks: {
				...hooks,
				onNonRetryableSideEffect: () => {
					record.nonRetryable = true;
					hooks?.onNonRetryableSideEffect?.();
				},
			},
		};
		const baseEnv = {
			...ctx.env,
			...saved.env,
			LOBSTER_RUN_ID: machine.store.runId,
			LOBSTER_ACTIVITY_ID: machine.store.runId + "/" + activityId,
			LOBSTER_ACTIVITY_ATTEMPT: String(count),
		};
		const env = history ? bindRecorder(baseEnv, history) : baseEnv;
		let result;
		if (saved.kind === "shell") {
			dispatched = true;
			// A pipeline reports its own command starts (onExecutionStart below).
			await hooks?.onExecutionStarted?.();
			const { stdout } = await runShellCommand({
				command: saved.text!,
				stdin: encodeShellInput(saved.input),
				env,
				cwd: inRunDirectory(scope.ctx, saved.cwd),
				signal: ctx.signal,
				forceTerminationSignal: ctx.forceTerminationSignal,
				assertInvocationCurrent: ctx.assertInvocationCurrent,
				onEffectDispatch: ctx.hooks?.onUnsafeDispatch,
				killSignal: () => (scope.timeoutSignal?.aborted ? "SIGKILL" : undefined),
			});
			result = { id: step.id, stdout, json: parseJson(stdout) };
		} else if (saved.kind === "pipeline") {
			if (!ctx.registry)
				throw new Error(
					`Workflow step ${step.id} requires a command registry for pipeline execution`,
				);
			result = await runPipelineStep({
				stepId: step.id,
				pipelineText: saved.text!,
				inputValue: saved.input,
				ctx,
				recordModelCall: ({ model, usage }) =>
					machine.cost.recordUsage(scope.accountAs ?? scope.costPrefix + step.id, model, usage),
				env,
				cwd: inRunDirectory(scope.ctx, saved.cwd),
				gatesRefused: scope.gatesRefused,
				resume: resumeOf(record.activity!),
				onExecutionStart: () => {
					dispatched = true;
					return ctx.hooks?.onExecutionStarted?.();
				},
				standalone: scope.definition.standalone,
			});
		} else result = createSyntheticStepResult(step.id, saved.input);
		ctx.signal?.throwIfAborted();
		record.result = result;
		record.phase = "completed";
		if (historyId) await history!.finish(historyId, "completed", result);
		// The result and completed marker are one checkpoint. Parent cursor advances later.
		await machine.save();
		// Once the result is saved, the step's own timer or heartbeat may no longer fail it:
		// that would retry, and run again, a command that already finished. Only a stop from
		// outside the step (the host, an enclosing step's timeout, a parallel sibling, a lost
		// lease) may still end the run here.
		if (stoppedFromOutside(scope)) throw scope.ctx.signal!.reason;
		return result;
	} catch (error) {
		if (error instanceof PipelinePausedError) {
			if (historyId) await history!.finish(historyId, "waiting");
			record.activity!.pause = error.pause;
			record.activity!.question = error.question;
			delete record.activity!.response;
			record.phase = "waiting";
			await machine.save();
			return activityStep(machine, step, key, scope);
		}
		// The run was stopped from outside the step or is no longer this invocation's: a command
		// that started may have acted, so the activity stays started and its outcome unknown.
		// Its own timeout is a failure like any other: the command was stopped by the step,
		// which then retries or fails as its settings say.
		if (lostRun(error) || stoppedFromOutside(scope)) {
			// Stopped before any command started (a history write failed, Ctrl-C while
			// preparing): the step can simply start again. If this save fails too, it stays
			// started and is treated as interrupted, which is never wrong.
			if (!dispatched) {
				record.phase = "ready";
				if (historyId) await history!.finish(historyId, "closed").catch(() => {});
				await machine.save().catch(() => {});
			}
			throw error;
		}
		machine.checkAuthority(scope.ctx);
		record.phase = "failed";
		record.error = messageOf(error);
		if (historyId) await history!.finish(historyId, "failed", error);
		await machine.save();
		throw error;
	}
}

/**
 * Whether the step was stopped by something other than its own clock: the host, an enclosing
 * step's timeout, a parallel sibling's failure, a lost lease. (When the step's clock fires
 * first, the combined signal carries the clock's reason.)
 */
function stoppedFromOutside(scope: Scope) {
	const signal = scope.ctx.signal;
	if (!signal?.aborted) return false;
	return !(scope.timeoutSignal?.aborted && signal.reason === scope.timeoutSignal.reason);
}

/**
 * The pipeline paused to ask its question: ask through a gate activation, which publishes it,
 * and keep the answer for the pipeline to continue with.
 */
async function answerPipelineQuestion(
	machine: Machine,
	step: WorkflowStep,
	key: string,
	scope: Scope,
	record: Activation,
) {
	const pause = record.activity!.pause!;
	const question = record.activity!.question!;
	if (question.kind === "approval") {
		// The pipeline halted after a stage to ask; it continues with the next one.
		const gateKey = keys.pipelineGate(key, pause.resumeAtIndex);
		await pipelineApproval(machine, step, gateKey, scope, question);
		return;
	}
	const gateKey =
		pause.mode === "next_stage"
			? keys.pipelineGate(key, pause.resumeAtIndex)
			: keys.pipelineInput(key, pause.resumeAtIndex, pause.commandInput.pending.requestIndex);
	// A question asked after a stage is `ask`'s, which takes a typed answer as text too.
	await inputStep(machine, step, gateKey, scope, {
		request: question,
		acceptsText: pause.mode === "next_stage",
	});
	record.activity!.response = activation(machine.state, gateKey).result!.response;
}

/**
 * Whether an activity interrupted by a crash may run again. A crash leaves no record of
 * whether a command that may not be repeated already acted, so a step running one never runs
 * again on recovery, whatever its `recovery` says.
 */
function mayRepeat(step: WorkflowStep, record: Activation, ctx: RunContext) {
	if (step.recovery !== "repeat" || record.nonRetryable) return false;
	const saved = record.activity?.input;
	if (saved?.kind !== "pipeline" || !saved.text || !ctx.registry) return true;
	const registry = ctx.registry;
	return !parsePipeline(saved.text, registry).some(
		(stage) => registry.get(stage.name)?.meta?.repeatable === false,
	);
}

/** Where a paused pipeline continues, with the question it asked and the answer. */
function resumeOf(activity: NonNullable<Activation["activity"]>) {
	const { pause, question, response } = activity;
	return pause && question ? { pause, question, response } : undefined;
}
