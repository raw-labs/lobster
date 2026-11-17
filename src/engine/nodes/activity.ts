import { activation, InterruptedActivityError } from "../state.js";
import type { Machine, Scope } from "../machine.js";
import type { WorkflowStep } from "../../workflows/types.js";
import { parsePipeline } from "../../pipeline/parser.js";
import { getStepExecution } from "../../workflows/load.js";
import { resolveTemplate } from "../../workflows/expressions.js";
import {
	mergeEnv,
	resolveCwd,
	resolveInputValue,
	encodeShellInput,
} from "../../workflows/values.js";
import { runShellCommand } from "../activities/shell.js";
import { runPipelineStep, WorkflowPipelineInputSuspension } from "../activities/pipeline.js";
import { parseJson, createSyntheticStepResult } from "../values.js";
import { bindRecorder, recorderFromEnv } from "../../persistence/context.js";
import { input } from "./input.js";
import { pipelineApproval } from "./approval.js";

export async function activity(machine: Machine, step: WorkflowStep, key: string, scope: Scope) {
	const record = activation(machine.state, key + "/activity");
	if (record.phase === "completed") return record.result!;
	if (record.phase === "waiting") {
		const paused = record.pipelineInput!;
		if (paused.mode === "next_stage") {
			// The pipeline halted after a stage to ask; it continues with the next one.
			const gateKey = key + "/pipeline-gate-" + paused.resumeAtIndex;
			if (paused.gate === "approval" && record.wait?.kind === "approval")
				await pipelineApproval(machine, step, gateKey, scope, record.wait);
			else {
				await input(machine, step, gateKey, scope, record.wait);
				record.response = activation(machine.state, gateKey).result!.response;
			}
		} else {
			const gateKey =
				key +
				"/pipeline-input-" +
				paused.resumeAtIndex +
				"-" +
				paused.commandInput!.pending.requestIndex;
			await input(machine, step, gateKey, scope, record.wait);
			record.response = activation(machine.state, gateKey).result!.response;
		}
	} else if (record.phase === "started") {
		if (
			record.input?.kind !== "none" &&
			(step.recovery !== "repeat" ||
				record.nonRetryable ||
				containsUnrepeatable(record.input, scope.ctx.registry))
		)
			throw new InterruptedActivityError(key);
		record.phase = "ready";
	}
	if (record.phase === "failed") throw new Error(record.error);
	if (!record.input) {
		const execution = getStepExecution(step);
		if (execution.kind !== "shell" && execution.kind !== "pipeline" && execution.kind !== "none")
			throw new Error("Control node dispatched as activity");
		record.input = {
			kind: execution.kind,
			...(execution.kind !== "none"
				? { text: resolveTemplate(execution.value, scope.args, scope.results) }
				: {}),
			input: resolveInputValue(step.stdin, scope.args, scope.results),
			// Do not persist the host environment or credentials. Resolve only explicit workflow values.
			env: mergeEnv(scope.baseEnv ?? {}, scope.env, step.env, scope.args, scope.results),
			cwd: resolveCwd(step.cwd ?? scope.cwd, scope.args) ?? scope.ctx.cwd,
		};
	}
	const saved = record.input;

	const activityKey = key.slice(0, key.lastIndexOf("/attempt-"));
	if (record.phase !== "waiting") {
		const logical = activation(machine.state, activityKey);
		record.attempt = logical.dispatchAttempts = (logical.dispatchAttempts ?? 0) + 1;
	}
	const history = recorderFromEnv(scope.ctx.env)?.child(key + "/");
	const historyId = await history?.start("activity", step.id);
	record.phase = "started";
	await machine.save();
	scope.startTimeout?.();
	scope.ctx.signal?.throwIfAborted();
	machine.checkAuthority(scope.ctx);
	await scope.ctx._onExecutionStarted?.();
	let result;
	try {
		const ctx = {
			...scope.ctx,
			_onNonRetryableSideEffect: () => {
				record.nonRetryable = true;
				scope.ctx._onNonRetryableSideEffect?.();
			},
			_onUnsafeDispatch: () => scope.ctx._onUnsafeDispatch?.(),
		};
		const baseEnv = {
			...ctx.env,
			...saved.env,
			LOBSTER_RUN_ID: machine.store.runId,
			LOBSTER_ACTIVITY_ID: machine.store.runId + "/" + activityKey,
			LOBSTER_ACTIVITY_ATTEMPT: String(record.attempt),
		};
		const env = history ? bindRecorder(baseEnv, history) : baseEnv;
		if (saved.kind === "shell") {
			const { stdout } = await runShellCommand({
				command: saved.text!,
				stdin: encodeShellInput(saved.input),
				env,
				cwd: saved.cwd,
				signal: ctx.signal,
				forceTerminationSignal: ctx.forceTerminationSignal,
				assertInvocationCurrent: ctx.assertInvocationCurrent,
				onEffectDispatch: ctx._onUnsafeDispatch,
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
				llmSpendLedger: scope.ledger,
				env,
				cwd: saved.cwd,
				requestInputEnabled: !scope.nested,
				resume: record.pipelineInput
					? { pipelineInput: record.pipelineInput, response: record.response }
					: undefined,
				onExecutionStart: () => ctx._onExecutionStarted?.(),
			});
		} else result = createSyntheticStepResult(step.id, saved.input);
		ctx.signal?.throwIfAborted();
		record.result = result;
		record.phase = "completed";
		if (historyId) await history!.finish(historyId, "completed", result);
		// The result and completed marker are one checkpoint. Parent cursor advances later.
		await machine.save();
		// Once the result is saved, the step's own timer or heartbeat may no longer fail it:
		// that would retry, and run again, a command that already finished. Only the host may
		// still stop the run here.
		if (!scope.timeoutSignal?.aborted) ctx.signal?.throwIfAborted();
		return result;
	} catch (error) {
		if (error instanceof WorkflowPipelineInputSuspension) {
			if (historyId) await history!.finish(historyId, "waiting");
			record.pipelineInput = error.pipelineInput;
			record.wait = error.request;
			record.phase = "waiting";
			await machine.save();
			return activity(machine, step, key, scope);
		}
		machine.checkAuthority(scope.ctx);
		if (machine.fatal(error) || scope.ctx.signal?.aborted) throw error;
		record.phase = "failed";
		record.error = error?.message ?? String(error);
		if (historyId) await history!.finish(historyId, "failed", error);
		await machine.save();
		throw error;
	}
}

/**
 * A crash leaves no record of whether a command that may not be repeated already acted, so a
 * step running one is never re-run on recovery, whatever its `recovery` says. Retries in the
 * same process stay precise: the command reports dispatch through `onNonRetryableSideEffect`.
 */
function containsUnrepeatable(
	saved: { kind: string; text?: string } | undefined,
	registry: { get(name: string): { meta?: { repeatable?: boolean } } | undefined } | undefined,
) {
	if (saved?.kind !== "pipeline" || !saved.text || !registry) return false;
	return parsePipeline(saved.text, registry as any).some(
		(stage) => registry.get(stage.name)?.meta?.repeatable === false,
	);
}
