import { activation, occurrence } from "../state.js";
import type { Machine, Scope } from "../machine.js";
import type { WorkflowStep, WorkflowStepResult } from "../../workflows/types.js";
import { evaluateCondition } from "../../workflows/conditions.js";
import { createRecord } from "../../workflows/values.js";
import { resolveRetryConfig } from "../retry.js";
import { sequence } from "./sequence.js";
import { attachPipelineSourceItems, pipelineSourceItems } from "../accounting.js";

const LOOP_RETRY =
	"approvals, input, sleeps and waits are not supported inside a for_each, repeat or switch with retry: a retry would run it again and ask again";

/**
 * Why the steps inside a loop or a switch cannot pause: its enclosing scope's reason, or a
 * retry of the step, which would run them again and ask again.
 */
export function loopGatesRefused(step: WorkflowStep, scope: Scope) {
	if (scope.gatesRefused) return scope.gatesRefused;
	return resolveRetryConfig(step.retry).max > 1 ? LOOP_RETRY : undefined;
}

/**
 * Runs the steps in rounds. After each round, `until` is checked against that round's
 * results; when it holds the loop ends, and after `max` rounds it fails (or, with
 * `on_max: continue`, ends). A round sees the results of the round before it, and only
 * the last round's results are kept: earlier rounds are forgotten as they finish, so a
 * long loop does not grow the saved state.
 *
 * Afterwards the last round's steps are visible to later steps, and the repeat's own
 * result is the result of the last step that ran in it, with `rounds`.
 */
export async function repeat(machine: Machine, step: WorkflowStep, key: string, scope: Scope) {
	const frame = activation(machine.state, key);
	const config = step.repeat!;
	frame.cursor ??= 0;
	frame.results ??= createRecord();
	const gatesRefused = loopGatesRefused(step, scope);
	while (true) {
		const seen = Object.assign(createRecord(scope.results), frame.results);
		// Decided from saved results each time, so recovery decides the same way.
		if (frame.cursor > 0) {
			if (evaluateCondition(config.until, seen)) break;
			if (frame.cursor >= config.max) {
				if (config.on_max === "continue") break;
				throw new Error(
					`Workflow step ${step.id} repeat: until did not hold after ${config.max} round${config.max === 1 ? "" : "s"}`,
				);
			}
		}
		const round = frame.cursor + 1;
		const bodyKey = occurrence(key, "round-" + round);
		await sequence(machine, step.steps!, bodyKey, {
			...scope,
			results: seen,
			gatesRefused,
			env: { ...scope.env, ...step.env },
			cwd: step.cwd ?? scope.cwd,
		});
		const body = activation(machine.state, bodyKey);
		frame.results = Object.assign(createRecord(), body.results);
		frame.lastStepId = body.lastStepId;
		frame.cursor = round;
		// The round's results are kept above; its records are not needed to recover.
		machine.forgetBelow(bodyKey, true, frame);
	}
	const result: WorkflowStepResult = { id: step.id, rounds: frame.cursor };
	const last = frame.lastStepId !== undefined ? frame.results[frame.lastStepId] : undefined;
	if (last) {
		for (const field of ["json", "stdout", "response"] as const)
			if (last[field] !== undefined) result[field] = last[field] as never;
		const items = pipelineSourceItems(last);
		if (items) attachPipelineSourceItems(result, items);
	}
	return result;
}
