import { activation, occurrence } from "../state.js";
import type { Machine, Scope } from "../machine.js";
import type { WorkflowStep, WorkflowStepResult } from "../../workflows/types.js";
import { evaluateCondition } from "../../workflows/conditions.js";
import { createRecord } from "../../workflows/values.js";
import { sequence } from "./sequence.js";
import { attachPipelineSourceItems, pipelineSourceItems } from "../accounting.js";

/**
 * Runs the steps of the first case whose `when` holds, or `else`. The choice is made once,
 * from saved results, and kept: a resumed or recovered run continues the same case.
 *
 * Afterwards the case's steps are visible to later steps like any other, and the steps of
 * every case that did not run count as skipped. The switch's own result is the result of
 * the last step that ran in its case, so `$switch.json` is "whichever branch ran".
 */
export async function switchStep(machine: Machine, step: WorkflowStep, key: string, scope: Scope) {
	const frame = activation(machine.state, key);
	const config = step.switch!;
	if (frame.case === undefined) {
		const index = config.cases.findIndex((entry) => evaluateCondition(entry.when, scope.results));
		if (index >= 0) frame.case = index + 1;
		else if (config.else) frame.case = "else";
		else throw new Error(`Workflow step ${step.id} switch: no case matched and there is no else`);
		// Deterministic from saved results; written with the case's first save.
		machine.defer();
	}
	const chosen = frame.case;
	const steps = chosen === "else" ? config.else! : config.cases[chosen - 1].steps;
	const bodyKey = occurrence(key, chosen === "else" ? "else" : `case-${chosen}`);
	const last = await sequence(machine, steps, bodyKey, scope);
	const body = activation(machine.state, bodyKey);
	const results: Record<string, WorkflowStepResult> = createRecord();
	const branches = [...config.cases.map((entry) => entry.steps), config.else ?? []];
	for (const other of branches) if (other !== steps) markSkipped(other, results);
	Object.assign(results, body.results);
	frame.results = results;
	// The value of the case's last step that ran, without its flags (approved, error...).
	const result: WorkflowStepResult = { id: step.id, case: chosen };
	if (body.lastStepId !== undefined) {
		for (const field of ["json", "stdout", "response"] as const)
			if (last[field] !== undefined) result[field] = last[field] as never;
		const items = pipelineSourceItems(last);
		if (items) attachPipelineSourceItems(result, items);
	}
	return result;
}

/** Every step a case could have run, nested switches included, as skipped. */
function markSkipped(steps: WorkflowStep[], into: Record<string, WorkflowStepResult>) {
	for (const step of steps) {
		into[step.id] = { id: step.id, skipped: true };
		for (const branch of step.parallel?.branches ?? [])
			into[branch.id] = { id: branch.id, skipped: true };
		if (step.switch) {
			for (const entry of step.switch.cases) markSkipped(entry.steps, into);
			markSkipped(step.switch.else ?? [], into);
		}
		if (step.repeat) markSkipped(step.steps ?? [], into);
	}
}
