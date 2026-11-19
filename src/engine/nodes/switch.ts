// A `switch:` step: the steps of the first case whose `when` holds, or `else`.
import type { WorkflowStep, WorkflowStepResult } from "../../workflows/types.js";
import { evaluateCondition } from "../../workflows/conditions.js";
import { createRecord } from "../../workflows/values.js";
import { activation, keys } from "../checkpoint.js";
import type { Machine } from "../machine.js";
import { bodyGatesRefused, innerSettings, type Scope } from "../scope.js";
import { markSkipped, resultOfLastStep } from "../results.js";
import { sequence } from "./sequence.js";

/**
 * The case is chosen once, from saved results, and kept: a resumed or recovered run continues
 * the same case. Afterwards the case's steps are visible to later steps like any other, and the
 * steps of every case that did not run count as skipped. The switch's own result is that of the
 * last step that ran in its case, so `$switch.json` is "whichever branch ran".
 */
export async function switchStep(machine: Machine, step: WorkflowStep, key: string, scope: Scope) {
	const frame = activation(machine.state, key);
	const config = step.switch!;
	if (!frame.switch) {
		const index = config.cases.findIndex((entry) => evaluateCondition(entry.when, scope.results));
		if (index >= 0) frame.switch = { case: index + 1 };
		else if (config.else) frame.switch = { case: "else" };
		else throw new Error(`Workflow step ${step.id} switch: no case matched and there is no else`);
		// Deterministic from saved results; written with the case's first save.
	}
	const chosen = frame.switch.case;
	const steps = chosen === "else" ? config.else! : config.cases[chosen - 1].steps;
	const bodyKey = keys.occurrence(key, chosen === "else" ? "else" : `case-${chosen}`);
	const last = await sequence(machine, steps, bodyKey, {
		...scope,
		...innerSettings(step, scope),
		gatesRefused: bodyGatesRefused(step, scope),
	});
	const body = activation(machine.state, bodyKey);
	const results: Record<string, WorkflowStepResult> = createRecord();
	for (const other of [...config.cases.map((entry) => entry.steps), config.else ?? []])
		if (other !== steps) markSkipped(other, results);
	frame.results = Object.assign(results, body.results);
	const ran = body.sequence?.lastStepId !== undefined;
	return resultOfLastStep(step.id, ran ? last : undefined, { case: chosen });
}
