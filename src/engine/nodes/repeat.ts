// A `repeat:` step: its steps in rounds until a condition holds.
import type { WorkflowStep } from "../../workflows/types.js";
import { evaluateCondition } from "../../workflows/conditions.js";
import { createRecord } from "../../workflows/values.js";
import { activation, keys } from "../checkpoint.js";
import type { Machine } from "../machine.js";
import { bodyGatesRefused, type Scope } from "../scope.js";
import { resultOfLastStep } from "../results.js";
import { sequence } from "./sequence.js";

/**
 * Run the steps in rounds. After each round, `until` is checked against that round's
 * results; when it holds the loop ends, and after `max` rounds it fails (or, with
 * `on_max: continue`, ends). A round sees the results of the round before it, and only
 * the last round's results are kept: earlier rounds are forgotten as they finish, so a
 * long loop does not grow the saved state.
 *
 * Afterwards the last round's steps are visible to later steps, and the repeat's own
 * result is the result of the last step that ran in it, with `rounds`.
 */
export async function repeatStep(machine: Machine, step: WorkflowStep, key: string, scope: Scope) {
	const frame = activation(machine.state, key);
	const config = step.repeat!;
	const progress = (frame.repeat ??= { rounds: 0 });
	frame.results ??= createRecord();
	const gatesRefused = bodyGatesRefused(step, scope);
	while (true) {
		const seen = Object.assign(createRecord(scope.results), frame.results);
		// Decided from saved results each time, so recovery decides the same way.
		if (progress.rounds > 0) {
			if (evaluateCondition(config.until, seen)) break;
			if (progress.rounds >= config.max) {
				if (config.on_max === "continue") break;
				throw new Error(
					`Workflow step ${step.id} repeat: until did not hold after ${config.max} round${config.max === 1 ? "" : "s"}`,
				);
			}
		}
		const bodyKey = keys.occurrence(key, "round-" + (progress.rounds + 1));
		await sequence(machine, step.steps!, bodyKey, {
			...scope,
			results: seen,
			gatesRefused,
			env: { ...scope.env, ...step.env },
			cwd: step.cwd ?? scope.cwd,
		});
		const body = activation(machine.state, bodyKey);
		frame.results = Object.assign(createRecord(), body.results);
		progress.lastStepId = body.sequence?.lastStepId;
		progress.rounds++;
		// The round's results are kept above; its records are not needed to recover.
		machine.forgetBelow(bodyKey, true, frame);
	}
	const last = progress.lastStepId !== undefined ? frame.results[progress.lastStepId] : undefined;
	return resultOfLastStep(step.id, last, { rounds: progress.rounds });
}
