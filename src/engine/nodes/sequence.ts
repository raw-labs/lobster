// Steps one after another: each sees the results of those before it.
import type { WorkflowStep, WorkflowStepResult } from "../../workflows/types.js";
import { createRecord } from "../../workflows/values.js";
import { activation, keys } from "../checkpoint.js";
import type { Machine } from "../machine.js";
import type { Scope } from "../scope.js";
import { markSkipped } from "../results.js";

/** Run `steps` in order; the result is that of the last step that ran. */
export async function sequence(
	machine: Machine,
	steps: WorkflowStep[],
	key: string,
	scope: Scope,
): Promise<WorkflowStepResult> {
	const frame = activation(machine.state, key);
	// The checkpoint holds only this sequence's own results. Inherited results (a loop
	// body sees everything before the loop) are combined in memory: copying them into
	// every iteration's checkpoint made state, and every save, grow with the square of
	// the item count.
	frame.results ??= createRecord();
	const progress = (frame.sequence ??= { cursor: 0 });
	const results = Object.assign(createRecord(scope.results), frame.results);
	while (progress.cursor < steps.length) {
		const step = steps[progress.cursor];
		const childKey = keys.occurrence(key, step.id);
		const result = await machine.visit(step, childKey, {
			...scope,
			results,
			lastStepId: progress.lastStepId,
			definitionPath: (scope.definitionPath ?? "") + "/" + step.id,
		});
		frame.results[step.id] = results[step.id] = result;
		// The steps inside it (a switch's case, a repeat's last round) are readable too.
		const inner = machine.state.activations[childKey]?.results;
		if (inner) {
			Object.assign(frame.results, inner);
			Object.assign(results, inner);
		}
		progress.cursor++;
		if (!result.skipped && !result.error) progress.lastStepId = step.id;
		if (result.error && step.on_error === "skip_rest") {
			// The rest did not run: recorded as skipped, so later steps can read them as such.
			const rest = createRecord<WorkflowStepResult>();
			markSkipped(steps.slice(progress.cursor), rest);
			Object.assign(frame.results, rest);
			Object.assign(results, rest);
			progress.cursor = steps.length;
		}
		// The cursor is past this step and its result is kept above: one copy is enough.
		machine.forgetBelow(childKey, true, frame);
	}
	frame.phase = "completed";
	return progress.lastStepId !== undefined
		? frame.results[progress.lastStepId]
		: { id: steps.at(-1)?.id ?? "" };
}
