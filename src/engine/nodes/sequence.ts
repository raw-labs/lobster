import { activation, occurrence } from "../state.js";
import type { Machine, Scope } from "../machine.js";
import type { WorkflowStep } from "../../workflows/types.js";
import { createRecord } from "../../workflows/values.js";

export async function sequence(machine: Machine, steps: WorkflowStep[], key: string, scope: Scope) {
	const frame = activation(machine.state, key);
	// The checkpoint holds only this sequence's own results. Inherited results (a loop
	// body sees everything before the loop) are combined in memory: copying them into
	// every iteration's checkpoint made state, and every save, grow with the square of
	// the item count.
	frame.results ??= createRecord();
	frame.cursor ??= 0;
	const results = Object.assign(createRecord(scope.results), frame.results);
	const local = { ...scope, results };
	for (; frame.cursor < steps.length;) {
		const step = steps[frame.cursor];
		const result = await machine.visit(step, occurrence(key, step.id), {
			...local,
			lastStepId: frame.lastStepId,
			definitionPath: (scope.definitionPath ?? "") + "/" + step.id,
		});
		frame.results[step.id] = results[step.id] = result;
		const childKey = occurrence(key, step.id);
		const node = activation(machine.state, childKey);
		if (node.results) {
			Object.assign(frame.results, node.results);
			Object.assign(results, node.results);
		}
		frame.cursor++;
		if (!result.skipped && !result.error) frame.lastStepId = step.id;
		if (result.error && step.on_error === "skip_rest") frame.cursor = steps.length;
		// The cursor is past this step and its result is kept above: one copy is enough.
		machine.forgetBelow(childKey, true, frame);
		machine.defer();
	}
	frame.phase = "completed";
	return frame.lastStepId !== undefined
		? frame.results[frame.lastStepId]
		: { id: steps.at(-1)?.id ?? "" };
}
