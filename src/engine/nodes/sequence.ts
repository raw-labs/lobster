import { activation, occurrence } from "../state.js";
import type { Machine, Scope } from "../machine.js";
import type { WorkflowStep } from "../../workflows/types.js";
import { createRecord } from "../../workflows/values.js";

export async function sequence(machine: Machine, steps: WorkflowStep[], key: string, scope: Scope) {
	const frame = activation(machine.state, key);
	frame.results ??= createRecord(scope.results);
	frame.cursor ??= 0;
	const local = { ...scope, results: frame.results };
	for (; frame.cursor < steps.length;) {
		const step = steps[frame.cursor];
		const result = await machine.visit(step, occurrence(key, step.id), {
			...local,
			definitionPath: (scope.definitionPath ?? "") + "/" + step.id,
		});
		frame.results[step.id] = result;
		const node = activation(machine.state, occurrence(key, step.id));
		if (node.results) Object.assign(frame.results, node.results);
		frame.cursor++;
		if (!result.skipped && !result.error) frame.result = result;
		if (result.error && step.on_error === "skip_rest") frame.cursor = steps.length;
		await machine.save();
	}
	frame.phase = "completed";
	return frame.result ?? { id: steps.at(-1)?.id ?? "" };
}
