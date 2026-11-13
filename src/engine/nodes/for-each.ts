import { activation, occurrence } from "../state.js";
import type { Machine, Scope } from "../machine.js";
import type { WorkflowStep } from "../../workflows/types.js";
import { createRecord, resolveInputValue } from "../../workflows/values.js";
import { sequence } from "./sequence.js";

export async function forEach(machine: Machine, step: WorkflowStep, key: string, scope: Scope) {
	const frame = activation(machine.state, key);
	if (!frame.items) {
		const items = resolveInputValue(step.for_each, scope.args, scope.results);
		if (!Array.isArray(items))
			throw new Error(`Workflow step ${step.id} for_each: expected array, got ${typeof items}`);
		frame.items = structuredClone(items);
		frame.cursor = 0;
		frame.iterations = [];
		await machine.save();
	}
	const itemVar = step.item_var ?? "item",
		indexVar = step.index_var ?? "index";
	while (frame.cursor! < frame.items.length) {
		const index = frame.cursor!;
		if (step.pause_ms && index > 0 && index % (step.batch_size ?? 1) === 0) {
			frame.retryAt ??= Date.now() + step.pause_ms;
			await machine.save();
			await machine.delay(frame.retryAt, scope.ctx.signal);
			delete frame.retryAt;
		}
		const results = createRecord(scope.results);
		results[itemVar] = {
			id: itemVar,
			json: frame.items[index],
			stdout:
				typeof frame.items[index] === "string"
					? (frame.items[index] as string)
					: JSON.stringify(frame.items[index]),
		};
		results[indexVar] = { id: indexVar, json: index, stdout: String(index) };
		const bodyKey = occurrence(key, "item-" + index);
		await sequence(machine, step.steps!, bodyKey, {
			...scope,
			results,
			nested: true,
			env: { ...scope.env, ...step.env },
			cwd: step.cwd ?? scope.cwd,
			costPrefix: scope.costPrefix + step.id + ".",
		});
		const body = activation(machine.state, bodyKey);
		const output: Record<string, unknown> = { [itemVar]: frame.items[index], [indexVar]: index };
		for (const child of step.steps!) {
			const result = body.results![child.id];
			if (result && !result.skipped)
				output[child.id] = result.json !== undefined ? result.json : result.stdout;
		}
		frame.iterations!.push(output);
		frame.cursor!++;
		await machine.save();
	}
	return { id: step.id, json: frame.iterations, stdout: JSON.stringify(frame.iterations) };
}
