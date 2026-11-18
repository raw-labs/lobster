import { activation, occurrence } from "../state.js";
import type { Machine, Scope } from "../machine.js";
import type { WorkflowStep } from "../../workflows/types.js";
import { createRecord, resolveInputValue } from "../../workflows/values.js";
import { sequence } from "./sequence.js";
import { loopGatesRefused } from "./repeat.js";

/**
 * Runs its steps once per item. The items, and each item's output, are kept beside the
 * checkpoint as records (see Machine.putRecord): written once, not with every save, so a
 * save costs the same at item 10 as at item 10,000. The checkpoint holds only how many
 * items there are and how many are done.
 */
export async function forEach(machine: Machine, step: WorkflowStep, key: string, scope: Scope) {
	const frame = activation(machine.state, key);
	const itemsKey = key + "/items";
	const outputKey = (index: number) => `${key}/output/${String(index).padStart(10, "0")}`;
	let items: unknown[] | undefined;
	if (frame.count === undefined) {
		if (frame.items) {
			// A checkpoint from before records: move its items and outputs out of it.
			items = frame.items;
			frame.iterations?.forEach((output, index) => machine.putRecord(outputKey(index), output));
			delete frame.items;
			delete frame.iterations;
		} else {
			const resolved = resolveInputValue(step.for_each, scope.args, scope.results);
			if (!Array.isArray(resolved))
				throw new Error(
					`Workflow step ${step.id} for_each: expected array, got ${typeof resolved}`,
				);
			items = structuredClone(resolved);
			frame.cursor = 0;
		}
		machine.putRecord(itemsKey, items);
		frame.count = items.length;
		machine.defer();
	}
	if (frame.cursor! < frame.count)
		items ??= (await machine.readRecords(itemsKey)).get(itemsKey) as unknown[] | undefined;
	if (frame.cursor! < frame.count && !Array.isArray(items))
		throw new Error(`Workflow step ${step.id} for_each: its saved items are missing`);
	const itemVar = step.item_var ?? "item",
		indexVar = step.index_var ?? "index";
	while (frame.cursor! < frame.count) {
		const index = frame.cursor!;
		const item = items![index];
		if (step.pause_ms && index > 0 && index % (step.batch_size ?? 1) === 0) {
			frame.retryAt ??= Date.now() + step.pause_ms;
			await machine.save();
			await machine.delay(frame.retryAt, scope.ctx.signal);
			delete frame.retryAt;
		}
		const results = createRecord(scope.results);
		results[itemVar] = {
			id: itemVar,
			json: item,
			stdout: typeof item === "string" ? item : JSON.stringify(item),
		};
		results[indexVar] = { id: indexVar, json: index, stdout: String(index) };
		const bodyKey = occurrence(key, "item-" + index);
		await sequence(machine, step.steps!, bodyKey, {
			...scope,
			results,
			gatesRefused: loopGatesRefused(step, scope),
			env: { ...scope.env, ...step.env },
			cwd: step.cwd ?? scope.cwd,
			costPrefix: scope.costPrefix + step.id + ".",
		});
		const body = activation(machine.state, bodyKey);
		const output: Record<string, unknown> = { [itemVar]: item, [indexVar]: index };
		for (const child of step.steps!) {
			const result = body.results![child.id];
			if (result && !result.skipped)
				output[child.id] = result.json !== undefined ? result.json : result.stdout;
		}
		// The output and the position move together, in the next save.
		machine.putRecord(outputKey(index), output);
		frame.cursor!++;
		// The iteration's output is kept above; its body is not needed to recover.
		machine.forgetBelow(bodyKey, true, frame);
		machine.defer();
	}
	const outputs = await machine.readRecords(key + "/output/");
	const iterations = [...outputs.keys()].sort().map((name) => outputs.get(name));
	if (iterations.length !== frame.count)
		throw new Error(`Workflow step ${step.id} for_each: saved outputs are missing`);
	// The result carries them from here; the records go with the save that completes the step.
	machine.dropRecords(key + "/");
	return { id: step.id, json: iterations, stdout: JSON.stringify(iterations) };
}
