import { activation, occurrence } from "../state.js";
import type { Machine, Scope } from "../machine.js";
import type { WorkflowStep } from "../../workflows/types.js";
import { createRecord, resolveInputValue } from "../../workflows/values.js";
import { sequence } from "./sequence.js";
import { loopGatesRefused } from "./repeat.js";
import type { Activation } from "../state.js";

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
	const concurrency = step.concurrency ?? 1;
	const gatesRefused =
		concurrency > 1
			? (scope.gatesRefused ??
				"approvals, input, sleeps and waits are not supported inside a for_each with concurrency: several items would wait at once")
			: loopGatesRefused(step, scope);
	/** Run one item's steps; its output is saved with the next save. */
	const runItem = async (index: number) => {
		const item = items![index];
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
			gatesRefused,
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
		machine.putRecord(outputKey(index), output);
		// The iteration's output is kept above; its body is not needed to recover.
		machine.forgetBelow(bodyKey, true, frame);
	};
	if (concurrency <= 1) {
		while (frame.cursor! < frame.count) {
			const index = frame.cursor!;
			if (step.pause_ms && index > 0 && index % (step.batch_size ?? 1) === 0) {
				frame.retryAt ??= Date.now() + step.pause_ms;
				await machine.save();
				await machine.delay(frame.retryAt, scope.ctx.signal);
				delete frame.retryAt;
			}
			await runItem(index);
			// The output and the position move together, in the next save.
			frame.cursor!++;
			machine.defer();
		}
	} else {
		await runAtOnce(concurrency, frame, runItem);
	}
	const outputs = await machine.readRecords(key + "/output/");
	const iterations = [...outputs.keys()].sort().map((name) => outputs.get(name));
	if (iterations.length !== frame.count)
		throw new Error(`Workflow step ${step.id} for_each: saved outputs are missing`);
	// The result carries them from here; the records go with the save that completes the step.
	machine.dropRecords(key + "/");
	return { id: step.id, json: iterations, stdout: JSON.stringify(iterations) };
}

/**
 * Run the items from `frame.cursor` on, up to `limit` at once. `cursor` moves past every item
 * finished in order; items finished ahead of it are in `frame.finished` (fewer than `limit`).
 * Recovery starts again from there: an item already under way resumes where it was.
 *
 * When an item fails, no new item starts; the items already running finish (their outputs
 * are kept, so a retry runs only what did not finish), and then the loop fails. Stopping
 * them midway would leave commands whose outcome nobody knows.
 */
async function runAtOnce(
	limit: number,
	frame: Activation,
	runItem: (index: number) => Promise<void>,
) {
	const finished = new Set(frame.finished ?? []);
	const running = new Set<Promise<void>>();
	let next = frame.cursor!;
	let failure: { error: unknown } | undefined;
	const start = (index: number) => {
		const task = runItem(index).then(
			() => {
				finished.add(index);
				while (finished.has(frame.cursor!)) finished.delete(frame.cursor!++);
				frame.finished = [...finished].sort((a, b) => a - b);
				if (!frame.finished.length) delete frame.finished;
			},
			(error) => {
				failure ??= { error };
			},
		);
		const tracked = task.finally(() => running.delete(tracked));
		running.add(tracked);
	};
	for (;;) {
		while (!failure && running.size < limit && next < frame.count!) {
			const index = next++;
			if (!finished.has(index)) start(index);
		}
		if (!running.size) break;
		await Promise.race(running);
	}
	if (failure) throw failure.error;
}
