// A `for_each:` step: its steps once per item, one at a time or up to `concurrency` at once.
import type { WorkflowStep } from "../../workflows/types.js";
import { createRecord, resolveInputValue } from "../../workflows/values.js";
import { sleepUntil } from "../../util/duration.js";
import { activation, keys, type Activation } from "../checkpoint.js";
import type { Machine } from "../machine.js";
import { bodyGatesRefused, GATES_REFUSED, innerSettings, type Scope } from "../scope.js";
import { stopsRun } from "../errors.js";
import { valueResult } from "../results.js";
import { deliberateWait } from "../step_clock.js";
import { sequence } from "./sequence.js";

type Progress = NonNullable<Activation["forEach"]>;

/**
 * The items, and each item's output, are kept beside the checkpoint as records (see
 * CheckpointRecords): written once, not with every save, so a save costs the same at item 10
 * as at item 10,000. The checkpoint holds only how many items there are and how many are done.
 */
export async function forEachStep(machine: Machine, step: WorkflowStep, key: string, scope: Scope) {
	const frame = activation(machine.state, key);
	const itemsKey = key + "/items";
	const outputKey = (index: number) => `${key}/output/${String(index).padStart(10, "0")}`;
	let items: unknown[] | undefined;
	if (!frame.forEach) {
		const resolved = resolveInputValue(step.for_each, scope.args, scope.results);
		if (!Array.isArray(resolved))
			throw new Error(`Workflow step ${step.id} for_each: expected array, got ${typeof resolved}`);
		items = structuredClone(resolved);
		machine.records.put(itemsKey, items);
		frame.forEach = { count: items.length, cursor: 0 };
	}
	const progress = frame.forEach;
	if (progress.cursor < progress.count) {
		const saved = items ?? (await machine.records.read(itemsKey)).get(itemsKey);
		if (!Array.isArray(saved))
			throw new Error(`Workflow step ${step.id} for_each: its saved items are missing`);
		items = saved;
	}
	const itemVar = step.item_var ?? "item";
	const indexVar = step.index_var ?? "index";
	const concurrency = step.concurrency ?? 1;
	const gatesRefused =
		concurrency > 1
			? (scope.gatesRefused ?? GATES_REFUSED.concurrent)
			: bodyGatesRefused(step, scope);
	/**
	 * Run one item's steps. `done` marks it finished in the same synchronous step that keeps its
	 * output and forgets its body: a save taken in between would see the body gone and the item
	 * not finished, and recovery would run it again.
	 */
	const runItem = async (index: number, done: () => void) => {
		const item = items![index];
		const results = createRecord(scope.results);
		results[itemVar] = valueResult(itemVar, item);
		results[indexVar] = valueResult(indexVar, index);
		const bodyKey = keys.occurrence(key, "item-" + index);
		await sequence(machine, step.steps!, bodyKey, {
			...scope,
			results,
			gatesRefused,
			...innerSettings(step, scope),
			costPrefix: scope.costPrefix + step.id + ".",
			// Several items at once: a wait in one is not a wait of the loop.
			holdClocks: concurrency > 1 ? undefined : scope.holdClocks,
		});
		const body = activation(machine.state, bodyKey);
		const output: Record<string, unknown> = { [itemVar]: item, [indexVar]: index };
		for (const child of step.steps!) {
			const result = body.results![child.id];
			if (result && !result.skipped)
				output[child.id] = result.json !== undefined ? result.json : result.stdout;
		}
		machine.records.put(outputKey(index), output);
		// The iteration's output is kept above; its body is not needed to recover.
		machine.forgetBelow(bodyKey, true, frame);
		done();
	};
	if (concurrency > 1) await runAtOnce(concurrency, progress, runItem);
	else
		while (progress.cursor < progress.count) {
			const index = progress.cursor;
			await pauseBetweenBatches(machine, step, progress, index, scope);
			// The output and the position move together, in the next save.
			await runItem(index, () => {
				progress.cursor++;
				delete progress.pausedBefore;
			});
		}
	const outputs = await machine.records.read(key + "/output/");
	const iterations = [...outputs.keys()].sort().map((name) => outputs.get(name));
	if (iterations.length !== progress.count)
		throw new Error(`Workflow step ${step.id} for_each: saved outputs are missing`);
	// The result carries them from here; the records go with the save that completes the step.
	machine.records.drop(key + "/");
	return { id: step.id, json: iterations, stdout: JSON.stringify(iterations) };
}

/**
 * With `pause_ms`, wait that long before each batch of `batch_size` items after the first.
 * The end of the pause is saved first, and `pausedBefore` keeps a recovered run from pausing
 * before the same item again.
 */
async function pauseBetweenBatches(
	machine: Machine,
	step: WorkflowStep,
	progress: Progress,
	index: number,
	scope: Scope,
) {
	if (!step.pause_ms || index === 0 || index % (step.batch_size ?? 1) !== 0) return;
	if (progress.pausedBefore === index) return;
	const until = (progress.pauseUntil ??= Date.now() + step.pause_ms);
	await deliberateWait(scope.holdClocks, async () => {
		await machine.save();
		await sleepUntil(until, scope.ctx.signal);
	});
	delete progress.pauseUntil;
	progress.pausedBefore = index;
}

/**
 * Run the items from `progress.cursor` on, up to `limit` at once. `cursor` moves past every item
 * finished in order; items finished ahead of it are in `progress.finished`. Recovery starts
 * again from there: an item already under way resumes where it was. No item starts more than
 * `window(limit)` places past `cursor`, so a slow item does not let `finished` (saved with every
 * checkpoint) grow with the loop.
 *
 * When an item fails, no new item starts; the items already running finish (their outputs
 * are kept, so a retry runs only what did not finish), and then the loop fails. Stopping
 * them midway would leave commands whose outcome nobody knows. An error that stops the run (a
 * stopped run, a lost lease) wins over an ordinary one, so the run ends as it must.
 */
async function runAtOnce(
	limit: number,
	progress: Progress,
	runItem: (index: number, done: () => void) => Promise<void>,
) {
	const finished = new Set(progress.finished ?? []);
	const running = new Set<Promise<void>>();
	const ahead = window(limit);
	let next = progress.cursor;
	let failure: { error: unknown } | undefined;
	const start = (index: number) => {
		const task = runItem(index, () => {
			finished.add(index);
			while (finished.has(progress.cursor)) finished.delete(progress.cursor++);
			progress.finished = [...finished].sort((a, b) => a - b);
			if (!progress.finished.length) delete progress.finished;
		}).catch((error) => {
			if (!failure || (stopsRun(error) && !stopsRun(failure.error))) failure = { error };
		});
		const tracked = task.finally(() => running.delete(tracked));
		running.add(tracked);
	};
	for (;;) {
		while (
			!failure &&
			running.size < limit &&
			next < progress.count &&
			next < progress.cursor + ahead
		) {
			const index = next++;
			if (!finished.has(index)) start(index);
		}
		if (!running.size) break;
		await Promise.race(running);
	}
	if (failure) throw failure.error;
}

/** How far past the first unfinished item a loop with `limit` at once may start items. */
function window(limit: number) {
	return Math.max(limit * 8, 64);
}
