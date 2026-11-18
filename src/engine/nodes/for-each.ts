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
	/**
	 * Run one item's steps. `done` marks it finished in the same synchronous step that keeps its
	 * output and forgets its body: a save taken in between would see the body gone and the item
	 * not finished, and recovery would run it again.
	 */
	const runItem = async (index: number, done: () => void) => {
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
		done();
	};
	if (concurrency <= 1) {
		while (frame.cursor! < frame.count) {
			const index = frame.cursor!;
			// Paced once per batch: `pausedFor` keeps a recovered run from pausing again.
			if (
				step.pause_ms &&
				index > 0 &&
				index % (step.batch_size ?? 1) === 0 &&
				frame.pausedFor !== index
			) {
				frame.retryAt ??= Date.now() + step.pause_ms;
				await machine.save();
				await machine.delay(frame.retryAt, scope.ctx.signal);
				delete frame.retryAt;
				frame.pausedFor = index;
			}
			// The output and the position move together, in the next save.
			await runItem(index, () => {
				frame.cursor!++;
				delete frame.pausedFor;
			});
			machine.defer();
		}
	} else {
		await runAtOnce(concurrency, frame, runItem, (error) => machine.fatal(error));
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
 * finished in order; items finished ahead of it are in `frame.finished`. Recovery starts again
 * from there: an item already under way resumes where it was. No item starts more than
 * `window(limit)` places past `cursor`, so a slow item does not let `finished` (saved with every
 * checkpoint) grow with the loop.
 *
 * When an item fails, no new item starts; the items already running finish (their outputs
 * are kept, so a retry runs only what did not finish), and then the loop fails. Stopping
 * them midway would leave commands whose outcome nobody knows. A fatal error (a stopped run,
 * a lost lease) wins over an ordinary one, so the run ends as it must.
 */
async function runAtOnce(
	limit: number,
	frame: Activation,
	runItem: (index: number, done: () => void) => Promise<void>,
	fatal: (error: unknown) => boolean,
) {
	const finished = new Set(frame.finished ?? []);
	const running = new Set<Promise<void>>();
	const ahead = window(limit);
	let next = frame.cursor!;
	let failure: { error: unknown } | undefined;
	const start = (index: number) => {
		const task = runItem(index, () => {
			finished.add(index);
			while (finished.has(frame.cursor!)) finished.delete(frame.cursor!++);
			frame.finished = [...finished].sort((a, b) => a - b);
			if (!frame.finished.length) delete frame.finished;
		}).catch((error) => {
			if (!failure || (fatal(error) && !fatal(failure.error))) failure = { error };
		});
		const tracked = task.finally(() => running.delete(tracked));
		running.add(tracked);
	};
	for (;;) {
		while (
			!failure &&
			running.size < limit &&
			next < frame.count! &&
			next < frame.cursor! + ahead
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
