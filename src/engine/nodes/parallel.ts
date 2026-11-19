// A `parallel:` step: its branches at once; it waits for all of them, or the first to answer.
import { messageOf } from "../../util/errors.js";
import { combineSignals } from "../../process/owner_signal.js";
import type { WorkflowStep, WorkflowStepResult } from "../../workflows/types.js";
import { activation, keys } from "../checkpoint.js";
import { createRecord } from "../../workflows/values.js";
import type { Machine } from "../machine.js";
import { GATES_REFUSED, innerSettings, type Scope } from "../scope.js";
import { stopsRun } from "../errors.js";

/**
 * Each branch saves its result as it finishes. With `wait: any` the first branch to finish
 * wins: it is saved before the others are stopped. The calls every branch made count in the
 * run's cost, the losers' too.
 * A failed branch stops the others (with `wait: all`); either way the step returns only once
 * every branch has settled, so none can write or dispatch after it.
 */
export async function parallelStep(
	machine: Machine,
	step: WorkflowStep,
	key: string,
	scope: Scope,
) {
	const frame = activation(machine.state, key);
	const config = step.parallel!;
	const race = config.wait === "any";
	const abort = new AbortController();
	const timeout = new AbortController();
	let timer: ReturnType<typeof setTimeout> | undefined;
	if (config.timeout_ms) {
		// Persisted by the branches' first checkpoint; writing it first would spend the budget.
		frame.timeoutAt ??= Date.now() + config.timeout_ms;
		timer = setTimeout(
			() =>
				timeout.abort(new Error(`Parallel step ${step.id} timed out after ${config.timeout_ms}ms`)),
			Math.max(0, frame.timeoutAt - Date.now()),
		);
	}
	const signal = combineSignals(scope.ctx.signal, abort.signal, timeout.signal);
	let onAbort: () => void;
	const interrupted = new Promise<never>((_, reject) => {
		onAbort = () => reject(signal.reason);
		if (signal.aborted) onAbort();
		else signal.addEventListener("abort", onAbort, { once: true });
	});
	void interrupted.catch(() => {});
	const branchScope = {
		...scope,
		gatesRefused: GATES_REFUSED.parallel,
		ctx: { ...scope.ctx, signal },
		...innerSettings(step, scope),
		// Branches run at once: a wait in one is not a wait of the step.
		holdClocks: undefined,
	};
	const lost = (branchId: string) => race && frame.parallel && frame.parallel.winner !== branchId;
	try {
		const run = async (branch: (typeof config.branches)[number]) => {
			if (lost(branch.id)) return;
			const result = await machine.visit(branch, keys.branch(key, branch.id), branchScope);
			if (lost(branch.id)) return;
			if (race) frame.parallel = { winner: branch.id };
			(frame.results ??= {})[branch.id] = result;
			await machine.save();
			return result;
		};
		const pending = config.branches.map(run);
		const outcomes = Promise.allSettled(pending);
		let failure: unknown;
		try {
			if (!race) await Promise.race([Promise.all(pending), interrupted]);
			else if (!frame.parallel) await Promise.race([...pending, interrupted]);
		} catch (error) {
			failure =
				race || stopsRun(error) || signal.aborted
					? error
					: new Error("Parallel branch failed: " + messageOf(error));
		}
		if (race || failure || signal.aborted) abort.abort();
		await settled(outcomes);
		if (failure) throw failure;
		if (timeout.signal.aborted) throw timeout.signal.reason;
		const winner = frame.parallel?.winner;
		const results: Record<string, WorkflowStepResult> = winner
			? { [winner]: frame.results![winner] }
			: (frame.results ?? {});
		// The step's output is the branches that count; the others (a race's losers) did not
		// finish, and are recorded as skipped so later steps can read them as such.
		frame.results = createRecord<WorkflowStepResult>();
		for (const branch of config.branches)
			frame.results[branch.id] = { id: branch.id, skipped: true };
		Object.assign(frame.results, results);
		const json = Object.fromEntries(
			Object.entries(results).map(([id, result]) => [id, result.json]),
		);
		return {
			id: step.id,
			json,
			stdout: race ? (Object.values(results)[0]?.stdout ?? "") : JSON.stringify(json),
		};
	} finally {
		if (timer) clearTimeout(timer);
		signal.removeEventListener("abort", onAbort!);
	}
}

/**
 * Wait for every branch to settle, so none can still write or dispatch once the step returns;
 * an error that stops the run wins. Branches told to stop get half a second.
 */
async function settled(outcomes: Promise<PromiseSettledResult<unknown>[]>) {
	let timer: ReturnType<typeof setTimeout> | undefined;
	const results = await Promise.race([
		outcomes,
		new Promise<never>((_, reject) => {
			timer = setTimeout(
				() => reject(new Error("Parallel branches did not settle after cancellation")),
				500,
			);
		}),
	]).finally(() => clearTimeout(timer));
	for (const entry of results)
		if (entry.status === "rejected" && stopsRun(entry.reason)) throw entry.reason;
}
