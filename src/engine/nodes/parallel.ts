// A `parallel:` step: its branches at once; it waits for all of them, or the first to answer.
import { messageOf } from "../../util/errors.js";
import { combineSignals } from "../../process/owner_signal.js";
import type { WorkflowStep, WorkflowStepResult } from "../../workflows/types.js";
import { activation, keys } from "../checkpoint.js";
import type { Machine } from "../machine.js";
import { GATES_REFUSED, type Scope } from "../scope.js";
import { branchSpendBuffer, type BranchSpendBuffer } from "../accounting.js";
import { stopsRun } from "../errors.js";

/**
 * Each branch saves its result as it finishes. With `wait: any` the first branch to finish
 * wins: it is saved before the others are stopped, and only its spend is charged to the run.
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
		env: { ...scope.env, ...step.env },
		cwd: step.cwd ?? scope.cwd,
	};
	const buffers = new Map<string, BranchSpendBuffer>();
	const lost = (branchId: string) => race && frame.parallel && frame.parallel.winner !== branchId;
	try {
		const run = async (branch: (typeof config.branches)[number]) => {
			if (lost(branch.id)) return;
			const buffer = race ? branchSpendBuffer(scope.ledger) : undefined;
			if (buffer) buffers.set(branch.id, buffer);
			const ledger = buffer ?? scope.ledger;
			const childKey = keys.occurrence(key, branch.id);
			const result = await machine.visit(branch, childKey, { ...branchScope, ledger }, false);
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
		if (winner) buffers.get(winner)?.release();
		const results: Record<string, WorkflowStepResult> = winner
			? { [winner]: frame.results![winner] }
			: (frame.results ?? {});
		frame.results = results;
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
