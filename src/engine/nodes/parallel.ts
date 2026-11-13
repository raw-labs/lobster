import { activation, occurrence } from "../state.js";
import type { Machine, Scope } from "../machine.js";
import type { WorkflowStep, WorkflowStepResult } from "../../workflows/types.js";
import { branchSpendBuffer, type BranchSpendBuffer } from "../accounting.js";

export async function parallel(machine: Machine, step: WorkflowStep, key: string, scope: Scope) {
	const frame = activation(machine.state, key);
	const config = step.parallel!,
		any = config.wait === "any";
	const abort = new AbortController();
	const timeout = new AbortController();
	let timer: ReturnType<typeof setTimeout> | undefined;
	if (config.timeout_ms) {
		frame.deadline ??= Date.now() + config.timeout_ms;
		await machine.save();
		timer = setTimeout(
			() =>
				timeout.abort(new Error(`Parallel step ${step.id} timed out after ${config.timeout_ms}ms`)),
			Math.max(0, frame.deadline - Date.now()),
		);
	}
	const signal = AbortSignal.any([scope.ctx.signal!, abort.signal, timeout.signal].filter(Boolean));
	const branchScope = {
		...scope,
		nested: true,
		timeoutSignal: scope.timeoutSignal
			? AbortSignal.any([scope.timeoutSignal, timeout.signal])
			: timeout.signal,
		ctx: { ...scope.ctx, signal },
		env: { ...scope.env, ...step.env },
		cwd: step.cwd ?? scope.cwd,
	};
	const buffers = new Map<string, BranchSpendBuffer>();
	try {
		const run = async (branch: (typeof config.branches)[number]) => {
			const childKey = occurrence(key, branch.id);
			if (frame.winner && frame.winner !== branch.id) return;
			const ledger = any ? branchSpendBuffer(scope.ledger) : scope.ledger;
			if (any) buffers.set(branch.id, ledger as BranchSpendBuffer);
			const result = await machine.visit(branch, childKey, { ...branchScope, ledger }, false);
			if (any && frame.winner && frame.winner !== branch.id) return;
			if (any) frame.winner = branch.id;
			frame.results ??= {};
			frame.results[branch.id] = result;
			await machine.save();
			return result;
		};
		const pending = config.branches.map(run);
		const settled = Promise.allSettled(pending);
		let failure: unknown;
		try {
			if (any && !frame.winner) await Promise.race(pending);
			else if (!any) {
				const outcomes = await settled;
				const rejected = outcomes.find((entry) => entry.status === "rejected");
				if (rejected?.status === "rejected")
					throw machine.fatal(rejected.reason)
						? rejected.reason
						: new Error(
								"Parallel branch failed: " + (rejected.reason?.message ?? String(rejected.reason)),
							);
			}
		} catch (error) {
			failure = error;
		}
		{
			if (any || failure || signal.aborted) abort.abort();
			// Do not release ownership while branch callbacks can still write or dispatch.
			let cancelTimer: ReturnType<typeof setTimeout> | undefined;
			const outcomes = await Promise.race([
				settled,
				new Promise<never>((_, reject) => {
					cancelTimer = setTimeout(
						() => reject(new Error("Parallel branches did not settle after cancellation")),
						500,
					);
				}),
			]).finally(() => {
				if (cancelTimer) clearTimeout(cancelTimer);
			});
			for (const entry of outcomes)
				if (entry.status === "rejected" && machine.fatal(entry.reason)) throw entry.reason;
		}
		if (failure) throw failure;
		if (timeout.signal.aborted) throw timeout.signal.reason;
		if (any && frame.winner) buffers.get(frame.winner)?.release();
		const results: Record<string, WorkflowStepResult> =
			any && frame.winner
				? { [frame.winner]: frame.results![frame.winner] }
				: (frame.results ?? {});
		frame.results = results;
		const json = Object.fromEntries(
			Object.entries(results).map(([id, result]) => [id, result.json]),
		);
		return {
			id: step.id,
			json,
			stdout: any ? (Object.values(results)[0]?.stdout ?? "") : JSON.stringify(json),
		};
	} finally {
		if (timer) clearTimeout(timer);
	}
}
