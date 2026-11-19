// Advancing many runs: every run whose time has come, or the runs an external event wakes.
// Each is advanced like `advanceWorkflowRun` advances one, under its own lease.
import { messageOf } from "../util/errors.js";
import { randomUUID } from "node:crypto";
import { withStoreContext } from "../persistence/execution.js";
import { sessionFromEnv } from "../persistence/context.js";
import { callerId } from "../persistence/waits.js";
import type { WorkflowRunResult } from "../workflows/types.js";
import { EVENT_NAME, EVENT_NAME_CHARS } from "../workflows/load.js";
import { waitsForEvent, type RunStatus } from "./checkpoint.js";
import type { RunContext } from "./context.js";
import { ExecutionConflictError } from "./errors.js";
import { advanceWorkflowRun } from "./runner.js";
import { checkLeaseMs } from "./store.js";

export type DueRunOutcome = {
	runId: string;
	/** What the advance returned, or the run's saved status when it threw ("busy": leased). */
	status: WorkflowRunResult["status"] | RunStatus | "busy";
	waitingFor?: WorkflowRunResult["waitingFor"];
	error?: string;
};

/** Advance one run, reporting the outcome instead of throwing (used for batches). */
async function advanceReporting(
	runId: string,
	ctx: RunContext,
	leaseMs?: number,
): Promise<DueRunOutcome> {
	try {
		const result = await advanceWorkflowRun({ runId, ctx, leaseMs });
		return {
			runId,
			status: result.status,
			...(result.waitingFor ? { waitingFor: result.waitingFor } : {}),
		};
	} catch (error) {
		// Another process holds the run's lease: it is being advanced right now.
		if (error instanceof ExecutionConflictError) return { runId, status: "busy" };
		const session = sessionFromEnv(ctx.env);
		// Reporting must not fail the batch: the other runs' outcomes would be lost.
		const run = session ? await session.getRunState(runId).catch(() => null) : null;
		// A run stopped mid-advance is still waiting or running (due again), not failed.
		return { runId, status: run?.execution?.status ?? "failed", error: messageOf(error) };
	}
}

/**
 * Advance every run whose time has come, then return: a crashed run whose lease lapsed, a
 * timer that is due, a question or event wait past its deadline. Lobster never loops on its
 * own; a host, cron job or person calls this. The usual rules apply: a step that may have
 * acted re-runs only when repeatable, everything else becomes blocked for a person.
 */
export async function advanceDueRuns(options: {
	ctx: RunContext;
	limit?: number;
	/** How many runs to advance at once (default 4). Each is a separate run with its own lease. */
	concurrency?: number;
	dryRun?: boolean;
	leaseMs?: number;
}): Promise<{ due: string[]; advanced: DueRunOutcome[] }> {
	const concurrency = options.concurrency ?? 4;
	const limit = options.limit ?? 100;
	for (const [name, value] of [
		["concurrency", concurrency],
		["limit", limit],
	] as const)
		if (!Number.isSafeInteger(value) || value < 1)
			throw new Error(`${name} must be a positive integer`);
	checkLeaseMs(options.leaseMs);
	return withStoreContext(options.ctx, async (ctx) => {
		const session = sessionFromEnv(ctx.env)!;
		const due = await session.dueRuns({
			limit,
			callerId: callerId(ctx.runMetadata),
		});
		// A dry run (asked here or by the context) lists what is due and advances nothing.
		if (options.dryRun || options.ctx.dryRun) return { due, advanced: [] };
		// Runs share nothing but the store, whose writes are serialized: a slow run no longer
		// delays the others beyond the number of runs advanced at once.
		const advanced: DueRunOutcome[] = Array.from({ length: due.length });
		let next = 0;
		const worker = async () => {
			while (next < due.length) {
				const index = next++;
				advanced[index] = await advanceReporting(due[index], ctx, options.leaseMs);
			}
		};
		await Promise.all(Array.from({ length: Math.min(concurrency, due.length) }, worker));
		return { due, advanced };
	});
}

/**
 * Deliver an external event: store it in the inbox (a repeated `id` is ignored) and advance
 * the waiting runs whose `wait:` step it matches. Runs that reach their `wait:` later still
 * find it, since they match events received since they started.
 */
export async function signalEvent(options: {
	name: string;
	data?: unknown;
	id?: string;
	ctx: RunContext;
	leaseMs?: number;
}): Promise<{ event: { id: string; name: string; inserted: boolean }; advanced: DueRunOutcome[] }> {
	if (typeof options.name !== "string" || !EVENT_NAME.test(options.name))
		throw new Error(`Event names use ${EVENT_NAME_CHARS}`);
	// Checked before the event is stored: a bad lease must not leave a delivered event behind.
	checkLeaseMs(options.leaseMs);
	// Signalling stores the event for real: a dry run is refused, never ignored.
	if (options.ctx.dryRun) throw new Error("A dry run applies to starting a workflow file");
	// The delivery ID makes delivery idempotent: a blank one would swallow every later event.
	if (
		options.id !== undefined &&
		(typeof options.id !== "string" || !options.id.trim() || options.id.length > 256)
	)
		throw new Error("Event ID must be a non-empty string of at most 256 characters");
	return withStoreContext(options.ctx, async (ctx) => {
		const session = sessionFromEnv(ctx.env)!;
		const id = options.id ?? randomUUID();
		const owner = callerId(ctx.runMetadata);
		// Scoped to the sender: its events reach only its own runs (or any run, for an unscoped
		// trusted caller), and its delivery IDs cannot collide with another caller's.
		const { inserted } = await session.appendEvent({
			id,
			name: options.name,
			data: options.data ?? null,
			at: Date.now(),
			owner: owner ?? "",
		});
		const waiting: string[] = [];
		for (let after = ""; ;) {
			const page = await session.listRuns({
				status: "waiting",
				after,
				limit: 100,
				callerId: owner,
			});
			for (const summary of page.runs) {
				const run = await session.getRunState(summary.id);
				if (run?.execution && waitsForEvent(run.execution, options.name, options.data ?? null))
					waiting.push(summary.id);
			}
			if (!page.nextCursor) break;
			after = page.nextCursor;
		}
		const advanced: DueRunOutcome[] = [];
		for (const runId of waiting) {
			// A run that is busy is usually just saving its wait; it looks at the inbox again
			// after saving, but wait for it so this delivery reports what happened.
			let outcome = await advanceReporting(runId, ctx, options.leaseMs);
			for (let delay = 25; outcome.status === "busy" && delay <= 1600; delay *= 2) {
				await new Promise((resolve) => setTimeout(resolve, delay));
				outcome = await advanceReporting(runId, ctx, options.leaseMs);
			}
			advanced.push(outcome);
		}
		return { event: { id, name: options.name, inserted }, advanced };
	});
}
