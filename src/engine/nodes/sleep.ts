// A `sleep:` step: waits in place when short, otherwise pauses the run until its time.
import type { WorkflowStep } from "../../workflows/types.js";
import { parseDuration, sleepUntil } from "../../util/duration.js";
import { activation } from "../checkpoint.js";
import type { Machine } from "../machine.js";
import type { Scope } from "../scope.js";
import { publishWait, resolveWait, suspend, waitOf } from "../waits.js";

const DEFAULT_INLINE_SLEEP_MS = 60_000;

/** How long a sleep may keep the process waiting instead of pausing the run. */
function inlineSleepMs(env: Record<string, string | undefined>) {
	const raw = env.LOBSTER_INLINE_SLEEP_MS;
	if (raw === undefined || raw === "") return DEFAULT_INLINE_SLEEP_MS;
	return parseDuration(Number.isFinite(Number(raw)) ? Number(raw) : raw, "LOBSTER_INLINE_SLEEP_MS");
}

/**
 * Publish a timer wait with a fixed wake time, then wait in place (short sleeps) or pause the
 * run until `lobster runs advance --due` comes back after that time.
 */
export async function sleepStep(machine: Machine, step: WorkflowStep, key: string, scope: Scope) {
	const frame = activation(machine.state, key);
	if (frame.phase === "completed") return frame.result!;
	// Fixed on first entry, so re-entering after a pause or a crash keeps the same time.
	const { until } = waitOf(frame, "timer", step.id, () => ({
		kind: "timer",
		until: Date.now() + parseDuration(step.sleep, `Step ${step.id} sleep`),
	}));
	const remaining = until - Date.now();
	if (remaining > 0) {
		if (remaining > inlineSleepMs(scope.ctx.env)) await suspend(machine, key, frame);
		// Saved first: a crash or Ctrl-C while waiting leaves an ordinary sleeping run.
		await publishWait(machine, key, frame);
		await sleepUntil(until, scope.ctx.signal);
	}
	return resolveWait(machine, frame, {
		id: step.id,
		json: { until: new Date(until).toISOString() },
	});
}
