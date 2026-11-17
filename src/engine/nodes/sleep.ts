import { activation } from "../state.js";
import type { Machine, Scope } from "../machine.js";
import type { WorkflowStep } from "../../workflows/types.js";
import { parseDuration } from "../../util/duration.js";

const DEFAULT_INLINE_SLEEP_MS = 60_000;

/** How long a sleep may keep the process waiting instead of pausing the run. */
function inlineSleepMs(env: Record<string, string | undefined>) {
	const raw = env.LOBSTER_INLINE_SLEEP_MS;
	if (raw === undefined || raw === "") return DEFAULT_INLINE_SLEEP_MS;
	return parseDuration(Number.isFinite(Number(raw)) ? Number(raw) : raw, "LOBSTER_INLINE_SLEEP_MS");
}

/**
 * `sleep:` publishes a timer wait with a fixed wake time, then either waits in place (short
 * sleeps) or pauses the run until `lobster runs advance --due` comes back after that time.
 */
export async function sleep(machine: Machine, step: WorkflowStep, key: string, scope: Scope) {
	const frame = activation(machine.state, key);
	frame.stepId = step.id;
	if (frame.phase === "completed") return frame.result!;
	// Fixed on first entry, so re-entering after a pause or a crash keeps the same time.
	frame.wait ??= {
		kind: "timer",
		until: Date.now() + parseDuration(step.sleep, `Step ${step.id} sleep`),
	};
	const wait = frame.wait;
	if (wait.kind !== "timer") throw new Error("Invalid sleep activation");
	const remaining = wait.until - Date.now();
	if (remaining > 0) {
		if (remaining > inlineSleepMs(scope.ctx.env)) await machine.suspend(key, frame);
		// Saved first: a crash or Ctrl-C while waiting leaves an ordinary sleeping run.
		await machine.publishWait(key, frame);
		await machine.delay(wait.until, scope.ctx.signal);
	}
	return machine.resolveWait(key, frame, {
		id: step.id,
		json: { until: new Date(wait.until).toISOString() },
	});
}
