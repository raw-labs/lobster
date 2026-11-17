import { activation } from "../state.js";
import type { Machine, Scope } from "../machine.js";
import type { WorkflowStep, WorkflowStepResult } from "../../workflows/types.js";
import { resolveInputValue } from "../../workflows/values.js";
import { parseDuration } from "../../util/duration.js";

/** Resolve `$step.path` references and `${arg}` templates inside a match pattern. */
function resolveMatch(
	value: unknown,
	args: Record<string, unknown>,
	results: Record<string, WorkflowStepResult>,
): unknown {
	if (typeof value === "string") return resolveInputValue(value, args, results);
	if (value && typeof value === "object" && !Array.isArray(value))
		return Object.fromEntries(
			Object.entries(value).map(([key, item]) => [key, resolveMatch(item, args, results)]),
		);
	return value;
}

/**
 * `wait:` completes with the oldest matching inbox event this run has not consumed, received
 * since the run started; otherwise it pauses the run until `lobster signal` delivers one or
 * its deadline passes.
 */
export async function eventWait(machine: Machine, step: WorkflowStep, key: string, scope: Scope) {
	const frame = activation(machine.state, key);
	frame.stepId = step.id;
	if (frame.phase === "completed") return frame.result!;
	const spec = step.wait!;
	frame.wait ??= {
		kind: "event",
		event: spec.event,
		...(spec.match
			? { match: resolveMatch(spec.match, scope.args, scope.results) as Record<string, unknown> }
			: {}),
		since: machine.state.startedAt ?? 0,
		...(spec.timeout !== undefined
			? {
					deadline: Date.now() + parseDuration(spec.timeout, `Step ${step.id} wait.timeout`),
					onTimeout: spec.on_timeout ?? "cancel",
				}
			: {}),
	};
	const wait = frame.wait;
	if (wait.kind !== "event") throw new Error("Invalid event wait activation");
	const event = await machine.findEvent(wait);
	if (event) {
		machine.consumeEvent(event.id);
		return machine.resolveWait(key, frame, {
			id: step.id,
			json: event.data,
			response: { id: event.id, event: event.name, at: new Date(event.at).toISOString() },
		});
	}
	if (machine.expired(frame)) return machine.timeOut(key, frame, { id: step.id });
	return machine.suspend(key, frame);
}
