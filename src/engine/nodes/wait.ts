// A `wait:` step: the run continues when a matching external event arrives (`lobster signal`).
import type { WorkflowStep, WorkflowStepResult } from "../../workflows/types.js";
import { resolveInputValue } from "../../workflows/values.js";
import type { InboxEvent } from "../../persistence/backend.js";
import { activation, matchesEvent, type WaitRequest } from "../checkpoint.js";
import type { Machine } from "../machine.js";
import type { Scope } from "../scope.js";
import { deliberateWait } from "../step_clock.js";
import {
	expired,
	publishWait,
	resolveWait,
	suspend,
	timeOut,
	waitDeadline,
	waitOf,
} from "../waits.js";

type EventWait = Extract<WaitRequest, { kind: "event" }>;

/**
 * Resolve `$step.path` references and `${arg}` templates inside a match pattern. A reference
 * that resolves to nothing fails the step: matching on "nothing" would accept every event.
 */
function resolveMatch(
	value: Record<string, unknown>,
	args: Record<string, unknown>,
	results: Record<string, WorkflowStepResult>,
	where: string,
): Record<string, unknown>;
function resolveMatch(
	value: unknown,
	args: Record<string, unknown>,
	results: Record<string, WorkflowStepResult>,
	where: string,
): unknown;
function resolveMatch(
	value: unknown,
	args: Record<string, unknown>,
	results: Record<string, WorkflowStepResult>,
	where: string,
): unknown {
	if (typeof value === "string") {
		const resolved = resolveInputValue(value, args, results);
		if (resolved === undefined || resolved === null || resolved === "")
			throw new Error(`${where} resolved to nothing (${JSON.stringify(value)})`);
		return resolved;
	}
	if (value && typeof value === "object" && !Array.isArray(value))
		return Object.fromEntries(
			Object.entries(value).map(([key, item]) => [
				key,
				resolveMatch(item, args, results, `${where}.${key}`),
			]),
		);
	return value;
}

/**
 * `wait:` completes with the oldest matching inbox event this run has not consumed, received
 * since the run started; otherwise it pauses the run until `lobster signal` delivers one or
 * its deadline passes.
 */
export async function waitStep(machine: Machine, step: WorkflowStep, key: string, scope: Scope) {
	const frame = activation(machine.state, key);
	if (frame.phase === "completed") return frame.result!;
	const spec = step.wait!;
	const wait = waitOf(frame, "event", step.id, () => ({
		kind: "event",
		event: spec.event,
		...(spec.match
			? {
					match: resolveMatch(spec.match, scope.args, scope.results, `Step ${step.id} wait.match`),
				}
			: {}),
		since: machine.state.startedAt ?? 0,
		...waitDeadline(spec, `Step ${step.id} wait`),
	}));
	const take = (event: InboxEvent) => {
		// Remembered with the step's result, so this run never consumes the event again.
		(machine.state.events ??= []).push(eventKey(event));
		return resolveWait(machine, frame, {
			id: step.id,
			json: event.data,
			response: { id: event.id, event: event.name, at: new Date(event.at).toISOString() },
		});
	};
	const event = await findEvent(machine, wait);
	if (event) return take(event);
	if (expired(frame)) return timeOut(machine, frame, { id: step.id });
	// Look once more after the wait is saved: an event signalled between the first look and
	// the save found no waiting run to advance, and this run would otherwise miss it. Saved
	// inside a hold, so a crash before the pause leaves no deadline on the steps around it.
	return deliberateWait(scope.holdClocks, async () => {
		await publishWait(machine, key, frame);
		const late = await findEvent(machine, wait);
		if (late) return take(late);
		return suspend(machine, key, frame);
	});
}

/** The oldest inbox event matching `wait` that this run has not consumed yet. */
async function findEvent(machine: Machine, wait: EventWait) {
	const { session, runId } = machine.store;
	// Only unscoped events and events from the caller that owns this run.
	const owner = (await session.getRunState(runId))?.metadata?.callerId ?? "";
	const consumed = new Set(machine.state.events ?? []);
	const events = await session.findEvents(wait.event, wait.since, owner);
	// An event received after the deadline does not count, however late the run is advanced.
	return events.find(
		(event) =>
			!consumed.has(eventKey(event)) &&
			(wait.deadline === undefined || event.at <= wait.deadline) &&
			matchesEvent(wait.match, event.data),
	);
}

/** Delivery IDs are unique per sender, so a consumed event is remembered by both. */
function eventKey(event: InboxEvent) {
	return JSON.stringify([event.owner, event.id]);
}
