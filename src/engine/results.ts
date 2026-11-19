// Step results: how a command's output becomes one, how one becomes output items, and the
// results compound steps build from the steps inside them.
import type { WorkflowStep, WorkflowStepResult } from "../workflows/types.js";
import { stepIds } from "../workflows/steps.js";
import { createRecord } from "../workflows/values.js";

export function parseJson(stdout: string) {
	const trimmed = stdout.trim();
	if (!trimmed) return undefined;
	try {
		return JSON.parse(trimmed);
	} catch {
		return undefined;
	}
}

/** A result's value as output items: its JSON, its response, or its text. */
export function toOutputItems(result: WorkflowStepResult | undefined) {
	if (!result) return [];
	if (result.json !== undefined) {
		return Array.isArray(result.json) ? result.json : [result.json];
	}
	if (result.response !== undefined) {
		return Array.isArray(result.response) ? result.response : [result.response];
	}
	if (result.stdout !== undefined) {
		return result.stdout === "" ? [] : [result.stdout];
	}
	return [];
}

/**
 * A value bound as a result, as a loop binds its item and index: `json` is the value itself,
 * `stdout` its text (a string as it is, anything else as JSON).
 */
export function valueResult(id: string, value: unknown): WorkflowStepResult {
	return { id, json: value, stdout: typeof value === "string" ? value : JSON.stringify(value) };
}

/** The result of a step whose value is `value` (a child workflow's output, a step's stdin). */
export function createSyntheticStepResult(stepId: string, value: unknown): WorkflowStepResult {
	if (value === null || value === undefined) {
		return { id: stepId };
	}
	if (typeof value === "string") {
		return {
			id: stepId,
			stdout: value,
			json: parseJson(value),
		};
	}
	return {
		id: stepId,
		stdout: serializeValueForStdout(value),
		json: value,
	};
}

/**
 * The result of a compound step (a switch, a repeat) that shows the value of `last`, the last
 * step that ran inside it, without that step's flags (approved, error...).
 */
export function resultOfLastStep(
	stepId: string,
	last: WorkflowStepResult | undefined,
	fields: Partial<WorkflowStepResult>,
): WorkflowStepResult {
	const result: WorkflowStepResult = { id: stepId, ...fields };
	if (!last) return result;
	for (const field of ["json", "stdout", "response"] as const)
		if (last[field] !== undefined) result[field] = last[field] as never;
	return result;
}

/** Every step `steps` could have run (see stepIds), as skipped. */
export function markSkipped(steps: WorkflowStep[], into: Record<string, WorkflowStepResult>) {
	for (const id of stepIds(steps)) into[id] = { id, skipped: true };
}

/**
 * The steps inside `step` (a switch's cases, a repeat's steps, parallel branches) as skipped,
 * for a step whose body did not run (its condition was false, or `on_error` tolerated its
 * failure). Later steps may read them, so each needs a result. Undefined when it has none.
 */
export function skippedInside(step: WorkflowStep): Record<string, WorkflowStepResult> | undefined {
	const inside = createRecord<WorkflowStepResult>();
	markSkipped([step], inside);
	delete inside[step.id];
	return Object.keys(inside).length ? inside : undefined;
}

/** A step's stdin value as pipeline input items: none, each array element, or the value. */
export function inputValueToPipelineItems(value: unknown): unknown[] {
	if (value === null || value === undefined) return [];
	return Array.isArray(value) ? [...value] : [value];
}

export function serializePipelineItemsToStdout(items: unknown[]) {
	if (!items.length) return "";
	if (items.every((item) => typeof item === "string")) {
		return items.map((item) => String(item)).join("\n");
	}
	if (items.length === 1) {
		return serializeValueForStdout(items[0]);
	}
	return JSON.stringify(items);
}

export function serializeValueForStdout(value: unknown) {
	if (value === null || value === undefined) return "";
	if (typeof value === "string") return value;
	return JSON.stringify(value);
}
