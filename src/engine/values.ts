import type { WorkflowStepResult } from "../workflows/types.js";
export function parseJson(stdout: string) {
	const trimmed = stdout.trim();
	if (!trimmed) return undefined;
	try {
		return JSON.parse(trimmed);
	} catch {
		return undefined;
	}
}

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

function* inputValueToItems(value: unknown) {
	if (value === null || value === undefined) return;
	if (Array.isArray(value)) {
		for (const item of value) yield item;
		return;
	}
	yield value;
}

export function inputValueToPipelineItems(value: unknown) {
	return [...inputValueToItems(value)];
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
