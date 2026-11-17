import type { WorkflowStepResult } from "./types.js";

export function resolveTemplate(
	input: string,
	args: Record<string, unknown>,
	results: Record<string, WorkflowStepResult>,
) {
	const withArgs = resolveArgsTemplate(input, args);
	return resolveStepRefs(withArgs, results);
}

export function resolveArgsTemplate(input: string, args: Record<string, unknown>) {
	return input.replace(/\$\{([A-Za-z0-9_-]+)\}/g, (match, key) => {
		if (Object.hasOwn(args, key)) return String(args[key]);
		return match;
	});
}

function resolveStepRefs(input: string, results: Record<string, WorkflowStepResult>) {
	return input.replace(
		/\$([A-Za-z0-9_-]+)\.([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*)/g,
		(match, id, pathValue) => {
			if (!(id in results)) {
				return match;
			}
			const refValue = getStepRefValue({ id, path: pathValue }, results, false);
			if (refValue === undefined) {
				if (pathValue === "approved" || pathValue === "skipped") return "false";
				return "";
			}
			return renderTemplateValue(refValue);
		},
	);
}

export function parseStepRef(value: string) {
	const match = value.match(/^\$([A-Za-z0-9_-]+)\.([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*)$/);
	if (!match) return null;
	return { id: match[1], path: match[2] };
}

export function getStepRefValue(
	ref: { id: string; path: string },
	results: Record<string, WorkflowStepResult>,
	strict: boolean,
) {
	const step = results[ref.id];
	if (!step) {
		if (strict) throw new Error(`Unknown step reference: ${ref.id}.${ref.path}`);
		return undefined;
	}
	return getValueByPath(step, ref.path);
}

function renderTemplateValue(value: unknown) {
	if (value === undefined || value === null) return "";
	if (typeof value === "string") return value;
	if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
		return String(value);
	}
	try {
		return JSON.stringify(value);
	} catch {
		return String(value);
	}
}

function getValueByPath(value: unknown, pathValue: string) {
	const fields = pathValue.split(".");
	let current: unknown = value;
	for (const field of fields) {
		if (current === null || current === undefined) return undefined;
		if (Array.isArray(current)) {
			const idx = Number(field);
			if (!Number.isInteger(idx) || idx < 0 || idx >= current.length) return undefined;
			current = current[idx];
			continue;
		}
		if (typeof current !== "object") return undefined;
		current = (current as Record<string, unknown>)[field];
	}
	return current;
}
