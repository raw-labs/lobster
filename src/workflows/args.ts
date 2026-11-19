import type { WorkflowFile } from "./types.js";
import { createRecord } from "./values.js";
import { closestMatch } from "../util/suggest.js";

/**
 * Refuse args a workflow that declares `args:` does not take, and declared args with no
 * default that are not given: either would leave `${name}` empty in its commands.
 */
export function checkWorkflowArgs(
	argDefs: WorkflowFile["args"],
	provided: Record<string, unknown>,
	where: string,
) {
	if (!argDefs) return;
	const names = Object.keys(argDefs);
	for (const name of Object.keys(provided))
		if (!Object.hasOwn(argDefs, name)) {
			const near = closestMatch(name, names);
			throw new Error(
				`${where} takes no arg "${name}"${near ? ` (did you mean "${near}"?)` : ""}. Args: ${names.join(", ") || "none"}`,
			);
		}
	for (const [name, definition] of Object.entries(argDefs))
		if (!("default" in definition) && !Object.hasOwn(provided, name))
			throw new Error(`${where} requires arg "${name}" (it has no default)`);
}
export function resolveWorkflowArgs(
	argDefs: WorkflowFile["args"],
	provided: Record<string, unknown> | undefined,
) {
	const resolved = createRecord<unknown>();
	if (argDefs) {
		for (const [key, def] of Object.entries(argDefs)) {
			if (def && typeof def === "object" && "default" in def) {
				resolved[key] = def.default;
			}
		}
	}
	if (provided) {
		for (const [key, value] of Object.entries(provided)) {
			resolved[key] = value;
		}
	}
	return resolved;
}
