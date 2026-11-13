import type { WorkflowFile } from "../workflows/types.js";
import { createRecord } from "../workflows/values.js";
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
