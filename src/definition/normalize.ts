import { promises as fs } from "node:fs";
import path from "node:path";
import { loadWorkflowFile } from "../workflows/load.js";
import { resolveTemplate } from "../workflows/expressions.js";
import { fingerprint } from "../persistence/model.js";
import { resolveWorkflowStepArgs } from "../workflows/values.js";
import { resolveWorkflowArgs } from "./args.js";
import type { Definition } from "../engine/state.js";
import type { WorkflowStep } from "../workflows/types.js";

/** Freeze the definition before effects; dynamic child paths are frozen on entry. */
export async function snapshotDefinition(
	filePath: string,
	args: Record<string, unknown>,
	ancestors: string[] = [],
): Promise<Definition> {
	const canonical = await fs.realpath(filePath);
	if (ancestors.includes(canonical)) throw new Error(`Workflow creates a cycle: ${canonical}`);
	const workflow = await loadWorkflowFile(canonical);
	const resolved = resolveWorkflowArgs(workflow.args, args);
	const children: Record<string, Definition> = {};
	const collect = async (steps: WorkflowStep[], prefix: string) => {
		for (const step of steps) {
			const key = prefix + "/" + step.id;
			if (step.workflow) {
				const ref = resolveTemplate(step.workflow, resolved, {});
				if (!ref.includes("$")) {
					children[key] = await snapshotDefinition(
						path.resolve(path.dirname(canonical), ref),
						resolveWorkflowStepArgs(step.workflow_args, resolved, {}),
						[...ancestors, canonical],
					);
				}
			}
			if (step.steps) await collect(step.steps, key);
		}
	};
	await collect(workflow.steps, "");
	return {
		filePath: canonical,
		hash: fingerprint({
			workflow,
			children: Object.fromEntries(
				Object.entries(children).map(([key, child]) => [key, child.hash]),
			),
		}),
		workflow,
		children,
	};
}
