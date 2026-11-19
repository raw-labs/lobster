// Freezing a workflow for a run: the file, its arguments and the child workflows it names,
// so a run that resumes later runs the same definition even if the files changed.
import { promises as fs } from "node:fs";
import path from "node:path";
import { loadWorkflowFile } from "./load.js";
import { resolveTemplate } from "./expressions.js";
import { fingerprint } from "../util/serialization.js";
import { messageOf } from "../util/errors.js";
import { resolveWorkflowStepArgs } from "./values.js";
import { checkWorkflowArgs, resolveWorkflowArgs } from "./args.js";
import type { WorkflowFile, WorkflowStep } from "./types.js";

/** A workflow frozen for a run: the file, its hash, and the child workflows it names. */
export type Definition = {
	filePath: string;
	hash: string;
	workflow: WorkflowFile;
	children: Record<string, Definition>;
	/**
	 * A standalone pipeline (`lobster '...'`, `runToolRequest({ pipeline })`) run as a one-step
	 * workflow: its output is the pipeline's items as they are, and in human mode renderers write
	 * to the terminal as the pipeline runs.
	 */
	standalone?: boolean;
	/** A standalone pipeline's input items, used as they are (never read as templates). */
	input?: unknown[];
};

/** Freeze the definition before effects; dynamic child paths are frozen on entry. */
export async function snapshotDefinition(
	filePath: string,
	args: Record<string, unknown>,
	ancestors: string[] = [],
): Promise<Definition> {
	const canonical = await fs.realpath(filePath).catch((error) => {
		if (error?.code !== "ENOENT") throw error;
		throw Object.assign(new Error(`Workflow file not found: ${filePath}`), { code: "ENOENT" });
	});
	if (ancestors.includes(canonical)) throw new Error(`Workflow creates a cycle: ${canonical}`);
	const workflow = await loadWorkflowFile(canonical);
	checkWorkflowArgs(workflow.args, args, `Workflow ${canonical}`);
	const resolved = resolveWorkflowArgs(workflow.args, args);
	const children: Record<string, Definition> = {};
	const collect = async (steps: WorkflowStep[], prefix: string) => {
		for (const step of steps) {
			const key = prefix + "/" + step.id;
			if (step.workflow) {
				const ref = resolveTemplate(step.workflow, resolved, {});
				if (!ref.includes("$")) {
					children[key] = await snapshotChild(
						step,
						path.resolve(path.dirname(canonical), ref),
						resolveWorkflowStepArgs(step.workflow_args, resolved, {}),
						[...ancestors, canonical],
					);
				}
			}
			if (step.steps) await collect(step.steps, key);
			for (const entry of step.switch?.cases ?? []) await collect(entry.steps, key);
			if (step.switch?.else) await collect(step.switch.else, key);
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

/**
 * Freeze the child workflow `step` names, from `file`. A child that cannot be frozen (missing,
 * invalid, a cycle, args it does not take) is reported with the step and the file.
 */
export async function snapshotChild(
	step: WorkflowStep,
	file: string,
	args: Record<string, unknown>,
	ancestors: string[],
) {
	try {
		return await snapshotDefinition(file, args, ancestors);
	} catch (error) {
		throw new Error(`Workflow step ${step.id} (${file}): ${messageOf(error)}`);
	}
}

/** A standalone pipeline as a one-step workflow definition, fed `input` items as they are. */
export function pipelineDefinition(text: string, input?: unknown[]): Definition {
	return {
		filePath: "",
		hash: fingerprint({ pipeline: text, input: input ?? null }),
		workflow: { steps: [{ id: "pipeline", pipeline: text }] },
		children: {},
		standalone: true,
		...(input !== undefined ? { input } : {}),
	};
}
