// A workflow's pipeline steps, checked as a run would check them, before anything runs: each
// parses, names known commands and only their flags. Parts built from step references are left
// to the run, which has them. The dry run checks each pipeline it plans the same way.
import { assertKnownFlags } from "../commands/define.js";
import type { CommandLookup } from "../commands/types.js";
import { parsePipeline } from "../pipeline/parser.js";
import type { Definition } from "./definition.js";
import { hasStepRef, resolveArgsTemplate } from "./expressions.js";
import { resolveWorkflowArgs } from "./args.js";
import { getStepExecution } from "./steps.js";
import type { WorkflowStep } from "./types.js";
import { resolveWorkflowStepArgs } from "./values.js";

/**
 * A pipeline's stages, checked as a run would check them: it parses, and every stage names a
 * known command and only its flags (unless its name holds a step reference, which the run fills in).
 */
export function checkedStages(
	pipelineText: string,
	registry: CommandLookup | undefined,
	what: string,
) {
	if (!registry) throw new Error(`${what} requires a command registry for pipeline execution`);
	const stages = parsePipeline(pipelineText, registry);
	for (const stage of stages) {
		if (hasStepRef(stage.name)) continue;
		const command = registry.get(stage.name);
		if (!command) throw new Error(`${what} pipeline references unknown command: ${stage.name}`);
		// Every flag name is checked now; a value holding a step reference, once the run has it.
		const args = Object.entries(stage.args ?? {}).map(([key, value]) => [
			key,
			typeof value === "string" && hasStepRef(value) ? undefined : value,
		]);
		assertKnownFlags(command, stage.name, Object.fromEntries(args));
	}
	return stages;
}

/**
 * Check every pipeline step of a frozen workflow, nested steps, parallel branches and the
 * child workflows it froze included, before the run starts: a typo in a later step must not
 * fail the run after earlier steps have acted.
 */
export function checkPipelineSteps(
	definition: Definition,
	args: Record<string, unknown>,
	registry: CommandLookup,
) {
	const visit = (steps: WorkflowStep[], args: Record<string, unknown>, prefix: string) => {
		for (const step of steps) {
			const key = prefix + "/" + step.id;
			for (const item of [step, ...(step.parallel?.branches ?? [])]) {
				const execution = getStepExecution(item);
				if (execution.kind !== "pipeline") continue;
				const what = item === step ? `Workflow step ${item.id}` : `Parallel branch ${item.id}`;
				checkedStages(resolveArgsTemplate(execution.value, args), registry, what);
			}
			if (step.steps) visit(step.steps, args, key);
			for (const entry of step.switch?.cases ?? []) visit(entry.steps, args, key);
			if (step.switch?.else) visit(step.switch.else, args, key);
			const child = definition.children[key];
			if (child)
				checkPipelineSteps(
					child,
					resolveWorkflowArgs(
						child.workflow.args,
						resolveWorkflowStepArgs(step.workflow_args, args, {}),
					),
					registry,
				);
		}
	};
	visit(definition.workflow.steps, args, "");
}
