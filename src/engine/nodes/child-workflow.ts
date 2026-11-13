import path from "node:path";
import { activation } from "../state.js";
import type { Machine, Scope } from "../machine.js";
import type { WorkflowStep } from "../../workflows/types.js";
import { resolveTemplate } from "../../workflows/expressions.js";
import { resolveWorkflowStepArgs, createRecord } from "../../workflows/values.js";
import { resolveWorkflowArgs } from "../../definition/args.js";
import { snapshotDefinition } from "../../definition/normalize.js";
import { sequence } from "./sequence.js";
import { toOutputItems, createSyntheticStepResult } from "../values.js";
import { pipelineSourceItems, attachPipelineSourceItems } from "../accounting.js";

export async function childWorkflow(
	machine: Machine,
	step: WorkflowStep,
	key: string,
	scope: Scope,
) {
	const frame = activation(machine.state, key);
	if (!frame.child) {
		const args = resolveWorkflowStepArgs(step.workflow_args, scope.args, scope.results);
		const file = path.resolve(
			path.dirname(scope.definition.filePath),
			resolveTemplate(step.workflow!, scope.args, scope.results),
		);
		if (scope.ancestors.includes(file))
			throw new Error(`Workflow step ${step.id} creates a cycle: ${file}`);
		frame.child =
			scope.definition.children[scope.definitionPath!] ??
			(await snapshotDefinition(file, args, scope.ancestors));
		frame.childArgs = resolveWorkflowArgs(frame.child.workflow.args, args);
		await machine.save();
	}
	const result = await sequence(machine, frame.child.workflow.steps, key + "/child", {
		...scope,
		nested: true,
		accountAs: scope.accountAs ?? scope.costPrefix + step.id,
		definition: frame.child,
		definitionPath: "",
		args: frame.childArgs!,
		results: createRecord(),
		env: { ...scope.env, ...step.env, ...frame.child.workflow.env },
		cwd: step.cwd ?? frame.child.workflow.cwd ?? scope.cwd,
		ancestors: [...scope.ancestors, frame.child.filePath],
		costPrefix: scope.costPrefix + step.id + ".",
	});
	const output = toOutputItems(result);
	const value = output.length === 1 ? output[0] : output;
	const converted = createSyntheticStepResult(step.id, value);
	const items = pipelineSourceItems(result);
	if (items) attachPipelineSourceItems(converted, items);
	return converted;
}
