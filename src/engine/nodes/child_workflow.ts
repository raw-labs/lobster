// A `workflow:` step: another workflow file, run inside this run.
import path from "node:path";
import type { WorkflowStep } from "../../workflows/types.js";
import { resolveTemplate } from "../../workflows/expressions.js";
import {
	resolveWorkflowStepArgs,
	createRecord,
	mergeEnv,
	resolveCwd,
} from "../../workflows/values.js";
import { resolveWorkflowArgs } from "../../workflows/args.js";
import { snapshotDefinition } from "../../workflows/definition.js";
import { activation } from "../checkpoint.js";
import type { Machine } from "../machine.js";
import { GATES_REFUSED, type Scope } from "../scope.js";
import { carrySourceItems, createSyntheticStepResult, toOutputItems } from "../results.js";
import { sequence } from "./sequence.js";

/**
 * The child's definition, arguments and environment are frozen on entry and saved, so a
 * recovered run continues the same child. Its steps are charged to this step, under this
 * workflow's cost limits as well as its own, and they cannot pause.
 */
export async function childWorkflowStep(
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
		const definition =
			scope.definition.children[scope.definitionPath!] ??
			(await snapshotDefinition(file, args, scope.ancestors));
		frame.child = {
			definition,
			args: resolveWorkflowArgs(definition.workflow.args, args),
			env: mergeEnv(scope.baseEnv ?? {}, scope.env, step.env, scope.args, scope.results),
			costStart: machine.cost.getSummary().estimatedCostUsd,
		};
		await machine.save();
	}
	const child = frame.child;
	const ownLimit = scope.definition.workflow.cost_limit;
	const result = await sequence(machine, child.definition.workflow.steps, key + "/child", {
		...scope,
		gatesRefused: GATES_REFUSED.child,
		accountAs: scope.accountAs ?? scope.costPrefix + step.id,
		definition: child.definition,
		definitionPath: "",
		args: child.args,
		results: createRecord(),
		baseEnv: child.env,
		costStart: child.costStart,
		env: child.definition.workflow.env,
		parentLimits: [
			...(scope.parentLimits ?? []),
			...(ownLimit ? [{ limit: ownLimit, start: scope.costStart ?? 0 }] : []),
		],
		cwd:
			step.cwd !== undefined
				? resolveCwd(step.cwd, scope.args)
				: (child.definition.workflow.cwd ?? resolveCwd(scope.cwd, scope.args)),
		ancestors: [...scope.ancestors, child.definition.filePath],
		costPrefix: scope.costPrefix + step.id + ".",
	});
	const output = toOutputItems(result);
	const value = output.length === 1 ? output[0] : output;
	return carrySourceItems(createSyntheticStepResult(step.id, value), result);
}
