// What kind of step a workflow step is, and the ids of the steps nested in it.
import type { WorkflowInputRequest, WorkflowStep } from "./types.js";

/** What a step does. The loader admits one kind per step (an approval may follow a command). */
export type StepKind =
	| "run"
	| "pipeline"
	| "workflow"
	| "parallel"
	| "for_each"
	| "repeat"
	| "switch"
	| "sleep"
	| "wait"
	| "input"
	| "approval"
	| "step";

export function stepKind(step: WorkflowStep): StepKind {
	if (step.parallel) return "parallel";
	if (step.switch) return "switch";
	if (step.repeat) return "repeat";
	if (step.for_each !== undefined) return "for_each";
	if (nonBlank(step.workflow)) return "workflow";
	if (nonBlank(step.pipeline)) return "pipeline";
	if (shellCommandOf(step)) return "run";
	if (step.sleep !== undefined) return "sleep";
	if (step.wait) return "wait";
	if (isInputStep(step.input)) return "input";
	if (isApprovalStep(step.approval)) return "approval";
	return "step";
}

/** Whether `approval` asks for one: any value but false, empty or missing. */
export function isApprovalStep(approval: WorkflowStep["approval"]) {
	if (approval === true) return true;
	if (typeof approval === "string" && approval.trim().length > 0) return true;
	if (approval && typeof approval === "object" && !Array.isArray(approval)) return true;
	return false;
}

export function isInputStep(input: WorkflowStep["input"]): input is WorkflowInputRequest {
	return Boolean(input && typeof input === "object" && !Array.isArray(input));
}

/** `value` when it is a string with something in it: a blank command is no command. */
export function nonBlank(value: unknown) {
	return typeof value === "string" && value.trim() ? value : undefined;
}

/** A step's shell command: `run`, or `command` when there is no `run`. */
export function shellCommandOf(step: Pick<WorkflowStep, "run" | "command">) {
	return nonBlank(typeof step.run === "string" ? step.run : step.command);
}

/** The command a step runs, if any: a shell command, a pipeline, a child workflow or branches. */
export function getStepExecution(step: WorkflowStep) {
	if (step.parallel && typeof step.parallel === "object" && !Array.isArray(step.parallel)) {
		return { kind: "parallel" as const, value: step.parallel };
	}
	const workflow = nonBlank(step.workflow);
	if (workflow) return { kind: "workflow" as const, value: workflow };
	const pipeline = nonBlank(step.pipeline);
	if (pipeline) return { kind: "pipeline" as const, value: pipeline };
	const shellCommand = shellCommandOf(step);
	if (shellCommand) return { kind: "shell" as const, value: shellCommand };
	return { kind: "none" as const };
}

/**
 * Every step id a list of steps produces: the steps, their parallel branches, and the steps
 * of their switch cases and repeats, nested ones included. A skipped step skips all of them.
 */
export function stepIds(steps: WorkflowStep[]): string[] {
	return steps.flatMap((step) => [
		step.id,
		...(step.parallel?.branches ?? []).map((branch) => branch.id),
		...(step.repeat ? stepIds(step.steps ?? []) : []),
		...(step.switch
			? [
					...step.switch.cases.flatMap((entry) => stepIds(entry.steps)),
					...stepIds(step.switch.else ?? []),
				]
			: []),
	]);
}
