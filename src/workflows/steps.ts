import type { WorkflowStep } from "./types.js";

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
	if (typeof step.workflow === "string" && step.workflow.trim()) return "workflow";
	if (typeof step.pipeline === "string" && step.pipeline.trim()) return "pipeline";
	if (typeof step.run === "string" || typeof step.command === "string") return "run";
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

export function isInputStep(input: WorkflowStep["input"]) {
	return Boolean(input && typeof input === "object" && !Array.isArray(input));
}

/** The command a step runs, if any: a shell command, a pipeline, a child workflow or branches. */
export function getStepExecution(step: WorkflowStep) {
	if (step.parallel && typeof step.parallel === "object" && !Array.isArray(step.parallel)) {
		return { kind: "parallel" as const, value: step.parallel };
	}
	if (typeof step.workflow === "string" && step.workflow.trim()) {
		return { kind: "workflow" as const, value: step.workflow };
	}
	if (typeof step.pipeline === "string" && step.pipeline.trim()) {
		return { kind: "pipeline" as const, value: step.pipeline };
	}
	const shellCommand = typeof step.run === "string" ? step.run : step.command;
	if (typeof shellCommand === "string" && shellCommand.trim()) {
		return { kind: "shell" as const, value: shellCommand };
	}
	return { kind: "none" as const };
}
