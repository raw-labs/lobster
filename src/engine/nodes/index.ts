// Which node runs each kind of step, and what the machine needs to know about it.
import type { WorkflowStep, WorkflowStepResult } from "../../workflows/types.js";
import { stepKind, type StepKind } from "../../workflows/steps.js";
import type { Machine } from "../machine.js";
import type { Scope } from "../scope.js";
import { activityStep } from "./activity.js";
import { childWorkflowStep } from "./child_workflow.js";
import { forEachStep } from "./for_each.js";
import { inputStep } from "./input.js";
import { parallelStep } from "./parallel.js";
import { repeatStep } from "./repeat.js";
import { sleepStep } from "./sleep.js";
import { switchStep } from "./switch.js";
import { waitStep } from "./wait.js";

export type NodeHandler = {
	/** Run one attempt of `step`; `key` is the attempt's activation. */
	run(machine: Machine, step: WorkflowStep, key: string, scope: Scope): Promise<WorkflowStepResult>;
	/**
	 * `activity`: runs a command; its timeout starts when the command does.
	 * `control`: runs other steps; its timeout covers all of them, from entry.
	 * `gate`: waits for a person, a time or an event; it has no timeout, and is refused where
	 * steps cannot pause.
	 */
	role: "activity" | "control" | "gate";
	/** Its inner steps are charged as they run, so the step is not charged again. */
	chargesInner?: boolean;
};

const activity: NodeHandler = { run: activityStep, role: "activity" };

const NODES: Record<StepKind, NodeHandler> = {
	run: activity,
	pipeline: activity,
	approval: activity,
	step: activity,
	workflow: { run: childWorkflowStep, role: "control", chargesInner: true },
	parallel: { run: parallelStep, role: "control" },
	for_each: { run: forEachStep, role: "control", chargesInner: true },
	repeat: { run: repeatStep, role: "control", chargesInner: true },
	switch: { run: switchStep, role: "control", chargesInner: true },
	input: { run: inputStep, role: "gate" },
	sleep: { run: sleepStep, role: "gate" },
	wait: { run: waitStep, role: "gate" },
};

/** The node that runs `step`. */
export function nodeFor(step: WorkflowStep): NodeHandler {
	return NODES[stepKind(step)];
}
