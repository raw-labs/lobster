import { evaluateCondition } from "../../workflows/conditions.js";
import type { WorkflowStep, WorkflowStepResult } from "../../workflows/types.js";
import { createRecord } from "../../workflows/values.js";
import { markSkipped } from "./switch.js";
import type { Machine, Scope } from "../machine.js";
import type { Activation } from "../state.js";

/** Evaluate once when entering a node, not again after recovering its children. */
export async function enter(machine: Machine, step: WorkflowStep, frame: Activation, scope: Scope) {
	if (frame.phase !== "ready") return;
	if (!evaluateCondition(step.when ?? step.condition, scope.results)) {
		frame.result = { id: step.id, skipped: true };
		// The steps inside it (a switch's cases, a repeat's steps, parallel branches) are known
		// to later steps too, so they count as skipped with it.
		const inside = createRecord<WorkflowStepResult>();
		markSkipped([step], inside);
		delete inside[step.id];
		if (Object.keys(inside).length) frame.results = inside;
		frame.phase = "completed";
	} else frame.phase = "started";
	// Deterministic from saved results; written with the step's first real save.
	return frame.phase === "completed";
}
