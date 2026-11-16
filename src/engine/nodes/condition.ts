import { evaluateCondition } from "../../workflows/expressions.js";
import type { WorkflowStep } from "../../workflows/types.js";
import type { Machine, Scope } from "../machine.js";
import type { Activation } from "../state.js";

/** Evaluate once when entering a node, not again after recovering its children. */
export async function enter(machine: Machine, step: WorkflowStep, frame: Activation, scope: Scope) {
	if (frame.phase !== "ready") return;
	if (!evaluateCondition(step.when ?? step.condition, scope.results)) {
		frame.result = { id: step.id, skipped: true };
		frame.phase = "completed";
	} else frame.phase = "started";
	// Deterministic from saved results; written with the step's first real save.
	machine.defer();
	return frame.phase === "completed";
}
