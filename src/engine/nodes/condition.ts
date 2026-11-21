// A step's `when:`/`condition:`, decided once when the step is entered.
import { evaluateCondition } from "../../workflows/conditions.js";
import type { WorkflowStep } from "../../workflows/types.js";
import type { Activation } from "../checkpoint.js";
import type { Scope } from "../scope.js";
import { skippedInside } from "../results.js";

/**
 * Decide a step's condition when it is entered, from saved results; not again after its
 * children recover. True when the step is skipped (its frame then holds its result).
 */
export function skippedByCondition(step: WorkflowStep, frame: Activation, scope: Scope): boolean {
	if (frame.phase !== "ready") return false;
	if (evaluateCondition(step.when ?? step.condition, scope.results)) {
		frame.phase = "started";
		return false;
	}
	frame.result = { id: step.id, skipped: true };
	const inside = skippedInside(step);
	if (inside) frame.results = inside;
	// Deterministic from saved results; written with the step's first real save.
	frame.phase = "completed";
	return true;
}
