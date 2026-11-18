// A step's `when:`/`condition:`, decided once when the step is entered.
import { evaluateCondition } from "../../workflows/conditions.js";
import type { WorkflowStep, WorkflowStepResult } from "../../workflows/types.js";
import { createRecord } from "../../workflows/values.js";
import type { Activation } from "../checkpoint.js";
import type { Scope } from "../scope.js";
import { markSkipped } from "../results.js";

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
	// The steps inside it (a switch's cases, a repeat's steps, parallel branches) are known
	// to later steps too, so they count as skipped with it.
	const inside = createRecord<WorkflowStepResult>();
	markSkipped([step], inside);
	delete inside[step.id];
	if (Object.keys(inside).length) frame.results = inside;
	// Deterministic from saved results; written with the step's first real save.
	frame.phase = "completed";
	return true;
}
