// What a node runs in: the workflow, the results it can read, its environment and cost
// limits, and whether the steps inside it may pause.
import type { WorkflowStep, WorkflowStepResult } from "../workflows/types.js";
import type { Definition } from "../workflows/definition.js";
import { resolveRetryConfig } from "../workflows/retry.js";
import type { Activation } from "./checkpoint.js";
import type { RunContext } from "./context.js";
import type { HoldClocks } from "./step_clock.js";

export type Scope = {
	definition: Definition;
	/** The step's place in the definition (`/a/b`), to find a child workflow frozen for it. */
	definitionPath?: string;
	args: Record<string, unknown>;
	/** Results the steps here can read: everything before them, in this workflow. */
	results: Record<string, WorkflowStepResult>;
	/** The last step before this one that ran (an input step's default subject). */
	lastStepId?: string;
	/** A child workflow's environment, resolved when it was entered. */
	baseEnv?: Record<string, string | undefined>;
	env?: Record<string, string>;
	cwd?: string;
	/** The workflow files above this one, to refuse a child workflow that calls itself. */
	ancestors: string[];
	ctx: RunContext;
	/**
	 * The child workflows this scope is inside, outermost first: each call made here is added to
	 * what each of them spent, and checked against each one's `cost_limit`.
	 */
	children?: Array<NonNullable<Activation["child"]>>;
	/** Prefix of the names steps' model calls are counted under (`loop.` inside a loop). */
	costPrefix: string;
	/** Count every step's model calls here under this name instead (a child workflow's steps). */
	accountAs?: string;
	/** Why steps here cannot pause, if they cannot (see GATES_REFUSED). */
	gatesRefused?: string;
	/** Aborts when the enclosing step times out. */
	timeoutSignal?: AbortSignal;
	/** Start the enclosing step's timeout; activities call it right before dispatch. */
	startTimeout?: () => void;
	/** Hold the clocks of the enclosing steps during a deliberate wait (see deliberateWait). */
	holdClocks?: HoldClocks;
};

/** The env and cwd of a compound step's inner steps: the step's own settings over the scope's. */
export function innerSettings(step: WorkflowStep, scope: Scope): Pick<Scope, "env" | "cwd"> {
	return { env: { ...scope.env, ...step.env }, cwd: step.cwd ?? scope.cwd };
}

const GATES = "approvals, input, sleeps and waits are not supported";

/** Why the steps inside a node cannot pause for a person, a time or an event. */
export const GATES_REFUSED = {
	child: `${GATES} inside a child workflow`,
	parallel: `${GATES} inside a parallel branch`,
	concurrent: `${GATES} inside a for_each with concurrency: several items would wait at once`,
	retried: `${GATES} inside a for_each, repeat or switch with retry: a retry would run it again and ask again`,
};

/**
 * Why the steps inside a loop or a switch cannot pause: the enclosing scope's reason, or a
 * retry of the step, which would run them again and ask again.
 */
export function bodyGatesRefused(step: WorkflowStep, scope: Scope): string | undefined {
	if (scope.gatesRefused) return scope.gatesRefused;
	return resolveRetryConfig(step.retry).max > 1 ? GATES_REFUSED.retried : undefined;
}
