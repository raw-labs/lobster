// The saved execution of a run: its definition, its activations, and what each waits for;
// how activation keys are built; and the queries the engine and persistence ask of it
// (persistence stores the checkpoint whole and reads it only through these).
import type { WorkflowApprovalIdentity, WorkflowStepResult } from "../workflows/types.js";
import type { Definition } from "../workflows/definition.js";
import type { CostSummary } from "../llm/cost_tracker.js";
import type { CommandInputState } from "../pipeline/input_request.js";

/** When a question or event wait stops waiting, and what happens then. */
export type WaitDeadline = { deadline?: number; onTimeout?: "cancel" | "continue" };

/** What a waiting activation waits for. */
export type WaitRequest =
	| ({
			kind: "input";
			prompt: string;
			responseSchema: unknown;
			defaults?: unknown;
			subject?: unknown;
	  } & WaitDeadline)
	| ({
			kind: "approval";
			prompt: string;
			items: unknown[];
			preview?: string;
			/** A rejection completes the step (`approved: false`) instead of cancelling the run. */
			onReject?: "continue";
	  } & WorkflowApprovalIdentity &
			WaitDeadline)
	/** A `sleep:` step: due at `until` (epoch ms). */
	| { kind: "timer"; until: number }
	/** A `wait:` step: the oldest matching event received at or after `since`. */
	| ({
			kind: "event";
			event: string;
			match?: Record<string, unknown>;
			since: number;
	  } & WaitDeadline);

/** What an activity runs, resolved once and saved before it is dispatched. */
export type ActivityInput = {
	kind: "shell" | "pipeline" | "none";
	/** The shell command or pipeline text, templates resolved. */
	text?: string;
	/** Its stdin value (a step's `stdin:`), or a standalone pipeline's input items. */
	input: unknown;
	/** Only the workflow's own env values: the host's environment is never saved. */
	env: Record<string, string | undefined>;
	cwd?: string;
};

/**
 * Where a paused pipeline continues. `same_stage`: a command asked mid-run
 * (`ctx.requestInput`), and that stage runs again with its saved `commandInput`.
 * `next_stage`: it halted after a stage to ask (`approve`, `ask --emit`), and the next stage
 * gets `items` after an approval, or `[response]` after an input (the question says which).
 */
export type PipelinePause = {
	/** The text of each stage: the pipeline continues only if it still has these stages. */
	stages: string[];
	resumeAtIndex: number;
	items: unknown[];
} & ({ mode: "same_stage"; commandInput: CommandInputState } | { mode: "next_stage" });

/**
 * One occurrence of a node: a step's attempt, a loop's item, a branch, a wait. Every activation
 * has the common fields; the rest belong to one kind of node each.
 */
export type Activation = {
	phase: "ready" | "started" | "waiting" | "completed" | "failed";
	result?: WorkflowStepResult;
	error?: string;
	/** A command that may not be repeated was dispatched here (or below, once forgotten). */
	nonRetryable?: boolean;
	/** Results of the steps inside this node, readable by the steps after it. */
	results?: Record<string, WorkflowStepResult>;
	/** A step or a parallel with a timeout: when the running attempt times out (epoch ms). */
	timeoutAt?: number;

	/** A step: its attempt (from 1), and when the next one starts if it is waiting to retry. */
	attempt?: number;
	retryAt?: number;
	/**
	 * The attempt its `retry` budget counts from: 1, or the attempt a failed run was retried
	 * at (`runs advance`), which starts a fresh budget. Attempt numbers keep growing, so every
	 * attempt keeps a key of its own.
	 */
	budgetFrom?: number;
	/** A step that runs a command: how many times one was dispatched, over all attempts. */
	dispatches?: number;
	/** A sequence: its next step, and the last step that ran (not skipped, not failed). */
	sequence?: { cursor: number; lastStepId?: string };
	/** A for_each: its item count and next item (items and outputs are kept as records). */
	forEach?: {
		count: number;
		cursor: number;
		/** Items past `cursor` that already finished (with `concurrency`). */
		finished?: number[];
		/** With `pause_ms`: the item it already paused before, and until when it pauses now. */
		pausedBefore?: number;
		pauseUntil?: number;
	};
	/** A repeat: the rounds run so far, and the last step that ran in the last one. */
	repeat?: { rounds: number; lastStepId?: string };
	/** A switch: the case it chose, from 1, or "else". */
	switch?: { case: number | "else" };
	/** A parallel with `wait: any`: the branch that answered first. */
	parallel?: { winner: string };
	/** A child workflow: its definition and arguments, frozen on entry. */
	child?: {
		definition: Definition;
		args: Record<string, unknown>;
		env: Record<string, string | undefined>;
		costStart: number;
	};
	/**
	 * An activity: what it runs, and its dispatch number. A pipeline that paused to ask keeps
	 * the question, where it continues, and the answer once given.
	 */
	activity?: {
		input: ActivityInput;
		dispatch: number;
		question?: Extract<WaitRequest, { kind: "approval" | "input" }>;
		pause?: PipelinePause;
		response?: unknown;
	};
	/** A wait: what it waits for, the step it belongs to, and its key once it is published. */
	wait?: { request: WaitRequest; stepId: string; key?: string };
};

/** Every status a run can have. `blocked`: interrupted while a step was running; its outcome is unknown. */
const RUN_STATUSES = ["running", "waiting", "blocked", "completed", "failed", "cancelled"] as const;
export type RunStatus = (typeof RUN_STATUSES)[number];

/** Whether `value` is a run status. */
export function isRunStatus(value: unknown): value is RunStatus {
	return (RUN_STATUSES as readonly unknown[]).includes(value);
}

export type ExecutionState = {
	version: 1;
	revision: number;
	definition: Definition;
	args: Record<string, unknown>;
	cwd?: string;
	activations: Record<string, Activation>;
	status: RunStatus;
	/** When the run started (epoch ms): event waits match events from then on. */
	startedAt?: number;
	/** Inbox events this run has consumed; each is consumed at most once. */
	events?: string[];
	output?: unknown[];
	error?: string;
	cost?: CostSummary;
};

/** The activation at `key`, created ready on first use. */
export function activation(state: ExecutionState, key: string): Activation {
	return (state.activations[key] ??= { phase: "ready" });
}

/**
 * Activation keys are paths: a step under its parent, its attempts under the step, and what
 * an attempt runs or asks under the attempt.
 */
export const keys = {
	/** `id`'s activation under `parent` (a step in a sequence, a branch, an item). */
	occurrence: (parent: string, id: string) => parent + "/" + encodeURIComponent(id),
	attempt: (step: string, attempt: number) => `${step}/attempt-${attempt}`,
	/** A key as people read it: the step ids, decoded, without the root (`deploy/attempt-2`). */
	describe: (key: string) => key.split("/").map(decodeURIComponent).slice(1).join("/"),
	/** The step an attempt belongs to. */
	stepOf: (attempt: string) => attempt.slice(0, attempt.lastIndexOf("/attempt-")),
	/**
	 * A parallel step's branch, under the attempt's `branches`: a branch id is the author's, and
	 * directly under the attempt one named `approval` or `activity` would take that key.
	 */
	branch: (attempt: string, id: string) => keys.occurrence(attempt + "/branches", id),
	activity: (attempt: string) => attempt + "/activity",
	approval: (attempt: string) => attempt + "/approval",
	// A pipeline's questions sit under the attempt's activity: when a failed run is retried and
	// the activity is cleared to run again, their answers go with it, and are asked again.
	/** A question a pipeline asked after the stage at `index` (`approve`, `ask --emit`). */
	pipelineGate: (attempt: string, index: number) => `${attempt}/activity/pipeline-gate-${index}`,
	/** The `request`th question a command asked mid-run in the stage at `index`. */
	pipelineInput: (attempt: string, index: number, request: number) =>
		`${attempt}/activity/pipeline-input-${index}-${request}`,
};

/**
 * Give the steps enclosing `key` a fresh time budget: their running attempts' deadlines are
 * dropped, and their clocks start again when they next run. A step's timeout covers its work,
 * not a pause for a person, a time or an event, nor the time a failed run waits to be retried.
 */
export function clearTimeoutsAbove(state: ExecutionState, key: string) {
	for (const [ancestor, frame] of Object.entries(state.activations))
		if (key.startsWith(ancestor + "/")) delete frame.timeoutAt;
}

/** The activations below `key` (and `key`'s own, when `inclusive`), with their paths. */
export function activationsBelow(
	state: ExecutionState,
	key: string,
	inclusive = false,
): Array<[string, Activation]> {
	return Object.entries(state.activations).filter(
		([path]) => path.startsWith(key + "/") || (inclusive && path === key),
	);
}

/** Whether the run (its checkpoint, or its record) has finished: completed, failed or cancelled. */
export function isFinished(run: { status: RunStatus }): boolean {
	return run.status === "completed" || run.status === "failed" || run.status === "cancelled";
}

/** When a wait is due: a timer's time, or a question's or event wait's deadline. */
function waitDueAt(wait: WaitRequest): number | undefined {
	return wait.kind === "timer" ? wait.until : wait.deadline;
}

/** When a question or event wait stops waiting, if it has a deadline. */
export function deadlineOf(wait: WaitRequest | undefined): number | undefined {
	return wait && wait.kind !== "timer" ? wait.deadline : undefined;
}

/** Whether `frame` waits under a published key (one withdrawn or not yet published does not). */
export function isPending(frame: Activation): boolean {
	return frame.phase === "waiting" && Boolean(frame.wait?.key);
}

/** The earliest time a pending wait is due, if any is. */
export function earliestWaitDue(execution: ExecutionState): number | undefined {
	let at: number | undefined;
	for (const frame of Object.values(execution.activations)) {
		if (!isPending(frame)) continue;
		const due = waitDueAt(frame.wait!.request);
		if (due !== undefined && (at === undefined || due < at)) at = due;
	}
	return at;
}

/** Activities that started and saved no result: their commands' outcomes are unknown. */
export function unresolvedActivities(execution: ExecutionState): string[] {
	return Object.entries(execution.activations)
		.filter(([, frame]) => frame.activity && frame.phase === "started")
		.map(([key]) => key);
}

/** The earliest time a step waiting to retry starts again, if any is. */
export function nextRetryAt(execution: ExecutionState): number | undefined {
	const times = Object.values(execution.activations).flatMap((frame) =>
		frame.retryAt === undefined ? [] : [frame.retryAt],
	);
	return times.length ? Math.min(...times) : undefined;
}

/**
 * What a published wait's key holds: the run and the activation that waits, and what it waits
 * for. A resume token names the key; answering it resumes that activation.
 */
export type Continuation = {
	machineVersion: 1;
	runId: string;
	activation: string;
	wait: WaitRequest;
};

export function continuationText(runId: string, activation: string, wait: WaitRequest): string {
	return JSON.stringify({ machineVersion: 1, runId, activation, wait } satisfies Continuation);
}

/** `value` as a continuation, or undefined when it is not one (a pause an older version saved). */
export function asContinuation(value: unknown): Continuation | undefined {
	const item = value as Partial<Continuation> | null;
	return item?.machineVersion === 1 && typeof item.runId === "string" && item.wait
		? (item as Continuation)
		: undefined;
}

/** Whether a waiting activation of `execution` waits for the event `name` with `data`. */
export function waitsForEvent(execution: ExecutionState, name: string, data: unknown): boolean {
	return Object.values(execution.activations).some(
		(frame) =>
			isPending(frame) &&
			frame.wait?.request.kind === "event" &&
			frame.wait.request.event === name &&
			matchesEvent(frame.wait.request.match, data),
	);
}

/** Every field of `expected` is in `actual`; nested objects match as subsets. */
export function matchesEvent(expected: unknown, actual: unknown): boolean {
	if (expected === undefined) return true;
	if (expected && typeof expected === "object" && !Array.isArray(expected)) {
		if (!actual || typeof actual !== "object" || Array.isArray(actual)) return false;
		return Object.entries(expected).every(([key, value]) =>
			matchesEvent(value, (actual as Record<string, unknown>)[key]),
		);
	}
	return JSON.stringify(expected) === JSON.stringify(actual);
}
