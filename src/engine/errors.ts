// The errors the engine throws. Those that extend RunStopError end a step at once: they are
// never retried as a step failure and never turned into a result by `on_error`.
import { PersistenceError, WaitConflictError } from "../persistence/backend.js";
import { RequestInputResumeError } from "../pipeline/input_request.js";
import { CostLimitExceededError } from "../llm/cost_tracker.js";
import { messageOf } from "../util/errors.js";

/** An error that stops the run where it is, instead of failing one step. */
export class RunStopError extends Error {}

/** The run paused for a person, a time or an event (control flow, not a failure). */
export class RunSuspendedError extends RunStopError {
	constructor(
		/** The activation that waits. */
		readonly key: string,
		/** False when the question was already asked by an earlier invocation. */
		readonly fresh = true,
	) {
		super("Workflow suspended");
		this.name = "RunSuspendedError";
	}
}

/** A rejection or a passed deadline cancelled the run; already saved. */
export class RunCancelledError extends RunStopError {
	readonly code = "LOBSTER_RUN_CANCELLED";
	constructor(message: string) {
		super(message);
		this.name = "RunCancelledError";
	}
}

/** A command was dispatched and no result was saved: its outcome is unknown. */
export class InterruptedActivityError extends RunStopError {
	readonly code = "LOBSTER_ACTIVITY_OUTCOME_UNKNOWN";
	constructor(readonly path: string) {
		super(
			`Activity ${path} was interrupted without a saved result; inspect its outcome before continuing`,
		);
		this.name = "InterruptedActivityError";
	}
}

/** The host retired this invocation (`ctx.assertInvocationCurrent` threw). */
export class InvocationRetiredError extends RunStopError {
	constructor(cause: unknown) {
		super(messageOf(cause), { cause });
		this.name = "InvocationRetiredError";
	}
}

/** Another executor owns the run, or its saved state changed under this one. */
export class ExecutionConflictError extends RunStopError {
	readonly code = "LOBSTER_EXECUTION_CONFLICT";
	constructor() {
		super("Workflow is owned by another executor or its saved state changed");
		this.name = "ExecutionConflictError";
	}
}

/** A pipeline stopped before it finished, where it cannot pause. */
export class PipelineHaltError extends RunStopError {
	constructor(message: string) {
		super(message);
		this.name = "PipelineHaltError";
	}
}

/** The answer does not fit the question (wrong kind, invalid response, wrong approver). */
export class InvalidAnswerError extends RunStopError {
	constructor(message: string) {
		super(message);
		this.name = "InvalidAnswerError";
	}
}

/**
 * The reference cannot be resumed here: already answered or cancelled, unknown, or a run
 * this caller cannot continue. Nothing ran.
 */
export class ResumeRefusedError extends RunStopError {
	constructor(message = "Resume state not found") {
		super(message);
		this.name = "ResumeRefusedError";
	}
}

/** A pause saved by an older Lobster. `lobster upgrade` converts it in place. */
export class UpgradeRequiredError extends Error {
	readonly code = "LOBSTER_UPGRADE_REQUIRED";
	constructor() {
		super(
			"This was paused by an older version of Lobster. Run `lobster upgrade`, then resume it again with the same token or approval ID.",
		);
		this.name = "UpgradeRequiredError";
	}
}

/**
 * Whether `error` means this invocation no longer owns the run (the host retired it, another
 * process took it, or storage failed): nothing more may be saved for it.
 */
export function lostRun(error: unknown): boolean {
	return (
		error instanceof InvocationRetiredError ||
		error instanceof PersistenceError ||
		error instanceof ExecutionConflictError
	);
}

/**
 * Whether `error` stops the run instead of failing a step: the engine's own stops, a storage
 * failure, a changed wait, a pipeline that cannot resume, or a spent cost limit.
 */
export function stopsRun(error: unknown): boolean {
	return (
		error instanceof RunStopError ||
		error instanceof PersistenceError ||
		error instanceof WaitConflictError ||
		error instanceof RequestInputResumeError ||
		error instanceof CostLimitExceededError
	);
}
