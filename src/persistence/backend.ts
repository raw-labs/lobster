import type {
	HistoryPage,
	LogicalArchive,
	RunRecord,
	RunSummary,
	RunFilter,
	RunState,
	StateRecord,
	Transition,
	TransitionResult,
} from "./model.js";

/**
 * The code of the error an exclusive write gets when its key already holds a value. Errors
 * cross the worker thread as a message and a code, so a code (not a class) says what happened.
 */
export const STATE_EXISTS = "LOBSTER_STATE_EXISTS";

export type BackendKind = "sqlite";

export type StateWriteOptions = {
	exclusive?: boolean;
	expectedWaitRevision?: number;
	/** Write the key as part of this run (the run then owns it). */
	runId?: string;
	signal?: AbortSignal;
	assertInvocationCurrent?: () => void;
};

export type StateWriteCommit =
	| { kind: "state" }
	| { kind: "run"; runId: string; operationId: string; revision: number };

export type StateWriteResult = StateWriteCommit & {
	status: "committed";
	confirmation: "acknowledged" | "reconciled";
	signalAbortedAfterCommit: boolean;
};

/**
 * An external event delivered with `lobster signal`. `owner` is the sender's caller ID ("" for
 * an unscoped, trusted caller); a run sees unscoped events and those from its own caller.
 */
export type InboxEvent = { id: string; name: string; data: unknown; at: number; owner: string };

export interface Backend {
	readonly kind: BackendKind;

	read(key: string): Promise<StateRecord>;
	listState(prefix: string): Promise<Record<string, string>>;
	write(key: string, text: string | null, options?: StateWriteOptions): Promise<StateWriteResult>;

	transition(
		request: Transition,
		signal?: AbortSignal,
		assertInvocationCurrent?: () => void,
	): Promise<TransitionResult>;
	acknowledge(runId: string, operationId: string): Promise<void>;
	getRun(id: string): Promise<RunRecord | null>;
	getRunState(id: string): Promise<RunState | null>;
	listRuns(): Promise<RunRecord[]>;
	listRunSummaries(after: string, limit: number, filter?: RunFilter): Promise<RunSummary[]>;
	inspectRun(id: string, afterSequence: number, limit: number): Promise<HistoryPage | null>;
	removeRun(id: string, before: string): Promise<boolean>;
	/** Remove finished runs last updated before `before`; throttled to once a day. */
	pruneFinished(before: string, now: string): Promise<string[]>;
	/** Runs whose wake time has passed, earliest first. */
	dueRuns(now: number, limit: number, callerId?: string): Promise<string[]>;
	/** Store an external event; an id seen before is ignored. */
	appendEvent(event: InboxEvent): Promise<{ inserted: boolean }>;
	/** Events with this name received at or after `since` that `owner` may see, oldest first. */
	findEvents(name: string, since: number, owner: string): Promise<InboxEvent[]>;
	/** A run's records (see RunRecord.records) whose keys start with `prefix`, by key. */
	readRecords(runId: string, prefix: string): Promise<Array<{ key: string; text: string }>>;

	export(): Promise<LogicalArchive>;
	import(data: LogicalArchive): Promise<void>;
	close(): Promise<void>;
}

export class WaitConflictError extends Error {
	readonly code = "LOBSTER_WAIT_CONFLICT";

	constructor() {
		super("Wait changed; inspect the pending wait before answering again");
		this.name = "WaitConflictError";
	}
}

export class PersistenceError extends Error {
	constructor(
		readonly operationId: string,
		readonly runId: string | undefined,
		cause: unknown,
		message = `Execution history write failed: ${errorMessage(cause)}`,
	) {
		super(message, { cause });
		this.name = "PersistenceError";
	}
}

export class PersistenceOutcomeUnknown extends PersistenceError {
	constructor(operationId: string, cause?: unknown, runId?: string) {
		super(
			operationId,
			runId,
			cause,
			`Persistence acknowledgement lost; reconcile operation ${operationId} before retrying execution`,
		);
		this.name = "PersistenceOutcomeUnknown";
	}
}

type RuntimeErrorDetails = {
	type: string;
	message: string;
	operationId?: string;
	runId?: string;
};

function errorMessage(error: unknown): string {
	if (
		error &&
		typeof error === "object" &&
		"message" in error &&
		typeof error.message === "string"
	) {
		return error.message;
	}
	return String(error);
}

export function runtimeErrorDetails(error: unknown, type = "runtime_error"): RuntimeErrorDetails {
	if (
		error instanceof WaitConflictError ||
		(error as { code?: string })?.code === "LOBSTER_WAIT_CONFLICT"
	) {
		type = "wait_conflict";
	} else if ((error as { code?: string })?.code === "LOBSTER_ACTIVITY_OUTCOME_UNKNOWN") {
		type = "activity_outcome_unknown";
	} else if ((error as { code?: string })?.code === "LOBSTER_EXECUTION_CONFLICT") {
		type = "execution_conflict";
	} else if ((error as { code?: string })?.code === "LOBSTER_UPGRADE_REQUIRED") {
		type = "upgrade_required";
	} else if (error instanceof PersistenceOutcomeUnknown) {
		type = "persistence_outcome_unknown";
	} else if (error instanceof PersistenceError) {
		type = "persistence_error";
	}

	return {
		type,
		message: errorMessage(error),
		...(error instanceof PersistenceError
			? { operationId: error.operationId, ...(error.runId ? { runId: error.runId } : {}) }
			: {}),
	};
}
