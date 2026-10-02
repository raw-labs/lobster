import type { AtomicExclusiveWriteOptions, AtomicWriteOptions } from "../state/atomic_file.js";
import type {
	HistoryPage,
	LogicalArchive,
	RunRecord,
	RunSummary,
	StateRecord,
	Transition,
	TransitionResult,
} from "./model.js";

export type BackendKind = "files" | "sqlite";

export type StateWriteOptions = {
	exclusive?: boolean;
	runId?: string;
	decision?: unknown;
	signal?: AbortSignal;
	atomicWriteOptions?: AtomicWriteOptions & AtomicExclusiveWriteOptions;
};

export type StateWriteResult = {
	revision?: number;
	conflict?: boolean;
	operationId?: string;
	runId?: string;
	signalAbortedAfterCommit?: boolean;
	reconciled?: boolean;
};

export interface Backend {
	readonly kind: BackendKind;

	read(key: string): Promise<StateRecord>;
	listState(prefix: string): Promise<Record<string, string>>;
	write(
		key: string,
		text: string | null,
		options?: StateWriteOptions,
	): Promise<StateWriteResult | undefined>;

	transition(request: Transition, signal?: AbortSignal): Promise<TransitionResult>;
	acknowledge(runId: string, operationId: string): Promise<void>;
	getRun(id: string): Promise<RunRecord | null>;
	listRuns(): Promise<RunRecord[]>;
	listRunSummaries(after: string, limit: number): Promise<RunSummary[]>;
	inspectRun(id: string, afterSequence: number, limit: number): Promise<HistoryPage | null>;
	removeRun(id: string, before: string): Promise<boolean>;

	export(): Promise<LogicalArchive>;
	import(data: LogicalArchive): Promise<void>;
	close(): Promise<void>;
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
	if (error instanceof PersistenceOutcomeUnknown) {
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
