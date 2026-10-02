import type {
	LogicalArchive,
	RunRecord,
	StateRecord,
	Transition,
	TransitionResult,
} from "./model.js";
export interface Backend {
	readonly kind: "files" | "sqlite";
	read(key: string): Promise<StateRecord>;
	listState(prefix: string): Promise<Record<string, string>>;
	write(
		key: string,
		text: string | null,
		options?: {
			exclusive?: boolean;
			runId?: string;
			signal?: AbortSignal;
			atomicWriteOptions?: any;
		},
	): Promise<any>;
	transition(request: Transition, signal?: AbortSignal): Promise<TransitionResult>;
	getRun(id: string): Promise<RunRecord | null>;
	acknowledge(runId: string, operationId: string): Promise<void>;
	listRuns(): Promise<RunRecord[]>;
	listRunSummaries(after: string, limit: number): Promise<any[]>;
	inspectRun(id: string, afterSequence: number, limit: number): Promise<any>;
	export(): Promise<LogicalArchive>;
	import(data: LogicalArchive): Promise<void>;
	removeRun(id: string, before: string): Promise<boolean>;
	close(): Promise<void>;
}
export class PersistenceError extends Error {
	constructor(
		readonly operationId: string,
		readonly runId: string | undefined,
		cause: unknown,
		message = `Execution history write failed: ${(cause as Error)?.message ?? String(cause)}`,
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

export function runtimeErrorDetails(error: unknown, type = "runtime_error") {
	return {
		type:
			error instanceof PersistenceOutcomeUnknown
				? "persistence_outcome_unknown"
				: error instanceof PersistenceError
					? "persistence_error"
					: type,
		message: (error as Error)?.message ?? String(error),
		...(error instanceof PersistenceError
			? { operationId: error.operationId, ...(error.runId ? { runId: error.runId } : {}) }
			: {}),
	};
}
