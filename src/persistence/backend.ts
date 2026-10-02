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
	listRuns(): Promise<RunRecord[]>;
	listRunSummaries(after: string, limit: number): Promise<any[]>;
	inspectRun(id: string, afterSequence: number, limit: number): Promise<any>;
	export(): Promise<LogicalArchive>;
	import(data: LogicalArchive): Promise<void>;
	removeRun(id: string, before: string): Promise<boolean>;
	close(): Promise<void>;
}
export class PersistenceOutcomeUnknown extends Error {
	constructor(
		readonly operationId: string,
		cause?: unknown,
	) {
		super(
			`Persistence acknowledgement lost; reconcile operation ${operationId} before retrying execution`,
			{ cause },
		);
		this.name = "PersistenceOutcomeUnknown";
	}
}
