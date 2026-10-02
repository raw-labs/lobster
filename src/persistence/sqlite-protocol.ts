import type { StateWriteResult } from "./backend.js";
import type {
	HistoryPage,
	LogicalArchive,
	RunRecord,
	RunSummary,
	StateRecord,
	Transition,
	TransitionResult,
} from "./model.js";

type WriteRequest = {
	key: string;
	text: string | null;
	operationId: string;
	exclusive?: boolean;
	runId?: string;
	decision?: unknown;
};

type Operations = {
	read: { args: { key: string }; result: StateRecord };
	listState: { args: { prefix: string }; result: Record<string, string> };
	write: { args: WriteRequest; result: StateWriteResult };
	transition: { args: Transition; result: TransitionResult };
	acknowledge: { args: { runId: string; operationId: string }; result: void };
	getRun: { args: { id: string }; result: RunRecord | null };
	listRuns: { args: Record<string, never>; result: RunRecord[] };
	listRunSummaries: { args: { after: string; limit: number }; result: RunSummary[] };
	inspectRun: {
		args: { id: string; afterSequence: number; limit: number };
		result: HistoryPage | null;
	};
	removeRun: { args: { id: string; before: string }; result: boolean };
	export: { args: Record<string, never>; result: LogicalArchive };
	import: { args: LogicalArchive; result: void };
	close: { args: Record<string, never>; result: void };
};

export type WorkerMethod = keyof Operations;

export type WorkerArgs<M extends WorkerMethod> = Operations[M]["args"];

export type WorkerResult<M extends WorkerMethod> = Operations[M]["result"];

export type WorkerRequest = {
	[M in WorkerMethod]: { id: string; method: M; args: WorkerArgs<M> };
}[WorkerMethod];

export type WorkerResponse =
	| { ready: true }
	| { id: string; result: unknown; error?: never }
	| { id: string; error: { message: string; code?: string }; result?: never };
