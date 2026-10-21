import { randomUUID } from "node:crypto";
import { Worker } from "node:worker_threads";
import {
	PersistenceOutcomeUnknown,
	type Backend,
	type StateWriteOptions,
	type StateWriteResult,
} from "./backend.js";
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
import type { WorkerArgs, WorkerMethod, WorkerResponse, WorkerResult } from "./sqlite-protocol.js";

type PendingRequest = {
	resolve: (value: unknown) => void;
	reject: (error: unknown) => void;
	mutation: boolean;
	operationId: string;
	runId?: string;
};

export class SqliteBackend implements Backend {
	readonly kind = "sqlite" as const;
	private worker: Worker;
	private pending = new Map<string, PendingRequest>();
	private ready: Promise<void>;
	private failed?: Error;
	private closing = false;
	private recovering?: Promise<void>;

	constructor(
		readonly databasePath: string,
		readonly readOnly = false,
		readonly maxPending = 256,
		startupSignal?: AbortSignal,
	) {
		if (!Number.isInteger(maxPending) || maxPending < 1 || maxPending > 4096) {
			throw new Error("Persistence admission limit must be 1..4096");
		}

		this.connect();

		if (startupSignal) {
			const abort = () => {
				void this.worker.terminate();
			};
			startupSignal.addEventListener("abort", abort, { once: true });
			void this.ready
				.finally(() => startupSignal.removeEventListener("abort", abort))
				.catch(() => {});

			if (startupSignal.aborted) {
				abort();
			}
		}
	}

	private connect(): void {
		this.worker = new Worker(new URL("./sqlite-worker.js", import.meta.url), {
			workerData: { path: this.databasePath, readOnly: this.readOnly },
			// This compiled worker needs no host loaders or process-only Node flags.
			execArgv: [],
		});
		this.ready = new Promise((resolve, reject) => {
			this.worker.once("error", reject);
			this.worker.once("exit", (code) => {
				reject(new Error(`Persistence worker exited before ready (${code})`));
			});
			const onMessage = (message: WorkerResponse) => {
				if ("ready" in message) {
					this.worker.off("message", onMessage);
					this.worker.unref();
					resolve();
				}
			};
			this.worker.on("message", onMessage);
		});

		this.worker.on("message", (message: WorkerResponse) => {
			if ("ready" in message) {
				return;
			}

			const request = this.pending.get(message.id);
			if (!request) {
				return;
			}

			this.pending.delete(message.id);
			if (message.error) {
				request.reject(
					Object.assign(new Error(message.error.message), { code: message.error.code }),
				);
			} else {
				request.resolve(message.result);
			}

			if (!this.pending.size) {
				this.worker.unref();
			}
		});

		const fail = (error: Error) => {
			this.failed = error;
			for (const request of this.pending.values()) {
				const failure = request.mutation
					? new PersistenceOutcomeUnknown(request.operationId, error, request.runId)
					: error;
				request.reject(failure);
			}
			this.pending.clear();
		};
		this.worker.on("error", fail);
		this.worker.on("exit", (code) => {
			if (!this.closing || code !== 0) {
				fail(new Error(`Persistence worker exited (${code})`));
			}
		});
	}

	async request<M extends WorkerMethod>(
		method: M,
		args: WorkerArgs<M>,
		signal?: AbortSignal,
		assertInvocationCurrent?: () => void,
	): Promise<WorkerResult<M>> {
		signal?.throwIfAborted();
		if (this.closing) {
			throw new Error("Persistence session is closed");
		}

		if (this.failed) {
			this.recovering ??= (async () => {
				await this.worker.terminate();
				this.failed = undefined;
				this.connect();
				await this.ready;
			})();
			try {
				await this.recovering;
			} finally {
				this.recovering = undefined;
			}
		}

		await this.ready;
		signal?.throwIfAborted();
		if (this.failed) {
			return this.request(method, args, signal, assertInvocationCurrent);
		}
		if (method !== "close" && this.pending.size >= this.maxPending) {
			throw new Error("Persistence admission limit reached");
		}

		assertInvocationCurrent?.();
		const id = randomUUID();
		const operationId =
			"operationId" in args && typeof args.operationId === "string" ? args.operationId : id;
		const runId = "runId" in args && typeof args.runId === "string" ? args.runId : undefined;
		const result = new Promise<unknown>((resolve, reject) => {
			this.pending.set(id, {
				resolve,
				reject,
				mutation: ["write", "transition", "import", "removeRun"].includes(method),
				operationId,
				runId,
			});
		});
		this.worker.ref();

		try {
			this.worker.postMessage({
				id,
				method,
				args: { ...args, ...(method === "write" ? { operationId } : {}) },
			});
		} catch (error) {
			this.pending.delete(id);
			if (!this.pending.size) {
				this.worker.unref();
			}
			throw error;
		}

		// The worker protocol pairs each method with its result type. Once admitted,
		// await the acknowledgement: cancellation is not proof of rollback.
		return result as Promise<WorkerResult<M>>;
	}

	listState(prefix: string): Promise<Record<string, string>> {
		return this.request("listState", { prefix });
	}

	read(key: string): Promise<StateRecord> {
		return this.request("read", { key });
	}

	async write(
		key: string,
		text: string | null,
		options: StateWriteOptions = {},
	): Promise<StateWriteResult> {
		const operationId = randomUUID();
		const result = await this.request(
			"write",
			{
				key,
				text,
				operationId,
				exclusive: options.exclusive,
				expectedWaitRevision: options.expectedWaitRevision,
				runId: options.runId,
				decision: options.decision,
			},
			options.signal,
			options.assertInvocationCurrent,
		);
		return {
			...result,
			status: "committed",
			confirmation: "acknowledged",
			signalAbortedAfterCommit: options.signal?.aborted === true,
		};
	}

	transition(
		request: Transition,
		signal?: AbortSignal,
		assertInvocationCurrent?: () => void,
	): Promise<TransitionResult> {
		return this.request("transition", request, signal, assertInvocationCurrent);
	}

	acknowledge(runId: string, operationId: string): Promise<void> {
		return this.request("acknowledge", { runId, operationId });
	}

	getRun(id: string): Promise<RunRecord | null> {
		return this.request("getRun", { id });
	}

	getRunState(id: string): Promise<RunState | null> {
		return this.request("getRunState", { id });
	}

	listRunSummaries(after: string, limit: number, filter?: RunFilter): Promise<RunSummary[]> {
		return this.request("listRunSummaries", { after, limit, filter });
	}

	inspectRun(id: string, afterSequence: number, limit: number): Promise<HistoryPage | null> {
		return this.request("inspectRun", { id, afterSequence, limit });
	}

	listRuns(): Promise<RunRecord[]> {
		return this.request("listRuns", {});
	}

	export(): Promise<LogicalArchive> {
		return this.request("export", {});
	}

	import(data: LogicalArchive): Promise<void> {
		return this.request("import", data);
	}

	removeRun(id: string, before: string): Promise<boolean> {
		return this.request("removeRun", { id, before });
	}

	async close(): Promise<void> {
		if (this.closing) {
			return;
		}
		if (this.failed) {
			this.closing = true;
			await this.worker.terminate();
			return;
		}

		const closed = this.request("close", {});
		this.closing = true;
		await closed;
	}
}
