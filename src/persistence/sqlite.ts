import { Worker } from "node:worker_threads";
import { randomUUID } from "node:crypto";
import type { Backend } from "./backend.js";
import { PersistenceOutcomeUnknown } from "./backend.js";
import type { LogicalArchive, RunRecord, Transition, TransitionResult } from "./model.js";

export class SqliteBackend implements Backend {
	readonly kind = "sqlite" as const;
	private worker: Worker;
	private pending = new Map<
		string,
		{
			resolve: (value: any) => void;
			reject: (error: any) => void;
			mutation: boolean;
			operationId: string;
		}
	>();
	private ready: Promise<void>;
	private failed?: Error;
	private closing = false;
	private recovering?: Promise<void>;
	constructor(
		readonly databasePath: string,
		readonly readOnly = false,
		readonly maxPending = 256,
	) {
		this.connect();
	}
	private connect() {
		const readOnly = this.readOnly;
		this.worker = new Worker(new URL("./sqlite-worker.js", import.meta.url), {
			workerData: { path: this.databasePath, readOnly },
			execArgv: process.execArgv.filter((arg) => !arg.startsWith("--input-type")),
		});
		this.ready = new Promise((resolve, reject) => {
			this.worker.once("error", reject);
			this.worker.once("exit", (code) =>
				reject(new Error(`Persistence worker exited before ready (${code})`)),
			);
			const onMessage = (message: any) => {
				if (message.ready) {
					this.worker.off("message", onMessage);
					this.worker.unref();
					resolve();
				}
			};
			this.worker.on("message", onMessage);
		});
		this.worker.on("message", (message) => {
			const request = this.pending.get(message.id);
			if (!request) return;
			this.pending.delete(message.id);
			if (message.error)
				request.reject(
					Object.assign(new Error(message.error.message), { code: message.error.code }),
				);
			else request.resolve(message.result);
			if (!this.pending.size) this.worker.unref();
		});
		const fail = (error: Error) => {
			this.failed = error;
			for (const request of this.pending.values())
				request.reject(
					request.mutation ? new PersistenceOutcomeUnknown(request.operationId, error) : error,
				);
			this.pending.clear();
		};
		this.worker.on("error", fail);
		this.worker.on("exit", (code) => {
			if (!this.closing || code !== 0) fail(new Error(`Persistence worker exited (${code})`));
		});
	}
	async request(method: string, args: any = {}, signal?: AbortSignal): Promise<any> {
		signal?.throwIfAborted();
		if (this.closing) throw new Error("Persistence session is closed");
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
		if (this.failed) return this.request(method, args, signal);
		if (method !== "close" && this.pending.size >= this.maxPending)
			throw new Error("Persistence admission limit reached");
		const id = randomUUID();
		const operationId = args.operationId ?? id;
		const result = new Promise((resolve, reject) => {
			this.pending.set(id, {
				resolve,
				reject,
				mutation: ["write", "transition", "import", "removeRun"].includes(method),
				operationId,
			});
		});
		this.worker.ref();
		this.worker.postMessage({
			id,
			method,
			args: { ...args, ...(method === "write" ? { operationId } : {}) },
		});
		// Once admitted, await the acknowledgement. Cancellation is not proof of rollback.
		return result;
	}
	listState(prefix: string) {
		return this.request("listState", { prefix });
	}
	read(key: string) {
		return this.request("read", { key });
	}
	async write(
		key: string,
		text: string | null,
		options: { exclusive?: boolean; runId?: string; decision?: unknown; signal?: AbortSignal } = {},
	) {
		const result = await this.request(
			"write",
			{ key, text, exclusive: options.exclusive, runId: options.runId, decision: options.decision },
			options.signal,
		);
		return { ...result, signalAbortedAfterCommit: options.signal?.aborted === true };
	}
	transition(request: Transition, signal?: AbortSignal): Promise<TransitionResult> {
		return this.request("transition", request, signal);
	}
	getRun(id: string): Promise<RunRecord | null> {
		return this.request("getRun", { id });
	}
	listRunSummaries(after: string, limit: number) {
		return this.request("listRunSummaries", { after, limit });
	}
	inspectRun(id: string, afterSequence: number, limit: number) {
		return this.request("inspectRun", { id, afterSequence, limit });
	}
	listRuns(): Promise<RunRecord[]> {
		return this.request("listRuns");
	}
	export(): Promise<LogicalArchive> {
		return this.request("export");
	}
	import(data: LogicalArchive) {
		return this.request("import", data);
	}
	removeRun(id: string, before: string): Promise<boolean> {
		return this.request("removeRun", { id, before });
	}
	async close() {
		if (this.closing) return;
		if (this.failed) {
			this.closing = true;
			await this.worker.terminate();
			return;
		}
		const closed = this.request("close");
		this.closing = true;
		await closed;
	}
}
