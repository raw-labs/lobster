import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import { ensureDirectory, writeFileAtomic } from "../state/atomic_file.js";
import { defaultStateDir, keyToPath, withFileLock } from "../state/store.js";
import {
	PersistenceError,
	PersistenceOutcomeUnknown,
	type Backend,
	type BackendKind,
	type StateWriteOptions,
	type StateWriteResult,
} from "./backend.js";
import { FileBackend } from "./files.js";
import {
	canRetain,
	fingerprint,
	summarize,
	type AttemptStatus,
	type HistoryPage,
	type OperationReceipt,
	type RunAction,
	type RunMetadata,
	type RunRecord,
	type RunStatus,
	type RunSummary,
	type StateRecord,
	type Transition,
	type TransitionResult,
} from "./model.js";

const SESSION = Symbol("lobster.storeSession");
const RECORDER = Symbol("lobster.executionRecorder");

export type StoreEnvironment = Record<string, string | undefined> & {
	[SESSION]?: StoreSession;
	[RECORDER]?: ExecutionRecorder;
};

export function sessionFromEnv(env: StoreEnvironment | undefined): StoreSession | undefined {
	return env?.[SESSION];
}

export function recorderFromEnv(env: StoreEnvironment | undefined): ExecutionRecorder | undefined {
	return env?.[RECORDER];
}

export function normalizedKey(key: string): string {
	return path.basename(keyToPath(".", key), ".json");
}

export function bindStoreSession(env: StoreEnvironment, session: StoreSession): StoreEnvironment {
	const bound: StoreEnvironment = {
		...env,
		LOBSTER_STATE_DIR: session.directory,
		LOBSTER_BACKEND: session.backend.kind,
		[SESSION]: session,
	};
	if (bound[RECORDER]?.session !== session) {
		delete bound[RECORDER];
	}
	return bound;
}

export function bindRecorder(env: StoreEnvironment, recorder: ExecutionRecorder): StoreEnvironment {
	return { ...env, [RECORDER]: recorder };
}

export type StoreOptions = {
	directory?: string;
	backend?: BackendKind;
	env?: Record<string, string | undefined>;
	readOnly?: boolean;
	history?: boolean;
	maxPending?: number;
	signal?: AbortSignal;
};

export class StoreSession {
	readonly history: boolean;
	private closed = false;
	private pending = new Set<Promise<unknown>>();
	private closePromise?: Promise<void>;

	constructor(
		readonly directory: string,
		readonly backend: Backend,
		history: boolean,
		private lease?: string,
		readonly readOnly = false,
	) {
		this.history = history;
	}

	private assertOpen(write = false): void {
		if (this.closed) {
			throw new Error("Persistence session is closed");
		}
		if (write && this.readOnly) {
			throw new Error("Persistence session is read-only");
		}
	}

	private request<T>(task: () => Promise<T>, write = false): Promise<T> {
		this.assertOpen(write);
		const result = task();
		this.pending.add(result);
		return result.finally(() => this.pending.delete(result));
	}

	async read(key: string): Promise<StateRecord> {
		return this.request(() => this.backend.read(normalizedKey(key)));
	}

	async write(
		key: string,
		text: string | null,
		options: StateWriteOptions = {},
	): Promise<StateWriteResult | undefined> {
		return this.request(async () => {
			try {
				const result = await this.backend.write(normalizedKey(key), text, options);
				if (result?.runId && result?.operationId) {
					await this.acknowledgeObserved(result.runId, result.operationId);
				}
				return result
					? {
							...result,
							signalAbortedAfterCommit:
								options.signal?.aborted === true || result.signalAbortedAfterCommit === true,
						}
					: result;
			} catch (error) {
				if (!(error instanceof PersistenceOutcomeUnknown)) {
					throw error;
				}
				// Replacement waits for the old worker to stop. While the caller still owns
				// the publication barrier, reconcile the mutation before releasing readers.
				const current = await this.backend.read(normalizedKey(key));
				if (current.runId) {
					const run = await this.backend.getRun(current.runId);
					const receipt = run?.receipts[error.operationId];
					if (receipt) {
						await this.acknowledgeObserved(current.runId, error.operationId);
						return {
							revision: receipt.revision,
							reconciled: true,
							signalAbortedAfterCommit: options.signal?.aborted === true,
						};
					}
				} else if (!options.exclusive && current.text === text) {
					return { reconciled: true, signalAbortedAfterCommit: options.signal?.aborted === true };
				}
				throw error;
			}
		}, true);
	}

	async createRun(
		kind: string,
		metadata?: RunMetadata,
		partialHistory = false,
	): Promise<ExecutionRecorder> {
		this.assertOpen(true);
		const id = randomUUID();
		const at = new Date().toISOString();
		const run: RunRecord = {
			version: 1,
			id,
			kind,
			status: "running",
			revision: 0,
			createdAt: at,
			updatedAt: at,
			partialHistory,
			definitions: [],
			metadata:
				metadata && typeof metadata === "object"
					? Object.fromEntries(
							Object.entries(metadata).filter(
								([key, value]) =>
									["hostRunId", "hostTaskId", "callerId"].includes(key) &&
									typeof value === "string" &&
									value.length <= 256,
							),
						)
					: undefined,
			attempts: [],
			events: [],
			waits: {},
			states: {},
			receipts: {},
		};
		const recorder = new ExecutionRecorder(this, id);
		await recorder.action({ kind: "create", run });
		return recorder;
	}

	async recorderForState(key: string): Promise<ExecutionRecorder | undefined> {
		let record = await this.read(key);
		if (record.text === null && /workflow[-_]resume_/.test(key)) {
			key = key.includes("workflow-resume_")
				? key.replace("workflow-resume_", "workflow_resume_")
				: key.replace("workflow_resume_", "workflow-resume_");
			record = await this.read(key);
		}
		if (record.text === null) {
			return undefined;
		}
		if (record.runId) {
			return new ExecutionRecorder(this, record.runId);
		}
		if (!this.history || JSON.parse(record.text)?.type === "lobster.consumed-resume-state.v1") {
			return undefined;
		}
		const recorder = await this.createRun("legacy", undefined, true);
		await recorder.action({ kind: "state", key: normalizedKey(key), text: record.text });
		return recorder;
	}

	async getRun(id: string): Promise<RunRecord | null> {
		return this.request(() => this.backend.getRun(id));
	}

	async listRuns({ limit = 20, after = "" } = {}): Promise<{
		runs: RunSummary[];
		nextCursor: string | null;
	}> {
		this.assertOpen();
		if (!Number.isInteger(limit) || limit < 1 || limit > 100) {
			throw new Error("History limit must be 1..100");
		}
		const runs = await this.request(() => this.backend.listRunSummaries(after, limit));
		return { runs, nextCursor: runs.at(-1)?.id ?? null };
	}

	async inspect(id: string, { afterSequence = 0, limit = 50 } = {}): Promise<HistoryPage | null> {
		this.assertOpen();
		if (
			!Number.isInteger(limit) ||
			limit < 1 ||
			limit > 100 ||
			!Number.isInteger(afterSequence) ||
			afterSequence < 0
		) {
			throw new Error("Invalid history page");
		}
		return this.request(() => this.backend.inspectRun(id, afterSequence, limit));
	}

	async retain({ before, dryRun = true }: { before: string; dryRun?: boolean }): Promise<string[]> {
		return this.request(async () => {
			if (!Number.isFinite(Date.parse(before))) {
				throw new Error("Invalid retention date");
			}
			const removed: string[] = [];
			for (const run of await this.backend.listRuns()) {
				if (!canRetain(run, before)) {
					continue;
				}
				if (dryRun || (await this.backend.removeRun(run.id, before))) {
					removed.push(run.id);
				}
			}
			return removed;
		}, !dryRun);
	}

	async acknowledge(runId: string, operationId: string): Promise<boolean> {
		return this.request(() => this.acknowledgeObserved(runId, operationId), true);
	}

	private async acknowledgeObserved(runId: string, operationId: string): Promise<boolean> {
		try {
			await this.backend.acknowledge(runId, operationId);
			return true;
		} catch {
			// The original commit is known. Keep its receipt protected.
			return false;
		}
	}

	async reconcile(runId: string, operationId: string): Promise<OperationReceipt | null> {
		const run = await this.getRun(runId);
		return run?.receipts[operationId] ?? null;
	}

	async transition(request: Transition, signal?: AbortSignal): Promise<TransitionResult> {
		return this.request(async () => {
			const result = await this.backend.transition(request, signal);
			if (!result.conflict) {
				await this.acknowledgeObserved(request.runId, request.operationId);
			}
			return result;
		}, true);
	}

	async close(): Promise<void> {
		if (this.closePromise) {
			return this.closePromise;
		}
		this.closed = true;
		this.closePromise = (async () => {
			await Promise.allSettled(this.pending);
			try {
				await this.backend.close();
			} finally {
				if (this.lease) {
					await fs.unlink(this.lease).catch((error) => {
						if (error.code !== "ENOENT") {
							throw error;
						}
					});
				}
			}
		})();
		return this.closePromise;
	}
}

export class ExecutionRecorder {
	ownsExecution = false;
	pipelineOffset = 0;
	decision?: unknown;
	decisionKey?: string;

	constructor(
		readonly session: StoreSession,
		readonly runId: string,
		readonly prefix = "",
	) {}

	child(path: string): ExecutionRecorder {
		return new ExecutionRecorder(this.session, this.runId, this.prefix + path + "/");
	}

	async action(action: RunAction): Promise<TransitionResult> {
		const operationId = randomUUID();
		let result: TransitionResult;
		try {
			result = await this.session.transition({ operationId, runId: this.runId, action });
		} catch (error) {
			if (error instanceof PersistenceError) {
				throw error;
			}
			throw new PersistenceError(operationId, this.runId, error);
		}
		return result;
	}

	async definition(value: unknown): Promise<void> {
		await this.action({ kind: "definition", fingerprint: fingerprint(value) });
	}

	async start(path: string, name: string, ordinal?: number): Promise<string> {
		const full = this.prefix + path;
		const id = randomUUID();
		await this.action({
			kind: "start",
			attempt: {
				id,
				path: full,
				name,
				ordinal,
				attempt: 0, // Assigned atomically by the backend.
				status: "started",
				startedAt: new Date().toISOString(),
			},
		});
		return id;
	}

	async finish(id: string, status: AttemptStatus, value?: unknown): Promise<void> {
		await this.action({ kind: "finish", id, status, summary: summarize(value) });
	}

	async settle(status: RunStatus, value?: unknown): Promise<void> {
		await this.action({ kind: "settle", status, summary: summarize(value) });
	}
}

export async function openStoreSession(options: StoreOptions = {}): Promise<StoreSession> {
	const env = options.env ?? process.env;
	const directory = path.resolve(options.directory ?? defaultStateDir(env));
	const open = async () => {
		options.signal?.throwIfAborted();
		let selected: { version: number; backend: string } | undefined;
		try {
			selected = JSON.parse(await fs.readFile(path.join(directory, ".backend"), "utf8"));
		} catch (error) {
			if (error.code !== "ENOENT") {
				throw error;
			}
		}
		if (selected && (selected.version !== 1 || !["files", "sqlite"].includes(selected.backend))) {
			throw new Error("Unsupported backend selection");
		}
		const kind = options.backend ?? env.LOBSTER_BACKEND ?? selected?.backend ?? "files";
		if (!["files", "sqlite"].includes(kind)) {
			throw new Error("LOBSTER_BACKEND must be files or sqlite");
		}
		if (selected && selected.backend !== kind) {
			throw new Error("Backend change requires verified migration");
		}
		const history = options.history ?? env.LOBSTER_HISTORY === "1";
		let backend: Backend | undefined;
		let lease: string | undefined;
		try {
			if (kind === "sqlite") {
				const [major, minor] = process.versions.node.split(".").map(Number);
				if (major < 22 || (major === 22 && minor < 13) || (major === 23 && minor < 4)) {
					throw new Error("SQLite backend requires Node >=22.13");
				}
				if (!selected) {
					const legacy = await new FileBackend(directory).export();
					if (Object.keys(legacy.states).length || legacy.runs.length) {
						throw new Error("Existing file state requires migration before SQLite selection");
					}
				}
				await ensureDirectory(directory);
				const databasePath = path.join(directory, "lobster.sqlite");
				if (selected) {
					try {
						await fs.access(databasePath);
					} catch {
						throw new Error("Selected SQLite database is missing; restore a verified backup");
					}
				}
				if (!options.readOnly) {
					const handle = await fs.open(databasePath, "a", 0o600);
					await handle.close();
				}
				const { SqliteBackend } = await import("./sqlite.js");
				backend = new SqliteBackend(
					databasePath,
					options.readOnly,
					options.maxPending,
					options.signal,
				);
				await backend.listRunSummaries("", 1);
				options.signal?.throwIfAborted();
				if (!selected && !options.readOnly) {
					await writeFileAtomic(
						path.join(directory, ".backend"),
						JSON.stringify({ version: 1, backend: kind }) + "\n",
					);
				}
			} else {
				backend = new FileBackend(directory, options.readOnly);
			}
			if (!selected && kind === "files" && history && !options.readOnly) {
				await writeFileAtomic(
					path.join(directory, ".backend"),
					JSON.stringify({ version: 1, backend: "files" }) + "\n",
				);
			}
			if (!options.readOnly) {
				lease = path.join(directory, `.session-${process.pid}-${randomUUID()}`);
				await fs.writeFile(lease, JSON.stringify({ pid: process.pid }), { mode: 0o600 });
			}
			return new StoreSession(directory, backend, history, lease, options.readOnly);
		} catch (error) {
			await backend?.close();
			if (lease) {
				await fs.unlink(lease).catch(() => {});
			}
			if (options.signal?.aborted) {
				options.signal.throwIfAborted();
			}
			throw error;
		}
	};
	return options.readOnly
		? open()
		: withFileLock({
				filePath: path.join(directory, ".backend-selection"),
				task: open,
				signal: options.signal,
			});
}
