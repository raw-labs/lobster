import type { ExecutionState } from "../engine/state.js";
import { assertSqliteNodeSupport } from "./sqlite-support.js";
import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import { ensureDirectory } from "../state/atomic_file.js";
import { withFileLock } from "../state/file_lock.js";
import { normalizeStateKey, resolveStateDirectory } from "../state/paths.js";
import {
	PersistenceError,
	PersistenceOutcomeUnknown,
	type Backend,
	type InboxEvent,
	type StateWriteOptions,
	type StateWriteResult,
} from "./backend.js";
import { sessionFromEnv } from "./context.js";
import {
	canRetain,
	captureRunMetadata,
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
	type RunFilter,
	type RunState,
	type StateRecord,
	type Transition,
	type TransitionResult,
} from "./model.js";

/** A checkpoint key that already belongs to a run (created by an earlier conversion). */
export class CheckpointTakenError extends Error {
	constructor() {
		super("Checkpoint already belongs to a run");
		this.name = "CheckpointTakenError";
	}
}

export {
	bindRecorder,
	bindStoreSession,
	recorderFromEnv,
	sessionFromEnv,
	type StoreEnvironment,
} from "./context.js";
export { normalizeStateKey as normalizedKey } from "../state/paths.js";

export type StoreOptions = {
	directory?: string;
	env?: Record<string, string | undefined>;
	readOnly?: boolean;
	history?: boolean;
	trackRuns?: boolean;
	maxPending?: number;
	signal?: AbortSignal;
	/**
	 * Days to keep finished runs (completed, failed, cancelled). Default 30, or
	 * `LOBSTER_RUN_RETENTION_DAYS`; 0 keeps them forever.
	 */
	retentionDays?: number;
};

const DEFAULT_RETENTION_DAYS = 30;

function retentionDays(options: StoreOptions, env: Record<string, string | undefined>): number {
	const raw = options.retentionDays ?? env.LOBSTER_RUN_RETENTION_DAYS;
	if (raw === undefined || raw === "") return DEFAULT_RETENTION_DAYS;
	const days = Number(raw);
	if (!Number.isFinite(days) || days < 0) {
		throw new Error(
			`LOBSTER_RUN_RETENTION_DAYS must be a number of days (0 keeps finished runs), got ${JSON.stringify(raw)}`,
		);
	}
	return days;
}

export class StoreSession {
	readonly history: boolean;
	private closed = false;
	private pending = new Set<Promise<unknown>>();
	private closePromise?: Promise<void>;

	constructor(
		readonly directory: string,
		readonly backend: Backend,
		history: boolean,
		readonly readOnly = false,
		readonly trackRuns = history,
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
		return this.request(() => this.backend.read(normalizeStateKey(key)));
	}

	async write(
		key: string,
		text: string | null,
		options: StateWriteOptions = {},
	): Promise<StateWriteResult> {
		return this.request(async () => {
			try {
				const result = await this.backend.write(normalizeStateKey(key), text, options);
				if (result.kind === "run") {
					await this.acknowledgeObserved(result.runId, result.operationId);
				}
				return {
					...result,
					signalAbortedAfterCommit:
						options.signal?.aborted === true || result.signalAbortedAfterCommit,
				};
			} catch (error) {
				return this.reconcileUnknown<StateWriteResult>(error, async (error) => {
					// Replacement waits for the old worker to stop. While the caller still owns
					// the publication barrier, reconcile the mutation before releasing readers.
					const current = await this.backend.read(normalizeStateKey(key));
					if (current.runId) {
						const run = await this.backend.getRun(current.runId);
						const receipt = run?.receipts[error.operationId];
						if (receipt) {
							await this.acknowledgeObserved(current.runId, error.operationId);
							return {
								status: "committed",
								kind: "run",
								runId: current.runId,
								operationId: error.operationId,
								revision: receipt.revision,
								confirmation: "reconciled",
								signalAbortedAfterCommit: options.signal?.aborted === true,
							};
						}
					} else if (!options.exclusive && current.text === text) {
						return {
							status: "committed",
							kind: "state",
							confirmation: "reconciled",
							signalAbortedAfterCommit: options.signal?.aborted === true,
						};
					}
				});
			}
		}, true);
	}

	async createRun(
		kind: string,
		metadata?: RunMetadata,
		partialHistory = false,
	): Promise<ExecutionRecorder> {
		return this.createRecorder({ kind, metadata, partialHistory });
	}

	private async createRecorder({
		id = randomUUID(),
		kind,
		metadata,
		partialHistory = false,
		checkpoint,
		execution,
		signal,
		assertInvocationCurrent,
	}: {
		id?: string;
		kind: string;
		metadata?: RunMetadata;
		partialHistory?: boolean;
		checkpoint?: { key: string; text: string };
		execution?: ExecutionState;
		signal?: AbortSignal;
		assertInvocationCurrent?: () => void;
	}): Promise<ExecutionRecorder> {
		this.assertOpen(true);
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
			historyEnabled: this.history,
			definitions: [],
			metadata: captureRunMetadata(metadata),
			attempts: [],
			events: [],
			waits: {},
			states: {},
			receipts: {},
			...(execution ? { execution } : {}),
		};
		const recorder = new ExecutionRecorder(this, id);
		await recorder.action(
			{ kind: "create", run, ...(checkpoint ? { checkpoint } : {}) },
			signal,
			assertInvocationCurrent,
		);
		return recorder;
	}

	/** The recorder of the run that owns this continuation key, if a run owns it. */
	async recorderForState(
		key: string,
		signal?: AbortSignal,
	): Promise<ExecutionRecorder | undefined> {
		signal?.throwIfAborted();
		const record = await this.read(normalizeStateKey(key));
		if (!record.runId) return undefined;
		const run = await this.getRunState(record.runId);
		if (!run) return undefined;
		return new ExecutionRecorder(this, run.id, "", run.historyEnabled !== false);
	}

	/**
	 * Upgrade only: register a converted workflow or pipeline pause as a run, with its execution
	 * state and continuation, in one transition. The key keeps its old token valid.
	 */
	/**
	 * Create a run waiting at `key`. With `once`, a key that marks the conversion done for good
	 * is checked and written under the same lock, just before the run: a second conversion waits
	 * for the first and finds it, and one that already happened is never repeated.
	 * Returns undefined when `once` was already written.
	 */
	async createWaitingRun({
		id,
		kind = "workflow",
		key,
		execution,
		continuation,
		once,
	}: {
		id: string;
		kind?: string;
		key: string;
		execution: ExecutionState;
		continuation: (runId: string) => string;
		once?: string;
	}): Promise<string | undefined> {
		this.assertOpen(true);
		key = normalizeStateKey(key);
		return withFileLock({
			filePath: path.join(this.directory, `${key}.json`),
			task: async () => {
				if (once && (await this.read(once)).text !== null) return undefined;
				if ((await this.read(key)).runId) throw new CheckpointTakenError();
				if (once)
					await this.write(once, JSON.stringify({ usedAt: new Date().toISOString() }) + "\n", {
						exclusive: true,
					});
				const recorder = await this.createRecorder({
					id,
					kind,
					checkpoint: { key, text: continuation(id) },
					execution,
				});
				return recorder.runId;
			},
		});
	}

	async getRun(id: string): Promise<RunRecord | null> {
		return this.request(() => this.backend.getRun(id));
	}

	async getRunState(id: string): Promise<RunState | null> {
		return this.request(() => this.backend.getRunState(id));
	}

	/** Runs whose wake time has passed: a lapsed lease, a timer, a question's deadline. */
	async dueRuns({
		now = Date.now(),
		limit = 100,
		callerId,
	}: { now?: number; limit?: number; callerId?: string } = {}): Promise<string[]> {
		return this.request(() => this.backend.dueRuns(now, limit, callerId));
	}

	async appendEvent(event: InboxEvent): Promise<{ inserted: boolean }> {
		return this.request(() => this.backend.appendEvent(event), true);
	}

	async findEvents(name: string, since: number, owner = ""): Promise<InboxEvent[]> {
		return this.request(() => this.backend.findEvents(name, since, owner));
	}

	async readRecords(runId: string, prefix: string): Promise<Array<{ key: string; text: string }>> {
		return this.request(() => this.backend.readRecords(runId, prefix));
	}

	async listRuns({
		limit = 20,
		after = "",
		...filter
	}: RunFilter & { limit?: number; after?: string } = {}): Promise<{
		runs: RunSummary[];
		nextCursor: string | null;
	}> {
		this.assertOpen();
		if (!Number.isInteger(limit) || limit < 1 || limit > 100) {
			throw new Error("Run limit must be 1..100");
		}
		const found = await this.request(() => this.backend.listRunSummaries(after, limit + 1, filter));
		const runs = found.slice(0, limit);
		return { runs, nextCursor: found.length > limit ? (runs.at(-1)?.id ?? null) : null };
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
		return this.request(async () => {
			try {
				await this.backend.acknowledge(runId, operationId);
				return true;
			} catch {
				return false;
			}
		}, true);
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

	async transition(
		request: Transition,
		signal?: AbortSignal,
		assertInvocationCurrent?: () => void,
	): Promise<TransitionResult> {
		return this.request(async () => {
			let result: TransitionResult;
			try {
				result = await this.backend.transition(request, signal, assertInvocationCurrent);
			} catch (error) {
				result = await this.reconcileUnknown(error, async () => {
					if (!request.action.kind.startsWith("execution-")) return;
					const run = await this.backend.getRun(request.runId);
					const receipt = run?.receipts[request.operationId];
					if (receipt?.fingerprint === fingerprint(request)) {
						return { status: "committed" as const, revision: receipt.revision };
					}
				});
			}
			if (result.status === "committed") {
				await this.acknowledgeObserved(request.runId, request.operationId);
			}
			return result;
		}, true);
	}

	// A failed reconciliation is not evidence of rollback. Preserve the original
	// uncertainty so workflow retry/on_error cannot replay a committed operation.
	private async reconcileUnknown<T>(
		error: unknown,
		readCommit: (error: PersistenceOutcomeUnknown) => Promise<T | undefined>,
	): Promise<T> {
		if (!(error instanceof PersistenceOutcomeUnknown)) throw error;
		try {
			const result = await readCommit(error);
			if (result !== undefined) return result;
		} catch {
			// The outcome remains unknown, including when recovery cannot reopen storage.
		}
		throw error;
	}

	async close(): Promise<void> {
		if (this.closePromise) {
			return this.closePromise;
		}
		this.closed = true;
		this.closePromise = (async () => {
			await Promise.allSettled(this.pending);
			await this.backend.close();
		})();
		return this.closePromise;
	}
}

export class ExecutionRecorder {
	pipelineOffset = 0;

	constructor(
		readonly session: StoreSession,
		readonly runId: string,
		readonly prefix = "",
		readonly historyEnabled = session.history,
	) {}

	child(path: string): ExecutionRecorder {
		return new ExecutionRecorder(
			this.session,
			this.runId,
			this.prefix + path + "/",
			this.historyEnabled,
		);
	}

	async action(
		action: RunAction,
		signal?: AbortSignal,
		assertInvocationCurrent?: () => void,
	): Promise<TransitionResult> {
		const operationId = randomUUID();
		let result: TransitionResult;
		try {
			result = await this.session.transition(
				{ operationId, runId: this.runId, action },
				signal,
				assertInvocationCurrent,
			);
		} catch (error) {
			if (error instanceof PersistenceError) {
				throw error;
			}
			throw new PersistenceError(operationId, this.runId, error);
		}
		return result;
	}

	async definition(value: unknown): Promise<void> {
		if (!this.historyEnabled) return;
		await this.action({ kind: "definition", fingerprint: fingerprint(value) });
	}

	async start(path: string, name: string, ordinal?: number): Promise<string> {
		const full = this.prefix + path;
		const id = randomUUID();
		if (!this.historyEnabled) return id;
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
		if (!this.historyEnabled) return;
		await this.action({ kind: "finish", id, status, summary: summarize(value) });
	}

	async settle(status: RunStatus, value?: unknown): Promise<void> {
		await this.action({ kind: "settle", status, summary: summarize(value) });
	}
}

/** Open the run store: the SQLite database `lobster.sqlite` in the state directory. */
export async function openStoreSession(options: StoreOptions = {}): Promise<StoreSession> {
	const env = options.env ?? process.env;
	const directory = path.resolve(
		options.directory ?? sessionFromEnv(env)?.directory ?? resolveStateDirectory(env),
	);
	options.signal?.throwIfAborted();
	assertSqliteNodeSupport();
	const history = options.history ?? env.LOBSTER_HISTORY === "1";
	const trackRuns = history || (options.trackRuns ?? env.LOBSTER_RUNS === "1");
	const keepDays = retentionDays(options, env);
	const databasePath = path.join(directory, "lobster.sqlite");
	const { SqliteBackend } = await import("./sqlite.js");
	let created = false;
	try {
		await fs.access(databasePath);
	} catch {
		// The schema is created by a writable connection, also for a first read.
		await ensureDirectory(directory);
		const handle = await fs.open(databasePath, "a", 0o600);
		await handle.close();
		created = true;
	}
	if (created && options.readOnly) await new SqliteBackend(databasePath).close();
	let backend: Backend | undefined;
	try {
		backend = new SqliteBackend(databasePath, options.readOnly, options.maxPending, options.signal);
		await backend.listRunSummaries("", 1);
		if (!options.readOnly && keepDays > 0) {
			const now = Date.now();
			const before = new Date(now - keepDays * 24 * 60 * 60 * 1000).toISOString();
			// Housekeeping: a busy or failed prune retries on a later open.
			await backend.pruneFinished(before, new Date(now).toISOString()).catch(() => {});
		}
		options.signal?.throwIfAborted();
		return new StoreSession(directory, backend, history, options.readOnly, trackRuns);
	} catch (error) {
		await backend?.close();
		if (options.signal?.aborted) options.signal.throwIfAborted();
		throw error;
	}
}
