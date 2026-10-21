import { readBackendSelection } from "./selection.js";
import { legacyCheckpointText, validateCheckpoint, type LegacyCheckpointPage } from "./legacy.js";
import { assertSqliteNodeSupport } from "./sqlite-support.js";
import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import { ensureDirectory, writeFileAtomic } from "../state/atomic_file.js";
import { withFileLock } from "../state/file_lock.js";
import { normalizeStateKey, resolveStateDirectory } from "../state/paths.js";
import {
	PersistenceError,
	PersistenceOutcomeUnknown,
	WaitConflictError,
	type Backend,
	type BackendKind,
	type StateWriteOptions,
	type StateWriteResult,
} from "./backend.js";
import { sessionFromEnv } from "./context.js";
import { FileBackend } from "./files.js";
import {
	canRetain,
	isRetiredCheckpoint,
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
	backend?: BackendKind;
	env?: Record<string, string | undefined>;
	readOnly?: boolean;
	history?: boolean;
	trackRuns?: boolean;
	maxPending?: number;
	signal?: AbortSignal;
};

export class StoreSession {
	readonly history: boolean;
	private closed = false;
	private pending = new Set<Promise<unknown>>();
	private closePromise?: Promise<void>;
	private selectedForTracking: boolean;

	constructor(
		readonly directory: string,
		readonly backend: Backend,
		history: boolean,
		private lease?: string,
		readonly readOnly = false,
		readonly trackRuns = history,
	) {
		this.history = history;
		this.selectedForTracking = trackRuns || backend.kind === "sqlite";
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
				if (!(error instanceof PersistenceOutcomeUnknown)) {
					throw error;
				}
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
				throw error;
			}
		}, true);
	}

	private async selectTrackedStorage(
		signal?: AbortSignal,
		assertInvocationCurrent?: () => void,
	): Promise<void> {
		if (this.selectedForTracking) return;
		await withFileLock({
			filePath: path.join(this.directory, ".backend-selection"),
			signal,
			task: async () => {
				this.assertOpen(true);
				const selected = await readBackendSelection(this.directory);
				if (selected && selected.backend !== this.backend.kind)
					throw new Error("Backend change requires verified migration");
				if (!selected) {
					await writeFileAtomic(
						path.join(this.directory, ".backend"),
						JSON.stringify({ version: 1, backend: this.backend.kind }) + "\n",
						{ signal, assertInvocationCurrent },
					);
				}
				this.selectedForTracking = true;
			},
		});
	}

	async createRun(
		kind: string,
		metadata?: RunMetadata,
		partialHistory = false,
	): Promise<ExecutionRecorder> {
		return this.createRecorder({ kind, metadata, partialHistory });
	}

	private async createRecorder({
		kind,
		metadata,
		partialHistory = false,
		checkpoint,
		signal,
		assertInvocationCurrent,
	}: {
		kind: string;
		metadata?: RunMetadata;
		partialHistory?: boolean;
		checkpoint?: { key: string; text: string };
		signal?: AbortSignal;
		assertInvocationCurrent?: () => void;
	}): Promise<ExecutionRecorder> {
		this.assertOpen(true);
		await this.selectTrackedStorage(signal, assertInvocationCurrent);
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
			historyEnabled: this.history,
			definitions: [],
			metadata: captureRunMetadata(metadata),
			attempts: [],
			events: [],
			waits: {},
			states: {},
			receipts: {},
		};
		const recorder = new ExecutionRecorder(this, id);
		await recorder.action(
			{ kind: "create", run, ...(checkpoint ? { checkpoint } : {}) },
			signal,
			assertInvocationCurrent,
		);
		return recorder;
	}

	async recorderForState(
		key: string,
		signal?: AbortSignal,
	): Promise<ExecutionRecorder | undefined> {
		return this.associateCheckpoint(key, undefined, signal);
	}

	/** Explicit host maintenance: attach a legacy wait to a trusted caller. */
	async adoptCheckpoint(
		key: string,
		metadata: RunMetadata,
		signal?: AbortSignal,
		assertInvocationCurrent?: () => void,
	): Promise<RunState> {
		this.assertOpen(true);
		if (
			typeof metadata?.callerId !== "string" ||
			!metadata.callerId.length ||
			metadata.callerId.length > 256
		) {
			throw new Error("Legacy adoption requires a trusted caller ID");
		}
		await this.selectTrackedStorage(signal, assertInvocationCurrent);
		const recorder = await this.associateCheckpoint(
			key,
			captureRunMetadata(metadata),
			signal,
			assertInvocationCurrent,
		);
		if (!recorder) throw new Error("Pending legacy checkpoint not found");
		return this.getRunState(recorder.runId);
	}

	private async associateCheckpoint(
		key: string,
		metadata?: RunMetadata,
		signal?: AbortSignal,
		assertInvocationCurrent?: () => void,
	): Promise<ExecutionRecorder | undefined> {
		signal?.throwIfAborted();
		key = normalizeStateKey(key);
		if (!/^(pipeline_resume_|workflow[-_]resume_|sdk_resume_)[a-z0-9._-]+$/.test(key)) {
			throw new Error("Invalid legacy checkpoint ID");
		}
		if ((await this.read(key)).text === null && /^workflow[-_]resume_/.test(key)) {
			key = key.includes("workflow-resume_")
				? key.replace("workflow-resume_", "workflow_resume_")
				: key.replace("workflow_resume_", "workflow-resume_");
		}
		return withFileLock({
			filePath: path.join(this.directory, `${key}.json`),
			signal,
			task: async () => {
				const record = await this.read(key);
				if (record.text === null) return undefined;
				if (record.runId) {
					const run = await this.getRunState(record.runId);
					if (!run) return undefined;
					const recorder = new ExecutionRecorder(this, run.id, "", run.historyEnabled !== false);
					if (metadata) {
						if (
							run.waits[key]?.status !== "pending" ||
							(run.metadata?.callerId !== undefined && run.metadata.callerId !== metadata.callerId)
						) {
							throw new WaitConflictError();
						}
						if (run.metadata?.callerId !== metadata.callerId) {
							const result = await recorder.action(
								{
									kind: "adopt",
									checkpoint: {
										key,
										status: "pending",
										fingerprint: fingerprint(JSON.parse(record.text)),
									},
									metadata,
								},
								signal,
								assertInvocationCurrent,
							);
							if (result.status === "conflict") throw new WaitConflictError();
						}
					}
					return recorder;
				}
				if (
					(!this.trackRuns && !metadata) ||
					JSON.parse(record.text)?.type === "lobster.consumed-resume-state.v1"
				)
					return undefined;
				validateCheckpoint(key, record.text);
				if (/^workflow[-_]resume_/.test(key)) {
					const alternate = key.includes("workflow-resume_")
						? key.replace("workflow-resume_", "workflow_resume_")
						: key.replace("workflow_resume_", "workflow-resume_");
					if (!isRetiredCheckpoint((await this.read(alternate)).text))
						throw new Error("Conflicting workflow checkpoint aliases");
				}
				return this.createRecorder({
					kind: "legacy",
					metadata,
					partialHistory: true,
					checkpoint: { key, text: legacyCheckpointText(key, record.text) },
					signal,
					assertInvocationCurrent,
				});
			},
		});
	}

	async listLegacyCheckpoints({
		after = "",
		limit = 20,
	}: { after?: string; limit?: number } = {}): Promise<LegacyCheckpointPage> {
		if (!Number.isInteger(limit) || limit < 1 || limit > 100)
			throw new Error("Checkpoint limit must be 1..100");
		return this.request(async () => {
			const checkpoints: LegacyCheckpointPage["checkpoints"] = [];
			const states = Object.assign(
				{},
				...(await Promise.all(
					["pipeline_resume_", "workflow_resume_", "workflow-resume_", "sdk_resume_"].map(
						(prefix) => this.backend.listState(prefix),
					),
				)),
			);
			for (const key of Object.keys(states)
				.filter((key) => key > after)
				.sort()) {
				const record = await this.backend.read(key);
				if (
					record.runId ||
					record.text === null ||
					JSON.parse(record.text)?.type === "lobster.consumed-resume-state.v1"
				)
					continue;
				validateCheckpoint(key, record.text);
				checkpoints.push({
					checkpointId: key,
					kind: key.startsWith("pipeline_")
						? "pipeline"
						: key.startsWith("sdk_")
							? "sdk"
							: "workflow",
				});
				if (checkpoints.length > limit) break;
			}
			return {
				checkpoints: checkpoints.slice(0, limit),
				nextCursor: checkpoints.length > limit ? checkpoints[limit - 1].checkpointId : null,
			};
		});
	}

	async getRun(id: string): Promise<RunRecord | null> {
		return this.request(() => this.backend.getRun(id));
	}

	async getRunState(id: string): Promise<RunState | null> {
		return this.request(() => this.backend.getRunState(id));
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
				if (!(error instanceof PersistenceOutcomeUnknown) || request.action.kind !== "handoff")
					throw error;
				// Read without the cancelled signal: a dispatched commit may have succeeded.
				let receipt: OperationReceipt | undefined;
				try {
					const run = await this.backend.getRun(request.runId);
					receipt = run?.receipts[request.operationId];
				} catch {
					throw error;
				}
				if (!receipt || receipt.fingerprint !== fingerprint(request)) throw error;
				result = { status: "committed", revision: receipt.revision };
			}
			if (result.status === "committed") {
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

export async function openStoreSession(options: StoreOptions = {}): Promise<StoreSession> {
	const env = options.env ?? process.env;
	const directory = path.resolve(
		options.directory ?? sessionFromEnv(env)?.directory ?? resolveStateDirectory(env),
	);
	const open = async () => {
		options.signal?.throwIfAborted();
		const selected = await readBackendSelection(directory);
		const kind = options.backend ?? env.LOBSTER_BACKEND ?? selected?.backend ?? "files";
		if (!["files", "sqlite"].includes(kind)) {
			throw new Error("LOBSTER_BACKEND must be files or sqlite");
		}
		if (selected && selected.backend !== kind) {
			throw new Error("Backend change requires verified migration");
		}
		const history = options.history ?? env.LOBSTER_HISTORY === "1";
		const trackRuns = history || (options.trackRuns ?? env.LOBSTER_RUNS === "1");
		let backend: Backend | undefined;
		let lease: string | undefined;
		try {
			if (kind === "sqlite") {
				assertSqliteNodeSupport();
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
			if (!selected && kind === "files" && trackRuns && !options.readOnly) {
				await writeFileAtomic(
					path.join(directory, ".backend"),
					JSON.stringify({ version: 1, backend: "files" }) + "\n",
				);
			}
			if (!options.readOnly) {
				lease = path.join(directory, `.session-${process.pid}-${randomUUID()}`);
				await fs.writeFile(lease, JSON.stringify({ pid: process.pid }), { mode: 0o600 });
			}
			return new StoreSession(directory, backend, history, lease, options.readOnly, trackRuns);
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
