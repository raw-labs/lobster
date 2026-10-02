import path from "node:path";
import { promises as fs } from "node:fs";
import { randomUUID } from "node:crypto";
import {
	defaultStateDir,
	keyToPath,
	ensureDirectory,
	withFileLock,
	writeFileAtomic,
} from "../state/store.js";
import { FileBackend } from "./files.js";
import { PersistenceOutcomeUnknown, type Backend } from "./backend.js";
import {
	fingerprint,
	canRetain,
	summarize,
	type RunRecord,
	type RunAction,
	type RunStatus,
	type AttemptStatus,
} from "./model.js";

const SESSION = Symbol("lobster.storeSession");
const RECORDER = Symbol("lobster.executionRecorder");
export function sessionFromEnv(env: any): StoreSession | undefined {
	return env?.[SESSION];
}
export function recorderFromEnv(env: any): ExecutionRecorder | undefined {
	return env?.[RECORDER];
}
export function normalizedKey(key: string) {
	return path.basename(keyToPath(".", key), ".json");
}
export function bindStoreSession(env: Record<string, string | undefined>, session: StoreSession) {
	return { ...env, LOBSTER_STATE_DIR: session.directory, [SESSION]: session };
}
export function bindRecorder(env: Record<string, string | undefined>, recorder: ExecutionRecorder) {
	return { ...env, [RECORDER]: recorder };
}
export type StoreOptions = {
	directory?: string;
	backend?: "files" | "sqlite";
	env?: Record<string, string | undefined>;
	readOnly?: boolean;
	history?: boolean;
	maxPending?: number;
};
export class StoreSession {
	readonly history: boolean;
	private closed = false;
	constructor(
		readonly directory: string,
		readonly backend: Backend,
		history: boolean,
		private lease?: string,
		readonly readOnly = false,
	) {
		this.history = history;
	}
	async read(key: string) {
		if (this.closed) throw new Error("Persistence session is closed");
		return this.backend.read(normalizedKey(key));
	}
	async write(key: string, text: string | null, options: any = {}) {
		if (this.closed) throw new Error("Persistence session is closed");
		if (this.readOnly) throw new Error("Persistence session is read-only");
		try {
			return await this.backend.write(normalizedKey(key), text, options);
		} catch (error) {
			if (!(error instanceof PersistenceOutcomeUnknown)) throw error;
			// Replacement waits for the old worker to stop. While the caller still owns
			// the publication barrier, reconcile the mutation before releasing readers.
			const current = await this.backend.read(normalizedKey(key));
			if (current.runId) {
				const run = await this.backend.getRun(current.runId);
				const receipt = run?.receipts[error.operationId];
				if (receipt)
					return {
						revision: receipt.revision,
						reconciled: true,
						signalAbortedAfterCommit: options.signal?.aborted === true,
					};
			} else if (current.text === text) {
				return { reconciled: true, signalAbortedAfterCommit: options.signal?.aborted === true };
			}
			throw error;
		}
	}
	async createRun(kind: string, metadata?: unknown, partialHistory = false) {
		if (this.readOnly) throw new Error("Persistence session is read-only");
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
			metadata: summarize(metadata),
			attempts: [],
			events: [],
			waits: {},
			states: {},
			receipts: {},
		};
		await this.backend.transition({
			operationId: randomUUID(),
			runId: id,
			action: { kind: "create", run },
		});
		return new ExecutionRecorder(this, id);
	}
	async recorderForState(key: string) {
		let record = await this.read(key);
		if (record.text === null && /workflow[-_]resume_/.test(key)) {
			key = key.includes("workflow-resume_")
				? key.replace("workflow-resume_", "workflow_resume_")
				: key.replace("workflow_resume_", "workflow-resume_");
			record = await this.read(key);
		}
		if (record.text === null) return undefined;
		if (record.runId) return new ExecutionRecorder(this, record.runId);
		if (!this.history || JSON.parse(record.text)?.type === "lobster.consumed-resume-state.v1")
			return undefined;
		const recorder = await this.createRun("legacy", undefined, true);
		await this.backend.transition({
			operationId: randomUUID(),
			runId: recorder.runId,
			action: { kind: "state", key: normalizedKey(key), text: record.text },
		});
		return recorder;
	}
	async getRun(id: string) {
		return this.backend.getRun(id);
	}
	async listRuns({ limit = 20, after = "" } = {}) {
		if (!Number.isInteger(limit) || limit < 1 || limit > 100)
			throw new Error("History limit must be 1..100");
		const runs = await this.backend.listRunSummaries(after, limit);
		return { runs, nextCursor: runs.at(-1)?.id ?? null };
	}
	async inspect(id: string, { afterSequence = 0, limit = 50 } = {}) {
		if (
			!Number.isInteger(limit) ||
			limit < 1 ||
			limit > 100 ||
			!Number.isInteger(afterSequence) ||
			afterSequence < 0
		)
			throw new Error("Invalid history page");
		return this.backend.inspectRun(id, afterSequence, limit);
	}
	async retain({ before, dryRun = true }: { before: string; dryRun?: boolean }) {
		if (!Number.isFinite(Date.parse(before))) throw new Error("Invalid retention date");
		const removed: string[] = [];
		for (const run of await this.backend.listRuns()) {
			if (!canRetain(run, before)) continue;
			if (dryRun || (await this.backend.removeRun(run.id, before))) removed.push(run.id);
		}
		return removed;
	}
	async reconcile(runId: string, operationId: string) {
		const run = await this.getRun(runId);
		return run?.receipts[operationId] ?? null;
	}

	async close() {
		if (this.closed) return;
		this.closed = true;
		try {
			await this.backend.close();
		} finally {
			if (this.lease)
				await fs.unlink(this.lease).catch((error) => {
					if (error.code !== "ENOENT") throw error;
				});
		}
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
	child(path: string) {
		return new ExecutionRecorder(this.session, this.runId, this.prefix + path + "/");
	}
	async action(action: RunAction) {
		return this.session.backend.transition({
			operationId: randomUUID(),
			runId: this.runId,
			action,
		});
	}
	async definition(value: unknown) {
		await this.action({ kind: "definition", fingerprint: fingerprint(value) });
	}
	async start(path: string, name: string, ordinal?: number) {
		const full = this.prefix + path;
		const attempt = 0;
		const id = randomUUID();
		await this.action({
			kind: "start",
			attempt: {
				id,
				path: full,
				name,
				ordinal,
				attempt,
				status: "started",
				startedAt: new Date().toISOString(),
			},
		});
		return id;
	}
	async finish(id: string, status: AttemptStatus, value?: unknown) {
		await this.action({ kind: "finish", id, status, summary: summarize(value) });
	}
	async settle(status: RunStatus, value?: unknown) {
		await this.action({ kind: "settle", status, summary: summarize(value) });
	}
}
export async function openStoreSession(options: StoreOptions = {}) {
	const env = options.env ?? process.env;
	const directory = path.resolve(options.directory ?? defaultStateDir(env));
	const open = async () => {
		let selected: any;
		try {
			selected = JSON.parse(await fs.readFile(path.join(directory, ".backend"), "utf8"));
		} catch (error) {
			if (error.code !== "ENOENT") throw error;
		}
		if (selected && (selected.version !== 1 || !["files", "sqlite"].includes(selected.backend)))
			throw new Error("Unsupported backend selection");
		const kind =
			options.backend ??
			(env.LOBSTER_BACKEND as "files" | "sqlite" | undefined) ??
			selected?.backend ??
			"files";
		if (!["files", "sqlite"].includes(kind))
			throw new Error("LOBSTER_BACKEND must be files or sqlite");
		if (selected && selected.backend !== kind)
			throw new Error("Backend change requires verified migration");
		const history = options.history ?? env.LOBSTER_HISTORY === "1";
		let backend: Backend;
		if (kind === "sqlite") {
			const [major, minor] = process.versions.node.split(".").map(Number);
			if (major < 22 || (major === 22 && minor < 13) || (major === 23 && minor < 4))
				throw new Error("SQLite backend requires Node >=22.13");
			if (!selected) {
				const legacy = await new FileBackend(directory).export();
				if (Object.keys(legacy.states).length || legacy.runs.length)
					throw new Error("Existing file state requires migration before SQLite selection");
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
			backend = new SqliteBackend(databasePath, options.readOnly, options.maxPending);
			try {
				await backend.listRuns();
			} catch (error) {
				await backend.close();
				throw error;
			}
			if (!selected && !options.readOnly)
				await writeFileAtomic(
					path.join(directory, ".backend"),
					JSON.stringify({ version: 1, backend: kind }) + "\n",
				);
		} else backend = new FileBackend(directory, options.readOnly);
		let lease: string | undefined;
		if (!options.readOnly) {
			lease = path.join(directory, `.session-${process.pid}-${randomUUID()}`);
			await fs.writeFile(lease, JSON.stringify({ pid: process.pid }), { mode: 0o600 });
		}
		return new StoreSession(directory, backend, history, lease, options.readOnly);
	};
	return options.readOnly
		? open()
		: withFileLock({ filePath: path.join(directory, ".backend-selection"), task: open });
}
