import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import {
	atomicWriteWasPublished,
	ensureDirectory,
	writeFileAtomic,
	writeFileAtomicExclusive,
} from "../state/atomic_file.js";
import type { AtomicExclusiveWriteOptions, AtomicWriteOptions } from "../state/atomic_file.js";
import { withFileLock } from "../state/file_lock.js";
import { keyToPath } from "../state/paths.js";
import {
	PersistenceOutcomeUnknown,
	WaitConflictError,
	type Backend,
	type StateWriteOptions,
	type StateWriteResult,
} from "./backend.js";
import {
	applyTransition,
	canRetain,
	historyPage,
	runSummary,
	validateArchive,
	type HistoryPage,
	type LogicalArchive,
	type RunRecord,
	type RunSummary,
	type RunFilter,
	type RunState,
	type StateRecord,
	type Transition,
	type TransitionResult,
} from "./model.js";

// Legacy state callers can supply filesystem publication hooks. They are local
// to this adapter; the common backend and SQLite worker never depend on them.
export type FileWriteOptions = StateWriteOptions & {
	atomicWriteOptions?: AtomicWriteOptions & AtomicExclusiveWriteOptions;
};

async function readText(file: string): Promise<string | null> {
	try {
		return await fs.readFile(file, "utf8");
	} catch (error) {
		if (error.code === "ENOENT") {
			return null;
		}
		throw error;
	}
}

export class FileBackend implements Backend {
	readonly kind = "files" as const;
	private locators = new Map<string, string>();

	constructor(
		readonly directory: string,
		readonly readOnly = false,
	) {}

	private runPath(id: string): string {
		if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
			throw new Error("Invalid run ID");
		}
		return path.join(this.directory, ".history", `${id}.json`);
	}

	async getRun(id: string): Promise<RunRecord | null> {
		const text = await readText(this.runPath(id));
		if (text === null) {
			return null;
		}
		const run = JSON.parse(text);
		validateArchive({ version: 1, states: {}, runs: [run] });
		return run;
	}

	async getRunState(id: string): Promise<RunState | null> {
		const run = await this.getRun(id);
		if (!run) return null;
		const { attempts: _attempts, events: _events, receipts: _receipts, ...state } = run;
		return state;
	}

	async listRuns(): Promise<RunRecord[]> {
		let names: string[];
		try {
			names = await fs.readdir(path.join(this.directory, ".history"));
		} catch (error) {
			if (error.code === "ENOENT") {
				return [];
			}
			throw error;
		}
		const runs: RunRecord[] = [];
		for (const name of names.sort()) {
			if (!name.endsWith(".json")) {
				continue;
			}
			const run = await this.getRun(name.slice(0, -5));
			if (run) {
				runs.push(run);
			}
		}
		return runs;
	}

	async listRunSummaries(
		after: string,
		limit: number,
		filter: RunFilter = {},
	): Promise<RunSummary[]> {
		let names: string[];
		try {
			names = await fs.readdir(path.join(this.directory, ".history"));
		} catch (error) {
			if (error.code === "ENOENT") {
				return [];
			}
			throw error;
		}
		const summaries: RunSummary[] = [];
		const ids = names
			.filter((name) => name.endsWith(".json"))
			.map((name) => name.slice(0, -5))
			.filter((id) => id > after)
			.sort();
		for (const id of ids) {
			const run = await this.getRun(id);
			if (
				run &&
				(!filter.status || run.status === filter.status) &&
				(filter.callerId === undefined || run.metadata?.callerId === filter.callerId)
			) {
				summaries.push(runSummary(run));
			}
			if (summaries.length === limit) {
				break;
			}
		}
		return summaries;
	}

	async inspectRun(id: string, afterSequence: number, limit: number): Promise<HistoryPage | null> {
		const run = await this.getRun(id);
		return run ? historyPage(run, afterSequence, limit) : null;
	}

	async read(key: string): Promise<StateRecord> {
		let id = this.locators.get(key);
		if (!id && /^(pipeline_resume_|workflow[-_]resume_|sdk_resume_)/.test(key)) {
			for (const run of await this.listRuns()) {
				for (const stateKey of Object.keys(run.states)) {
					const existing = this.locators.get(stateKey);
					if (existing && existing !== run.id) {
						throw new Error("Continuation associated with multiple runs");
					}
					this.locators.set(stateKey, run.id);
				}
			}
			id = this.locators.get(key);
		}
		if (id) {
			const run = await this.getRun(id);
			// Retention never falls back to a stale legacy capability.
			return { text: run?.states[key] ?? null, runId: id };
		}
		return { text: await readText(path.join(this.directory, `${key}.json`)) };
	}

	async write(
		key: string,
		text: string | null,
		options: FileWriteOptions = {},
	): Promise<StateWriteResult> {
		if (this.readOnly) {
			throw new Error("Persistence session is read-only");
		}
		const existing = await this.read(key);
		const id = existing.runId ?? options.runId;
		if (id) {
			const operationId = randomUUID();
			const result = await this.transition(
				{
					operationId,
					runId: id,
					action: {
						kind: "state",
						key,
						text,
						expectedWaitRevision: options.expectedWaitRevision,
						...(options.decision ? { decision: options.decision } : {}),
					},
				},
				options.signal,
				options.assertInvocationCurrent,
			);
			if (result.status === "conflict") {
				throw new WaitConflictError();
			}

			this.locators.set(key, id);
			return {
				status: "committed",
				kind: "run",
				runId: id,
				operationId,
				revision: result.revision,
				confirmation: "acknowledged",
				signalAbortedAfterCommit: options.signal?.aborted === true,
			};
		}
		const file = path.join(this.directory, `${key}.json`);
		if (text === null) {
			await fs.unlink(file).catch((error) => {
				if (error.code !== "ENOENT") {
					throw error;
				}
			});
		} else {
			await ensureDirectory(this.directory);
			if (options.exclusive) {
				await writeFileAtomicExclusive(file, text, {
					...options.atomicWriteOptions,
					assertInvocationCurrent:
						options.assertInvocationCurrent ?? options.atomicWriteOptions?.assertInvocationCurrent,
				});
			} else {
				await writeFileAtomic(file, text, {
					...options.atomicWriteOptions,
					assertInvocationCurrent:
						options.assertInvocationCurrent ?? options.atomicWriteOptions?.assertInvocationCurrent,
					signal: options.signal,
				});
			}
		}

		return {
			status: "committed",
			kind: "state",
			confirmation: "acknowledged",
			signalAbortedAfterCommit: options.signal?.aborted === true,
		};
	}

	async transition(
		request: Transition,
		signal?: AbortSignal,
		assertInvocationCurrent?: () => void,
	): Promise<TransitionResult> {
		if (this.readOnly) {
			throw new Error("Persistence session is read-only");
		}
		const commit = () =>
			withFileLock({
				filePath: this.runPath(request.runId),
				signal,
				task: async () => {
					signal?.throwIfAborted();
					const current = await this.getRun(request.runId);
					const successor =
						request.action.kind === "handoff"
							? await this.read(request.action.successor.key)
							: undefined;
					const applied = applyTransition(current, request, {
						successorExists: successor?.runId !== undefined || successor?.text != null,
					});
					if (applied.status === "committed" && applied.run !== current) {
						try {
							await writeFileAtomic(
								this.runPath(request.runId),
								JSON.stringify(applied.run) + "\n",
								{ signal, assertInvocationCurrent },
							);
						} catch (error) {
							if (atomicWriteWasPublished(error)) {
								throw new PersistenceOutcomeUnknown(request.operationId, error, request.runId);
							}
							throw error;
						}
					}
					return { status: applied.status, revision: applied.revision };
				},
			});
		if (request.action.kind !== "handoff") return commit();
		// Reserve the fresh successor key before taking the run lock. State writers
		// use the same key lock, so another run cannot acquire this checkpoint.
		return withFileLock({
			filePath: keyToPath(this.directory, request.action.successor.key),
			signal,
			task: commit,
		});
	}

	async listState(prefix: string): Promise<Record<string, string>> {
		const values: Record<string, string> = {};
		let names: string[];
		try {
			names = await fs.readdir(this.directory);
		} catch (error) {
			if (error.code === "ENOENT") {
				return values;
			}
			throw error;
		}
		for (const name of names) {
			if (name.startsWith(prefix) && name.endsWith(".json")) {
				const text = await readText(path.join(this.directory, name));
				if (text !== null) {
					values[name.slice(0, -5)] = text;
				}
			}
		}
		return values;
	}

	async export(): Promise<LogicalArchive> {
		const states: Record<string, string> = {};
		let names: string[];
		try {
			names = await fs.readdir(this.directory);
		} catch (error) {
			if (error.code === "ENOENT") {
				names = [];
			} else {
				throw error;
			}
		}
		for (const name of names.sort()) {
			if (!/^[a-z0-9._-]+\.json$/.test(name)) {
				continue;
			}
			const text = await readText(path.join(this.directory, name));
			if (text !== null) {
				states[name.slice(0, -5)] = text;
			}
		}
		const runs = await this.listRuns();
		for (const run of runs) {
			for (const key of Object.keys(run.states)) {
				delete states[key];
			}
		}
		const archive: LogicalArchive = { version: 1, states, runs };
		validateArchive(archive);
		return archive;
	}

	async import(data: LogicalArchive): Promise<void> {
		if (this.readOnly) {
			throw new Error("Persistence session is read-only");
		}
		validateArchive(data);
		if (Object.keys((await this.export()).states).length || (await this.listRuns()).length) {
			throw new Error("Import destination must be empty");
		}
		await ensureDirectory(this.directory);
		for (const [key, text] of Object.entries(data.states)) {
			await this.write(key, text);
		}
		for (const run of data.runs) {
			await ensureDirectory(path.dirname(this.runPath(run.id)));
			await writeFileAtomic(this.runPath(run.id), JSON.stringify(run) + "\n");
		}
	}

	async acknowledge(runId: string, operationId: string): Promise<void> {
		if (this.readOnly) {
			throw new Error("Persistence session is read-only");
		}
		await withFileLock({
			filePath: this.runPath(runId),
			task: async () => {
				const run = await this.getRun(runId);
				const receipt = run?.receipts[operationId];
				if (!receipt || receipt.acknowledged) {
					return;
				}
				receipt.acknowledged = true;
				await writeFileAtomic(this.runPath(runId), JSON.stringify(run) + "\n");
			},
		});
	}

	async removeRun(id: string, before: string): Promise<boolean> {
		if (this.readOnly) {
			throw new Error("Persistence session is read-only");
		}
		return withFileLock({
			filePath: this.runPath(id),
			task: async () => {
				const run = await this.getRun(id);
				if (!run || !canRetain(run, before)) {
					return false;
				}
				for (const key of Object.keys(run.states)) {
					await fs.unlink(path.join(this.directory, `${key}.json`)).catch((error) => {
						if (error.code !== "ENOENT") {
							throw error;
						}
					});
				}
				await fs.unlink(this.runPath(id));
				return true;
			},
		});
	}

	async close(): Promise<void> {}
}
