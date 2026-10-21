import { readBackendSelection } from "./selection.js";
import { validateContinuations } from "./legacy.js";
import { assertSqliteRuntime } from "./sqlite-support.js";
import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import { ensureDirectory, writeFileAtomic } from "../state/atomic_file.js";
import { withFileLock } from "../state/file_lock.js";
import type { Backend, BackendKind } from "./backend.js";
import { FileBackend } from "./files.js";
import { fingerprint, validateArchive, type LogicalArchive } from "./model.js";

async function assertOffline(directory: string) {
	for (const name of await fs.readdir(directory)) {
		if (name.endsWith(".lock") && name !== ".backend-selection.lock") {
			throw new Error("State writers must be stopped before migration");
		}
		if (!name.startsWith(".session-")) {
			continue;
		}
		const lease = JSON.parse(await fs.readFile(path.join(directory, name), "utf8"));
		try {
			process.kill(lease.pid, 0);
			throw new Error("Close all persistence sessions before migration");
		} catch (error) {
			if (error.code !== "ESRCH") {
				throw error;
			}
		}
		await fs.unlink(path.join(directory, name));
	}
}

function canonical(data: LogicalArchive) {
	return fingerprint({
		version: data.version,
		states: Object.fromEntries(Object.entries(data.states).sort(([a], [b]) => a.localeCompare(b))),
		runs: [...data.runs].sort((a, b) => a.id.localeCompare(b.id)),
	});
}

async function backendFor(kind: "files" | "sqlite", directory: string): Promise<Backend> {
	if (kind === "files") {
		return new FileBackend(directory);
	}
	const { SqliteBackend } = await import("./sqlite.js");
	return new SqliteBackend(path.join(directory, "lobster.sqlite"));
}

export type MigrationPhase = "backed_up" | "imported" | "verified" | "prepared" | "activated";

export type MigrationOptions = {
	directory: string;
	backend: BackendKind;
	offline: boolean;
	onPhase?: (phase: MigrationPhase) => Promise<void>;
};

/**
 * Explicit offline conversion. Older binaries do not honor session leases, so the
 * caller must coordinate them before invoking this API. Originals remain private.
 */
export async function migrateStore({ directory, backend, offline, onPhase }: MigrationOptions) {
	if (offline !== true) {
		throw new Error("Migration requires an explicit offline writer-coordination declaration");
	}
	if (!["files", "sqlite"].includes(backend)) {
		throw new Error("Invalid migration backend");
	}
	if (backend === "sqlite") assertSqliteRuntime();

	directory = path.resolve(directory);
	await ensureDirectory(directory);
	return withFileLock({
		filePath: path.join(directory, ".backend-selection"),
		task: async () => {
			await assertOffline(directory);
			const selected = await readBackendSelection(directory);
			const from = selected?.backend ?? "files";
			if (from === "sqlite") {
				try {
					await fs.access(path.join(directory, "lobster.sqlite"));
				} catch {
					throw new Error("Selected SQLite database is missing; restore a verified backup");
				}
			}
			if (from === backend) {
				return { changed: false, backend };
			}
			const source = await backendFor(from, directory);
			let archive: LogicalArchive;
			try {
				archive = await source.export();
			} finally {
				await source.close();
			}
			validateArchive(archive);
			validateContinuations(archive);
			const digest = canonical(archive);
			const backup = path.join(directory, `.backup-${digest}.archive`);
			await writeFileAtomic(backup, JSON.stringify(archive) + "\n");
			await onPhase?.("backed_up");
			const staging = path.join(directory, `.migration-${randomUUID()}`);
			await ensureDirectory(staging);
			let destination: Backend;
			try {
				destination = await backendFor(backend, staging);
				try {
					await destination.import(archive);
					await onPhase?.("imported");
					const copied = await destination.export();
					if (canonical(copied) !== canonical(archive)) {
						throw new Error("Migration verification failed");
					}
					await onPhase?.("verified");
				} finally {
					await destination.close();
				}
				// After closing SQLite, all committed pages are checkpointed into this file.
				// A failed activation leaves the source selected and permits a fresh retry.
				if (backend === "sqlite") {
					const file = path.join(staging, "lobster.sqlite");
					await fs.chmod(file, 0o600);
					await fs.rename(file, path.join(directory, "lobster.sqlite"));
				} else {
					const oldFiles = path.join(directory, `.previous-files-${randomUUID()}`);
					await ensureDirectory(oldFiles);
					for (const name of await fs.readdir(directory)) {
						if (/^[a-z0-9._-]+\.json$/.test(name) || name === ".history") {
							await fs.rename(path.join(directory, name), path.join(oldFiles, name));
						}
					}
					for (const name of await fs.readdir(staging)) {
						await fs.rename(path.join(staging, name), path.join(directory, name));
					}
				}
				await onPhase?.("prepared");
				await writeFileAtomic(
					path.join(directory, ".backend"),
					JSON.stringify({ version: 1, backend }) + "\n",
				);
				await onPhase?.("activated");
				return { changed: true, backend, backup, digest };
			} finally {
				await fs.rm(staging, { recursive: true, force: true });
			}
		},
	});
}
