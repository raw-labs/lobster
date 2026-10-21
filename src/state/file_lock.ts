import { randomBytes } from "node:crypto";
import { promises as fsp, type Stats } from "node:fs";
import path from "node:path";
import { ensureDirectory } from "./atomic_file.js";

export const FILE_LOCK_RETRY_MS = 10;
const FILE_LOCK_ORPHAN_MS = 30_000;
const FILE_LOCK_HEARTBEAT_MS = 250;

function hasErrorCode(error: unknown, code: string): boolean {
	return error !== null && typeof error === "object" && "code" in error && error.code === code;
}

async function waitForFileLock(signal?: AbortSignal): Promise<void> {
	await new Promise<void>((resolve, reject) => {
		const onAbort = () => {
			clearTimeout(timer);
			try {
				signal?.throwIfAborted();
			} catch (err) {
				reject(err);
			}
		};
		const timer = setTimeout(() => {
			signal?.removeEventListener("abort", onAbort);
			resolve();
		}, FILE_LOCK_RETRY_MS);
		if (signal?.aborted) {
			onAbort();
			return;
		}
		signal?.addEventListener("abort", onAbort, { once: true });
	});
}

function isProcessAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (err: unknown) {
		return !hasErrorCode(err, "ESRCH");
	}
}

type FileLockOwner = {
	pid: number;
	processStartIdentity: string | null;
	nonce: string;
};

function parseFileLockOwner(ownerText: string): FileLockOwner | null {
	const parts = ownerText.trim().split(":");
	if (parts.length !== 3) {
		return null;
	}
	const pid = Number(parts[0]);
	const processStartIdentity = parts[1] || null;
	const nonce = parts[2];
	if (!Number.isInteger(pid) || pid <= 0 || !nonce) {
		return null;
	}
	return { pid, processStartIdentity, nonce };
}

async function readProcessStartIdentity(pid: number): Promise<string | null> {
	if (process.platform !== "linux") {
		return null;
	}
	try {
		const stat = await fsp.readFile(`/proc/${pid}/stat`, "utf8");
		const closeParen = stat.lastIndexOf(")");
		if (closeParen < 0) {
			return null;
		}
		// The remainder starts at procfs field 3; starttime is field 22.
		const fields = stat
			.slice(closeParen + 1)
			.trim()
			.split(/\s+/);
		return fields[19] || null;
	} catch {
		return null;
	}
}

async function isFileLockOld(lockPath: string): Promise<boolean> {
	try {
		const stat = await fsp.stat(lockPath);
		return Date.now() - stat.mtimeMs >= FILE_LOCK_ORPHAN_MS;
	} catch (err: unknown) {
		if (hasErrorCode(err, "ENOENT")) {
			return true;
		}
		throw err;
	}
}

async function hasExpiredFileLockLease(lockPath: string): Promise<boolean> {
	let first: Stats;
	try {
		first = await fsp.stat(lockPath);
	} catch (err: unknown) {
		if (hasErrorCode(err, "ENOENT")) {
			return true;
		}
		throw err;
	}
	if (Date.now() - first.mtimeMs < FILE_LOCK_ORPHAN_MS) {
		return false;
	}

	await new Promise<void>((resolve) => setTimeout(resolve, FILE_LOCK_HEARTBEAT_MS));
	try {
		const second = await fsp.stat(lockPath);
		return second.mtimeMs === first.mtimeMs && Date.now() - second.mtimeMs >= FILE_LOCK_ORPHAN_MS;
	} catch (err: unknown) {
		if (hasErrorCode(err, "ENOENT")) {
			return true;
		}
		throw err;
	}
}

export async function reclaimOrphanedFileLock(lockPath: string): Promise<boolean> {
	let observedLock: { dev: number; ino: number };
	try {
		const stat = await fsp.lstat(lockPath);
		observedLock = { dev: stat.dev, ino: stat.ino };
	} catch (err: unknown) {
		if (hasErrorCode(err, "ENOENT")) {
			return true;
		}
		throw err;
	}

	const ownerPath = path.join(lockPath, "owner");
	let observedOwner: string | undefined;
	let stale = false;
	try {
		observedOwner = await fsp.readFile(ownerPath, "utf8");
		const owner = parseFileLockOwner(observedOwner);
		if (owner && isProcessAlive(owner.pid)) {
			const processStartIdentity = await readProcessStartIdentity(owner.pid);
			if (owner.processStartIdentity && processStartIdentity) {
				stale = owner.processStartIdentity !== processStartIdentity;
			} else {
				// A live PID without a matching process-instance identity may have
				// been reused. Require a conservative expired lease and a second
				// unchanged observation before reclaiming it.
				stale = await hasExpiredFileLockLease(ownerPath);
			}
		} else if (owner) {
			stale = true;
		} else {
			stale = await isFileLockOld(lockPath);
		}
	} catch (err: unknown) {
		if (hasErrorCode(err, "ENOENT")) {
			stale = await isFileLockOld(lockPath);
		} else {
			throw err;
		}
	}
	if (!stale) {
		return false;
	}

	// Claim reclamation inside the observed directory before removing it. Renaming
	// `lockPath` directly is unsafe: another reclaimer can replace that pathname
	// with a live lock after the stale observation, and a later recursive cleanup
	// would then delete the new owner's lock while it is in use.
	const reclaimPath = path.join(lockPath, ".reclaiming");
	try {
		await fsp.mkdir(reclaimPath, { mode: 0o700 });
	} catch (err: unknown) {
		if (hasErrorCode(err, "ENOENT")) {
			return true;
		}
		if (hasErrorCode(err, "EEXIST")) {
			return false;
		}
		throw err;
	}

	try {
		const currentLock = await fsp.lstat(lockPath);
		if (currentLock.dev !== observedLock.dev || currentLock.ino !== observedLock.ino) {
			return false;
		}
		// A replacement directory can reuse the inode; its owner must still match too.
		let currentOwner: string | undefined;
		try {
			currentOwner = await fsp.readFile(ownerPath, "utf8");
		} catch (err: unknown) {
			if (!hasErrorCode(err, "ENOENT")) {
				throw err;
			}
		}
		if (currentOwner !== observedOwner) {
			return false;
		}
		await fsp.rm(lockPath, { recursive: true, force: true });
	} finally {
		// If the path changed before the reclamation marker was claimed, leave the
		// replacement lock intact and only remove our harmless marker.
		await fsp.rmdir(reclaimPath).catch(() => {});
	}
	return true;
}

async function stillOwnsFileLock(ownerPath: string, owner: string): Promise<boolean> {
	for (let attempt = 0; attempt < 2; attempt++) {
		try {
			return (await fsp.readFile(ownerPath, "utf8")) === owner;
		} catch (err: unknown) {
			if (hasErrorCode(err, "ENOENT") || attempt === 1) {
				return false;
			}
			await new Promise<void>((resolve) => setTimeout(resolve, FILE_LOCK_RETRY_MS));
		}
	}
	return false;
}

/**
 * Serialize a transition for an arbitrary durable file. Readers that need a
 * publish-or-rollback decision use the same lock as writers, rather than
 * observing an atomic rename that cancellation may still need to undo.
 */
export async function withFileLock<T>({
	filePath,
	signal,
	task,
}: {
	filePath: string;
	signal?: AbortSignal;
	task: () => Promise<T>;
}): Promise<T> {
	await ensureDirectory(path.dirname(filePath));
	const lockPath = `${filePath}.lock`;
	let acquired = false;
	let owner: string | undefined;
	let ownerWritten = false;
	let heartbeat: ReturnType<typeof setInterval> | undefined;
	try {
		while (!acquired) {
			signal?.throwIfAborted();
			try {
				await fsp.mkdir(lockPath, { mode: 0o700 });
				acquired = true;
				const processStartIdentity = await readProcessStartIdentity(process.pid);
				owner = `${process.pid}:${processStartIdentity ?? ""}:${randomBytes(6).toString("hex")}\n`;
				await fsp.writeFile(path.join(lockPath, "owner"), owner, {
					encoding: "utf8",
					mode: 0o600,
				});
				ownerWritten = true;
				const ownerPath = path.join(lockPath, "owner");
				heartbeat = setInterval(() => {
					void fsp.utimes(ownerPath, new Date(), new Date()).catch(() => {});
				}, FILE_LOCK_HEARTBEAT_MS);
				heartbeat.unref?.();
			} catch (err: unknown) {
				if (acquired) {
					throw err;
				}
				if (!hasErrorCode(err, "EEXIST")) {
					throw err;
				}
				if (!(await reclaimOrphanedFileLock(lockPath))) {
					await waitForFileLock(signal);
				}
			}
		}
		return await task();
	} finally {
		if (heartbeat) {
			clearInterval(heartbeat);
		}
		if (acquired) {
			const ownerPath = path.join(lockPath, "owner");
			if (!ownerWritten || !owner || (await stillOwnsFileLock(ownerPath, owner))) {
				// Detach an owned lock before best-effort cleanup. If a filesystem error
				// prevents removing the detached directory, the canonical lock path is
				// already free for a subsequent operation; leaving it in place would
				// instead look like a live lock owned by this still-running process.
				const cleanupPath = `${lockPath}.release-${process.pid}-${randomBytes(6).toString("hex")}`;
				try {
					await fsp.rename(lockPath, cleanupPath);
					await fsp.rm(cleanupPath, { recursive: true, force: true }).catch(() => {});
				} catch (err: unknown) {
					if (!hasErrorCode(err, "ENOENT")) {
						await fsp.rm(lockPath, { recursive: true, force: true }).catch(() => {});
					}
				}
			}
		}
	}
}
