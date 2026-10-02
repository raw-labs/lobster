import { withStoreContext } from "../persistence/execution.js";
import { sessionFromEnv, recorderFromEnv } from "../persistence/session.js";
import {
	atomicWriteWasPublished,
	ensureDirectory,
	isAtomicExclusiveUnsupportedError,
	writeFileAtomic,
	writeFileAtomicExclusive,
} from "./atomic_file.js";
import type { AtomicWriteOptions, AtomicExclusiveWriteOptions } from "./atomic_file.js";
export {
	atomicWriteWasPublished,
	ensureDirectory,
	isAtomicExclusiveUnsupportedError,
	writeFileAtomic,
	writeFileAtomicExclusive,
	stripExtendedLengthPrefix,
} from "./atomic_file.js";
import os from "node:os";
import path from "node:path";
import { promises as fsp } from "node:fs";
import { randomBytes } from "node:crypto";

const CONSUMED_RESUME_STATE_TYPE = "lobster.consumed-resume-state.v1";

export type ConsumedResumeState = {
	type: typeof CONSUMED_RESUME_STATE_TYPE;
	consumedAt: string;
	claimId: string;
};

export function isConsumedResumeState(value: unknown): value is ConsumedResumeState {
	return (
		value !== null &&
		typeof value === "object" &&
		(value as { type?: unknown }).type === CONSUMED_RESUME_STATE_TYPE
	);
}

export function defaultStateDir(env) {
	const session = sessionFromEnv(env);
	if (session) return session.directory;
	return (
		(env?.LOBSTER_STATE_DIR && String(env.LOBSTER_STATE_DIR).trim()) ||
		path.join(os.homedir(), ".lobster", "state")
	);
}

export function keyToPath(stateDir, key) {
	let safe = String(key)
		.toLowerCase()
		.replace(/[^a-z0-9._-]+/g, "_")
		.replace(/_+/g, "_");
	if (safe.startsWith("_")) safe = safe.slice(1);
	if (safe.endsWith("_")) safe = safe.slice(0, -1);
	if (!safe) throw new Error("state key is empty/invalid");
	return path.join(stateDir, `${safe}.json`);
}

export function stableStringify(value) {
	return JSON.stringify(value, (_k, v) => {
		if (v && typeof v === "object" && !Array.isArray(v)) {
			return Object.fromEntries(
				Object.keys(v)
					.sort()
					.map((k) => [k, v[k]]),
			);
		}
		return v;
	});
}

const STATE_LOCK_RETRY_MS = 10;
const STATE_LOCK_ORPHAN_MS = 30_000;
const STATE_LOCK_HEARTBEAT_MS = 250;
const TERMINAL_RESUME_CLEANUP_TIMEOUT_MS = STATE_LOCK_RETRY_MS * 10;

export function isJsonSyntaxError(err) {
	return err instanceof SyntaxError;
}

async function waitForStateLock(signal?: AbortSignal) {
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
		}, STATE_LOCK_RETRY_MS);
		if (signal?.aborted) {
			onAbort();
			return;
		}
		signal?.addEventListener("abort", onAbort, { once: true });
	});
}

function isProcessAlive(pid: number) {
	try {
		process.kill(pid, 0);
		return true;
	} catch (err: any) {
		return err?.code !== "ESRCH";
	}
}

type StateLockOwner = {
	pid: number;
	processStartIdentity: string | null;
	nonce: string;
};

function parseStateLockOwner(ownerText: string): StateLockOwner | null {
	const parts = ownerText.trim().split(":");
	if (parts.length !== 3) return null;
	const pid = Number(parts[0]);
	const processStartIdentity = parts[1] || null;
	const nonce = parts[2];
	if (!Number.isInteger(pid) || pid <= 0 || !nonce) return null;
	return { pid, processStartIdentity, nonce };
}

async function readProcessStartIdentity(pid: number): Promise<string | null> {
	if (process.platform !== "linux") return null;
	try {
		const stat = await fsp.readFile(`/proc/${pid}/stat`, "utf8");
		const closeParen = stat.lastIndexOf(")");
		if (closeParen < 0) return null;
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

async function isStateLockOld(lockPath: string) {
	try {
		const stat = await fsp.stat(lockPath);
		return Date.now() - stat.mtimeMs >= STATE_LOCK_ORPHAN_MS;
	} catch (err: any) {
		if (err?.code === "ENOENT") return true;
		throw err;
	}
}

async function hasExpiredStateLockLease(lockPath: string) {
	let first;
	try {
		first = await fsp.stat(lockPath);
	} catch (err: any) {
		if (err?.code === "ENOENT") return true;
		throw err;
	}
	if (Date.now() - first.mtimeMs < STATE_LOCK_ORPHAN_MS) return false;

	await new Promise<void>((resolve) => setTimeout(resolve, STATE_LOCK_HEARTBEAT_MS));
	try {
		const second = await fsp.stat(lockPath);
		return second.mtimeMs === first.mtimeMs && Date.now() - second.mtimeMs >= STATE_LOCK_ORPHAN_MS;
	} catch (err: any) {
		if (err?.code === "ENOENT") return true;
		throw err;
	}
}

async function reclaimOrphanedStateLock(lockPath: string) {
	let observedLock: { dev: number; ino: number };
	try {
		const stat = await fsp.lstat(lockPath);
		observedLock = { dev: stat.dev, ino: stat.ino };
	} catch (err: any) {
		if (err?.code === "ENOENT") return true;
		throw err;
	}

	const ownerPath = path.join(lockPath, "owner");
	let observedOwner: string | undefined;
	let stale = false;
	try {
		observedOwner = await fsp.readFile(ownerPath, "utf8");
		const owner = parseStateLockOwner(observedOwner);
		if (owner && isProcessAlive(owner.pid)) {
			const processStartIdentity = await readProcessStartIdentity(owner.pid);
			if (owner.processStartIdentity && processStartIdentity) {
				stale = owner.processStartIdentity !== processStartIdentity;
			} else {
				// A live PID without a matching process-instance identity may have
				// been reused. Require a conservative expired lease and a second
				// unchanged observation before reclaiming it.
				stale = await hasExpiredStateLockLease(ownerPath);
			}
		} else if (owner) {
			stale = true;
		} else {
			stale = await isStateLockOld(lockPath);
		}
	} catch (err: any) {
		if (err?.code === "ENOENT") {
			stale = await isStateLockOld(lockPath);
		} else {
			throw err;
		}
	}
	if (!stale) return false;

	// Claim reclamation inside the observed directory before removing it. Renaming
	// `lockPath` directly is unsafe: another reclaimer can replace that pathname
	// with a live lock after the stale observation, and a later recursive cleanup
	// would then delete the new owner's lock while it is in use.
	const reclaimPath = path.join(lockPath, ".reclaiming");
	try {
		await fsp.mkdir(reclaimPath, { mode: 0o700 });
	} catch (err: any) {
		if (err?.code === "ENOENT") return true;
		if (err?.code === "EEXIST") return false;
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
		} catch (err: any) {
			if (err?.code !== "ENOENT") throw err;
		}
		if (currentOwner !== observedOwner) return false;
		await fsp.rm(lockPath, { recursive: true, force: true });
	} finally {
		// If the path changed before the reclamation marker was claimed, leave the
		// replacement lock intact and only remove our harmless marker.
		await fsp.rmdir(reclaimPath).catch(() => {});
	}
	return true;
}

async function stillOwnsStateLock(ownerPath: string, owner: string) {
	for (let attempt = 0; attempt < 2; attempt++) {
		try {
			return (await fsp.readFile(ownerPath, "utf8")) === owner;
		} catch (err: any) {
			if (err?.code === "ENOENT" || attempt === 1) return false;
			await new Promise<void>((resolve) => setTimeout(resolve, STATE_LOCK_RETRY_MS));
		}
	}
	return false;
}

async function withStateKeyLock<T>({
	env,
	key,
	signal,
	task,
}: {
	env: Record<string, string | undefined>;
	key: string;
	signal?: AbortSignal;
	task: () => Promise<T>;
}): Promise<T> {
	const stateDir = defaultStateDir(env);
	return withFileLock({
		filePath: keyToPath(stateDir, key),
		signal,
		task,
	});
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
				}, STATE_LOCK_HEARTBEAT_MS);
				heartbeat.unref?.();
			} catch (err: any) {
				if (acquired) throw err;
				if (err?.code !== "EEXIST") throw err;
				if (!(await reclaimOrphanedStateLock(lockPath))) {
					await waitForStateLock(signal);
				}
			}
		}
		return await task();
	} finally {
		if (heartbeat) clearInterval(heartbeat);
		if (acquired) {
			const ownerPath = path.join(lockPath, "owner");
			if (!ownerWritten || !owner || (await stillOwnsStateLock(ownerPath, owner))) {
				// Detach an owned lock before best-effort cleanup. If a filesystem error
				// prevents removing the detached directory, the canonical lock path is
				// already free for a subsequent operation; leaving it in place would
				// instead look like a live lock owned by this still-running process.
				const cleanupPath = `${lockPath}.release-${process.pid}-${randomBytes(6).toString("hex")}`;
				try {
					await fsp.rename(lockPath, cleanupPath);
					await fsp.rm(cleanupPath, { recursive: true, force: true }).catch(() => {});
				} catch (err: any) {
					if (err?.code !== "ENOENT") {
						await fsp.rm(lockPath, { recursive: true, force: true }).catch(() => {});
					}
				}
			}
		}
	}
}

function isOptionalApprovalIndexPersistenceError(err: any): boolean {
	return (
		isAtomicExclusiveUnsupportedError(err) ||
		["EACCES", "EDQUOT", "EIO", "ENOSPC", "EPERM", "EROFS"].includes(err?.code)
	);
}

/** Backend-neutral committed bytes, used to retain trusted in-process provenance. */
async function readStateTextInternal({
	env,
	key,
}: {
	env: Record<string, string | undefined>;
	key: string;
}): Promise<string | null> {
	const session = sessionFromEnv(env);
	if (session) return (await session.read(key)).text;
	try {
		return await fsp.readFile(keyToPath(defaultStateDir(env), key), "utf8");
	} catch (error) {
		if (error.code === "ENOENT") return null;
		throw error;
	}
}

async function readStateJsonInternal({
	env,
	key,
	signal = undefined,
}: {
	env: Record<string, string | undefined>;
	key: string;
	signal?: AbortSignal;
}) {
	signal?.throwIfAborted();

	try {
		const text = await readStateText({ env, key });
		if (text === null) return null;
		const value = JSON.parse(text);
		signal?.throwIfAborted();
		return value;
	} catch (err) {
		if (err?.code === "ENOENT") return null;
		throw err;
	}
}

/**
 * Read a state record only after any in-progress publish-or-rollback transition
 * for the same key has settled. Callers that compose state with another durable
 * resource use this to avoid observing a value that cancellation may undo.
 */
async function readStateJsonWithLockInternal({
	env,
	key,
	signal = undefined,
}: {
	env: Record<string, string | undefined>;
	key: string;
	signal?: AbortSignal;
}) {
	try {
		return await withStateKeyLock({ env, key, signal, task: () => readStateJson({ env, key }) });
	} catch (err: any) {
		// A readable state directory can deliberately be mounted read-only. In that
		// case no writer can begin a paired publish-or-rollback transition, so use
		// the non-mutating JSON read instead of requiring creation of a lock path.
		if (["EACCES", "EPERM", "EROFS"].includes(err?.code)) {
			return readStateJson({ env, key, signal });
		}
		throw err;
	}
}

async function writeStateJsonUnlocked({
	env,
	key,
	value,
	signal = undefined,
	atomicWriteOptions = undefined,
}: {
	env: Record<string, string | undefined>;
	key: string;
	value: unknown;
	signal?: AbortSignal;
	atomicWriteOptions?: Omit<AtomicWriteOptions, "signal">;
}) {
	const stateDir = defaultStateDir(env);
	const filePath = keyToPath(stateDir, key);

	const session = sessionFromEnv(env);
	if (session) {
		const recorder = recorderFromEnv(env);
		const result = await session.write(key, JSON.stringify(value, null, 2) + "\n", {
			signal,
			atomicWriteOptions,
			...(recorder?.decisionKey === key.replace("workflow-resume_", "workflow_resume_")
				? { decision: recorder.decision }
				: {}),
			...(/^(pipeline_resume_|workflow_resume_|sdk_resume_)/.test(key) && recorder
				? { runId: recorder.runId }
				: {}),
		});
		if (recorder && isConsumedResumeState(value)) recorder.ownsExecution = true;
		return result;
	}
	await ensureDirectory(stateDir);
	return writeFileAtomic(filePath, JSON.stringify(value, null, 2) + "\n", {
		...atomicWriteOptions,
		signal,
	});
}

async function writeStateJsonInternal({
	env,
	key,
	value,
	signal = undefined,
	atomicWriteOptions = undefined,
}: {
	env: Record<string, string | undefined>;
	key: string;
	value: unknown;
	signal?: AbortSignal;
	atomicWriteOptions?: Omit<AtomicWriteOptions, "signal">;
}) {
	return withStateKeyLock({
		env,
		key,
		signal,
		task: () => writeStateJsonUnlocked({ env, key, value, signal, atomicWriteOptions }),
	});
}

/**
 * Retire a resume capability before its first unsafe execution boundary. The
 * replacement is deliberately persistent: a failed later unlink must not make
 * the original token replayable.
 */
async function consumeResumeStateInternal({
	env,
	key,
	expectedState,
	signal = undefined,
}: {
	env: Record<string, string | undefined>;
	key: string;
	expectedState: unknown;
	signal?: AbortSignal;
}) {
	return withStateKeyLock({
		env,
		key,
		signal,
		task: async () => {
			const currentState = await readStateJson({ env, key });
			// A resume snapshot is loaded before command/workflow setup. Re-check it
			// while holding the state lock so two callers cannot both turn the same
			// approval into an executable invocation.
			if (stableStringify(currentState) !== stableStringify(expectedState)) {
				return { consumed: false as const };
			}
			const claimId = randomBytes(16).toString("hex");
			try {
				const result = await writeStateJsonUnlocked({
					env,
					key,
					value: {
						type: CONSUMED_RESUME_STATE_TYPE,
						consumedAt: new Date().toISOString(),
						claimId,
					},
					signal,
				});
				return {
					consumed: true as const,
					claimId,
					signalAbortedAfterCommit: result?.signalAbortedAfterCommit === true,
				};
			} catch (err) {
				if (atomicWriteWasPublished(err)) {
					const latest = await readStateJson({ env, key });
					if (isConsumedResumeState(latest) && latest.claimId === claimId) {
						await writeStateJsonUnlocked({ env, key, value: expectedState });
					}
				}
				throw err;
			}
		},
	});
}

/**
 * Undo a just-published consumed marker only when it is still owned by the
 * caller's pre-dispatch claim. This never overwrites a replacement state or a
 * concurrent claimant's marker.
 */
async function restoreConsumedResumeStateInternal({
	env,
	key,
	expectedState,
	claimId,
}: {
	env: Record<string, string | undefined>;
	key: string;
	expectedState: unknown;
	claimId: string;
}) {
	return withStateKeyLock({
		env,
		key,
		task: async () => {
			const currentState = await readStateJson({ env, key });
			if (!isConsumedResumeState(currentState) || currentState.claimId !== claimId) {
				return false;
			}
			await writeStateJsonUnlocked({ env, key, value: expectedState });
			return true;
		},
	});
}

/**
 * Retire an unconsumed capability at a safe terminal boundary. The snapshot is
 * first replaced by a caller-owned marker under the state lock, so a
 * cancellation can restore only the state this caller claimed. A stale resume
 * that merely observed an earlier snapshot can never recreate a state another
 * resume has already settled.
 */
async function deleteResumeStateWithRollbackInternal({
	env,
	key,
	expectedState,
	signal = undefined,
}: {
	env: Record<string, string | undefined>;
	key: string;
	expectedState: unknown;
	signal?: AbortSignal;
}): Promise<boolean> {
	return withStateKeyLock({
		env,
		key,
		signal,
		task: async () => {
			signal?.throwIfAborted();
			const currentState = await readStateJson({ env, key });
			if (stableStringify(currentState) !== stableStringify(expectedState)) return false;

			const claimId = randomBytes(16).toString("hex");
			let claimPublished = false;
			let claimMayBePublished = false;
			let deleted = false;
			try {
				let result;
				try {
					result = await writeStateJsonUnlocked({
						env,
						key,
						value: {
							type: CONSUMED_RESUME_STATE_TYPE,
							consumedAt: new Date().toISOString(),
							claimId,
						},
						signal,
					});
				} catch (err) {
					claimMayBePublished = atomicWriteWasPublished(err);
					throw err;
				}
				claimPublished = true;
				if (result?.signalAbortedAfterCommit) signal?.throwIfAborted();
				signal?.throwIfAborted();
				await deleteStateJsonUnlocked({ env, key });
				deleted = true;
				signal?.throwIfAborted();
				return true;
			} catch (err) {
				if ((signal?.aborted && claimPublished) || claimMayBePublished) {
					const latest = await readStateJson({ env, key });
					if (
						(deleted && latest === null) ||
						(isConsumedResumeState(latest) && latest.claimId === claimId)
					) {
						await writeStateJsonUnlocked({ env, key, value: expectedState });
					}
				}
				throw err;
			}
		},
	});
}

/**
 * Check physical state presence without parsing it. Cancellation uses this to
 * retain the authoritative workflow spelling even if the state file is corrupt.
 */
async function stateJsonExistsInternal({
	env,
	key,
}: {
	env: Record<string, string | undefined>;
	key: string;
}) {
	const session = sessionFromEnv(env);
	if (session) return (await session.read(key)).text !== null;
	const filePath = keyToPath(defaultStateDir(env), key);
	try {
		await fsp.access(filePath);
		return true;
	} catch (err: any) {
		if (err?.code === "ENOENT") return false;
		throw err;
	}
}

async function deleteStateJsonUnlocked({ env, key }) {
	const session = sessionFromEnv(env);
	if (session) {
		const before = await session.read(key);
		const currentRecorder = recorderFromEnv(env);
		await session.write(
			key,
			null,
			currentRecorder?.decisionKey === key.replace("workflow-resume_", "workflow_resume_")
				? { decision: currentRecorder.decision }
				: {},
		);
		if (before.runId && before.text !== null) {
			const recorder = recorderFromEnv(env);
			if (recorder?.runId === before.runId) recorder.ownsExecution = true;
		}
		return;
	}
	const stateDir = defaultStateDir(env);
	const filePath = keyToPath(stateDir, key);
	try {
		await fsp.unlink(filePath);
	} catch (err) {
		if (err?.code === "ENOENT") return;
		throw err;
	}
}

async function deleteStateJsonInternal({
	env,
	key,
	signal = undefined,
}: {
	env: Record<string, string | undefined>;
	key: string;
	signal?: AbortSignal;
}) {
	return withStateKeyLock({
		env,
		key,
		signal,
		task: () => deleteStateJsonUnlocked({ env, key }),
	});
}

/**
 * After an effect has started, its consumed marker already prevents replay.
 * Give terminal cleanup a small, bounded opportunity to remove that marker,
 * but never let a live state writer turn cancellation into an unbounded wait.
 */
async function deleteStateJsonWithBoundedResumeCleanupInternal({
	env,
	key,
}: {
	env: Record<string, string | undefined>;
	key: string;
}) {
	const controller = new AbortController();
	const timeout = setTimeout(() => controller.abort(), TERMINAL_RESUME_CLEANUP_TIMEOUT_MS);
	try {
		await deleteStateJson({ env, key, signal: controller.signal });
	} finally {
		clearTimeout(timeout);
	}
}

/**
 * Retire a resume capability only while it is still a live, unclaimed state.
 * A consumed marker belongs to a resume that has already started its atomic
 * predecessor-to-successor handoff, so cancellation must not delete it and
 * report success while that successor remains executable.
 */
async function deleteUnconsumedResumeStateInternal({
	env,
	key,
	signal = undefined,
}: {
	env: Record<string, string | undefined>;
	key: string;
	signal?: AbortSignal;
}): Promise<"deleted" | "missing" | "claimed"> {
	return withStateKeyLock({
		env,
		key,
		signal,
		task: async () => {
			let currentState: unknown;
			try {
				currentState = await readStateJson({ env, key });
			} catch (err) {
				// A corrupt state is not resumable, so explicit cancellation may still
				// remove it. This keeps the legacy workflow-alias recovery behavior.
				if (!isJsonSyntaxError(err)) throw err;
				await deleteStateJsonUnlocked({ env, key });
				return "deleted";
			}
			if (currentState === null) return "missing";
			if (isConsumedResumeState(currentState)) return "claimed";
			await deleteStateJsonUnlocked({ env, key });
			return "deleted";
		},
	});
}

function sanitizeApprovalId(approvalId: string): string {
	return approvalId.replace(/[^a-f0-9]/g, "");
}

/**
 * Generate a short, human-friendly approval ID (8 hex chars).
 * These are easy to copy/paste in chat interfaces where full
 * base64url resume tokens are unwieldy.
 */
export function generateApprovalId(): string {
	return randomBytes(4).toString("hex");
}

/**
 * Write a reverse-index file that maps approvalId → stateKey.
 * Call this after writeStateJson to enable short-ID resume.
 */
async function writeApprovalIndexInternal({
	env,
	stateKey,
	approvalId,
	options,
}: {
	env: Record<string, string | undefined>;
	stateKey: string;
	approvalId: string;
	options?: AtomicExclusiveWriteOptions;
}) {
	const stateDir = defaultStateDir(env);
	const safe = sanitizeApprovalId(approvalId);
	if (!safe) return;
	await ensureDirectory(stateDir);
	const indexPath = path.join(stateDir, `approval_${safe}.json`);
	const session = sessionFromEnv(env);
	if (session) {
		await session.write(
			`approval_${safe}`,
			JSON.stringify({ stateKey, createdAt: new Date().toISOString() }) + "\n",
			{ exclusive: true, atomicWriteOptions: options },
		);
		return;
	}
	await writeFileAtomicExclusive(
		indexPath,
		JSON.stringify({ stateKey, createdAt: new Date().toISOString() }) + "\n",
		options,
	);
}

/**
 * Create a unique approval ID index without ever overwriting an existing mapping.
 */
async function createApprovalIndexInternal({
	env,
	stateKey,
	options,
}: {
	env: Record<string, string | undefined>;
	stateKey: string;
	options?: AtomicExclusiveWriteOptions;
}): Promise<string | null> {
	for (let attempt = 0; attempt < 16; attempt++) {
		const approvalId = generateApprovalId();
		try {
			await writeApprovalIndex({ env, stateKey, approvalId, options });
			return approvalId;
		} catch (err: any) {
			if (err?.code === "EEXIST") continue;
			if (isOptionalApprovalIndexPersistenceError(err)) return null;
			throw err;
		}
	}
	throw new Error("Could not allocate a unique approval ID");
}

/**
 * Look up a state key by short approval ID.
 * Returns the stateKey string or null if not found.
 */
async function findStateKeyByApprovalIdInternal({
	env,
	approvalId,
}: {
	env: Record<string, string | undefined>;
	approvalId: string;
}): Promise<string | null> {
	const safe = sanitizeApprovalId(approvalId);
	if (!safe) return null;
	try {
		const text = await readStateText({ env, key: `approval_${safe}` });
		if (text === null) return null;
		const data = JSON.parse(text);
		return typeof data?.stateKey === "string" ? data.stateKey : null;
	} catch (err: any) {
		if (err?.code === "ENOENT") return null;
		if (isJsonSyntaxError(err)) return null;
		throw err;
	}
}

/**
 * Delete the approval ID index file (cleanup after resume or cancel).
 */
async function deleteApprovalIdInternal({
	env,
	approvalId,
}: {
	env: Record<string, string | undefined>;
	approvalId: string;
}) {
	const stateDir = defaultStateDir(env);
	const safe = sanitizeApprovalId(approvalId);
	if (!safe) return;
	const session = sessionFromEnv(env);
	if (session) {
		await session.write(`approval_${safe}`, null);
		return;
	}
	const indexPath = path.join(stateDir, `approval_${safe}.json`);
	try {
		await fsp.unlink(indexPath);
	} catch (err: any) {
		if (err?.code === "ENOENT") return;
		throw err;
	}
}

/**
 * Clean up any approval index file that points to the given stateKey.
 * Used when resuming via --token (where we don't know the approvalId).
 * Scans index files in the state dir — O(n) but n is tiny in practice.
 */
async function cleanupApprovalIndexByStateKeyInternal({
	env,
	stateKey,
}: {
	env: Record<string, string | undefined>;
	stateKey: string;
}) {
	const session = sessionFromEnv(env);
	if (session) {
		for (const [key, text] of Object.entries(await session.backend.listState("approval_"))) {
			if (!key.startsWith("approval_")) continue;
			let data: any;
			try {
				data = JSON.parse(text);
			} catch {
				continue;
			}
			if (data?.stateKey === stateKey) {
				await session.write(key, null);
				return;
			}
		}
		return;
	}
	const stateDir = defaultStateDir(env);
	let files: string[];
	try {
		files = await fsp.readdir(stateDir);
	} catch (err: any) {
		if (err?.code === "ENOENT") return;
		throw err;
	}
	for (const file of files) {
		if (!file.startsWith("approval_") || !file.endsWith(".json")) continue;
		try {
			const text = await fsp.readFile(path.join(stateDir, file), "utf8");
			const data = JSON.parse(text);
			if (data?.stateKey === stateKey) {
				await fsp.unlink(path.join(stateDir, file)).catch(() => {});
				return; // one index per stateKey
			}
		} catch {
			/* skip corrupt files */
		}
	}
}

async function diffAndStoreInternal({
	env,
	key,
	value,
	signal = undefined,
	atomicWriteOptions = undefined,
	afterStore = undefined,
}: {
	env: Record<string, string | undefined>;
	key: string;
	value: unknown;
	signal?: AbortSignal;
	atomicWriteOptions?: Omit<AtomicWriteOptions, "signal">;
	afterStore?: (snapshot: { before: unknown; after: unknown; changed: boolean }) => Promise<void>;
}) {
	return withStateKeyLock({
		env,
		key,
		signal,
		task: async () => {
			const beforeExists = await stateJsonExists({ env, key });
			const before = await readStateJson({ env, key }).catch((err) => {
				if (isJsonSyntaxError(err)) return null;
				throw err;
			});
			const changed = stableStringify(before) !== stableStringify(value);
			const snapshot = { before, after: value, changed };
			let stored = false;
			try {
				signal?.throwIfAborted();
				await writeStateJsonUnlocked({ env, key, value, signal, atomicWriteOptions });
				stored = true;
				signal?.throwIfAborted();
				await afterStore?.(snapshot);
			} catch (err) {
				// A caller can publish another resource while this state lock is held.
				// If that coordinated publication fails, restore the state snapshot before
				// releasing the lock so readers never reuse a cancelled result.
				const stateWasPublished = stored || atomicWriteWasPublished(err);
				if (stateWasPublished && (signal?.aborted || afterStore || atomicWriteWasPublished(err))) {
					if (!beforeExists) {
						await deleteStateJsonUnlocked({ env, key });
					} else {
						await writeStateJsonUnlocked({ env, key, value: before });
					}
				}
				throw err;
			}
			return snapshot;
		},
	});
}

export async function readStateText(
	options: Parameters<typeof readStateTextInternal>[0],
): ReturnType<typeof readStateTextInternal> {
	return withStoreContext(
		{ env: options.env, readOnly: true, signal: (options as any).signal },
		(ctx) => readStateTextInternal({ ...options, env: ctx.env }),
	);
}

export async function readStateJson(
	options: Parameters<typeof readStateJsonInternal>[0],
): ReturnType<typeof readStateJsonInternal> {
	return withStoreContext(
		{ env: options.env, readOnly: true, signal: (options as any).signal },
		(ctx) => readStateJsonInternal({ ...options, env: ctx.env }),
	);
}

export async function readStateJsonWithLock(
	options: Parameters<typeof readStateJsonWithLockInternal>[0],
): ReturnType<typeof readStateJsonWithLockInternal> {
	return withStoreContext(
		{ env: options.env, readOnly: true, signal: (options as any).signal },
		(ctx) => readStateJsonWithLockInternal({ ...options, env: ctx.env }),
	);
}

export async function writeStateJson(
	options: Parameters<typeof writeStateJsonInternal>[0],
): ReturnType<typeof writeStateJsonInternal> {
	return withStoreContext({ env: options.env, signal: (options as any).signal }, (ctx) =>
		writeStateJsonInternal({ ...options, env: ctx.env }),
	);
}

export async function consumeResumeState(
	options: Parameters<typeof consumeResumeStateInternal>[0],
): ReturnType<typeof consumeResumeStateInternal> {
	return withStoreContext({ env: options.env, signal: (options as any).signal }, (ctx) =>
		consumeResumeStateInternal({ ...options, env: ctx.env }),
	);
}

export async function restoreConsumedResumeState(
	options: Parameters<typeof restoreConsumedResumeStateInternal>[0],
): ReturnType<typeof restoreConsumedResumeStateInternal> {
	return withStoreContext({ env: options.env, signal: (options as any).signal }, (ctx) =>
		restoreConsumedResumeStateInternal({ ...options, env: ctx.env }),
	);
}

export async function deleteResumeStateWithRollback(
	options: Parameters<typeof deleteResumeStateWithRollbackInternal>[0],
): ReturnType<typeof deleteResumeStateWithRollbackInternal> {
	return withStoreContext({ env: options.env, signal: (options as any).signal }, (ctx) =>
		deleteResumeStateWithRollbackInternal({ ...options, env: ctx.env }),
	);
}

export async function stateJsonExists(
	options: Parameters<typeof stateJsonExistsInternal>[0],
): ReturnType<typeof stateJsonExistsInternal> {
	return withStoreContext(
		{ env: options.env, readOnly: true, signal: (options as any).signal },
		(ctx) => stateJsonExistsInternal({ ...options, env: ctx.env }),
	);
}

export async function deleteStateJson(
	options: Parameters<typeof deleteStateJsonInternal>[0],
): ReturnType<typeof deleteStateJsonInternal> {
	return withStoreContext({ env: options.env, signal: (options as any).signal }, (ctx) =>
		deleteStateJsonInternal({ ...options, env: ctx.env }),
	);
}

export async function deleteStateJsonWithBoundedResumeCleanup(
	options: Parameters<typeof deleteStateJsonWithBoundedResumeCleanupInternal>[0],
): ReturnType<typeof deleteStateJsonWithBoundedResumeCleanupInternal> {
	return withStoreContext({ env: options.env, signal: (options as any).signal }, (ctx) =>
		deleteStateJsonWithBoundedResumeCleanupInternal({ ...options, env: ctx.env }),
	);
}

export async function deleteUnconsumedResumeState(
	options: Parameters<typeof deleteUnconsumedResumeStateInternal>[0],
): ReturnType<typeof deleteUnconsumedResumeStateInternal> {
	return withStoreContext({ env: options.env, signal: (options as any).signal }, (ctx) =>
		deleteUnconsumedResumeStateInternal({ ...options, env: ctx.env }),
	);
}

export async function writeApprovalIndex(
	options: Parameters<typeof writeApprovalIndexInternal>[0],
): ReturnType<typeof writeApprovalIndexInternal> {
	return withStoreContext({ env: options.env, signal: (options as any).signal }, (ctx) =>
		writeApprovalIndexInternal({ ...options, env: ctx.env }),
	);
}

export async function createApprovalIndex(
	options: Parameters<typeof createApprovalIndexInternal>[0],
): ReturnType<typeof createApprovalIndexInternal> {
	return withStoreContext({ env: options.env, signal: (options as any).signal }, (ctx) =>
		createApprovalIndexInternal({ ...options, env: ctx.env }),
	);
}

export async function findStateKeyByApprovalId(
	options: Parameters<typeof findStateKeyByApprovalIdInternal>[0],
): ReturnType<typeof findStateKeyByApprovalIdInternal> {
	return withStoreContext(
		{ env: options.env, readOnly: true, signal: (options as any).signal },
		(ctx) => findStateKeyByApprovalIdInternal({ ...options, env: ctx.env }),
	);
}

export async function deleteApprovalId(
	options: Parameters<typeof deleteApprovalIdInternal>[0],
): ReturnType<typeof deleteApprovalIdInternal> {
	return withStoreContext({ env: options.env, signal: (options as any).signal }, (ctx) =>
		deleteApprovalIdInternal({ ...options, env: ctx.env }),
	);
}

export async function cleanupApprovalIndexByStateKey(
	options: Parameters<typeof cleanupApprovalIndexByStateKeyInternal>[0],
): ReturnType<typeof cleanupApprovalIndexByStateKeyInternal> {
	return withStoreContext({ env: options.env, signal: (options as any).signal }, (ctx) =>
		cleanupApprovalIndexByStateKeyInternal({ ...options, env: ctx.env }),
	);
}

export async function diffAndStore(
	options: Parameters<typeof diffAndStoreInternal>[0],
): ReturnType<typeof diffAndStoreInternal> {
	return withStoreContext({ env: options.env, signal: (options as any).signal }, (ctx) =>
		diffAndStoreInternal({ ...options, env: ctx.env }),
	);
}
