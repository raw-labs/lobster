import { stableStringify } from "./serialization.js";
import { randomBytes } from "node:crypto";
import { promises as fsp } from "node:fs";
import path from "node:path";
import { recorderFromEnv, sessionFromEnv, resumeGuardFromEnv } from "../persistence/context.js";
import { withStoreContext } from "../persistence/execution.js";
import type { FileWriteOptions } from "../persistence/files.js";
import {
	atomicWriteWasPublished,
	ensureDirectory,
	isAtomicExclusiveUnsupportedError,
	writeFileAtomic,
	writeFileAtomicExclusive,
} from "./atomic_file.js";
import type { AtomicWriteOptions, AtomicExclusiveWriteOptions } from "./atomic_file.js";
import { FILE_LOCK_RETRY_MS, withFileLock } from "./file_lock.js";
import { keyToPath, resolveStateDirectory } from "./paths.js";

export { withFileLock } from "./file_lock.js";
export { keyToPath } from "./paths.js";

export {
	atomicWriteWasPublished,
	ensureDirectory,
	isAtomicExclusiveUnsupportedError,
	writeFileAtomic,
	writeFileAtomicExclusive,
	stripExtendedLengthPrefix,
} from "./atomic_file.js";

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

export function defaultStateDir(env: Record<string, string | undefined>): string {
	return sessionFromEnv(env)?.directory ?? resolveStateDirectory(env);
}

export { stableStringify } from "./serialization.js";

const TERMINAL_RESUME_CLEANUP_TIMEOUT_MS = FILE_LOCK_RETRY_MS * 10;

export function isJsonSyntaxError(err) {
	return err instanceof SyntaxError;
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
	if (session) {
		return (await session.read(key)).text;
	}
	try {
		return await fsp.readFile(keyToPath(defaultStateDir(env), key), "utf8");
	} catch (error) {
		if (error.code === "ENOENT") {
			return null;
		}
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
		if (text === null) {
			return null;
		}
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

function isContinuationValue(key: string, value: unknown): boolean {
	if (!value || typeof value !== "object") {
		return false;
	}
	const record = value as Record<string, unknown>;
	if (key.startsWith("pipeline_resume_")) {
		return Array.isArray(record.pipeline) && Number.isInteger(record.resumeAtIndex);
	}
	if (key.startsWith("workflow_resume_")) {
		return (
			typeof record.filePath === "string" &&
			Boolean(record.steps) &&
			Number.isInteger(record.resumeAtIndex)
		);
	}
	if (key.startsWith("sdk_resume_")) {
		return (
			record.type === "lobster.sdk-tracked.v1" ||
			(Boolean(record.commandInput) && Number.isInteger(record.resumeAtIndex))
		);
	}
	return false;
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
		const guard = resumeGuardFromEnv(env);
		const guarded = guard && !guard.admitted && guard.key === key && isConsumedResumeState(value);
		const writeOptions: FileWriteOptions = {
			signal,
			assertInvocationCurrent: atomicWriteOptions?.assertInvocationCurrent,
			atomicWriteOptions,
			...(guarded ? { expectedWaitRevision: guard.expectedRevision } : {}),
			...(recorder?.decisionKey === key.replace("workflow-resume_", "workflow_resume_")
				? { decision: recorder.decision }
				: {}),
			...(isContinuationValue(key, value) && recorder ? { runId: recorder.runId } : {}),
		};
		const result = await session.write(key, JSON.stringify(value, null, 2) + "\n", writeOptions);
		if (guarded) guard.admitted = true;
		if (recorder && isConsumedResumeState(value)) {
			recorder.ownsExecution = true;
		}
		return { signalAbortedAfterCommit: result.signalAbortedAfterCommit };
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
			if (
				isConsumedResumeState(currentState) ||
				stableStringify(currentState) !== stableStringify(expectedState)
			) {
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
						const recorder = recorderFromEnv(env);
						if (recorder) {
							recorder.ownsExecution = false;
						}
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
	approvalId,
}: {
	env: Record<string, string | undefined>;
	key: string;
	expectedState: unknown;
	claimId: string;
	approvalId?: string;
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
			const recorder = recorderFromEnv(env);
			if (recorder) {
				recorder.ownsExecution = false;
			}
			if (approvalId) {
				await restoreApprovalIndex({ env, stateKey: key, approvalId }).catch(() => {});
			}
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
	assertInvocationCurrent = undefined,
}: {
	env: Record<string, string | undefined>;
	key: string;
	expectedState: unknown;
	signal?: AbortSignal;
	assertInvocationCurrent?: () => void;
}): Promise<boolean> {
	return withStateKeyLock({
		env,
		key,
		signal,
		task: async () => {
			signal?.throwIfAborted();
			assertInvocationCurrent?.();
			const currentState = await readStateJson({ env, key });
			if (stableStringify(currentState) !== stableStringify(expectedState)) return false;

			const claimId = randomBytes(16).toString("hex");
			let claimPublished = false;
			let claimMayBePublished = false;
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
						atomicWriteOptions: { assertInvocationCurrent },
					});
				} catch (err) {
					claimMayBePublished = atomicWriteWasPublished(err);
					throw err;
				}
				claimPublished = true;
				if (result?.signalAbortedAfterCommit) signal?.throwIfAborted();
				signal?.throwIfAborted();
				assertInvocationCurrent?.();
				await deleteStateJsonUnlocked({ env, key });
				signal?.throwIfAborted();
				assertInvocationCurrent?.();
				return true;
			} catch (err) {
				if (
					((signal?.aborted || assertInvocationCurrent) && claimPublished) ||
					claimMayBePublished
				) {
					const latest = await readStateJson({ env, key });
					if (
						(claimPublished && latest === null) ||
						(isConsumedResumeState(latest) && latest.claimId === claimId)
					) {
						await writeStateJsonUnlocked({ env, key, value: expectedState });
						const recorder = recorderFromEnv(env);
						if (recorder) {
							recorder.ownsExecution = false;
						}
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
	if (session) {
		return (await session.read(key)).text !== null;
	}
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
		const guard = resumeGuardFromEnv(env);
		const guarded = guard && !guard.admitted && guard.key === key;
		await session.write(key, null, {
			...(guarded ? { expectedWaitRevision: guard.expectedRevision } : {}),
			...(currentRecorder?.decisionKey === key.replace("workflow-resume_", "workflow_resume_")
				? { decision: currentRecorder.decision }
				: {}),
		});
		if (guarded) guard.admitted = true;
		if (before.runId && before.text !== null) {
			const recorder = recorderFromEnv(env);
			if (recorder?.runId === before.runId) {
				recorder.ownsExecution = true;
			}
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
		const writeOptions: FileWriteOptions = {
			exclusive: true,
			atomicWriteOptions: options,
			assertInvocationCurrent: options?.assertInvocationCurrent,
		};
		await session.write(
			`approval_${safe}`,
			JSON.stringify({ stateKey, createdAt: new Date().toISOString() }) + "\n",
			writeOptions,
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
		if (text === null) {
			return null;
		}
		const data = JSON.parse(text);
		return typeof data?.stateKey === "string" ? data.stateKey : null;
	} catch (err: any) {
		if (err?.code === "ENOENT") return null;
		if (isJsonSyntaxError(err)) return null;
		throw err;
	}
}

/** Locate the short ID through the selected store before retiring its index. */
export async function findApprovalIdByStateKey({
	env,
	stateKey,
}: {
	env: Record<string, string | undefined>;
	stateKey: string;
}): Promise<string | null> {
	return withStoreContext({ env, readOnly: true }, async (ctx) => {
		const session = sessionFromEnv(ctx.env);
		if (session) {
			for (const [key, text] of Object.entries(await session.backend.listState("approval_"))) {
				const match = /^approval_([a-f0-9]+)$/.exec(key);
				if (!match) continue;
				try {
					if (JSON.parse(text)?.stateKey === stateKey) return match[1];
				} catch {
					/* Ignore unrelated corrupt optional indexes. */
				}
			}
			return null;
		}
		const directory = defaultStateDir(ctx.env);
		let files: string[];
		try {
			files = await fsp.readdir(directory);
		} catch (error) {
			if (error.code === "ENOENT") return null;
			throw error;
		}
		for (const file of files) {
			const match = /^approval_([a-f0-9]+)\.json$/.exec(file);
			if (!match) continue;
			try {
				const value = JSON.parse(await fsp.readFile(path.join(directory, file), "utf8"));
				if (value?.stateKey === stateKey) return match[1];
			} catch {
				/* Ignore unrelated corrupt optional indexes. */
			}
		}
		return null;
	});
}

/** Restore only a missing mapping; another allocation must never be overwritten. */
async function restoreApprovalIndex({
	env,
	stateKey,
	approvalId,
}: {
	env: Record<string, string | undefined>;
	stateKey: string;
	approvalId: string;
}): Promise<boolean> {
	const mappedStateKey = await findStateKeyByApprovalId({ env, approvalId });
	if (mappedStateKey === stateKey) return true;
	if (mappedStateKey !== null) return false;
	try {
		await writeApprovalIndex({ env, stateKey, approvalId });
		return true;
	} catch (err: any) {
		if (err?.code !== "EEXIST") throw err;
		return (await findStateKeyByApprovalId({ env, approvalId })) === stateKey;
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
			if (!key.startsWith("approval_")) {
				continue;
			}
			let data: { stateKey?: string } | null;
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
	return withStoreContext({ env: options.env, readOnly: true }, (ctx) =>
		readStateTextInternal({ ...options, env: ctx.env }),
	);
}

export async function readStateJson(
	options: Parameters<typeof readStateJsonInternal>[0],
): ReturnType<typeof readStateJsonInternal> {
	return withStoreContext({ env: options.env, readOnly: true, signal: options.signal }, (ctx) =>
		readStateJsonInternal({ ...options, env: ctx.env }),
	);
}

export async function readStateJsonWithLock(
	options: Parameters<typeof readStateJsonWithLockInternal>[0],
): ReturnType<typeof readStateJsonWithLockInternal> {
	return withStoreContext({ env: options.env, readOnly: true, signal: options.signal }, (ctx) =>
		readStateJsonWithLockInternal({ ...options, env: ctx.env }),
	);
}

export async function writeStateJson(
	options: Parameters<typeof writeStateJsonInternal>[0],
): ReturnType<typeof writeStateJsonInternal> {
	return withStoreContext({ env: options.env, signal: options.signal }, (ctx) =>
		writeStateJsonInternal({ ...options, env: ctx.env }),
	);
}

export async function consumeResumeState(
	options: Parameters<typeof consumeResumeStateInternal>[0],
): ReturnType<typeof consumeResumeStateInternal> {
	return withStoreContext({ env: options.env, signal: options.signal }, (ctx) =>
		consumeResumeStateInternal({ ...options, env: ctx.env }),
	);
}

export async function restoreConsumedResumeState(
	options: Parameters<typeof restoreConsumedResumeStateInternal>[0],
): ReturnType<typeof restoreConsumedResumeStateInternal> {
	return withStoreContext({ env: options.env }, (ctx) =>
		restoreConsumedResumeStateInternal({ ...options, env: ctx.env }),
	);
}

export async function deleteResumeStateWithRollback(
	options: Parameters<typeof deleteResumeStateWithRollbackInternal>[0],
): ReturnType<typeof deleteResumeStateWithRollbackInternal> {
	return withStoreContext({ env: options.env, signal: options.signal }, (ctx) =>
		deleteResumeStateWithRollbackInternal({ ...options, env: ctx.env }),
	);
}

export async function stateJsonExists(
	options: Parameters<typeof stateJsonExistsInternal>[0],
): ReturnType<typeof stateJsonExistsInternal> {
	return withStoreContext({ env: options.env, readOnly: true }, (ctx) =>
		stateJsonExistsInternal({ ...options, env: ctx.env }),
	);
}

export async function deleteStateJson(
	options: Parameters<typeof deleteStateJsonInternal>[0],
): ReturnType<typeof deleteStateJsonInternal> {
	return withStoreContext({ env: options.env, signal: options.signal }, (ctx) =>
		deleteStateJsonInternal({ ...options, env: ctx.env }),
	);
}

export async function deleteStateJsonWithBoundedResumeCleanup(
	options: Parameters<typeof deleteStateJsonWithBoundedResumeCleanupInternal>[0],
): ReturnType<typeof deleteStateJsonWithBoundedResumeCleanupInternal> {
	return withStoreContext({ env: options.env }, (ctx) =>
		deleteStateJsonWithBoundedResumeCleanupInternal({ ...options, env: ctx.env }),
	);
}

export async function deleteUnconsumedResumeState(
	options: Parameters<typeof deleteUnconsumedResumeStateInternal>[0],
): ReturnType<typeof deleteUnconsumedResumeStateInternal> {
	return withStoreContext({ env: options.env, signal: options.signal }, (ctx) =>
		deleteUnconsumedResumeStateInternal({ ...options, env: ctx.env }),
	);
}

export async function writeApprovalIndex(
	options: Parameters<typeof writeApprovalIndexInternal>[0],
): ReturnType<typeof writeApprovalIndexInternal> {
	return withStoreContext({ env: options.env }, (ctx) =>
		writeApprovalIndexInternal({ ...options, env: ctx.env }),
	);
}

export async function createApprovalIndex(
	options: Parameters<typeof createApprovalIndexInternal>[0],
): ReturnType<typeof createApprovalIndexInternal> {
	return withStoreContext({ env: options.env }, (ctx) =>
		createApprovalIndexInternal({ ...options, env: ctx.env }),
	);
}

export async function findStateKeyByApprovalId(
	options: Parameters<typeof findStateKeyByApprovalIdInternal>[0],
): ReturnType<typeof findStateKeyByApprovalIdInternal> {
	return withStoreContext({ env: options.env, readOnly: true }, (ctx) =>
		findStateKeyByApprovalIdInternal({ ...options, env: ctx.env }),
	);
}

export async function deleteApprovalId(
	options: Parameters<typeof deleteApprovalIdInternal>[0],
): ReturnType<typeof deleteApprovalIdInternal> {
	return withStoreContext({ env: options.env }, (ctx) =>
		deleteApprovalIdInternal({ ...options, env: ctx.env }),
	);
}

export async function cleanupApprovalIndexByStateKey(
	options: Parameters<typeof cleanupApprovalIndexByStateKeyInternal>[0],
): ReturnType<typeof cleanupApprovalIndexByStateKeyInternal> {
	return withStoreContext({ env: options.env }, (ctx) =>
		cleanupApprovalIndexByStateKeyInternal({ ...options, env: ctx.env }),
	);
}

export async function diffAndStore(
	options: Parameters<typeof diffAndStoreInternal>[0],
): ReturnType<typeof diffAndStoreInternal> {
	return withStoreContext({ env: options.env, signal: options.signal }, (ctx) =>
		diffAndStoreInternal({ ...options, env: ctx.env }),
	);
}
