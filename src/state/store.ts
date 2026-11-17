import { stableStringify } from "./serialization.js";
import { randomBytes } from "node:crypto";
import { sessionFromEnv } from "../persistence/context.js";
import { withStoreContext } from "../persistence/execution.js";
import { atomicWriteWasPublished, isAtomicExclusiveUnsupportedError } from "./atomic_file.js";
import type { AtomicWriteOptions, AtomicExclusiveWriteOptions } from "./atomic_file.js";
import { withFileLock } from "./file_lock.js";
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

export function defaultStateDir(env: Record<string, string | undefined>): string {
	return sessionFromEnv(env)?.directory ?? resolveStateDirectory(env);
}

/** Every state operation runs inside a store session (see withStoreContext). */
function storeOf(env: Record<string, string | undefined>) {
	const session = sessionFromEnv(env);
	if (!session) throw new Error("State access requires a store session");
	return session;
}

export { stableStringify } from "./serialization.js";

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
	return (await storeOf(env).read(key)).text;
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
	const result = await storeOf(env).write(key, JSON.stringify(value, null, 2) + "\n", {
		signal,
		assertInvocationCurrent: atomicWriteOptions?.assertInvocationCurrent,
	});
	return { signalAbortedAfterCommit: result.signalAbortedAfterCommit };
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

async function stateJsonExistsInternal({
	env,
	key,
}: {
	env: Record<string, string | undefined>;
	key: string;
}) {
	return (await storeOf(env).read(key)).text !== null;
}

async function deleteStateJsonUnlocked({ env, key }) {
	await storeOf(env).write(key, null);
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
	const safe = sanitizeApprovalId(approvalId);
	if (!safe) return;
	await storeOf(env).write(
		`approval_${safe}`,
		JSON.stringify({ stateKey, createdAt: new Date().toISOString() }) + "\n",
		{ exclusive: true, assertInvocationCurrent: options?.assertInvocationCurrent },
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
		const session = storeOf(ctx.env);
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
	});
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
	const safe = sanitizeApprovalId(approvalId);
	if (!safe) return;
	await storeOf(env).write(`approval_${safe}`, null);
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
	const session = storeOf(env);
	{
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
