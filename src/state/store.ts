import { stableStringify } from "../util/serialization.js";
import { sessionFromEnv } from "../persistence/context.js";
import { withStoreContext } from "../persistence/execution.js";
import { STATE_EXISTS } from "../persistence/backend.js";
import type { StoreSession } from "../persistence/session.js";
import { withFileLock } from "./file_lock.js";
import { generateApprovalId, isApprovalId } from "./approval_id.js";
import { keyToPath, resolveStateDirectory } from "./paths.js";

export { withFileLock } from "./file_lock.js";
export { keyToPath } from "./paths.js";
export { atomicWriteWasPublished, ensureDirectory, writeFileAtomic } from "./atomic_file.js";

type Env = Record<string, string | undefined>;

export function defaultStateDir(env: Env): string {
	return sessionFromEnv(env)?.directory ?? resolveStateDirectory(env);
}

/**
 * Run `task` with the store session of `env`, opening one when the caller has none: every
 * state operation is a store operation.
 */
function inStore<T>(
	env: Env,
	options: { readOnly?: boolean; signal?: AbortSignal },
	task: (session: StoreSession, env: Env) => Promise<T>,
): Promise<T> {
	return withStoreContext({ env, ...options }, (ctx) => {
		const session = sessionFromEnv(ctx.env);
		if (!session) throw new Error("State access requires a store session");
		return task(session, ctx.env);
	});
}

/**
 * Hold the lock of `key` while `task` runs. Reads and writes of one value are single store
 * transactions; this lock is for what spans several (diffAndStore reads, writes, publishes
 * something else and may roll back), across processes.
 */
function withStateKeyLock<T>(
	env: Env,
	key: string,
	signal: AbortSignal | undefined,
	task: () => Promise<T>,
) {
	return withFileLock({ filePath: keyToPath(defaultStateDir(env), key), signal, task });
}

export function isJsonSyntaxError(err: unknown) {
	return err instanceof SyntaxError;
}

async function readJson(session: StoreSession, key: string, signal?: AbortSignal) {
	signal?.throwIfAborted();
	const text = (await session.read(key)).text;
	if (text === null) return null;
	const value = JSON.parse(text);
	signal?.throwIfAborted();
	return value;
}

/**
 * Read under the lock of `key`: only after any composite change of it in progress (see
 * withStateKeyLock) has settled, so a value that cancellation may still roll back is never seen.
 */
function readLocked<T>(
	env: Env,
	key: string,
	signal: AbortSignal | undefined,
	read: (session: StoreSession) => Promise<T>,
) {
	return inStore(env, { readOnly: true, signal }, async (session, env) => {
		try {
			return await withStateKeyLock(env, key, signal, () => read(session));
		} catch (err) {
			// A state directory mounted read-only cannot hold a lock file; nothing can be
			// changing the value there either, so read it directly.
			if (["EACCES", "EPERM", "EROFS"].includes(err?.code)) return read(session);
			throw err;
		}
	});
}

export function readStateJsonWithLock({
	env,
	key,
	signal,
}: {
	env: Env;
	key: string;
	signal?: AbortSignal;
}) {
	return readLocked(env, key, signal, (session) => readJson(session, key, signal));
}

/** The stored text of `key` (or null), read under its lock like readStateJsonWithLock. */
export function readStateTextWithLock({
	env,
	key,
	signal,
}: {
	env: Env;
	key: string;
	signal?: AbortSignal;
}): Promise<string | null> {
	return readLocked(env, key, signal, async (session) => (await session.read(key)).text);
}

type WriteOptions = {
	env: Env;
	key: string;
	value: unknown;
	signal?: AbortSignal;
	/** Refuse the write once this caller no longer owns the work (see host authority). */
	assertInvocationCurrent?: () => void;
};

/** Write `value` under `key` as state text; returns the write's result and the text written. */
async function writeJson(
	session: StoreSession,
	{ key, value, signal, assertInvocationCurrent }: WriteOptions,
) {
	const text = JSON.stringify(value, null, 2) + "\n";
	const result = await session.write(key, text, { signal, assertInvocationCurrent });
	return { ...result, text };
}

/** Write `value` under its key; returns the text written (what a later read gets back). */
export function writeStateJson(options: WriteOptions) {
	return inStore(options.env, { signal: options.signal }, async (session, env) => {
		const { signalAbortedAfterCommit, text } = await withStateKeyLock(
			env,
			options.key,
			options.signal,
			() => writeJson(session, options),
		);
		return { signalAbortedAfterCommit, text };
	});
}

/**
 * Store `value` under `key` and report whether it changed. `afterStore` may publish something
 * that depends on it; if that fails, or the caller is cancelled, the previous value is put back
 * before the lock is released, so no reader ever sees a value that was rolled back.
 */
export function diffAndStore(
	options: WriteOptions & {
		afterStore?: (snapshot: { before: unknown; after: unknown; changed: boolean }) => Promise<void>;
	},
) {
	const { env, key, value, signal, afterStore } = options;
	return inStore(env, { signal }, (session, env) =>
		withStateKeyLock(env, key, signal, async () => {
			const beforeText = (await session.read(key)).text;
			let before: unknown = null;
			if (beforeText !== null) {
				try {
					before = JSON.parse(beforeText);
				} catch (err) {
					if (!isJsonSyntaxError(err)) throw err;
				}
			}
			const changed = stableStringify(before) !== stableStringify(value);
			const snapshot = { before, after: value, changed };
			let stored = false;
			try {
				signal?.throwIfAborted();
				await writeJson(session, options);
				stored = true;
				signal?.throwIfAborted();
				await afterStore?.(snapshot);
			} catch (err) {
				if (stored && (signal?.aborted || afterStore)) {
					if (beforeText === null) await session.write(key, null);
					else await writeJson(session, { ...options, value: before, signal: undefined });
				}
				throw err;
			}
			return snapshot;
		}),
	);
}

// ---------------------------------------------------------------------------------------
// Short approval IDs: `approval_<id>` maps an 8-hex-character ID to a resume state key.

/** The state key an approval ID is kept under, or undefined for a malformed ID. */
export function approvalIndexKey(approvalId: string): string | undefined {
	return isApprovalId(approvalId) ? `approval_${approvalId}` : undefined;
}

/** Map `approvalId` to `stateKey`; never overwrites an existing mapping. */
export function writeApprovalIndex({
	env,
	stateKey,
	approvalId,
}: {
	env: Env;
	stateKey: string;
	approvalId: string;
}) {
	const key = approvalIndexKey(approvalId);
	if (!key) throw new Error("Invalid approval ID");
	return inStore(env, {}, async (session) => {
		await session.write(
			key,
			JSON.stringify({ stateKey, createdAt: new Date().toISOString() }) + "\n",
			{ exclusive: true },
		);
	});
}

/** Give `stateKey` a new short approval ID. */
export function createApprovalIndex(options: { env: Env; stateKey: string }): Promise<string> {
	return inStore(options.env, {}, async (_session, env) => {
		for (let attempt = 0; attempt < 16; attempt++) {
			const approvalId = generateApprovalId();
			try {
				await writeApprovalIndex({ ...options, env, approvalId });
				return approvalId;
			} catch (err) {
				if (err?.code === STATE_EXISTS) continue;
				throw err;
			}
		}
		throw new Error("Could not allocate a unique approval ID");
	});
}

/** The state key a short approval ID stands for, or null. */
export function findStateKeyByApprovalId({
	env,
	approvalId,
}: {
	env: Env;
	approvalId: string;
}): Promise<string | null> {
	const key = approvalIndexKey(approvalId);
	return inStore(env, { readOnly: true }, async (session) => {
		if (!key) return null;
		const text = (await session.read(key)).text;
		if (text === null) return null;
		try {
			const data = JSON.parse(text);
			return typeof data?.stateKey === "string" ? data.stateKey : null;
		} catch (err) {
			if (isJsonSyntaxError(err)) return null;
			throw err;
		}
	});
}

/** The short approval ID of `stateKey`, or null. */
export function findApprovalIdByStateKey({
	env,
	stateKey,
}: {
	env: Env;
	stateKey: string;
}): Promise<string | null> {
	return inStore(env, { readOnly: true }, async (session) => {
		for (const [key, text] of Object.entries(await session.backend.listState("approval_"))) {
			const approvalId = key.slice("approval_".length);
			if (!isApprovalId(approvalId)) continue;
			try {
				if (JSON.parse(text)?.stateKey === stateKey) return approvalId;
			} catch {
				/* Ignore unrelated corrupt optional indexes. */
			}
		}
		return null;
	});
}

/** Retire the short approval ID that points to `stateKey`, if there is one. */
export function cleanupApprovalIndexByStateKey({ env, stateKey }: { env: Env; stateKey: string }) {
	return inStore(env, {}, async (session) => {
		for (const [key, text] of Object.entries(await session.backend.listState("approval_"))) {
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
	});
}
