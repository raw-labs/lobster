import { stableStringify } from "../util/serialization.js";
import { randomBytes } from "node:crypto";
import { sessionFromEnv } from "../persistence/context.js";
import { withStoreContext } from "../persistence/execution.js";
import { STATE_EXISTS } from "../persistence/backend.js";
import type { StoreSession } from "../persistence/session.js";
import { withFileLock } from "./file_lock.js";
import { keyToPath, resolveStateDirectory } from "./paths.js";

export { withFileLock } from "./file_lock.js";
export { keyToPath } from "./paths.js";
export { atomicWriteWasPublished, ensureDirectory, writeFileAtomic } from "./atomic_file.js";
export { stableStringify } from "../util/serialization.js";

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

/** The stored text of `key`, or null. */
export function readStateText({ env, key }: { env: Env; key: string }): Promise<string | null> {
	return inStore(env, { readOnly: true }, async (session) => (await session.read(key)).text);
}

async function readJson(session: StoreSession, key: string, signal?: AbortSignal) {
	signal?.throwIfAborted();
	const text = (await session.read(key)).text;
	if (text === null) return null;
	const value = JSON.parse(text);
	signal?.throwIfAborted();
	return value;
}

export function readStateJson({
	env,
	key,
	signal,
}: {
	env: Env;
	key: string;
	signal?: AbortSignal;
}) {
	return inStore(env, { readOnly: true, signal }, (session) => readJson(session, key, signal));
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
		} catch (err: any) {
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

function writeJson(
	session: StoreSession,
	{ key, value, signal, assertInvocationCurrent }: WriteOptions,
) {
	return session.write(key, JSON.stringify(value, null, 2) + "\n", {
		signal,
		assertInvocationCurrent,
	});
}

export function writeStateJson(options: WriteOptions) {
	return inStore(options.env, { signal: options.signal }, async (session, env) => {
		const result = await withStateKeyLock(env, options.key, options.signal, () =>
			writeJson(session, options),
		);
		return { signalAbortedAfterCommit: result.signalAbortedAfterCommit };
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

function sanitizeApprovalId(approvalId: string): string {
	return approvalId.replace(/[^a-f0-9]/g, "");
}

/** A short, human-friendly approval ID (8 hex chars), easy to copy in a chat. */
export function generateApprovalId(): string {
	return randomBytes(4).toString("hex");
}

/** Map `approvalId` to `stateKey`; never overwrites an existing mapping. */
export function writeApprovalIndex({
	env,
	stateKey,
	approvalId,
	assertInvocationCurrent,
}: {
	env: Env;
	stateKey: string;
	approvalId: string;
	assertInvocationCurrent?: () => void;
}) {
	const safe = sanitizeApprovalId(approvalId);
	return inStore(env, {}, async (session) => {
		if (!safe) return;
		await session.write(
			`approval_${safe}`,
			JSON.stringify({ stateKey, createdAt: new Date().toISOString() }) + "\n",
			{ exclusive: true, assertInvocationCurrent },
		);
	});
}

/** Give `stateKey` a new short approval ID. */
export function createApprovalIndex(options: {
	env: Env;
	stateKey: string;
	assertInvocationCurrent?: () => void;
}): Promise<string> {
	return inStore(options.env, {}, async (_session, env) => {
		for (let attempt = 0; attempt < 16; attempt++) {
			const approvalId = generateApprovalId();
			try {
				await writeApprovalIndex({ ...options, env, approvalId });
				return approvalId;
			} catch (err: any) {
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
	const safe = sanitizeApprovalId(approvalId);
	return inStore(env, { readOnly: true }, async (session) => {
		if (!safe) return null;
		const text = (await session.read(`approval_${safe}`)).text;
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

/** Retire a short approval ID (after its question was answered or cancelled). */
export function deleteApprovalId({ env, approvalId }: { env: Env; approvalId: string }) {
	const safe = sanitizeApprovalId(approvalId);
	return inStore(env, {}, async (session) => {
		if (safe) await session.write(`approval_${safe}`, null);
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
