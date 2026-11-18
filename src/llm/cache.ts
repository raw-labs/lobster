// Answers kept so a repeated call does not pay again: the per-run state an answer is reused
// from on resume, and the file cache shared by calls with the same cache key.
import path from "node:path";
import { promises as fsp } from "node:fs";
import {
	diffAndStore,
	ensureDirectory,
	atomicWriteWasPublished,
	isJsonSyntaxError,
	readStateJsonWithLock,
	withFileLock,
	writeFileAtomic,
} from "../state/store.js";
import { STATE_VERSION, type NormalizedInvocationItem } from "./protocol.js";

type CacheEntry = {
	items: NormalizedInvocationItem[];
	cacheKey: string;
	storedAt: string;
};

export async function persistOutputs({
	env,
	stateKey,
	cacheKey,
	items,
	stateType,
	signal,
	assertInvocationCurrent,
	afterStore,
}: {
	env: any;
	stateKey: string | null;
	cacheKey: string;
	items: NormalizedInvocationItem[];
	stateType: string;
	signal?: AbortSignal;
	assertInvocationCurrent?: () => void;
	afterStore?: () => Promise<void>;
}) {
	if (!stateKey) {
		await afterStore?.();
		return;
	}
	const record = {
		type: stateType,
		version: STATE_VERSION,
		cacheKey,
		items,
		storedAt: new Date().toISOString(),
	};
	await diffAndStore({
		env,
		key: stateKey,
		value: record,
		signal,
		assertInvocationCurrent,
		afterStore: afterStore ? () => afterStore() : undefined,
	});
}

export async function readReusableLlmState(env: any, stateKey: string, signal?: AbortSignal) {
	try {
		return await readStateJsonWithLock({ env, key: stateKey, signal });
	} catch (err: any) {
		if (isJsonSyntaxError(err)) return null;
		throw err;
	}
}

export function pickReusableState(stored: any, cacheKey: string, stateType: string) {
	if (!stored || typeof stored !== "object") return null;
	if (stored.type !== stateType) return null;
	if (stored.cacheKey !== cacheKey) return null;
	if (!Array.isArray(stored.items)) return null;
	return { items: stored.items as NormalizedInvocationItem[] };
}

export async function readCacheEntry(
	env: any,
	key: string,
	cacheNamespace: string,
	signal?: AbortSignal,
): Promise<CacheEntry | null> {
	const filePath = path.join(getCacheDir(env), cacheNamespace, `${key}.json`);
	const read = async () => {
		try {
			const text = await fsp.readFile(filePath, "utf8");
			const parsed = JSON.parse(text) as Partial<CacheEntry>;
			if (parsed?.cacheKey !== key || !Array.isArray(parsed.items)) return null;
			return parsed as CacheEntry;
		} catch (err: any) {
			if (err?.code === "ENOENT") return null;
			if (isJsonSyntaxError(err)) return null;
			throw err;
		}
	};
	try {
		return await withFileLock({ filePath, signal, task: read });
	} catch (err: any) {
		if (err?.code === "ENOENT" || err?.code === "ENOTDIR") return null;
		// A cache mounted read-only cannot have an active local writer because a
		// writer first creates the same coordination lock. Preserve its reusable
		// entries instead of requiring a lock-directory write for a read.
		if (["EACCES", "EPERM", "EROFS"].includes(err?.code)) return read();
		throw err;
	}
}

export async function writeCacheEntry(
	env: any,
	key: string,
	items: NormalizedInvocationItem[],
	cacheNamespace: string,
	signal?: AbortSignal,
	assertInvocationCurrent?: () => void,
) {
	const dir = path.join(getCacheDir(env), cacheNamespace);
	signal?.throwIfAborted();
	await ensureDirectory(dir);
	const filePath = path.join(dir, `${key}.json`);
	const content =
		JSON.stringify({ items, cacheKey: key, storedAt: new Date().toISOString() }, null, 2) + "\n";
	await withFileLock({
		filePath,
		signal,
		task: async () => {
			let previousContent: Buffer | null = null;
			try {
				previousContent = await fsp.readFile(filePath);
			} catch (err: any) {
				if (err?.code !== "ENOENT") throw err;
			}
			const restorePreviousContent = async () => {
				// No cache reader or competing cache writer can observe this entry
				// until this lock is released. Restore an entry replaced by a refresh;
				// only remove the just-published file when none existed beforehand.
				if (previousContent === null) await fsp.rm(filePath, { force: true });
				else await writeFileAtomic(filePath, previousContent);
			};
			let cacheWasPublished = false;
			try {
				signal?.throwIfAborted();
				const result = await writeFileAtomic(filePath, content, {
					signal,
					assertInvocationCurrent,
				});
				cacheWasPublished = true;
				if (result?.signalAbortedAfterCommit || signal?.aborted) {
					await restorePreviousContent();
					cacheWasPublished = false;
					signal?.throwIfAborted();
					throw new Error("LLM cache publication cancelled");
				}
			} catch (err) {
				if (cacheWasPublished || atomicWriteWasPublished(err)) {
					await restorePreviousContent();
				}
				throw err;
			}
		},
	});
}

function getCacheDir(env: any) {
	if (env?.LOBSTER_CACHE_DIR) return String(env.LOBSTER_CACHE_DIR);
	return path.join(process.cwd(), ".lobster-cache");
}
