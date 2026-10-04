import { sessionFromEnv } from "../persistence/context.js";
import { openStoreSession, type StoreSession } from "../persistence/session.js";
import type { RunStatus, RunSummary } from "../persistence/model.js";
import {
	callerId,
	findWait,
	runView,
	visibleTo,
	type RunView,
	type WaitView,
} from "../persistence/waits.js";
import type { ToolRunContext } from "./tool_runtime.js";

export type RunPage = { runs: RunSummary[]; nextCursor: string | null };

export type RunQuery = { status?: RunStatus; after?: string; limit?: number; ctx?: ToolRunContext };

async function readRuns<T>(
	ctx: ToolRunContext,
	task: (store: StoreSession) => Promise<T>,
): Promise<T> {
	ctx.signal?.throwIfAborted();
	const store = ctx.storeSession ?? sessionFromEnv(ctx.env);
	if (store) return task(store);
	const owned = await openStoreSession({
		env: { ...process.env, ...ctx.env },
		readOnly: true,
		signal: ctx.signal,
	});
	try {
		return await task(owned);
	} finally {
		await owned.close();
	}
}

/** List persisted runs; callerId comes from trusted host context, not model arguments. */
export async function listRuns({
	ctx = {},
	status,
	after,
	limit,
}: RunQuery = {}): Promise<RunPage> {
	if (
		status !== undefined &&
		!["running", "waiting", "completed", "failed", "cancelled"].includes(status)
	) {
		throw new Error("Invalid run status");
	}
	if (
		after !== undefined &&
		(typeof after !== "string" || (after !== "" && !/^[a-zA-Z0-9_-]{1,128}$/.test(after)))
	) {
		throw new Error("Invalid run cursor");
	}
	return readRuns(ctx, (store) =>
		store.listRuns({ status, after, limit, callerId: callerId(ctx.runMetadata) }),
	);
}

export async function getRun({
	runId,
	ctx = {},
}: {
	runId: string;
	ctx?: ToolRunContext;
}): Promise<RunView | null> {
	if (typeof runId !== "string" || !/^[a-zA-Z0-9_-]{1,128}$/.test(runId))
		throw new Error("Invalid run ID");
	return readRuns(ctx, async (store) => {
		const run = await store.getRunState(runId);
		return run && visibleTo(run, callerId(ctx.runMetadata)) ? runView(run) : null;
	});
}

/** Full saved review context, separate from bounded diagnostic history. */
export async function getWait({
	waitId,
	ctx = {},
}: {
	waitId: string;
	ctx?: ToolRunContext;
}): Promise<WaitView | null> {
	return readRuns(
		ctx,
		async (store) => (await findWait(store, waitId, callerId(ctx.runMetadata)))?.view ?? null,
	);
}
