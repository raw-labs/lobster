import type { LegacyCheckpointPage } from "../persistence/legacy.js";
import { withStoreContext } from "../persistence/execution.js";
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

export type { LegacyCheckpointPage } from "../persistence/legacy.js";

/** Host maintenance inventory. Unowned legacy checkpoints have no caller scope. */
export async function listLegacyCheckpoints({
	ctx = {},
	after = "",
	limit = 20,
}: {
	ctx?: ToolRunContext;
	after?: string;
	limit?: number;
} = {}): Promise<LegacyCheckpointPage> {
	if (callerId(ctx.runMetadata) !== undefined)
		throw new Error("Legacy inventory requires unscoped host access");
	if (typeof after !== "string" || (after !== "" && !/^[a-z0-9._-]{1,256}$/.test(after))) {
		throw new Error("Invalid legacy checkpoint cursor");
	}
	return readRuns(ctx, (store) => store.listLegacyCheckpoints({ after, limit }));
}

/** Register a saved legacy pause without executing or answering it. */
export async function adoptLegacyCheckpoint({
	checkpointId,
	ctx = {},
}: {
	checkpointId: string;
	ctx?: ToolRunContext;
}): Promise<RunView> {
	if (
		typeof checkpointId !== "string" ||
		!/^(pipeline_resume_|workflow[-_]resume_|sdk_resume_)[a-z0-9._-]{1,128}$/.test(checkpointId)
	) {
		throw new Error("Invalid legacy checkpoint ID");
	}
	if (callerId(ctx.runMetadata) === undefined)
		throw new Error("Legacy adoption requires a trusted caller ID");
	return withStoreContext(
		{ ...ctx, env: { ...process.env, ...ctx.env }, trackRuns: true },
		async (runtime) => {
			const store = runtime.storeSession ?? sessionFromEnv(runtime.env);
			if (!store) throw new Error("Legacy adoption requires a persistence session");
			return runView(
				await store.adoptCheckpoint(
					checkpointId,
					ctx.runMetadata,
					ctx.signal,
					ctx.assertInvocationCurrent,
				),
			);
		},
	);
}
