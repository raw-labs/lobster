// Listing and inspecting runs and waits, for hosts and the CLI (`runs list/show`, `waits show`).
import { sessionFromEnv } from "../persistence/context.js";
import { withStoreContext } from "../persistence/execution.js";
import type { StoreSession } from "../persistence/session.js";
import { checkRunId, isRunId, type RunStatus, type RunSummary } from "../persistence/model.js";
import { isRunStatus } from "../engine/checkpoint.js";
import {
	callerId,
	findWait,
	runIdOfWait,
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
	return withStoreContext(
		{ ...ctx, env: { ...process.env, ...ctx.env }, readOnly: true },
		(bound) => task(sessionFromEnv(bound.env)!),
	);
}

/** List persisted runs; callerId comes from trusted host context, not model arguments. */
export async function listRuns({
	ctx = {},
	status,
	after,
	limit,
}: RunQuery = {}): Promise<RunPage> {
	if (status !== undefined && !isRunStatus(status)) {
		throw new Error("Invalid run status");
	}
	if (after !== undefined && (typeof after !== "string" || (after !== "" && !isRunId(after)))) {
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
	checkRunId(runId);
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
	runIdOfWait(waitId); // Refused before the store is opened, like a malformed run ID.
	return readRuns(
		ctx,
		async (store) => (await findWait(store, waitId, callerId(ctx.runMetadata)))?.view ?? null,
	);
}
