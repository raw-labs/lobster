import { PersistenceOutcomeUnknown } from "./backend.js";
import {
	bindRecorder,
	bindStoreSession,
	sessionFromEnv,
	type StoreEnvironment,
	type PersistenceOptions,
	resolveStoreSession,
} from "./context.js";
import type { RunKind } from "./model.js";
import { openStoreSession, type ExecutionRecorder } from "./session.js";

type ExecutionContext = PersistenceOptions & {
	env?: StoreEnvironment;
	readOnly?: boolean;
	signal?: AbortSignal;
	dryRun?: boolean;
};

type ExecutionOutcome = { ok?: boolean; error?: { type?: string } };

export async function withStoreContext<C extends ExecutionContext, T>(
	ctx: C,
	task: (ctx: C & { env: StoreEnvironment }) => Promise<T>,
): Promise<T> {
	const env = ctx.env ?? process.env;
	const injected = ctx.storeSession ? resolveStoreSession(ctx.storeSession) : sessionFromEnv(env);
	if (injected) {
		return task({ ...ctx, env: bindStoreSession(env, injected) });
	}

	const session = await openStoreSession({
		env,
		history: ctx.history,
		readOnly: ctx.readOnly,
		signal: ctx.signal,
	});
	try {
		return await task({ ...ctx, env: bindStoreSession(env, session) });
	} finally {
		await session.close();
	}
}

/**
 * Record a run around `task` (a `lobster run` or a tool request). Runs on the engine settle
 * their own status; this only settles requests that never reached it, and drops the record of
 * one that failed before anything started.
 */
export function withExecution<C extends ExecutionContext, T>(
	ctx: C,
	kind: RunKind,
	task: (ctx: C) => Promise<T>,
): Promise<T & { runId?: string }>;

export async function withExecution<C extends ExecutionContext, T>(
	ctx: C,
	kind: RunKind,
	task: (ctx: C) => Promise<T>,
): Promise<T> {
	if (ctx.dryRun) {
		return task(ctx);
	}

	return withStoreContext(ctx, async (runtime) => {
		const session = sessionFromEnv(runtime.env)!;
		const recorder: ExecutionRecorder = await session.createRun(kind, runtime.runMetadata);
		const bound = { ...runtime, env: bindRecorder(runtime.env, recorder) };
		let discarded = false;
		// The engine settles every run it reached. A request that failed before (an invalid
		// workflow file, a pipeline that does not parse) is not a run: settle it and remove the
		// record, instead of keeping a run nobody can do anything with.
		const settleUnstarted = async (error?: unknown) => {
			const run = await session.getRunState(recorder.runId);
			if (!run || run.execution || run.status !== "running") return;
			await recorder.settle(runtime.signal?.aborted ? "cancelled" : "failed", error);
			const full = await session.getRun(recorder.runId);
			if (
				full &&
				!full.definitions.length &&
				!full.attempts.length &&
				!Object.keys(full.states).length
			)
				discarded = await session.backend.removeRun(recorder.runId, "9999-12-31T00:00:00.000Z");
		};
		try {
			const result = await task(bound);
			const outcome =
				result && typeof result === "object" ? (result as ExecutionOutcome) : undefined;
			// An unknown outcome is reconciled later, never settled here.
			if (outcome?.ok === false && outcome.error?.type !== "persistence_outcome_unknown")
				await settleUnstarted();
			if (!discarded && result && typeof result === "object" && Object.isExtensible(result)) {
				Object.assign(result, { runId: recorder.runId });
			}
			return result;
		} catch (error) {
			if (!(error instanceof PersistenceOutcomeUnknown)) await settleUnstarted(error);
			throw error;
		}
	});
}
