import { PersistenceOutcomeUnknown } from "./backend.js";
import {
	bindRecorder,
	bindStoreSession,
	recorderFromEnv,
	sessionFromEnv,
	type StoreEnvironment,
	type PersistenceOptions,
	resolveStoreSession,
} from "./context.js";
import { openStoreSession, type ExecutionRecorder } from "./session.js";

type ExecutionContext = PersistenceOptions & {
	env?: StoreEnvironment;
	readOnly?: boolean;
	signal?: AbortSignal;
	dryRun?: boolean;
};

type ExecutionOutcome = {
	ok?: boolean;
	status?: string;
	output?: unknown;
	error?: { type?: string };
};

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
		trackRuns: ctx.trackRuns,
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
	kind: string,
	task: (ctx: C) => Promise<T>,
): Promise<T & { runId?: string }>;

export async function withExecution<C extends ExecutionContext, T>(
	ctx: C,
	kind: string,
	task: (ctx: C) => Promise<T>,
): Promise<T> {
	if (ctx.dryRun) {
		return task(ctx);
	}

	return withStoreContext(ctx, async (runtime) => {
		const session = sessionFromEnv(runtime.env);
		if (
			!session ||
			(!session.trackRuns && !runtime.trackRuns) ||
			recorderFromEnv(runtime.env) ||
			runtime.dryRun
		) {
			return task(runtime);
		}

		const recorder: ExecutionRecorder = await session.createRun(kind, runtime.runMetadata);

		const bound = { ...runtime, env: bindRecorder(runtime.env, recorder) };
		// A request that fails before anything started (an invalid workflow file, a
		// pipeline that does not parse) is not a run: remove the record instead of
		// keeping a failed run nobody can do anything with.
		let discarded = false;
		const discardIfUnstarted = async () => {
			const full = await session.getRun(recorder.runId);
			if (
				full &&
				!full.execution &&
				!full.definitions.length &&
				!full.attempts.length &&
				!Object.keys(full.states).length &&
				!Object.keys(full.waits).length
			)
				discarded = await session.backend.removeRun(recorder.runId, "9999-12-31T00:00:00.000Z");
		};
		try {
			const result = await task(bound);
			const outcome =
				result && typeof result === "object" ? (result as ExecutionOutcome) : undefined;
			const run =
				outcome?.error?.type === "persistence_outcome_unknown"
					? null
					: await session.getRunState(recorder.runId);
			const pending = Object.values(run?.waits ?? {}).some((wait) => wait.status === "pending");

			if (run?.execution) {
				// The saved-state workflow engine owns status, including recoverable interruption.
			} else if (outcome?.error?.type === "persistence_outcome_unknown") {
				// Reconcile the admitted mutation before assigning an execution outcome.
			} else if (outcome?.ok === false) {
				if (!pending && run?.status === "running") {
					await recorder.settle(runtime.signal?.aborted ? "cancelled" : "failed");
					await discardIfUnstarted();
				}
			} else if (outcome?.status === "needs_approval" || outcome?.status === "needs_input") {
				// Continuation publication already committed waiting status with its history.
			} else if (!pending) {
				await recorder.settle(
					outcome?.status === "cancelled" ? "cancelled" : "completed",
					outcome?.output,
				);
			}

			if (!discarded && result && typeof result === "object" && Object.isExtensible(result)) {
				Object.assign(result, { runId: recorder.runId });
			}
			return result;
		} catch (error) {
			if (error instanceof PersistenceOutcomeUnknown) {
				throw error;
			}

			const run = await session.getRunState(recorder.runId);
			const pending = Object.values(run?.waits ?? {}).some((wait) => wait.status === "pending");
			if (!run?.execution && !pending) {
				await recorder.settle(runtime.signal?.aborted ? "cancelled" : "failed", error);
				await discardIfUnstarted();
			}
			throw error;
		}
	});
}
