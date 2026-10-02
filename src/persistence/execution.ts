import { promises as fs } from "node:fs";
import path from "node:path";
import { defaultStateDir, withFileLock } from "../state/store.js";
import { PersistenceOutcomeUnknown } from "./backend.js";
import { fingerprint, type RunMetadata } from "./model.js";
import {
	bindRecorder,
	bindStoreSession,
	openStoreSession,
	recorderFromEnv,
	sessionFromEnv,
	type ExecutionRecorder,
	type StoreEnvironment,
	type StoreSession,
} from "./session.js";

type ExecutionContext = {
	env?: StoreEnvironment;
	storeSession?: StoreSession;
	history?: boolean;
	readOnly?: boolean;
	signal?: AbortSignal;
	dryRun?: boolean;
	runMetadata?: RunMetadata;
	_decision?: { approved?: boolean; cancel?: boolean; response?: unknown };
};

type ExecutionOutcome = {
	ok?: boolean;
	status?: string;
	output?: unknown;
	error?: { type?: string };
};

export async function withStoreContext<C extends ExecutionContext, T>(
	ctx: C,
	task: (ctx: C) => Promise<T>,
): Promise<T> {
	const env = ctx.env ?? process.env;
	const injected = ctx.storeSession ?? sessionFromEnv(env);
	if (injected) {
		return task({ ...ctx, env: bindStoreSession(env, injected) });
	}

	let selected = false;
	try {
		await fs.access(path.join(defaultStateDir(env), ".backend"));
		selected = true;
	} catch (error) {
		if (error.code !== "ENOENT") {
			throw error;
		}
	}
	if (!selected && !env.LOBSTER_BACKEND && env.LOBSTER_HISTORY !== "1" && !ctx.history) {
		return task(ctx);
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

export async function withExecution<C extends ExecutionContext, T>(
	ctx: C,
	kind: string,
	resumeKey: string | undefined,
	task: (ctx: C) => Promise<T>,
): Promise<T> {
	if (ctx.dryRun) {
		return task(ctx);
	}

	return withStoreContext(ctx, async (runtime) => {
		const session = sessionFromEnv(runtime.env);
		if (
			!session ||
			(!session.history && !resumeKey) ||
			recorderFromEnv(runtime.env) ||
			runtime.dryRun
		) {
			return task(runtime);
		}

		let recorder: ExecutionRecorder;
		if (resumeKey) {
			recorder = await withFileLock({
				filePath: path.join(
					session.directory,
					`.association-${resumeKey.replace(/[^a-zA-Z0-9._-]/g, "_")}`,
				),
				signal: runtime.signal,
				task: () => session.recorderForState(resumeKey),
			});
			if (!recorder) {
				return task(runtime);
			}
		} else {
			recorder = await session.createRun(kind, runtime.runMetadata);
		}

		if (resumeKey && runtime._decision) {
			const decision = runtime._decision;
			recorder.decisionKey = resumeKey.replace("workflow-resume_", "workflow_resume_");
			recorder.decision = {
				...(typeof decision.approved === "boolean" ? { approved: decision.approved } : {}),
				...(decision.cancel === true ? { cancel: true } : {}),
				...(decision.response !== undefined
					? { responseHash: fingerprint(decision.response) }
					: {}),
			};
		}

		const bound = { ...runtime, env: bindRecorder(runtime.env, recorder) };
		try {
			const result = await task(bound);
			const outcome =
				result && typeof result === "object" ? (result as ExecutionOutcome) : undefined;
			const run = await session.getRun(recorder.runId);
			const pending = Object.values(run?.waits ?? {}).some((wait) => wait.status === "pending");
			const ownsExecution = !resumeKey || recorder.ownsExecution;

			if (outcome?.error?.type === "persistence_outcome_unknown") {
				// Reconcile the admitted mutation before assigning an execution outcome.
			} else if (outcome?.ok === false) {
				if (ownsExecution && !pending && run?.status === "running") {
					await recorder.settle(runtime.signal?.aborted ? "cancelled" : "failed");
				}
			} else if (outcome?.status === "needs_approval" || outcome?.status === "needs_input") {
				// Continuation publication already committed waiting status with its history.
			} else if (ownsExecution && !pending) {
				await recorder.settle(
					outcome?.status === "cancelled" ? "cancelled" : "completed",
					outcome?.output,
				);
			}

			if (result && typeof result === "object" && Object.isExtensible(result)) {
				Object.assign(result, { runId: recorder.runId });
			}
			return result;
		} catch (error) {
			if (error instanceof PersistenceOutcomeUnknown) {
				throw error;
			}

			const run = await session.getRun(recorder.runId);
			const pending = Object.values(run?.waits ?? {}).some((wait) => wait.status === "pending");
			if ((!resumeKey || recorder.ownsExecution) && !pending) {
				await recorder.settle(runtime.signal?.aborted ? "cancelled" : "failed", error);
			}
			throw error;
		}
	});
}
