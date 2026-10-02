import { fingerprint } from "./model.js";
import { PersistenceOutcomeUnknown } from "./backend.js";
import path from "node:path";
import { promises as fs } from "node:fs";
import {
	openStoreSession,
	sessionFromEnv,
	recorderFromEnv,
	bindStoreSession,
	bindRecorder,
	type StoreSession,
} from "./session.js";
import { withFileLock } from "../state/store.js";

export async function withStoreContext<T>(ctx: any, task: (ctx: any) => Promise<T>): Promise<T> {
	const env = ctx.env ?? process.env;
	const injected: StoreSession | undefined = ctx.storeSession ?? sessionFromEnv(env);
	if (injected) return task({ ...ctx, env: bindStoreSession(env, injected) });
	let selected = false;
	try {
		await fs.access(
			path.join(
				String(
					env.LOBSTER_STATE_DIR ??
						path.join((await import("node:os")).homedir(), ".lobster", "state"),
				),
				".backend",
			),
		);
		selected = true;
	} catch (error) {
		if (error.code !== "ENOENT") throw error;
	}
	if (!selected && !env.LOBSTER_BACKEND && env.LOBSTER_HISTORY !== "1" && !ctx.history)
		return task(ctx);
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

export async function withExecution<T>(
	ctx: any,
	kind: string,
	resumeKey: string | undefined,
	task: (ctx: any) => Promise<T>,
): Promise<T> {
	if (ctx.dryRun) return task(ctx);
	return withStoreContext(ctx, async (runtime) => {
		const session = sessionFromEnv(runtime.env);
		if (
			!session ||
			(!session.history && !resumeKey) ||
			recorderFromEnv(runtime.env) ||
			runtime.dryRun
		)
			return task(runtime);
		let recorder;
		if (resumeKey) {
			recorder = await withFileLock({
				filePath: path.join(
					session.directory,
					`.association-${resumeKey.replace(/[^a-zA-Z0-9._-]/g, "_")}`,
				),
				signal: runtime.signal,
				task: () => session.recorderForState(resumeKey),
			});
			if (!recorder) return task(runtime);
		} else recorder = await session.createRun(kind, runtime.runMetadata);
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
			const result: any = await task(bound);
			const run = await session.getRun(recorder.runId);
			const pending = Object.values(run?.waits ?? {}).some(
				(wait: any) => wait.status === "pending",
			);
			if (result?.error?.type === "persistence_outcome_unknown") {
				// Reconcile the admitted mutation before assigning an execution outcome.
			} else if (result?.ok === false) {
				if ((!resumeKey || recorder.ownsExecution) && !pending && run?.status === "running")
					await recorder.settle(runtime.signal?.aborted ? "cancelled" : "failed");
			} else if (["needs_approval", "needs_input"].includes(result?.status)) {
				// The continuation publication already committed waiting status with its history.
			} else if ((!resumeKey || recorder.ownsExecution) && !pending)
				await recorder.settle(
					result?.status === "cancelled" ? "cancelled" : "completed",
					result?.output,
				);
			if (result && typeof result === "object" && Object.isExtensible(result))
				Object.assign(result, { runId: recorder.runId });
			return result;
		} catch (error) {
			if (error instanceof PersistenceOutcomeUnknown) throw error;
			const run = await session.getRun(recorder.runId);
			if (
				(!resumeKey || recorder.ownsExecution) &&
				!Object.values(run?.waits ?? {}).some((wait: any) => wait.status === "pending")
			)
				await recorder.settle(runtime.signal?.aborted ? "cancelled" : "failed", error);
			throw error;
		}
	});
}
