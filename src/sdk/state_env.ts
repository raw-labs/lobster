import { bindStoreSession } from "../persistence/context.js";
import type { StoreSession } from "../persistence/session.js";

export function stateEnv(
	ctx: { stateDir?: string; env?: NodeJS.ProcessEnv; storeSession?: StoreSession } | undefined,
) {
	const env = ctx?.stateDir
		? { ...(ctx?.env ?? process.env), LOBSTER_STATE_DIR: ctx.stateDir }
		: (ctx?.env ?? process.env);
	return ctx?.storeSession ? bindStoreSession(env, ctx.storeSession) : env;
}
