import type { ExecutionRecorder, StoreSession } from "./session.js";

const SESSION = Symbol("lobster.storeSession");
const RECORDER = Symbol("lobster.executionRecorder");

export type StoreEnvironment = Record<string, string | undefined> & {
	[SESSION]?: StoreSession;
	[RECORDER]?: ExecutionRecorder;
};

export function sessionFromEnv(env: StoreEnvironment | undefined): StoreSession | undefined {
	return env?.[SESSION];
}

export function recorderFromEnv(env: StoreEnvironment | undefined): ExecutionRecorder | undefined {
	return env?.[RECORDER];
}

export function bindStoreSession(env: StoreEnvironment, session: StoreSession): StoreEnvironment {
	const bound: StoreEnvironment = {
		...env,
		LOBSTER_STATE_DIR: session.directory,
		LOBSTER_BACKEND: session.backend.kind,
		[SESSION]: session,
	};
	if (bound[RECORDER]?.session !== session) {
		delete bound[RECORDER];
	}
	return bound;
}

export function bindRecorder(env: StoreEnvironment, recorder: ExecutionRecorder): StoreEnvironment {
	return { ...env, [RECORDER]: recorder };
}
