import type { RunView, WaitView } from "./waits.js";
import type { ExecutionRecorder, StoreSession } from "./session.js";
import type { RunMetadata } from "./model.js";

export type RunScope = { callerId?: string };

export type StoreHandle = Pick<
	StoreSession,
	| "directory"
	| "history"
	| "trackRuns"
	| "readOnly"
	| "listRuns"
	| "inspect"
	| "retain"
	| "reconcile"
	| "acknowledge"
	| "close"
>;

export type PublicStoreSession = StoreHandle & {
	getRun(runId: string, scope?: RunScope): Promise<RunView | null>;
	getWait(waitId: string, scope?: RunScope): Promise<WaitView | null>;
};

export type PersistenceOptions = {
	storeSession?: StoreHandle;
	history?: boolean;
	trackRuns?: boolean;
	runMetadata?: RunMetadata;
};

const handles = new WeakMap<StoreHandle, StoreSession>();

/** Let `handle` (a host's view of `session`) stand for it wherever a store session is taken. */
export function registerHandle(handle: StoreHandle, session: StoreSession) {
	handles.set(handle, session);
}

export function resolveStoreSession(handle: StoreHandle): StoreSession {
	const session = handles.get(handle);
	if (session) return session;
	// Internal callers already own the full session; never copy it into a facade.
	if ("backend" in handle) return handle as StoreSession;
	throw new Error("Invalid persistence session");
}

export type ResumeGuard = { key: string; expectedRevision: number; admitted: boolean };

type PersistenceContext = {
	session: StoreSession;
	recorder?: ExecutionRecorder;
	resumeGuard?: ResumeGuard;
};

// Only the adapters in this module transport context through existing env-based
// state APIs. One typed value keeps execution objects out of process variables
// while preserving them when workflow/SDK code spreads an environment.
const PERSISTENCE = Symbol("lobster.persistence");
export type StoreEnvironment = Record<string, string | undefined> & {
	[PERSISTENCE]?: PersistenceContext;
};

export function sessionFromEnv(env: StoreEnvironment | undefined): StoreSession | undefined {
	return env?.[PERSISTENCE]?.session;
}

export function recorderFromEnv(env: StoreEnvironment | undefined): ExecutionRecorder | undefined {
	return env?.[PERSISTENCE]?.recorder;
}

export function bindStoreSession(env: StoreEnvironment, handle: StoreHandle): StoreEnvironment {
	const session = resolveStoreSession(handle);
	return {
		...env,
		LOBSTER_STATE_DIR: session.directory,
		[PERSISTENCE]: env[PERSISTENCE]?.session === session ? env[PERSISTENCE] : { session },
	};
}

export function bindRecorder(env: StoreEnvironment, recorder: ExecutionRecorder): StoreEnvironment {
	const bound = bindStoreSession(env, recorder.session);
	return { ...bound, [PERSISTENCE]: { ...bound[PERSISTENCE], recorder } };
}

export function bindResumeGuard(env: StoreEnvironment, guard: ResumeGuard): StoreEnvironment {
	const context = env[PERSISTENCE];
	if (!context) throw new Error("Resume guard requires a persistence session");
	return { ...env, [PERSISTENCE]: { ...context, resumeGuard: guard } };
}

export function resumeGuardFromEnv(env: StoreEnvironment): ResumeGuard | undefined {
	return env[PERSISTENCE]?.resumeGuard;
}
