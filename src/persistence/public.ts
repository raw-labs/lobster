import { getRun, getWait } from "../core/runs.js";
import { registerHandle, type PublicStoreSession, type RunScope } from "./context.js";
import {
	openStoreSession as openSession,
	type StoreOptions,
	type StoreSession as Session,
} from "./session.js";

export { bindStoreSession, type PublicStoreSession as StoreSession } from "./context.js";
export type { StoreOptions } from "./session.js";

/** A host's view of a session: inspection and lifecycle, not executor transitions. */
export function exposeStoreSession(session: Session): PublicStoreSession {
	const handle: PublicStoreSession = Object.freeze({
		directory: session.directory,
		history: session.history,
		trackRuns: session.trackRuns,
		readOnly: session.readOnly,
		listRuns: session.listRuns.bind(session),
		getRun: (runId: string, scope: RunScope = {}) =>
			getRun({
				runId,
				ctx: { storeSession: handle, runMetadata: scope },
			}),
		getWait: (waitId: string, scope: RunScope = {}) =>
			getWait({
				waitId,
				ctx: { storeSession: handle, runMetadata: scope },
			}),
		inspect: session.inspect.bind(session),
		retain: session.retain.bind(session),
		reconcile: session.reconcile.bind(session),
		acknowledge: session.acknowledge.bind(session),
		close: session.close.bind(session),
	});
	registerHandle(handle, session);
	return handle;
}

/** Embedded hosts own lifecycle and inspection, not executor transitions. */
export async function openStoreSession(options: StoreOptions = {}) {
	return exposeStoreSession(await openSession(options));
}
