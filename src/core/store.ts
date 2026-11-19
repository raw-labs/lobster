// The run store as hosts see it: open a session, inspect runs and waits, close it. Executor
// transitions stay inside Lobster.
import { getRun, getWait, listRuns } from "./runs.js";
import { registerHandle, type PublicStoreSession, type RunScope } from "../persistence/context.js";
import {
	openStoreSession as openSession,
	type StoreOptions,
	type StoreSession as Session,
} from "../persistence/session.js";

export {
	bindStoreSession,
	type PublicStoreSession as StoreSession,
} from "../persistence/context.js";
export type { StoreOptions } from "../persistence/session.js";

/** A host's view of a session: inspection and lifecycle, not executor transitions. */
function exposeStoreSession(session: Session): PublicStoreSession {
	const handle: PublicStoreSession = Object.freeze({
		directory: session.directory,
		history: session.history,
		readOnly: session.readOnly,
		// Through core, as getRun and getWait: one check of the filter, whichever way runs are listed.
		listRuns: ({ callerId, ...query }: Parameters<Session["listRuns"]>[0] = {}) =>
			listRuns({
				...query,
				ctx: { storeSession: handle, runMetadata: callerId === undefined ? {} : { callerId } },
			}),
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
