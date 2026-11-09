import { exposeStoreSession } from "./context.js";
import { openStoreSession as openSession, type StoreOptions } from "./session.js";

export { bindStoreSession, type PublicStoreSession as StoreSession } from "./context.js";
export type { StoreOptions } from "./session.js";

/** Embedded hosts own lifecycle and inspection, not executor transitions. */
export async function openStoreSession(options: StoreOptions = {}) {
	return exposeStoreSession(await openSession(options));
}
