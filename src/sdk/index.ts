/**
 * Lobster SDK - Workflow runtime for AI agents
 *
 * @example
 * import { Lobster, approve, exec } from '@clawdbot/lobster';
 *
 * const workflow = new Lobster()
 *   .pipe(exec('gh pr view 123 --repo owner/repo --json title,url'))
 *   .pipe(items => items.filter(e => e.unread))
 *   .pipe(approve({ prompt: 'Process these emails?' }))
 *   .pipe(async function* (items) {
 *     for (const item of items) {
 *       yield { ...item, processed: true };
 *     }
 *   });
 *
 * const result = await workflow.run();
 */

export { Lobster, type LobsterOptions } from "./Lobster.js";
export { approve } from "./primitives/approve.js";
export { exec } from "./primitives/exec.js";
export { stateGet, stateSet, state } from "./primitives/state.js";
export { diffLast } from "./primitives/diff.js";
export { runPipeline } from "./runtime.js";

export {
	openStoreSession,
	bindStoreSession,
	type StoreSession,
	type StoreOptions,
} from "../persistence/session.js";
export { migrateStore } from "../persistence/migrate.js";
export {
	PersistenceError,
	PersistenceOutcomeUnknown,
	WaitConflictError,
} from "../persistence/backend.js";

export { listRuns, getRun, getWait, type RunQuery, type RunPage } from "../core/runs.js";
export type { RunView, WaitView, WaitSummary, WaitRequest } from "../persistence/waits.js";
export type { RunStatus, RunSummary } from "../persistence/model.js";
