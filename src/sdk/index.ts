/**
 * Lobster SDK - Workflow runtime for AI agents
 *
 * @example
 * import { Lobster, approve, exec } from '@clawdbot/lobster';
 *
 * const workflow = new Lobster()
 *   .pipe(exec('gh pr list --repo owner/repo --json title,url,isDraft'))
 *   .pipe((prs: Array<{ isDraft: boolean }>) => prs.filter((pr) => !pr.isDraft))
 *   .pipe(approve({ prompt: 'Process these PRs?' }))
 *   .pipe(async function* (items) {
 *     for await (const item of items) {
 *       yield { ...item, processed: true };
 *     }
 *   });
 *
 * const result = await workflow.run();
 */

export { Lobster, type LobsterOptions } from "./Lobster.js";
export {
	approve,
	exec,
	stateGet,
	stateSet,
	state,
	diffLast,
	type ApproveOptions,
	type ExecOptions,
	type DiffLastOptions,
} from "./primitives.js";
export {
	runPipeline,
	type RunPipelineOptions,
	type RunPipelineResult,
} from "../pipeline/run_once.js";
export { type SdkStage, type SdkStageObject, type SdkStageArgs } from "./runtime.js";

export {
	openStoreSession,
	bindStoreSession,
	type StoreSession,
	type StoreOptions,
} from "../core/store.js";
export {
	PersistenceError,
	PersistenceOutcomeUnknown,
	WaitConflictError,
} from "../persistence/backend.js";
// Thrown by `resume`, `resumeWait` and `advance` when nothing ran: refused, or a bad answer.
export { InvalidAnswerError, ResumeRefusedError } from "../engine/errors.js";

export { listRuns, getRun, getWait, type RunQuery, type RunPage } from "../core/runs.js";
export type { RunView, WaitView, WaitSummary, WaitRequest } from "../persistence/waits.js";
export type { RunStatus, RunSummary } from "../persistence/model.js";

export { upgradeStore, type UpgradeReport } from "../engine/upgrade.js";

export type { ResumeDecision, WaitResumeOptions, ResumeTarget } from "../resume/options.js";
export type { PersistenceOptions, RunScope } from "../persistence/context.js";
export type {
	RunMetadata,
	HistoryPage,
	RunEvent,
	Attempt,
	AttemptStatus,
	OperationReceipt,
} from "../persistence/model.js";
export type { SdkResult, SdkApprovalRequest, SdkInputRequest } from "./types.js";
