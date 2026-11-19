// Waits as hosts see them: their IDs, revisions and requests, and the run views that list
// them. Finding a wait by ID is checked against the caller that owns the run.
import { createHash } from "node:crypto";
import { WaitConflictError } from "./backend.js";
import { isWaitId, runSummary, type RunMetadata, type RunState, type RunSummary } from "./model.js";
import type { StoreSession } from "./session.js";
import {
	asContinuation,
	nextRetryAt,
	unresolvedActivities,
	type RunStatus,
	type WaitRequest,
} from "../engine/checkpoint.js";

export type { WaitRequest };

export type WaitSummary = {
	id: string;
	runId: string;
	revision: number;
	kind: "input" | "approval" | "timer" | "event";
};

export type WaitView = WaitSummary & { request: WaitRequest | null };

export type RunView = RunSummary & {
	historyEnabled: boolean;
	execution?: {
		version: number;
		status: RunStatus;
		error?: string;
		nextRetryAt?: number;
		/** When `runs advance --due` next picks this run up (epoch ms). */
		wakeAt?: number;
		active: boolean;
		unresolvedActivities: string[];
	};
	metadata?: RunMetadata;
	pendingWaits: WaitSummary[];
};

function waitId(runId: string, key: string): string {
	return `${runId}.${createHash("sha256").update(key).digest("hex")}`;
}

function waitSummary(run: RunState, key: string): WaitSummary {
	return {
		id: waitId(run.id, key),
		runId: run.id,
		revision: run.waits[key].revision ?? 0,
		kind:
			run.waits[key].kind === "input_request"
				? "input"
				: run.waits[key].kind === "timer" || run.waits[key].kind === "event"
					? run.waits[key].kind
					: "approval",
	};
}

export function runView(run: RunState): RunView {
	const retryAt = run.execution ? nextRetryAt(run.execution) : undefined;
	return {
		...runSummary(run),
		historyEnabled: run.historyEnabled !== false,
		...(run.execution
			? {
					execution: {
						version: run.execution.version,
						status: run.execution.status,
						...(run.execution.error ? { error: run.execution.error } : {}),
						...(retryAt !== undefined ? { nextRetryAt: retryAt } : {}),
						...(run.wakeAt !== undefined ? { wakeAt: run.wakeAt } : {}),
						active: (run.executionLease?.expiresAt ?? 0) > Date.now(),
						unresolvedActivities: unresolvedActivities(run.execution),
					},
				}
			: {}),
		...(run.metadata ? { metadata: run.metadata } : {}),
		pendingWaits: Object.keys(run.waits)
			.filter((key) => run.waits[key].status === "pending" && typeof run.states[key] === "string")
			.map((key) => waitSummary(run, key)),
	};
}

export function callerId(metadata?: RunMetadata): string | undefined {
	const id = metadata?.callerId;
	if (id !== undefined && (typeof id !== "string" || !id.length || id.length > 256)) {
		throw new Error("Invalid caller ID");
	}
	return id;
}

export function visibleTo(run: RunState, owner: string | undefined): boolean {
	return owner === undefined || run.metadata?.callerId === owner;
}

/** A wait reference's shape: its ID and a non-negative revision. Checked before any lookup. */
export function checkWaitReference(waitId: string, revision: number) {
	runIdOfWait(waitId);
	if (!Number.isSafeInteger(revision) || revision < 0)
		throw new Error("Invalid expected wait revision");
}

/** The run a wait ID names (`<run id>.<64 hex>`); a malformed ID is refused. */
export function runIdOfWait(id: string): string {
	if (!isWaitId(id)) throw new Error("Invalid wait ID");
	return id.slice(0, id.lastIndexOf("."));
}

export async function findWait(store: StoreSession, id: string, owner?: string) {
	const run = await store.getRunState(runIdOfWait(id));
	if (!run || !visibleTo(run, owner)) return null;
	const key = Object.keys(run.waits).find((key) => waitId(run.id, key) === id);
	if (!key || run.waits[key].status !== "pending" || !run.states[key]) return null;
	// The question the wait's saved continuation asks (engine/checkpoint.ts writes it whole).
	const request = asContinuation(JSON.parse(run.states[key]))?.wait ?? null;
	return { run, key, view: { ...waitSummary(run, key), request } };
}

/** The wait `id`, which must still be at `revision`: an answer to an older question conflicts. */
export async function findWaitAtRevision(
	store: StoreSession,
	id: string,
	revision: number,
	owner?: string,
) {
	// The reference's shape was checked before the store was opened (checkWaitReference).
	const wait = await findWait(store, id, owner);
	if (!wait || wait.view.revision !== revision) throw new WaitConflictError();
	return wait;
}
