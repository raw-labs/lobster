import { cleanupApprovalIndexByStateKey } from "../state/store.js";
import { withFileLock } from "../state/file_lock.js";
import { keyToPath, normalizeStateKey } from "../state/paths.js";
import { WaitConflictError } from "./backend.js";
import { recorderFromEnv, resumeGuardFromEnv, type StoreEnvironment } from "./context.js";
import { fingerprint, type CheckpointGuard } from "./model.js";

export type ContinuationPredecessor = {
	key: string;
	state: unknown;
	claimId?: string;
};

// Executors supply the complete checkpoint and their existing claim. Storage owns
// the atomic handoff; executors still decide when an unsafe step needs a claim.
export async function publishTrackedCheckpoint({
	env,
	key,
	state,
	predecessor,
	signal,
	assertInvocationCurrent,
}: {
	env: StoreEnvironment;
	key: string;
	state: unknown;
	predecessor?: ContinuationPredecessor;
	signal?: AbortSignal;
	assertInvocationCurrent?: () => void;
}): Promise<void> {
	const recorder = recorderFromEnv(env);
	if (!recorder) throw new Error("Checkpoint publication requires a tracked run");

	signal?.throwIfAborted();
	const successor = { key: normalizeStateKey(key), text: JSON.stringify(state) + "\n" };
	const guard = resumeGuardFromEnv(env);
	let checkpointGuard: CheckpointGuard | undefined;
	if (predecessor) {
		const previousKey = normalizeStateKey(predecessor.key);
		checkpointGuard = predecessor.claimId
			? { key: previousKey, status: "consumed", claimId: predecessor.claimId }
			: {
					key: previousKey,
					status: "pending",
					fingerprint: fingerprint(predecessor.state),
					...(guard && !guard.admitted && normalizeStateKey(guard.key) === previousKey
						? { expectedWaitRevision: guard.expectedRevision }
						: {}),
				};
	}

	const commit = () =>
		recorder.action(
			checkpointGuard
				? { kind: "handoff", predecessor: checkpointGuard, successor, decision: recorder.decision }
				: { kind: "state", ...successor },
			signal,
			assertInvocationCurrent,
		);
	// Legacy token consumers check their snapshot while holding this key lock.
	// Share that barrier so a stale claimant cannot overwrite a retired wait.
	const result = checkpointGuard
		? await withFileLock({
				filePath: keyToPath(recorder.session.directory, checkpointGuard.key),
				signal,
				task: commit,
			})
		: await commit();
	if (result.status === "conflict") throw new WaitConflictError();

	// Publication is final, including a commit recovered from a lost reply. Never
	// restore the old question or delete the new one after this point.
	recorder.ownsExecution = true;
	if (guard && checkpointGuard && normalizeStateKey(guard.key) === checkpointGuard.key) {
		guard.admitted = true;
	}
	if (checkpointGuard) {
		await cleanupApprovalIndexByStateKey({ env, stateKey: checkpointGuard.key }).catch(() => {});
	}
}
