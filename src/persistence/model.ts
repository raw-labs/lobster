import {
	asContinuation,
	earliestWaitDue,
	isFinished,
	isRunStatus,
	unresolvedActivities,
	type ExecutionState,
	type RunStatus,
} from "../engine/checkpoint.js";
import { fingerprint } from "../util/serialization.js";

/** What a run ID may be: letters, digits, `_` and `-`, at most 128 (a wait ID starts with one). */
const RUN_ID_PATTERN = "[a-zA-Z0-9_-]{1,128}";
const RUN_ID = new RegExp(`^${RUN_ID_PATTERN}$`);

const WAIT_ID = new RegExp(`^${RUN_ID_PATTERN}\\.[a-f0-9]{64}$`);

/** A wait ID: `<run id>.<64 hex>`. */
export function isWaitId(value: unknown): value is string {
	return typeof value === "string" && WAIT_ID.test(value);
}

export function isRunId(value: unknown): value is string {
	return typeof value === "string" && RUN_ID.test(value);
}

/** Refuse a malformed run ID: checked before any store is opened. */
export function checkRunId(id: unknown): asserts id is string {
	if (!isRunId(id)) throw new Error("Invalid run ID");
}

/** Who executes a run: the owner holding its lease, and until when (renewed while it runs). */
type ExecutionLease = { owner: string; epoch: number; expiresAt: number };

export const SCHEMA_VERSION = 1;
const CAPTURE_BYTES = 4096;

export type { RunStatus };

const ATTEMPT_STATUSES = [
	"started",
	"completed",
	"failed",
	"closed",
	"waiting",
	"skipped",
] as const;
export type AttemptStatus = (typeof ATTEMPT_STATUSES)[number];

export type StateRecord = { text: string | null; runId?: string };

export type Attempt = {
	id: string;
	path: string;
	name: string;
	ordinal?: number;
	attempt: number;
	status: AttemptStatus;
	startedAt: string;
	finishedAt?: string;
	summary?: unknown;
};

export type RunMetadata = {
	hostRunId?: string;
	hostTaskId?: string;
	callerId?: string;
};

/** Persist only bounded host references, never arbitrary invocation context. */
export function captureRunMetadata(metadata?: RunMetadata): RunMetadata | undefined {
	if (!metadata || typeof metadata !== "object") return undefined;
	return Object.fromEntries(
		Object.entries(metadata).filter(
			([key, value]) =>
				["hostRunId", "hostTaskId", "callerId"].includes(key) &&
				typeof value === "string" &&
				value.length > 0 &&
				value.length <= 256,
		),
	);
}

const WAIT_STATUSES = ["pending", "consumed", "retired"] as const;

export type WaitRecord = {
	revision?: number;
	status: (typeof WAIT_STATUSES)[number];
	kind: string;
	claimId?: string;
};

export type OperationReceipt = {
	fingerprint: string;
	revision: number;
	acknowledged?: boolean;
};

export type RunEvent = {
	sequence: number;
	kind: string;
	at: string;
	payload?: {
		id?: string;
		path?: string;
		key?: string;
		status?: AttemptStatus | RunStatus | WaitRecord["status"];
		summary?: unknown;
		fingerprint?: string;
		restored?: boolean;
	};
};

/** What a new run is: a workflow file, a pipeline, or an SDK pipeline. */
export type RunKind = "workflow" | "pipeline" | "sdk";

export type RunRecord = {
	execution?: ExecutionState;
	executionLease?: ExecutionLease;
	/** When the run next needs attention (epoch ms); see wakeTime. */
	wakeAt?: number;
	version: 1;
	id: string;
	/** A RunKind; stores written by older versions also hold "cli". */
	kind: string;
	status: RunStatus;
	revision: number;
	createdAt: string;
	updatedAt: string;
	partialHistory: boolean;
	historyEnabled?: boolean;
	definitions: string[];
	metadata?: RunMetadata;
	attempts: Attempt[];
	events: RunEvent[];
	waits: Record<string, WaitRecord>;
	// Continuation payloads are executable state, not bounded history summaries.
	states: Record<string, string | null>;
	receipts: Record<string, OperationReceipt>;
	/**
	 * Large parts of a running execution kept beside its checkpoint (a loop's items and each
	 * item's output), so a save does not rewrite them. Only in full reads and exports.
	 */
	records?: Record<string, string>;
};

/** Records written with a checkpoint save, in the same transaction. */
export type RecordChanges = {
	/** Delete every record whose key starts with one of these. Applied before `put`. */
	drop?: string[];
	put?: Record<string, string>;
};

export type RunAction =
	| { kind: "execution-claim"; owner: string; expiresAt: number; epoch: number }
	| { kind: "execution-renew"; owner: string; epoch: number; expiresAt: number }
	| { kind: "execution-release"; owner: string; epoch: number }
	| {
			kind: "execution-save";
			owner: string;
			epoch: number;
			expectedVersion: number;
			execution: ExecutionState;
			publish?: { key: string; text: string };
			consume?: { key: string; expectedWaitRevision?: number };
			records?: RecordChanges;
	  }
	| { kind: "create"; run: RunRecord; checkpoint?: { key: string; text: string } }
	| { kind: "definition"; fingerprint: string }
	| { kind: "start"; attempt: Attempt }
	| { kind: "finish"; id: string; status: AttemptStatus; summary?: unknown }
	| { kind: "settle"; status: RunStatus; summary?: unknown }
	| {
			kind: "state";
			key: string;
			text: string | null;
	  };

export type Transition = {
	operationId: string;
	runId: string;
	expectedRevision?: number;
	action: RunAction;
};

export type TransitionResult =
	| { status: "committed"; revision: number }
	| { status: "conflict"; revision: number };

type TransitionApplication =
	| { status: "committed"; revision: number; run: RunRecord }
	| { status: "conflict"; revision: number; run: RunRecord | null };

export type LogicalArchive = { version: 1; states: Record<string, string>; runs: RunRecord[] };

export type RunFilter = { status?: RunStatus; callerId?: string };

export type RunState = Omit<RunRecord, "attempts" | "events" | "receipts">;

export type RunSummary = Pick<
	RunRecord,
	"id" | "kind" | "status" | "revision" | "createdAt" | "updatedAt" | "partialHistory"
>;

export type HistoryPage = Omit<
	RunRecord,
	"states" | "receipts" | "records" | "execution" | "executionLease"
> & {
	definitionCount: number;
	nextCursor: number;
};

// Default history contains shape/counts, never arbitrary output, arguments, environment,
// prompts or credentials. Explicit diagnostic capture remains bounded and opt-in.
export function summarize(value: unknown): unknown {
	if (value instanceof Error) {
		return { error: value.name.slice(0, 128) };
	}
	if (Array.isArray(value)) {
		return { type: "array", count: value.length };
	}
	if (value === null) {
		return { type: "null" };
	}
	if (value && typeof value === "object") {
		return { type: "object", properties: Object.keys(value).length };
	}
	return { type: typeof value };
}

function capture(value: unknown): unknown {
	const text = JSON.stringify(value);
	if (text === undefined) {
		return null;
	}
	if (Buffer.byteLength(text) <= CAPTURE_BYTES) {
		return JSON.parse(text);
	}
	return { truncated: true, bytes: Buffer.byteLength(text) };
}

/** The kind of wait a saved continuation publishes ("legacy" for anything else). */
function savedWaitKind(value: unknown): string {
	const kind = asContinuation(value)?.wait.kind;
	if (kind === undefined) return "legacy";
	return kind === "input" ? "input_request" : kind === "approval" ? "approval_request" : kind;
}

export function applyTransition(
	current: RunRecord | null,
	request: Transition,
	projection: { previousAttempts?: number } = {},
): TransitionApplication {
	if (!/^[a-zA-Z0-9._-]{1,128}$/.test(request.operationId) || !isRunId(request.runId)) {
		throw new Error("Invalid persistence operation identity");
	}
	if (
		request.expectedRevision !== undefined &&
		(!Number.isSafeInteger(request.expectedRevision) || request.expectedRevision < 0)
	) {
		throw new Error("Invalid expected revision");
	}
	if (
		(current && current.id !== request.runId) ||
		(request.action.kind === "create" && request.action.run.id !== request.runId)
	) {
		throw new Error("Run identity mismatch");
	}
	const hash = fingerprint(request);
	const receipt = current?.receipts[request.operationId];
	if (receipt && current) {
		if (receipt.fingerprint !== hash) {
			throw new Error("Operation ID reused with different input");
		}
		return { status: "committed", revision: receipt.revision, run: current };
	}
	if (
		request.expectedRevision !== undefined &&
		request.expectedRevision !== (current?.revision ?? 0)
	) {
		return { status: "conflict", revision: current?.revision ?? 0, run: current };
	}
	const action = request.action;
	if (action.kind.startsWith("execution-") && action.kind !== "execution-claim") {
		const owned = action as Extract<RunAction, { owner: string }>;
		const lease = current?.executionLease;
		// Expiry only allows someone else to claim the run, and every claim moves the
		// epoch. While the epoch is unchanged no one has, so a holder that was merely
		// paused (laptop sleep, debugger, slow disk) keeps its run.
		if (!lease || lease.owner !== owned.owner || lease.epoch !== owned.epoch)
			return { status: "conflict", revision: current?.revision ?? 0, run: current };
	}
	if (action.kind === "execution-claim") {
		const lease = current?.executionLease;
		if (
			(lease && lease.expiresAt > Date.now()) ||
			action.epoch !== (lease?.epoch ?? 0) + 1 ||
			!Number.isFinite(action.expiresAt) ||
			action.expiresAt <= Date.now()
		)
			return { status: "conflict", revision: current?.revision ?? 0, run: current };
	}
	if (action.kind === "execution-save") {
		if (
			action.expectedVersion !== (current?.execution?.revision ?? 0) ||
			action.execution.version !== 1 ||
			action.execution.revision !== action.expectedVersion + 1
		)
			return { status: "conflict", revision: current?.revision ?? 0, run: current };
		if (action.consume) {
			const wait = current?.waits[action.consume.key];
			if (
				!wait ||
				wait.status !== "pending" ||
				(action.consume.expectedWaitRevision !== undefined &&
					wait.revision !== action.consume.expectedWaitRevision)
			)
				return { status: "conflict", revision: current?.revision ?? 0, run: current };
		}
	}
	if (action.kind === "create" && current) {
		throw new Error("Run already exists");
	}
	const source = action.kind === "create" ? action.run : current;
	if (!source) {
		throw new Error("Run not found");
	}

	const run = structuredClone(source);
	const at = new Date().toISOString();
	let payload: RunEvent["payload"];
	if (action.kind === "execution-claim") {
		run.executionLease = { owner: action.owner, epoch: action.epoch, expiresAt: action.expiresAt };
		// The previous owner is gone. Diagnostic attempts are no longer live;
		// unresolved effects remain authoritative in the execution checkpoint.
		for (const attempt of run.attempts) {
			if (attempt.status === "started") {
				attempt.status = "closed";
				attempt.finishedAt = at;
			}
		}
	} else if (action.kind === "execution-renew") {
		run.executionLease!.expiresAt = action.expiresAt;
	} else if (action.kind === "execution-release") {
		run.executionLease!.expiresAt = 0;
	} else if (action.kind === "execution-save") {
		run.execution = action.execution;
		if (action.consume) setCheckpoint(run, action.consume.key, null);
		if (action.publish) setCheckpoint(run, action.publish.key, action.publish.text);
		run.status = action.execution.status;
		// The wait it published or consumed (a save does one or the other), so history can show it.
		const key = action.publish?.key ?? action.consume?.key;
		if (key) payload = { key, status: run.waits[key]?.status };
	} else if (action.kind === "create" && action.checkpoint) {
		setCheckpoint(run, action.checkpoint.key, action.checkpoint.text);
	} else if (action.kind === "definition") {
		run.definitions.push(action.fingerprint);
		payload = { fingerprint: action.fingerprint };
	} else if (action.kind === "start") {
		const previousAttempts =
			projection.previousAttempts ??
			run.attempts.filter((a) => a.path === action.attempt.path).length;
		run.attempts.push({
			...action.attempt,
			attempt: action.attempt.attempt || previousAttempts + 1,
		});
		payload = { id: action.attempt.id, path: action.attempt.path };
	} else if (action.kind === "finish") {
		const attempt = run.attempts.find((a) => a.id === action.id);
		if (!attempt || attempt.status !== "started") {
			throw new Error("Attempt is not active");
		}
		Object.assign(attempt, {
			status: action.status,
			finishedAt: at,
			summary: capture(action.summary),
		});
		payload = { id: action.id, status: action.status };
	} else if (action.kind === "settle") {
		run.status = action.status;
		payload = { status: action.status, summary: capture(action.summary) };
	} else if (action.kind === "state") {
		const previous = run.states[action.key];
		setCheckpoint(run, action.key, action.text);
		payload = {
			key: action.key,
			status: run.waits[action.key]?.status,
			restored: previous !== undefined && previous !== null,
		};
	}
	const wakeAt = wakeTime(run);
	if (wakeAt === undefined) delete run.wakeAt;
	else run.wakeAt = wakeAt;
	run.updatedAt = at;
	run.revision++;
	if (run.historyEnabled !== false)
		run.events.push({
			sequence: run.revision,
			kind: action.kind,
			at,
			...(payload === undefined ? {} : { payload }),
		});
	run.receipts[request.operationId] = {
		fingerprint: hash,
		revision: run.revision,
		acknowledged: false,
	};
	return { status: "committed", revision: run.revision, run };
}

function setCheckpoint(run: RunRecord, key: string, text: string | null): void {
	run.states[key] = text;
	if (text === null) {
		if (run.waits[key]) run.waits[key].status = "retired";
	} else {
		const value = JSON.parse(text);
		if (value?.type === "lobster.consumed-resume-state.v1") {
			run.waits[key] ??= { kind: "legacy", status: "pending" };
			Object.assign(run.waits[key], { status: "consumed", claimId: value.claimId });
			run.status = "running";
		} else {
			run.waits[key] = { status: "pending", kind: savedWaitKind(value) };
			run.status = "waiting";
		}
	}
	const wait = run.waits[key];
	if (wait) wait.revision = run.revision + 1;
}

export function validateArchive(value: unknown): asserts value is LogicalArchive {
	const data = value as LogicalArchive;
	if (
		data?.version !== 1 ||
		!data.states ||
		typeof data.states !== "object" ||
		Array.isArray(data.states) ||
		!Array.isArray(data.runs)
	) {
		throw new Error("Unsupported persistence archive");
	}
	const ids = new Set<string>();
	const stateKeys = new Set(Object.keys(data.states));
	for (const [key, text] of Object.entries(data.states)) {
		if (!/^[a-z0-9._-]+$/.test(key) || typeof text !== "string") {
			throw new Error("Invalid state archive entry");
		}
		JSON.parse(text);
	}
	for (const run of data.runs) {
		if (
			!run ||
			run.version !== 1 ||
			!isRunId(run.id) ||
			ids.has(run.id) ||
			!Number.isInteger(run.revision) ||
			run.revision < 1 ||
			!isRunStatus(run.status) ||
			!Array.isArray(run.events) ||
			!Array.isArray(run.attempts) ||
			!Array.isArray(run.definitions) ||
			!run.states ||
			!run.waits ||
			!run.receipts
		) {
			throw new Error("Invalid run archive entry");
		}
		if (run.historyEnabled !== undefined && typeof run.historyEnabled !== "boolean") {
			throw new Error("Invalid run history setting");
		}
		if (
			run.records !== undefined &&
			(!run.records ||
				typeof run.records !== "object" ||
				Array.isArray(run.records) ||
				Object.values(run.records).some((text) => typeof text !== "string"))
		) {
			throw new Error("Invalid run records");
		}
		if (run.execution !== undefined) {
			const execution = run.execution;
			if (
				execution.version !== 1 ||
				!Number.isSafeInteger(execution.revision) ||
				execution.revision < 1 ||
				!execution.definition?.workflow ||
				!Array.isArray(execution.definition.workflow.steps) ||
				typeof execution.definition.filePath !== "string" ||
				!execution.activations ||
				!execution.args ||
				!isRunStatus(execution.status)
			)
				throw new Error("Invalid workflow execution snapshot");
		}
		ids.add(run.id);
		if (
			!Number.isFinite(Date.parse(run.createdAt)) ||
			!Number.isFinite(Date.parse(run.updatedAt))
		) {
			throw new Error("Invalid run timestamps");
		}
		for (const attempt of run.attempts) {
			if (
				!attempt ||
				typeof attempt.id !== "string" ||
				typeof attempt.path !== "string" ||
				typeof attempt.name !== "string" ||
				!Number.isInteger(attempt.attempt) ||
				attempt.attempt < 1 ||
				!(ATTEMPT_STATUSES as readonly string[]).includes(attempt.status)
			) {
				throw new Error("Invalid attempt archive entry");
			}
		}
		for (const [key, wait] of Object.entries(run.waits)) {
			if (
				!Object.hasOwn(run.states, key) ||
				!wait ||
				!(WAIT_STATUSES as readonly string[]).includes(wait.status) ||
				(wait.revision !== undefined &&
					(!Number.isSafeInteger(wait.revision) ||
						wait.revision < 0 ||
						wait.revision > run.revision))
			) {
				throw new Error("Invalid wait archive entry");
			}
		}
		for (const receipt of Object.values(run.receipts)) {
			if (
				!receipt ||
				typeof receipt.fingerprint !== "string" ||
				!Number.isInteger(receipt.revision) ||
				receipt.revision < 1 ||
				receipt.revision > run.revision
			) {
				throw new Error("Invalid operation receipt");
			}
		}
		let previous = 0;
		for (const event of run.events) {
			if (
				!Number.isInteger(event.sequence) ||
				event.sequence <= previous ||
				event.sequence > run.revision
			) {
				throw new Error("Invalid event sequence");
			}
			previous = event.sequence;
		}
		for (const [key, text] of Object.entries(run.states)) {
			if (stateKeys.has(key)) {
				throw new Error("Duplicate state association in archive");
			}
			stateKeys.add(key);
			if (!/^[a-z0-9._-]+$/.test(key) || (text !== null && typeof text !== "string")) {
				throw new Error("Invalid continuation archive entry");
			}
			if (text !== null) {
				JSON.parse(text);
			}
		}
	}
}

function isRetiredCheckpoint(text: string | null): boolean {
	if (text === null) return true;
	try {
		const value = JSON.parse(text);
		return (
			value?.type === "lobster.consumed-resume-state.v1" &&
			typeof value.claimId === "string" &&
			value.claimId.length > 0 &&
			Number.isFinite(Date.parse(value.consumedAt))
		);
	} catch {
		return false;
	}
}

export function canRetain(run: RunRecord, before: string): boolean {
	if (run.execution && unresolvedActivities(run.execution).length) return false;
	return (
		isFinished(run) &&
		Date.parse(run.updatedAt) < Date.parse(before) &&
		Object.values(run.states).every(isRetiredCheckpoint) &&
		!Object.values(run.waits).some((wait) => wait.status === "pending") &&
		!run.attempts.some((attempt) => attempt.status === "started") &&
		!Object.values(run.receipts).some((receipt) => receipt.acknowledged !== true)
	);
}

export function runSummary(run: RunSummary): RunSummary {
	const { id, kind, status, revision, createdAt, updatedAt, partialHistory } = run;
	return { id, kind, status, revision, createdAt, updatedAt, partialHistory };
}

export function historyPage(run: RunRecord, afterSequence: number, limit: number): HistoryPage {
	const {
		states: _states,
		receipts: _receipts,
		records: _records,
		execution: _execution,
		executionLease: _executionLease,
		events,
		attempts,
		waits,
		definitions,
		...metadata
	} = run;
	const page = events.filter((event) => event.sequence > afterSequence).slice(0, limit);
	const ids = new Set(page.map((event) => event.payload?.id));
	const keys = new Set(page.map((event) => event.payload?.key));
	return {
		...metadata,
		definitions: definitions.slice(-32),
		definitionCount: definitions.length,
		waits: Object.fromEntries(Object.entries(waits).filter(([key]) => keys.has(key))),
		events: page,
		attempts: attempts.filter((attempt) => ids.has(attempt.id)),
		nextCursor: page.at(-1)?.sequence ?? afterSequence,
	};
}

/**
 * When a run next needs attention, if ever on its own. A running run is due when its lease
 * lapses (a live owner keeps renewing it; a crashed one leaves it in the past). A waiting run
 * is due at its earliest timer or question deadline. Blocked and finished runs never are.
 */
function wakeTime(run: Pick<RunRecord, "execution" | "executionLease">): number | undefined {
	const execution = run.execution;
	if (!execution) return undefined;
	if (execution.status === "running") return run.executionLease?.expiresAt ?? 0;
	if (execution.status !== "waiting") return undefined;
	return earliestWaitDue(execution);
}
