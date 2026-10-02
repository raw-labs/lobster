import { createHash } from "node:crypto";

export const SCHEMA_VERSION = 1;
export const CAPTURE_BYTES = 4096;

export type RunStatus = "running" | "waiting" | "completed" | "failed" | "cancelled";

export type AttemptStatus = "started" | "completed" | "failed" | "closed" | "waiting" | "skipped";

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

export type WaitRecord = {
	status: "pending" | "consumed" | "retired";
	kind: string;
	prompt?: unknown;
	claimId?: string;
	decision?: unknown;
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

export type RunRecord = {
	version: 1;
	id: string;
	kind: string;
	status: RunStatus;
	revision: number;
	createdAt: string;
	updatedAt: string;
	partialHistory: boolean;
	definitions: string[];
	metadata?: RunMetadata;
	attempts: Attempt[];
	events: RunEvent[];
	waits: Record<string, WaitRecord>;
	// Continuation payloads are executable state, not bounded history summaries.
	states: Record<string, string | null>;
	receipts: Record<string, OperationReceipt>;
};

export type RunAction =
	| { kind: "create"; run: RunRecord }
	| { kind: "definition"; fingerprint: string }
	| { kind: "start"; attempt: Attempt }
	| { kind: "finish"; id: string; status: AttemptStatus; summary?: unknown }
	| { kind: "settle"; status: RunStatus; summary?: unknown }
	| { kind: "state"; key: string; text: string | null; decision?: unknown };

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

export type RunSummary = Pick<
	RunRecord,
	"id" | "kind" | "status" | "revision" | "createdAt" | "updatedAt" | "partialHistory"
>;

export type HistoryPage = Omit<RunRecord, "states" | "receipts"> & {
	definitionCount: number;
	nextCursor: number;
};

export function fingerprint(value: unknown): string {
	const text = JSON.stringify(value, (_key, item: unknown) => {
		if (!item || typeof item !== "object" || Array.isArray(item)) {
			return item;
		}

		const record = item as Record<string, unknown>;
		const keys = Object.keys(record).sort();
		return Object.fromEntries(keys.map((key) => [key, record[key]]));
	});
	return createHash("sha256").update(text).digest("hex");
}

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

export function capture(value: unknown): unknown {
	const text = JSON.stringify(value);
	if (text === undefined) {
		return null;
	}
	if (Buffer.byteLength(text) <= CAPTURE_BYTES) {
		return JSON.parse(text);
	}
	return { truncated: true, bytes: Buffer.byteLength(text) };
}

export function applyTransition(
	current: RunRecord | null,
	request: Transition,
	projection: { previousAttempts?: number } = {},
): TransitionApplication {
	if (
		!/^[a-zA-Z0-9._-]{1,128}$/.test(request.operationId) ||
		!/^[a-zA-Z0-9_-]{1,128}$/.test(request.runId)
	) {
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
	if (action.kind === "definition") {
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
		// A stale/rejected resume must never settle another caller's run.
		run.status = action.status;
		payload = { status: action.status, summary: capture(action.summary) };
	} else if (action.kind === "state") {
		const previous = run.states[action.key];
		run.states[action.key] = action.text;
		if (action.text === null) {
			if (run.waits[action.key]) {
				run.waits[action.key].status = "retired";
			}
		} else {
			const value = JSON.parse(action.text);
			if (value?.type === "lobster.consumed-resume-state.v1") {
				run.waits[action.key] ??= { kind: "legacy", status: "pending" };
				Object.assign(run.waits[action.key], { status: "consumed", claimId: value.claimId });
				run.status = "running";
			} else {
				run.waits[action.key] = {
					status: "pending",
					kind:
						value.haltType ??
						(value.inputStepId ||
						value.inputSchema !== undefined ||
						value.payload?.inputSchema !== undefined
							? "input_request"
							: "approval_request"),
				};
				run.status = "waiting";
			}
		}
		if (run.waits[action.key] && action.decision !== undefined) {
			run.waits[action.key].decision = action.decision;
		}
		payload = {
			key: action.key,
			status: run.waits[action.key]?.status,
			restored: previous !== undefined && previous !== null,
		};
	}
	run.updatedAt = at;
	run.revision++;
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
			!/^[a-zA-Z0-9_-]+$/.test(run.id) ||
			ids.has(run.id) ||
			!Number.isInteger(run.revision) ||
			run.revision < 1 ||
			!["running", "waiting", "completed", "failed", "cancelled"].includes(run.status) ||
			!Array.isArray(run.events) ||
			!Array.isArray(run.attempts) ||
			!Array.isArray(run.definitions) ||
			!run.states ||
			!run.waits ||
			!run.receipts
		) {
			throw new Error("Invalid run archive entry");
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
				!["started", "completed", "failed", "closed", "waiting", "skipped"].includes(attempt.status)
			) {
				throw new Error("Invalid attempt archive entry");
			}
		}
		for (const [key, wait] of Object.entries(run.waits)) {
			if (
				!Object.hasOwn(run.states, key) ||
				!wait ||
				!["pending", "consumed", "retired"].includes(wait.status)
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

export function canRetain(run: RunRecord, before: string): boolean {
	return (
		["completed", "failed", "cancelled"].includes(run.status) &&
		Date.parse(run.updatedAt) < Date.parse(before) &&
		!Object.values(run.states).some((text) => text !== null) &&
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

export function validateContinuations(data: LogicalArchive) {
	const entries = data.runs.flatMap((run) => Object.entries(run.states));
	for (const [key, text] of entries) {
		if (text === null || !/^(pipeline_resume_|workflow[-_]resume_|sdk_resume_)/.test(key)) {
			continue;
		}
		const value = JSON.parse(text);
		if (value?.type === "lobster.consumed-resume-state.v1") {
			if (typeof value.claimId !== "string" || !Number.isFinite(Date.parse(value.consumedAt))) {
				throw new Error("Invalid consumed continuation");
			}
		} else if (key.startsWith("pipeline_resume_")) {
			if (
				!value ||
				!Array.isArray(value.pipeline) ||
				!Array.isArray(value.items) ||
				!Number.isInteger(value.resumeAtIndex) ||
				value.resumeAtIndex < 0 ||
				value.resumeAtIndex > value.pipeline.length ||
				value.pipeline.some(
					(stage) =>
						typeof stage?.name !== "string" ||
						!stage.name ||
						!stage.args ||
						typeof stage.args !== "object" ||
						Array.isArray(stage.args),
				)
			) {
				throw new Error("Invalid pipeline continuation");
			}
		} else if (key.startsWith("sdk_resume_")) {
			const frame = value?.type === "lobster.sdk-tracked.v1" ? value.payload : value;
			if (!frame || !Number.isInteger(frame.resumeAtIndex) || frame.resumeAtIndex < 0) {
				throw new Error("Invalid SDK continuation");
			}
		} else if (
			!value ||
			typeof value.filePath !== "string" ||
			!value.steps ||
			typeof value.steps !== "object" ||
			Array.isArray(value.steps) ||
			!value.args ||
			typeof value.args !== "object" ||
			Array.isArray(value.args) ||
			!Number.isInteger(value.resumeAtIndex) ||
			value.resumeAtIndex < 0
		) {
			throw new Error("Invalid workflow continuation");
		}
	}
}
