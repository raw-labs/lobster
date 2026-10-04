import { createHash } from "node:crypto";
import { WaitConflictError } from "./backend.js";
import { runSummary, type RunMetadata, type RunState, type RunSummary } from "./model.js";
import type { StoreSession } from "./session.js";

export type WaitRequest =
	| {
			kind: "input";
			prompt: string;
			responseSchema: unknown;
			defaults?: unknown;
			subject?: unknown;
	  }
	| { kind: "approval"; prompt: string; items: unknown[]; preview?: string };

export type WaitSummary = {
	id: string;
	runId: string;
	revision: number;
	kind: "input" | "approval";
};

export type WaitView = WaitSummary & { request: WaitRequest | null };

export type RunView = RunSummary & {
	historyEnabled: boolean;
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
		kind: run.waits[key].kind === "input_request" ? "input" : "approval",
	};
}

export function runView(run: RunState): RunView {
	return {
		...runSummary(run),
		historyEnabled: run.historyEnabled !== false,
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

export async function findWait(store: StoreSession, id: string, owner?: string) {
	const match = /^([a-zA-Z0-9_-]{1,128})\.([a-f0-9]{64})$/.exec(id);
	if (!match) throw new Error("Invalid wait ID");
	const run = await store.getRunState(match[1]);
	if (!run || !visibleTo(run, owner)) return null;
	const key = Object.keys(run.waits).find((key) => waitId(run.id, key) === id);
	if (!key || run.waits[key].status !== "pending" || !run.states[key]) return null;
	const state = JSON.parse(run.states[key]);
	return { run, key, view: { ...waitSummary(run, key), request: savedRequest(state) } };
}

function savedRequest(state: Record<string, unknown>): WaitRequest | null {
	const request = state.wait as WaitRequest | undefined;
	if (
		request &&
		["input", "approval"].includes(request.kind) &&
		typeof request.prompt === "string"
	) {
		if (request.kind === "input" && request.responseSchema !== undefined) {
			return {
				kind: "input",
				prompt: request.prompt,
				responseSchema: request.responseSchema,
				...(request.defaults !== undefined ? { defaults: request.defaults } : {}),
				...(request.subject !== undefined ? { subject: request.subject } : {}),
			};
		}
		if (request.kind === "approval" && Array.isArray(request.items)) {
			return {
				kind: "approval",
				prompt: request.prompt,
				items: request.items,
				...(typeof request.preview === "string" ? { preview: request.preview } : {}),
			};
		}
	}
	// Older pipeline checkpoints saved the prompt and schema, but not the entire
	// review context. Never reconstruct an old question from a changed workflow file.
	if (typeof state.prompt === "string") {
		if (state.inputSchema !== undefined) {
			return { kind: "input", prompt: state.prompt, responseSchema: state.inputSchema };
		}
		return {
			kind: "approval",
			prompt: state.prompt,
			items: Array.isArray(state.items) ? state.items : [],
		};
	}
	return null;
}

export async function resolveWait(
	store: StoreSession,
	id: string,
	revision: number,
	owner?: string,
) {
	if (!Number.isSafeInteger(revision) || revision < 0)
		throw new Error("Invalid expected wait revision");
	const wait = await findWait(store, id, owner);
	if (!wait || wait.view.revision !== revision) throw new WaitConflictError();
	return wait;
}
