// What a run that paused reports to its caller: the question and how to answer it (a resume
// token, an approval ID), or what it waits for. The request must fit in a tool envelope.
import { PREVIEW_CHARS } from "../io/terminal.js";
import { encodeToken } from "../resume/token.js";
import { createApprovalIndex, findApprovalIdByStateKey } from "../state/store.js";
import type { WorkflowRunResult } from "../workflows/types.js";
import type { Activation } from "./checkpoint.js";

const MAX_NEEDS_INPUT_SUBJECT_BYTES = 192_000;
const DEFAULT_TOOL_ENVELOPE_MAX_BYTES = 512_000;
const RESUME_TOKEN_PLACEHOLDER = "x".repeat(220);

/** The result of a run paused at `frame`. `fresh`: the question was asked by this call. */
export async function pausedResult(
	frame: Activation,
	env: Record<string, string | undefined>,
	fresh: boolean,
): Promise<WorkflowRunResult> {
	const { request: wait, stepId, key } = frame.wait!;
	if (wait.kind === "timer")
		return {
			status: "waiting",
			output: [],
			waitingFor: { kind: "timer", stepId, until: new Date(wait.until).toISOString() },
		};
	if (wait.kind === "event")
		return {
			status: "waiting",
			output: [],
			waitingFor: {
				kind: "event",
				stepId,
				event: wait.event,
				...(wait.deadline !== undefined ? { until: new Date(wait.deadline).toISOString() } : {}),
			},
		};
	const stateKey = key!;
	const resumeToken = encodeToken({ protocolVersion: 1, v: 1, kind: "workflow-file", stateKey });
	if (wait.kind === "approval") {
		// A question asked before keeps its short ID instead of minting another.
		const approvalId =
			(!fresh && (await findApprovalIdByStateKey({ env, stateKey }))) ||
			(await createApprovalIndex({ env, stateKey }));
		return {
			status: "needs_approval",
			output: [],
			requiresApproval: {
				type: "approval_request",
				prompt: wait.prompt,
				items: wait.items,
				...(wait.preview ? { preview: wait.preview } : {}),
				...(wait.initiatedBy ? { initiatedBy: wait.initiatedBy } : {}),
				...(wait.requiredApprover ? { requiredApprover: wait.requiredApprover } : {}),
				...(wait.requireDifferentApprover ? { requireDifferentApprover: true } : {}),
				resumeToken,
				approvalId,
			},
		};
	}
	const request = needsInputRequest({
		stepId,
		prompt: wait.prompt,
		responseSchema: wait.responseSchema,
		defaults: wait.defaults,
		subject: wait.subject,
		maxEnvelopeBytes: toolEnvelopeMaxBytes(env),
	});
	return { status: "needs_input", output: [], requiresInput: { ...request, resumeToken } };
}

/** The largest tool envelope a host accepts (`LOBSTER_MAX_TOOL_ENVELOPE_BYTES`). */
function toolEnvelopeMaxBytes(env: Record<string, string | undefined>) {
	const parsed = Number(env.LOBSTER_MAX_TOOL_ENVELOPE_BYTES);
	if (!Number.isFinite(parsed) || parsed < 1024) return DEFAULT_TOOL_ENVELOPE_MAX_BYTES;
	return Math.floor(parsed);
}

/**
 * An input request that fits in a `needs_input` envelope of `maxEnvelopeBytes`: with its
 * subject whole, else truncated to a preview, else omitted.
 */
function needsInputRequest(params: {
	stepId: string;
	prompt: string;
	responseSchema: unknown;
	defaults: unknown;
	subject: unknown;
	maxEnvelopeBytes: number;
}) {
	const base = {
		type: "input_request" as const,
		prompt: params.prompt,
		responseSchema: params.responseSchema,
		...(params.defaults !== undefined ? { defaults: params.defaults } : null),
	};
	const omitted = {
		truncated: true,
		bytes: serializedBytes(params.subject),
		preview: "[subject omitted: envelope size limit]",
	};
	for (const subject of [params.subject, truncatedSubject(params.subject), omitted]) {
		const request = { ...base, subject };
		if (fitsEnvelope(request, params.maxEnvelopeBytes)) return request;
	}
	throw new Error(
		`Workflow input step ${params.stepId} needs_input envelope exceeds ${params.maxEnvelopeBytes} bytes even after subject truncation`,
	);
}

/** The subject, or a preview of it when it is over MAX_NEEDS_INPUT_SUBJECT_BYTES. */
function truncatedSubject(subject: unknown): unknown {
	let serialized: string;
	try {
		serialized = JSON.stringify(subject ?? null);
	} catch {
		return { truncated: true, bytes: 0, preview: "[unserializable subject]" };
	}
	const bytes = Buffer.byteLength(serialized, "utf8");
	if (bytes <= MAX_NEEDS_INPUT_SUBJECT_BYTES) return subject;
	return { truncated: true, bytes, preview: serialized.slice(0, PREVIEW_CHARS) };
}

function fitsEnvelope(request: object, maxEnvelopeBytes: number) {
	const envelope = {
		protocolVersion: 1,
		ok: true,
		status: "needs_input",
		output: [],
		requiresApproval: null,
		requiresInput: { ...request, resumeToken: RESUME_TOKEN_PLACEHOLDER },
	};
	return serializedBytes(envelope) <= maxEnvelopeBytes;
}

function serializedBytes(value: unknown) {
	try {
		return Buffer.byteLength(JSON.stringify(value), "utf8");
	} catch {
		return Number.POSITIVE_INFINITY;
	}
}
