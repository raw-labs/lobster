import type {
	WorkflowStep,
	WorkflowApprovalIdentity,
	WorkflowStepResult,
	WorkflowRunResult,
} from "../workflows/types.js";
import { compileCached } from "../validation.js";
import { serializeValueForStdout } from "./values.js";
import { resolveInputValue } from "../workflows/values.js";

export class WorkflowResumeArgumentError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "WorkflowResumeArgumentError";
	}
}

export function extractApprovalRequest(
	step: WorkflowStep,
	result: WorkflowStepResult,
	env?: Record<string, string | undefined>,
) {
	const approvalConfig = normalizeApprovalConfig(step.approval);
	const configIdentity = approvalIdentityFromRaw(approvalConfig);
	if (!configIdentity.initiatedBy) {
		const fromEnv = String(env?.LOBSTER_APPROVAL_INITIATED_BY ?? "").trim();
		if (fromEnv) configIdentity.initiatedBy = fromEnv;
	}
	if (!configIdentity.requiredApprover) {
		const fromEnv = String(env?.LOBSTER_APPROVAL_REQUIRED_APPROVER ?? "").trim();
		if (fromEnv) configIdentity.requiredApprover = fromEnv;
	}
	if (!configIdentity.requireDifferentApprover) {
		const fromEnv = parseBoolLike(env?.LOBSTER_APPROVAL_REQUIRE_DIFFERENT_APPROVER);
		if (fromEnv === true) configIdentity.requireDifferentApprover = true;
	}
	const fallbackPrompt = approvalConfig.prompt ?? `Approve ${step.id}?`;
	const json = result.json;

	if (json && typeof json === "object" && !Array.isArray(json)) {
		const candidate = json as {
			requiresApproval?: {
				prompt?: string;
				items?: unknown[];
				preview?: string;
				initiated_by?: string;
				initiatedBy?: string;
				required_approver?: string;
				requiredApprover?: string;
				require_different_approver?: boolean;
				requireDifferentApprover?: boolean;
			};
			prompt?: string;
			items?: unknown[];
			preview?: string;
			initiated_by?: string;
			initiatedBy?: string;
			required_approver?: string;
			requiredApprover?: string;
			require_different_approver?: boolean;
			requireDifferentApprover?: boolean;
		};
		if (candidate.requiresApproval?.prompt) {
			const identity = {
				...configIdentity,
				...approvalIdentityFromRaw(candidate.requiresApproval as Record<string, unknown>),
			};
			return {
				type: "approval_request" as const,
				prompt: candidate.requiresApproval.prompt,
				items: candidate.requiresApproval.items ?? [],
				...(candidate.requiresApproval.preview
					? { preview: candidate.requiresApproval.preview }
					: null),
				...(identity.initiatedBy ? { initiatedBy: identity.initiatedBy } : null),
				...(identity.requiredApprover ? { requiredApprover: identity.requiredApprover } : null),
				...(identity.requireDifferentApprover ? { requireDifferentApprover: true } : null),
			};
		}
		if (candidate.prompt) {
			const identity = {
				...configIdentity,
				...approvalIdentityFromRaw(candidate as Record<string, unknown>),
			};
			return {
				type: "approval_request" as const,
				prompt: candidate.prompt,
				items: candidate.items ?? [],
				...(candidate.preview ? { preview: candidate.preview } : null),
				...(identity.initiatedBy ? { initiatedBy: identity.initiatedBy } : null),
				...(identity.requiredApprover ? { requiredApprover: identity.requiredApprover } : null),
				...(identity.requireDifferentApprover ? { requireDifferentApprover: true } : null),
			};
		}
	}

	const items = approvalConfig.items ?? normalizeApprovalItems(result.json);
	const preview = approvalConfig.preview ?? buildResultPreview(result);

	return {
		type: "approval_request" as const,
		prompt: fallbackPrompt,
		items,
		...(preview ? { preview } : null),
		...(configIdentity.initiatedBy ? { initiatedBy: configIdentity.initiatedBy } : null),
		...(configIdentity.requiredApprover
			? { requiredApprover: configIdentity.requiredApprover }
			: null),
		...(configIdentity.requireDifferentApprover ? { requireDifferentApprover: true } : null),
	};
}

export function approvalIdentityFromRaw(
	raw: Record<string, unknown> | NormalizedApprovalConfig | null | undefined,
): WorkflowApprovalIdentity {
	if (!raw) return {};
	const value = raw as Record<string, unknown>;
	const initiatedBy = String(value.initiatedBy ?? value.initiated_by ?? "").trim() || undefined;
	const requiredApprover =
		String(value.requiredApprover ?? value.required_approver ?? "").trim() || undefined;
	const requireDifferentRaw = value.requireDifferentApprover ?? value.require_different_approver;
	const requireDifferentApprover =
		typeof requireDifferentRaw === "boolean" ? requireDifferentRaw : undefined;
	return {
		...(initiatedBy ? { initiatedBy } : null),
		...(requiredApprover ? { requiredApprover } : null),
		...(requireDifferentApprover === true ? { requireDifferentApprover: true } : null),
	};
}

export function approvalIdentityFromRequest(
	request: WorkflowRunResult["requiresApproval"],
): WorkflowApprovalIdentity {
	if (!request) return {};
	return {
		...(request.initiatedBy ? { initiatedBy: request.initiatedBy } : null),
		...(request.requiredApprover ? { requiredApprover: request.requiredApprover } : null),
		...(request.requireDifferentApprover ? { requireDifferentApprover: true } : null),
	};
}

export function enforceApprovalIdentity({
	stepId,
	identity,
	approvedBy,
}: {
	stepId: string;
	identity: WorkflowApprovalIdentity | undefined;
	approvedBy?: string;
}) {
	const policy = identity ?? {};
	const approver = String(approvedBy ?? "").trim() || undefined;

	if (!policy.requiredApprover && !policy.requireDifferentApprover) return;
	if (!approver) {
		throw new WorkflowResumeArgumentError(
			`Workflow step ${stepId} approval requires approver identity; set LOBSTER_APPROVAL_APPROVED_BY`,
		);
	}
	if (policy.requiredApprover && approver !== policy.requiredApprover) {
		throw new WorkflowResumeArgumentError(
			`Workflow step ${stepId} approval requires approver '${policy.requiredApprover}', got '${approver}'`,
		);
	}
	if (policy.requireDifferentApprover && policy.initiatedBy && approver === policy.initiatedBy) {
		throw new WorkflowResumeArgumentError(
			`Workflow step ${stepId} approval must be granted by someone other than '${policy.initiatedBy}'`,
		);
	}
}

export function parseBoolLike(value: unknown): boolean | undefined {
	if (typeof value === "boolean") return value;
	if (value === undefined || value === null) return undefined;
	const normalized = String(value).trim().toLowerCase();
	if (["1", "true", "yes", "y"].includes(normalized)) return true;
	if (["0", "false", "no", "n"].includes(normalized)) return false;
	return undefined;
}

export function isInteractive(stdin: NodeJS.ReadableStream) {
	return Boolean((stdin as NodeJS.ReadStream).isTTY);
}

export function parseApprovalTimeoutMs(env: Record<string, string | undefined>) {
	const raw = env?.LOBSTER_APPROVAL_INPUT_TIMEOUT_MS;
	const value = Number(raw);
	if (!Number.isFinite(value) || value <= 0) return 0;
	return Math.floor(value);
}

const MAX_NEEDS_INPUT_SUBJECT_BYTES = 192_000;
const DEFAULT_TOOL_ENVELOPE_MAX_BYTES = 512_000;
const RESUME_TOKEN_PLACEHOLDER = "x".repeat(220);

export function parseResponseJson(raw: string): unknown {
	try {
		return JSON.parse(raw);
	} catch {
		throw new Error("Input response must be valid JSON");
	}
}

export function validateInputResponse(params: {
	schema: unknown;
	response: unknown;
	stepId: string;
}) {
	const validator = compileCached(params.schema as object);
	const ok = validator(params.response);
	if (ok) return;
	const first = validator.errors?.[0];
	const pathValue = first?.instancePath || "/";
	const reason = first?.message ? ` ${first.message}` : "";
	throw new Error(
		`Workflow input step ${params.stepId} response failed schema validation at ${pathValue}:${reason}`,
	);
}

export function resolveInputSubject(params: {
	step: WorkflowStep;
	args: Record<string, unknown>;
	results: Record<string, WorkflowStepResult>;
	lastStepId: string | null;
}) {
	if (params.step.stdin !== undefined) {
		return resolveInputValue(params.step.stdin, params.args, params.results);
	}
	if (!params.lastStepId) return null;
	const previous = params.results[params.lastStepId];
	if (!previous) return null;
	if (previous.json !== undefined) return previous.json;
	if (previous.response !== undefined) return previous.response;
	if (previous.stdout !== undefined) return previous.stdout;
	return null;
}

export function maybeTruncateInputSubject(subject: unknown): unknown {
	let serialized = "";
	try {
		serialized = JSON.stringify(subject ?? null);
	} catch {
		return {
			truncated: true,
			bytes: 0,
			preview: "[unserializable subject]",
		};
	}
	const byteLength = Buffer.byteLength(serialized, "utf8");
	if (byteLength <= MAX_NEEDS_INPUT_SUBJECT_BYTES) return subject;
	return {
		truncated: true,
		bytes: byteLength,
		preview: serialized.slice(0, 2000),
	};
}

export function resolveToolEnvelopeMaxBytes(env: Record<string, string | undefined>) {
	const raw = env?.LOBSTER_MAX_TOOL_ENVELOPE_BYTES;
	const parsed = Number(raw);
	if (!Number.isFinite(parsed) || parsed < 1024) {
		return DEFAULT_TOOL_ENVELOPE_MAX_BYTES;
	}
	return Math.floor(parsed);
}

export function buildNeedsInputRequest(params: {
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

	let subject = params.subject;
	let request = { ...base, subject };
	if (fitsNeedsInputEnvelope(request, params.maxEnvelopeBytes)) return request;

	subject = maybeTruncateInputSubject(subject);
	request = { ...base, subject };
	if (fitsNeedsInputEnvelope(request, params.maxEnvelopeBytes)) return request;

	request = {
		...base,
		subject: {
			truncated: true,
			bytes: estimateSerializedBytes(params.subject),
			preview: "[subject omitted: envelope size limit]",
		},
	};
	if (fitsNeedsInputEnvelope(request, params.maxEnvelopeBytes)) return request;

	throw new Error(
		`Workflow input step ${params.stepId} needs_input envelope exceeds ${params.maxEnvelopeBytes} bytes even after subject truncation`,
	);
}

export function fitsNeedsInputEnvelope(
	request: {
		type: "input_request";
		prompt: string;
		responseSchema: unknown;
		defaults?: unknown;
		subject: unknown;
	},
	maxEnvelopeBytes: number,
) {
	const envelope = {
		protocolVersion: 1,
		ok: true,
		status: "needs_input",
		output: [],
		requiresApproval: null,
		requiresInput: {
			...request,
			resumeToken: RESUME_TOKEN_PLACEHOLDER,
		},
	};
	return estimateSerializedBytes(envelope) <= maxEnvelopeBytes;
}

export function estimateSerializedBytes(value: unknown) {
	try {
		return Buffer.byteLength(JSON.stringify(value), "utf8");
	} catch {
		return Number.POSITIVE_INFINITY;
	}
}

export type NormalizedApprovalConfig = {
	prompt?: string;
	items?: unknown[];
	preview?: string;
	initiatedBy?: string;
	requiredApprover?: string;
	requireDifferentApprover?: boolean;
};

export function normalizeApprovalConfig(
	approval: WorkflowStep["approval"],
): NormalizedApprovalConfig {
	if (
		approval === true ||
		approval === "required" ||
		approval === undefined ||
		approval === false
	) {
		return {};
	}
	if (typeof approval === "string") {
		return { prompt: approval };
	}
	if (approval && typeof approval === "object" && !Array.isArray(approval)) {
		const value = approval as Record<string, unknown>;
		return {
			...(typeof value.prompt === "string" ? { prompt: value.prompt } : null),
			...(Array.isArray(value.items) ? { items: value.items } : null),
			...(typeof value.preview === "string" ? { preview: value.preview } : null),
			...(typeof value.initiatedBy === "string"
				? { initiatedBy: value.initiatedBy }
				: typeof value.initiated_by === "string"
					? { initiatedBy: value.initiated_by }
					: null),
			...(typeof value.requiredApprover === "string"
				? { requiredApprover: value.requiredApprover }
				: typeof value.required_approver === "string"
					? { requiredApprover: value.required_approver }
					: null),
			...(typeof value.requireDifferentApprover === "boolean"
				? { requireDifferentApprover: value.requireDifferentApprover }
				: typeof value.require_different_approver === "boolean"
					? { requireDifferentApprover: value.require_different_approver }
					: null),
		};
	}
	return {};
}

export function normalizeApprovalItems(value: unknown) {
	if (value === undefined) return [];
	return Array.isArray(value) ? value : [value];
}

export function buildResultPreview(result: WorkflowStepResult) {
	if (result.stdout) return result.stdout.trim().slice(0, 2000);
	if (result.json !== undefined) return serializeValueForStdout(result.json).trim().slice(0, 2000);
	return undefined;
}
