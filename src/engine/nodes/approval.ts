// Approvals: a step's `approval:` gate, an `approve` inside a pipeline, and who may decide.
import type {
	WorkflowApprovalIdentity,
	WorkflowStep,
	WorkflowStepResult,
} from "../../workflows/types.js";
import { parseBoolLike } from "../../util/bool.js";
import { isPlainObject } from "../../util/objects.js";
import { PREVIEW_CHARS } from "../../io/terminal.js";
import { activation, keys, type Activation, type WaitRequest } from "../checkpoint.js";
import type { Machine } from "../machine.js";
import type { Scope } from "../scope.js";
import { decideApproval } from "../asking.js";
import { askAtTerminal, cancelRun, resolveWait, waitDeadline, waitOf } from "../waits.js";
import { serializeValueForStdout } from "../results.js";

type ApprovalWait = Extract<WaitRequest, { kind: "approval" }>;

/**
 * The approval gate after a step: asks about the step's result (`subject`), and completes with
 * it plus `approved`. The node before it has finished and been charged; it never runs again
 * to ask.
 */
export async function approval(
	machine: Machine,
	step: WorkflowStep,
	key: string,
	scope: Scope,
	subject: WorkflowStepResult,
) {
	const gateKey = keys.approval(key);
	const frame = activation(machine.state, gateKey);
	if (frame.phase === "completed") return frame.result!;
	const config = typeof step.approval === "object" ? step.approval : undefined;
	const wait = waitOf(frame, "approval", step.id, () => ({
		...approvalRequest(step, subject, scope.ctx.env),
		...waitDeadline(config, `Step ${step.id} approval`),
		...(config?.on_reject === "continue" ? { onReject: "continue" as const } : {}),
	}));
	frame.result ??= { ...subject, ...(wait.initiatedBy ? { initiatedBy: wait.initiatedBy } : {}) };
	return ask(machine, step.id, gateKey, frame, wait, scope);
}

/**
 * An `approve` inside a pipeline step: the pipeline halted with this request, and continues
 * with the next stage once it is approved. A rejection cancels the run, as for a step approval.
 */
export async function pipelineApproval(
	machine: Machine,
	step: WorkflowStep,
	key: string,
	scope: Scope,
	request: ApprovalWait,
) {
	const frame = activation(machine.state, key);
	if (frame.phase === "completed") return frame.result!;
	frame.result ??= { id: step.id };
	const wait = waitOf(frame, "approval", step.id, () => request);
	return ask(machine, step.id, key, frame, wait, scope);
}

/**
 * Ask a person, or pause the run when nobody can be asked here. A "no" cancels the run, unless
 * `on_reject: continue` lets it go on with `approved: false`.
 */
async function ask(
	machine: Machine,
	stepId: string,
	key: string,
	frame: Activation,
	wait: ApprovalWait,
	scope: Scope,
) {
	const read = await askAtTerminal(
		machine,
		key,
		frame,
		scope,
		wait.prompt + " [y/N] ",
		frame.result!,
	);
	if ("timedOut" in read) return read.timedOut;
	const approved = /^y(es)?$/i.test(read.answer);
	if (!approved && wait.onReject !== "continue") return cancelRun(machine, frame, "Not approved");
	// Through resolveWait: a question published earlier (in tool mode) is consumed with it.
	return resolveWait(machine, frame, {
		...frame.result!,
		...decideApproval(stepId, wait, approved, scope.ctx.env),
	});
}

/**
 * The question an approval asks about `subject`. A step whose output asks for approval itself
 * (`{ requiresApproval: { prompt, ... } }`, or `{ prompt, ... }`) supplies the question;
 * otherwise it comes from the step's `approval:` setting, about the step's output. Identity
 * settings the workflow does not give come from the LOBSTER_APPROVAL_* variables.
 */
export function approvalRequest(
	step: WorkflowStep,
	subject: WorkflowStepResult,
	env?: Record<string, string | undefined>,
): ApprovalWait {
	const setting =
		typeof step.approval === "object"
			? readApprovalFields(step.approval)
			: readApprovalFields(
					typeof step.approval === "string" && step.approval !== "required"
						? { prompt: step.approval }
						: {},
				);
	const identity = { ...identityFromEnv(env), ...setting.identity };
	const json = subject.json;
	if (isPlainObject(json)) {
		const nested = json.requiresApproval;
		const asked = readApprovalFields(isPlainObject(nested) && nested.prompt ? nested : json);
		if (asked.prompt)
			return {
				kind: "approval",
				prompt: asked.prompt,
				items: asked.items ?? [],
				...(asked.preview ? { preview: asked.preview } : {}),
				...identity,
				...asked.identity,
			};
	}
	const preview = setting.preview ?? resultPreview(subject);
	return {
		kind: "approval",
		prompt: setting.prompt ?? `Approve ${step.id}?`,
		items: setting.items ?? (json === undefined ? [] : Array.isArray(json) ? json : [json]),
		...(preview ? { preview } : {}),
		...identity,
	};
}

/** The approval fields of a setting or of a step's output, in either spelling (camel or snake). */
function readApprovalFields(raw: Record<string, unknown>) {
	const text = (...values: unknown[]) =>
		values.find((value): value is string => typeof value === "string");
	const name = (...values: unknown[]) => text(...values)?.trim() || undefined;
	const identity: WorkflowApprovalIdentity = {};
	const initiatedBy = name(raw.initiatedBy, raw.initiated_by);
	const requiredApprover = name(raw.requiredApprover, raw.required_approver);
	const requireDifferent = [raw.requireDifferentApprover, raw.require_different_approver].find(
		(value) => typeof value === "boolean",
	);
	if (initiatedBy) identity.initiatedBy = initiatedBy;
	if (requiredApprover) identity.requiredApprover = requiredApprover;
	if (requireDifferent === true) identity.requireDifferentApprover = true;
	return {
		prompt: text(raw.prompt) || undefined,
		items: Array.isArray(raw.items) ? raw.items : undefined,
		preview: text(raw.preview) || undefined,
		identity,
	};
}

function identityFromEnv(env: Record<string, string | undefined> | undefined) {
	const identity: WorkflowApprovalIdentity = {};
	const initiatedBy = String(env?.LOBSTER_APPROVAL_INITIATED_BY ?? "").trim();
	const requiredApprover = String(env?.LOBSTER_APPROVAL_REQUIRED_APPROVER ?? "").trim();
	if (initiatedBy) identity.initiatedBy = initiatedBy;
	if (requiredApprover) identity.requiredApprover = requiredApprover;
	if (parseBoolLike(env?.LOBSTER_APPROVAL_REQUIRE_DIFFERENT_APPROVER) === true)
		identity.requireDifferentApprover = true;
	return identity;
}

function resultPreview(result: WorkflowStepResult) {
	const text = result.stdout || serializeValueForStdout(result.json);
	return text.trim().slice(0, PREVIEW_CHARS) || undefined;
}
