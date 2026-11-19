// Approvals: a step's `approval:` gate, an `approve` inside a pipeline, and who may decide.
import type {
	WorkflowApprovalIdentity,
	WorkflowStep,
	WorkflowStepResult,
} from "../../workflows/types.js";
import { parseBoolLike } from "../../util/bool.js";
import { isPlainObject } from "../../util/objects.js";
import { PREVIEW_CHARS, yesOrNoPrompt } from "../../io/terminal.js";
import { activation, keys, type Activation, type WaitRequest } from "../checkpoint.js";
import type { Machine } from "../machine.js";
import type { Scope } from "../scope.js";
import { decideApproval } from "../asking.js";
import {
	askAtTerminal,
	cancelRun,
	failOnlyTheQuestion,
	resolveWait,
	waitDeadline,
	waitOf,
} from "../waits.js";
import { InvalidAnswerError } from "../errors.js";
import { serializeValueForStdout } from "../results.js";

type ApprovalWait = Extract<WaitRequest, { kind: "approval" }>;

/**
 * The approval gate after a step: asks about the step's result (`subject`), and completes with
 * it plus `approved`. The node before it has finished; it never runs again to ask.
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
 * with the next stage once it is approved. The host's approver rules (LOBSTER_APPROVAL_*)
 * apply, and a rejection cancels the run, as for a step approval.
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
	const wait = waitOf(frame, "approval", step.id, () => ({
		...request,
		...identityFromEnv(scope.ctx.env),
	}));
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
	return failOnlyTheQuestion(machine, key, frame, scope, async () => {
		const read = await askAtTerminal(
			machine,
			key,
			frame,
			scope,
			wait.prompt + " [y/N] ",
			frame.result!,
		);
		if ("timedOut" in read) return read.timedOut;
		// Any answer but yes or no is a typo, which fails only the question (see
		// failOnlyTheQuestion), as `resume` refuses it.
		const approved = yesOrNoPrompt(read.answer);
		if (approved === undefined)
			throw new InvalidAnswerError(`Answer yes or no (got ${JSON.stringify(read.answer)})`);
		if (!approved && wait.onReject !== "continue") return cancelRun(machine, frame, "Not approved");
		// Through resolveWait: a question published earlier (in tool mode) is consumed with it.
		return resolveWait(machine, frame, {
			...frame.result!,
			...decideApproval(
				{ stepId, standalone: scope.definition.standalone },
				wait,
				approved,
				scope.ctx.env,
			),
		});
	});
}

/**
 * The question an approval asks about `subject`. A step whose output asks for approval itself
 * (`{ requiresApproval: { prompt, ... } }`, or `{ prompt, ... }`) supplies the question, but
 * only where the workflow and the host set nothing: a command's output may fill in a prompt,
 * items or an approver rule, never replace the step's `approval:` settings or the
 * LOBSTER_APPROVAL_* variables. Without such output, the question comes from the step's
 * `approval:` setting, about the step's output.
 */
export function approvalRequest(
	step: WorkflowStep,
	subject: WorkflowStepResult,
	env: Record<string, string | undefined>,
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
		const asked = readApprovalFields(
			isPlainObject(nested) && nested.prompt ? nested : json,
			`Workflow step ${step.id}: its output asking for approval`,
		);
		if (asked.prompt) {
			const preview = setting.preview ?? asked.preview;
			return {
				kind: "approval",
				prompt: setting.prompt ?? asked.prompt,
				items: setting.items ?? asked.items ?? [],
				...(preview ? { preview } : {}),
				...asked.identity,
				...identity,
			};
		}
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

/**
 * The approval fields of a setting or of a step's output, in either spelling (camel or snake)
 * but not both: only one would be read, and the other dropped without a word.
 */
function readApprovalFields(raw: Record<string, unknown>, what = "An approval") {
	const either = (camel: string, snake: string) => {
		if (raw[camel] !== undefined && raw[snake] !== undefined)
			throw new Error(`${what} takes ${snake} or ${camel}, not both`);
		return raw[camel] ?? raw[snake];
	};
	const text = (value: unknown) => (typeof value === "string" && value) || undefined;
	const identity: WorkflowApprovalIdentity = {};
	const initiatedBy = text(either("initiatedBy", "initiated_by"))?.trim();
	const requiredApprover = text(either("requiredApprover", "required_approver"))?.trim();
	if (initiatedBy) identity.initiatedBy = initiatedBy;
	if (requiredApprover) identity.requiredApprover = requiredApprover;
	if (either("requireDifferentApprover", "require_different_approver") === true)
		identity.requireDifferentApprover = true;
	return {
		prompt: text(raw.prompt),
		items: Array.isArray(raw.items) ? raw.items : undefined,
		preview: text(raw.preview),
		identity,
	};
}

function identityFromEnv(env: Record<string, string | undefined>) {
	const identity: WorkflowApprovalIdentity = {};
	const initiatedBy = String(env.LOBSTER_APPROVAL_INITIATED_BY ?? "").trim();
	const requiredApprover = String(env.LOBSTER_APPROVAL_REQUIRED_APPROVER ?? "").trim();
	if (initiatedBy) identity.initiatedBy = initiatedBy;
	if (requiredApprover) identity.requiredApprover = requiredApprover;
	if (parseBoolLike(env.LOBSTER_APPROVAL_REQUIRE_DIFFERENT_APPROVER) === true)
		identity.requireDifferentApprover = true;
	return identity;
}

function resultPreview(result: WorkflowStepResult) {
	const text = result.stdout || serializeValueForStdout(result.json);
	return text.trim().slice(0, PREVIEW_CHARS) || undefined;
}
