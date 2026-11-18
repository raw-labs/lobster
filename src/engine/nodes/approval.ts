import { activation, type Activation } from "../state.js";
import type { Machine, Scope } from "../machine.js";
import type { WorkflowStep, WorkflowStepResult } from "../../workflows/types.js";
import {
	extractApprovalRequest,
	approvalIdentityFromRequest,
	enforceApprovalIdentity,
	questionDeadline,
	readAnswer,
} from "../gates.js";
import { isInteractive } from "../../io/terminal.js";

export async function approval(
	machine: Machine,
	step: WorkflowStep,
	key: string,
	scope: Scope,
	subject: WorkflowStepResult,
) {
	const frame = activation(machine.state, key + "/approval");
	if (frame.phase === "completed") return frame.result!;
	frame.approval ??= extractApprovalRequest(step, subject, scope.ctx.env);
	frame.result ??= { ...subject };
	if (frame.approval.initiatedBy) frame.result.initiatedBy = frame.approval.initiatedBy;
	frame.wait ??= {
		kind: "approval",
		prompt: frame.approval.prompt,
		items: frame.approval.items,
		...(frame.approval.preview ? { preview: frame.approval.preview } : {}),
		...questionDeadline(
			typeof step.approval === "object" ? step.approval : undefined,
			`Step ${step.id} approval`,
		),
		...(typeof step.approval === "object" && step.approval.on_reject === "continue"
			? { onReject: "continue" as const }
			: {}),
	};
	return ask(machine, step.id, key + "/approval", frame, scope);
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
	request: { prompt: string; items: unknown[]; preview?: string },
) {
	const frame = activation(machine.state, key);
	if (frame.phase === "completed") return frame.result!;
	frame.approval ??= {
		type: "approval_request",
		prompt: request.prompt,
		items: request.items,
		...(request.preview ? { preview: request.preview } : {}),
	};
	frame.result ??= { id: step.id };
	frame.stepId = step.id;
	frame.wait ??= { kind: "approval", ...request };
	return ask(machine, step.id, key, frame, scope);
}

/** Ask a person, or pause the run when nobody can be asked here. */
async function ask(machine: Machine, stepId: string, key: string, frame: Activation, scope: Scope) {
	const approval = frame.approval!;
	const deadline = frame.wait?.kind === "approval" ? frame.wait.deadline : undefined;
	if (machine.expired(frame)) return machine.timeOut(key, frame, frame.result!);
	if (scope.ctx.mode === "tool" || !isInteractive(scope.ctx.stdin))
		await machine.suspend(key, frame);
	scope.ctx.stdout.write(approval.prompt + " [y/N] ");
	const read = await readAnswer(scope, deadline);
	if ("expired" in read) return machine.timeOut(key, frame, frame.result!);
	const approved = /^y(es)?$/i.test(String(read.answer).trim());
	const continues = frame.wait?.kind === "approval" && frame.wait.onReject === "continue";
	if (!approved && !continues) throw new Error("Not approved");
	const approvedBy = String(scope.ctx.env.LOBSTER_APPROVAL_APPROVED_BY ?? "").trim() || undefined;
	// A rejection that lets the run go on is a decision too: only an allowed approver makes it.
	enforceApprovalIdentity({
		stepId,
		identity: approvalIdentityFromRequest(approval),
		approvedBy,
	});
	// Through resolveWait: a question published earlier (in tool mode) is consumed with it.
	return machine.resolveWait(key, frame, {
		...frame.result!,
		approved,
		...(approvedBy ? { approvedBy } : {}),
	});
}
