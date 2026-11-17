import { activation } from "../state.js";
import type { Machine, Scope } from "../machine.js";
import type { WorkflowStep, WorkflowStepResult } from "../../workflows/types.js";
import {
	extractApprovalRequest,
	approvalIdentityFromRequest,
	enforceApprovalIdentity,
	isInteractive,
	questionDeadline,
	readAnswer,
} from "../gates.js";

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
	};
	const deadline = frame.wait.kind === "approval" ? frame.wait.deadline : undefined;
	if (machine.expired(frame)) return machine.timeOut(key + "/approval", frame, frame.result);
	if (scope.ctx.mode === "tool" || !isInteractive(scope.ctx.stdin))
		await machine.suspend(key + "/approval", frame);
	scope.ctx.stdout.write(frame.approval.prompt + " [y/N] ");
	const read = await readAnswer(scope, deadline);
	if ("expired" in read) return machine.timeOut(key + "/approval", frame, frame.result);
	if (!/^y(es)?$/i.test(String(read.answer).trim())) throw new Error("Not approved");
	const approvedBy = String(scope.ctx.env.LOBSTER_APPROVAL_APPROVED_BY ?? "").trim() || undefined;
	enforceApprovalIdentity({
		stepId: step.id,
		identity: approvalIdentityFromRequest(frame.approval),
		approvedBy,
	});
	// Through resolveWait: a question published earlier (in tool mode) is consumed with it.
	return machine.resolveWait(key + "/approval", frame, {
		...frame.result,
		approved: true,
		...(approvedBy ? { approvedBy } : {}),
	});
}
