import { activation } from "../state.js";
import type { Machine, Scope } from "../machine.js";
import type { WorkflowStep, WorkflowStepResult } from "../../workflows/types.js";
import {
	extractApprovalRequest,
	approvalIdentityFromRequest,
	enforceApprovalIdentity,
	isInteractive,
	parseApprovalTimeoutMs,
} from "../gates.js";
import { readLineFromStream } from "../../read_line.js";

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
	};
	if (scope.ctx.mode === "tool" || !isInteractive(scope.ctx.stdin))
		await machine.suspend(key + "/approval", frame);
	scope.ctx.stdout.write(frame.approval.prompt + " [y/N] ");
	const answer = await readLineFromStream(scope.ctx.stdin, {
		timeoutMs: parseApprovalTimeoutMs(scope.ctx.env),
		signal: scope.ctx.signal,
	});
	if (!/^y(es)?$/i.test(String(answer).trim())) throw new Error("Not approved");
	const approvedBy = String(scope.ctx.env.LOBSTER_APPROVAL_APPROVED_BY ?? "").trim() || undefined;
	enforceApprovalIdentity({
		stepId: step.id,
		identity: approvalIdentityFromRequest(frame.approval),
		approvedBy,
	});
	frame.result.approved = true;
	if (approvedBy) frame.result.approvedBy = approvedBy;
	frame.phase = "completed";
	await machine.save();
	return frame.result;
}
