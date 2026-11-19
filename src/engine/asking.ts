// Asking a person: reading an answer at a terminal, and checking an answer and who gave it.
import { timerUntil } from "../util/duration.js";
import { combineSignals } from "../process/owner_signal.js";
import { readLineFromStream } from "../io/read_line.js";
import { approvalInputTimeoutMs } from "../io/terminal.js";
import { responseChecker } from "../util/validation.js";
import type { WorkflowApprovalIdentity } from "../workflows/types.js";
import type { RunContext } from "./context.js";
import { InvalidAnswerError } from "./errors.js";

/** Read a line from a person, stopping at the question's deadline. */
export async function readAnswer(
	ctx: Pick<RunContext, "stdin" | "env" | "signal">,
	deadline: number | undefined,
): Promise<{ answer: string } | { expired: true }> {
	const expiry = new AbortController();
	const cancelExpiry =
		deadline === undefined ? undefined : timerUntil(deadline, () => expiry.abort());
	try {
		const answer = await readLineFromStream(ctx.stdin, {
			timeoutMs: approvalInputTimeoutMs(ctx.env),
			signal: combineSignals(ctx.signal, expiry.signal),
		});
		return { answer };
	} catch (error) {
		if (expiry.signal.aborted && !ctx.signal?.aborted) return { expired: true };
		throw error;
	} finally {
		cancelExpiry?.();
	}
}

/**
 * How messages name the step that asked for input. A standalone pipeline has only the one
 * step, so it is not named.
 */
export function inputAsker(stepId: string, standalone: boolean | undefined) {
	return standalone ? "Input" : `Workflow input step ${stepId}`;
}

export function validateInputResponse(params: {
	schema: unknown;
	response: unknown;
	/** The step that asked. */
	stepId: string;
	/** A standalone pipeline has only the one step, so the message does not name it. */
	standalone?: boolean;
}) {
	responseChecker(params.schema, inputAsker(params.stepId, params.standalone))(params.response);
}

/**
 * The decision's fields for the step's result, once the approver may make it: the one named by
 * `requiredApprover`, or anyone but `initiatedBy` when `requireDifferentApprover`. A rejection
 * that lets the run go on is a decision too. The approver is `LOBSTER_APPROVAL_APPROVED_BY`.
 */
export function decideApproval(
	/** The step that asked; a standalone pipeline has only the one, so messages do not name it. */
	asker: { stepId: string; standalone?: boolean },
	identity: WorkflowApprovalIdentity,
	approved: boolean,
	env: Record<string, string | undefined>,
): { approved: boolean; approvedBy?: string } {
	const approvedBy = String(env.LOBSTER_APPROVAL_APPROVED_BY ?? "").trim() || undefined;
	const { requiredApprover, requireDifferentApprover, initiatedBy } = identity;
	if (requiredApprover || requireDifferentApprover) {
		const where = asker.standalone ? "Approval" : `Workflow step ${asker.stepId} approval`;
		if (!approvedBy)
			throw new InvalidAnswerError(
				`${where} requires approver identity; set LOBSTER_APPROVAL_APPROVED_BY`,
			);
		if (requiredApprover && approvedBy !== requiredApprover)
			throw new InvalidAnswerError(
				`${where} requires approver '${requiredApprover}', got '${approvedBy}'`,
			);
		if (requireDifferentApprover && initiatedBy && approvedBy === initiatedBy)
			throw new InvalidAnswerError(
				`${where} must be granted by someone other than '${initiatedBy}'`,
			);
	}
	return { approved, ...(approvedBy ? { approvedBy } : {}) };
}
