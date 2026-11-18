// Asking a person: reading an answer at a terminal, and checking an answer and who gave it.
import { timerUntil } from "../util/duration.js";
import { combineSignals } from "../process/owner_signal.js";
import { readLineFromStream } from "../io/read_line.js";
import { approvalInputTimeoutMs } from "../io/terminal.js";
import { prepareSchema, schemaValidationMessage } from "../util/validation.js";
import type { WorkflowApprovalIdentity } from "../workflows/types.js";
import type { RunContext } from "./context.js";
import { InvalidAnswerError } from "./errors.js";

/** Read a line from a person, stopping at the question's deadline. */
export async function readAnswer(
	ctx: Pick<RunContext, "stdin" | "env" | "signal" | "hooks">,
	deadline: number | undefined,
): Promise<{ answer: string } | { expired: true }> {
	const expiry = new AbortController();
	const cancelExpiry =
		deadline === undefined ? undefined : timerUntil(deadline, () => expiry.abort());
	// Waiting for a person is not a hung step: keep a heartbeat limit from expiring meanwhile.
	const heartbeat = ctx.hooks?.heartbeat;
	const alive = heartbeat ? setInterval(heartbeat, 1000) : undefined;
	heartbeat?.();
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
		if (alive) clearInterval(alive);
	}
}

export function validateInputResponse(params: {
	schema: unknown;
	response: unknown;
	/** The step that asked. */
	stepId: string;
	/** A standalone pipeline has only the one step, so the message does not name it. */
	standalone?: boolean;
}) {
	const result = prepareSchema(params.schema).validate(params.response);
	if (result.ok) return;
	const asker = params.standalone ? "Input" : `Workflow input step ${params.stepId}`;
	throw new Error(schemaValidationMessage(`${asker} response`, result.errors));
}

/**
 * The decision's fields for the step's result, once the approver may make it: the one named by
 * `requiredApprover`, or anyone but `initiatedBy` when `requireDifferentApprover`. A rejection
 * that lets the run go on is a decision too. The approver is `LOBSTER_APPROVAL_APPROVED_BY`.
 */
export function decideApproval(
	stepId: string,
	identity: WorkflowApprovalIdentity,
	approved: boolean,
	env: Record<string, string | undefined>,
): { approved: boolean; approvedBy?: string } {
	const approvedBy = String(env.LOBSTER_APPROVAL_APPROVED_BY ?? "").trim() || undefined;
	const { requiredApprover, requireDifferentApprover, initiatedBy } = identity;
	if (requiredApprover || requireDifferentApprover) {
		const where = `Workflow step ${stepId} approval`;
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
