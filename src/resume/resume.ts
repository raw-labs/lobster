// `lobster resume`: its command line, the three kinds of reference it takes, and turning a
// resume token into the key of the wait it answers.
import { decodeToken, decodeWorkflowResumePayload, workflowResumePayload } from "./token.js";
import type { ResumeDecision } from "./options.js";
import { parseArgv } from "../util/argv.js";
import { isPlainObject } from "../util/objects.js";
import { yesOrNo } from "../io/terminal.js";
import { isApprovalId } from "../state/approval_id.js";
import { isWaitId } from "../persistence/model.js";

/**
 * What a resume reference names. The three forms cannot be confused: a short approval ID is
 * 8 hex characters, a wait ID is `<run id>.<64 hex>`, and a token is base64url JSON.
 */
function classifyResumeRef(
	ref: string,
): { approvalId: string } | { waitId: string } | { token: string } {
	if (isApprovalId(ref)) return { approvalId: ref };
	if (isWaitId(ref)) return { waitId: ref };
	return { token: ref };
}

/** A parsed `lobster resume`: the wait it answers, and the one decision. */
export type ResumeArgs = {
	/** A wait ID may carry the revision the answer is for (the stale-question guard). */
	target:
		| { token: string }
		| { approvalId: string }
		| { waitId: string; expectedRevision?: number };
	decision: ResumeDecision;
};

/**
 * `lobster resume <ref> --approve yes|no | --response-json '{...}' | --cancel`.
 * `<ref>` is a resume token, a short approval ID or a wait ID. The released spellings
 * `--token <t>`, `--id <approvalId>` and `--decision` keep working.
 */
export function parseResumeArgs(argv: string[]): ResumeArgs {
	const { flags, positional } = parseArgv(
		argv,
		{
			"expected-revision": { type: "integer", min: 0 },
			token: { type: "string" },
			id: { type: "string" },
			"response-json": { type: "string" },
			cancel: { type: "boolean" },
			approve: { type: "string", aliases: ["decision"] },
		},
		{
			usage:
				"resume <token | approval ID | wait ID> --approve yes|no | --response-json '{...}' | --cancel",
		},
	);
	if (positional.length > 1) throw new Error("resume takes one reference");
	const args = {
		decision: (flags.approve as string | undefined) ?? null,
		ref: positional[0] ?? null,
		token: (flags.token as string | undefined) ?? null,
		approvalId: (flags.id as string | undefined) ?? null,
		responseJson: (flags["response-json"] as string | undefined) ?? null,
		cancel: flags.cancel === true,
		expectedRevision: (flags["expected-revision"] as number | undefined) ?? null,
	};

	const named = [args.ref, args.token, args.approvalId].filter((value) => value);
	if (named.length !== 1)
		throw new Error(
			named.length === 0
				? "resume requires a token, approval ID or wait ID"
				: "resume takes one reference",
		);
	const target = args.ref
		? classifyResumeRef(args.ref)
		: args.token
			? { token: String(args.token) }
			: { approvalId: String(args.approvalId) };
	if (args.expectedRevision !== null && !("waitId" in target))
		throw new Error("resume --expected-revision applies only to a wait ID");
	const withRevision =
		"waitId" in target && args.expectedRevision !== null
			? { ...target, expectedRevision: args.expectedRevision }
			: target;
	const intentCount =
		Number(Boolean(args.decision)) + Number(args.responseJson !== null) + Number(args.cancel);
	if (intentCount > 1) {
		throw new Error("resume accepts only one of --approve, --response-json, or --cancel");
	}
	if (intentCount === 0) {
		throw new Error("resume requires --approve yes|no, --response-json, or --cancel");
	}

	return { target: withRevision, decision: parseDecision(args) };
}

function parseDecision(args: {
	cancel: boolean;
	responseJson: string | null;
	decision: string | null;
}): ResumeDecision {
	if (args.cancel) return { cancel: true };
	if (args.responseJson !== null) {
		try {
			return { response: JSON.parse(args.responseJson) };
		} catch {
			throw new Error("resume --response-json must be valid JSON");
		}
	}
	const approved = yesOrNo(String(args.decision));
	if (approved === undefined) throw new Error("resume --approve must be yes or no");
	return { approved };
}

/** The workflow resume payload a token carries, checked. */
export function decodeResumeToken(token: string) {
	const payload = decodeToken(token);
	if (!isPlainObject(payload)) throw new Error("Invalid token");
	if (payload.protocolVersion !== 1) throw new Error("Unsupported protocol version");
	if (payload.v !== 1) throw new Error("Unsupported token version");
	// Tokens handed out for paused pipelines before they became runs name their state key
	// the same way; `lobster upgrade` converted that state into a run under the same key.
	if (payload.kind === "pipeline-resume") {
		if (!payload.stateKey || typeof payload.stateKey !== "string") throw new Error("Invalid token");
		return workflowResumePayload(payload.stateKey);
	}
	const workflowPayload = decodeWorkflowResumePayload(payload);
	if (workflowPayload) return workflowPayload;
	throw new Error("Invalid token");
}
