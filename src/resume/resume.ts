import { decodeToken, encodeToken } from "./token.js";
import { decodeWorkflowResumePayload, type WorkflowResumePayload } from "../engine/runner.js";
import { findStateKeyByApprovalId } from "../state/store.js";
import { parseArgv } from "../util/argv.js";

const APPROVAL_ID = /^[0-9a-f]{8}$/;
const WAIT_ID = /^[0-9a-f-]{36}\.[0-9a-f]{64}$/;

/**
 * What a resume reference names. The three forms cannot be confused: a short approval ID is
 * 8 hex characters, a wait ID is `<run id>.<64 hex>`, and a token is base64url JSON.
 */
export function classifyResumeRef(
	ref: string,
): { approvalId: string } | { waitId: string } | { token: string } {
	if (APPROVAL_ID.test(ref)) return { approvalId: ref };
	if (WAIT_ID.test(ref)) return { waitId: ref };
	return { token: ref };
}

/**
 * `lobster resume <ref> --approve yes|no | --response-json '{...}' | --cancel`.
 * `<ref>` is a resume token, a short approval ID or a wait ID. The released spellings
 * `--token <t>`, `--id <approvalId>` and `--decision` keep working.
 */
export function parseResumeArgs(argv: string[]) {
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
	const base = {
		token: "token" in target ? target.token : null,
		approvalId: "approvalId" in target ? target.approvalId : null,
		...("waitId" in target
			? { waitId: target.waitId, expectedRevision: args.expectedRevision }
			: {}),
	};
	const intentCount =
		Number(Boolean(args.decision)) + Number(args.responseJson !== null) + Number(args.cancel);
	if (intentCount > 1) {
		throw new Error("resume accepts only one of --approve, --response-json, or --cancel");
	}
	if (intentCount === 0) {
		throw new Error("resume requires --approve yes|no, --response-json, or --cancel");
	}

	if (args.cancel) return { ...base, cancel: true };

	if (args.responseJson !== null) {
		try {
			return { ...base, response: JSON.parse(String(args.responseJson)) };
		} catch {
			throw new Error("resume --response-json must be valid JSON");
		}
	}

	const decision = String(args.decision).toLowerCase();
	if (!["yes", "y", "no", "n"].includes(decision))
		throw new Error("resume --approve must be yes or no");
	return { ...base, approved: decision === "yes" || decision === "y" };
}

/** Resolve an approval ID to a resume token by looking up the state key. */
export async function resolveApprovalId(
	approvalId: string,
	env: Record<string, string | undefined>,
): Promise<string> {
	const stateKey = await findStateKeyByApprovalId({ env, approvalId });
	if (!stateKey) {
		throw new Error(`Approval ID "${approvalId}" not found or expired`);
	}

	return encodeToken({
		protocolVersion: 1,
		v: 1,
		kind: "workflow-file",
		stateKey,
	});
}

export function decodeResumeToken(token) {
	const payload = decodeToken(token);
	if (!payload || typeof payload !== "object") throw new Error("Invalid token");
	if (payload.protocolVersion !== 1) throw new Error("Unsupported protocol version");
	if (payload.v !== 1) throw new Error("Unsupported token version");
	// Tokens handed out for paused pipelines before they became runs name their state key
	// the same way; `lobster upgrade` converted that state into a run under the same key.
	if (payload.kind === "pipeline-resume") {
		if (!payload.stateKey || typeof payload.stateKey !== "string") throw new Error("Invalid token");
		return {
			protocolVersion: 1,
			v: 1,
			kind: "workflow-file",
			stateKey: payload.stateKey,
		} satisfies WorkflowResumePayload as WorkflowResumePayload;
	}
	const workflowPayload = decodeWorkflowResumePayload(payload);
	if (workflowPayload) return workflowPayload;
	throw new Error("Invalid token");
}
