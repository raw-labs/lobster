import { decodeToken, encodeToken } from "./token.js";
import { decodeWorkflowResumePayload, type WorkflowResumePayload } from "../workflows/file.js";
import { findStateKeyByApprovalId } from "../state/store.js";
import { parseBoolLike } from "../util/bool.js";

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
export function parseResumeArgs(argv) {
	const args = {
		decision: null,
		ref: null as string | null,
		token: null,
		approvalId: null,
		responseJson: null,
		cancel: false,
		expectedRevision: null as number | null,
	};

	for (let i = 0; i < argv.length; i++) {
		const tok = argv[i];
		if (tok === "--expected-revision" || tok.startsWith("--expected-revision=")) {
			const value =
				tok === "--expected-revision" ? argv[++i] : tok.slice("--expected-revision=".length);
			if (!value || !/^\d+$/.test(value))
				throw new Error("resume --expected-revision requires a non-negative integer");
			args.expectedRevision = Number(value);
			if (!Number.isSafeInteger(args.expectedRevision))
				throw new Error("Invalid expected wait revision");
			continue;
		}
		if (tok === "--token") {
			args.token = argv[i + 1];
			i++;
			continue;
		}
		if (tok.startsWith("--token=")) {
			args.token = tok.slice("--token=".length);
			continue;
		}
		if (tok === "--id") {
			args.approvalId = argv[i + 1];
			i++;
			continue;
		}
		if (tok.startsWith("--id=")) {
			args.approvalId = tok.slice("--id=".length);
			continue;
		}
		if (tok === "--response-json") {
			args.responseJson = argv[i + 1];
			i++;
			continue;
		}
		if (tok.startsWith("--response-json=")) {
			args.responseJson = tok.slice("--response-json=".length);
			continue;
		}
		if (tok === "--cancel") {
			const next = argv[i + 1];
			if (typeof next === "string" && !next.startsWith("--")) {
				const parsed = parseBoolLike(next);
				if (parsed !== undefined) {
					args.cancel = parsed;
					i++;
					continue;
				}
			}
			args.cancel = true;
			continue;
		}
		if (tok.startsWith("--cancel=")) {
			const parsed = parseBoolLike(tok.slice("--cancel=".length));
			if (parsed === undefined) throw new Error("resume --cancel must be true or false");
			args.cancel = parsed;
			continue;
		}
		if (tok === "--approve" || tok === "--decision") {
			args.decision = argv[i + 1];
			i++;
			continue;
		}
		if (tok.startsWith("--approve=")) {
			args.decision = tok.slice("--approve=".length);
			continue;
		}
		if (tok.startsWith("--decision=")) {
			args.decision = tok.slice("--decision=".length);
			continue;
		}
		if (tok.startsWith("--")) throw new Error(`resume has no flag ${tok.split("=")[0]}`);
		if (args.ref !== null) throw new Error("resume takes one reference");
		args.ref = String(tok);
	}

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
