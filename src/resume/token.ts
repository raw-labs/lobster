import { Buffer } from "node:buffer";

export function encodeToken(obj: unknown) {
	const json = JSON.stringify(obj);
	return Buffer.from(json, "utf8").toString("base64url");
}

/** A token's JSON, unchecked (`decodeResumeToken` and the SDK check what it holds). */
export function decodeToken(token: string): unknown {
	try {
		const json = Buffer.from(String(token), "base64url").toString("utf8");
		return JSON.parse(json);
	} catch (_err) {
		throw new Error("Invalid token");
	}
}

/** What a workflow resume token encodes: the key its continuation is published under. */
export type WorkflowResumePayload = {
	protocolVersion: 1;
	v: 1;
	kind: "workflow-file";
	stateKey: string;
};

/** The payload of a token that resumes the wait published under `stateKey`. */
export function workflowResumePayload(stateKey: string): WorkflowResumePayload {
	return { protocolVersion: 1, v: 1, kind: "workflow-file", stateKey };
}

/**
 * A decoded token (a JSON object of protocol version 1) as a workflow resume payload, or null
 * when it is another kind of token.
 */
export function decodeWorkflowResumePayload(
	payload: Record<string, unknown>,
): WorkflowResumePayload | null {
	if (payload.kind !== "workflow-file") return null;
	if (payload.v !== 1) throw new Error("Unsupported token version");
	if (payload.stateKey === undefined)
		throw new Error(
			"This resume token is from a Lobster version that kept workflow state inside the token; it cannot be resumed",
		);
	if (typeof payload.stateKey !== "string" || !payload.stateKey) throw new Error("Invalid token");
	return workflowResumePayload(payload.stateKey);
}
