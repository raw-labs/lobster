import { Buffer } from "node:buffer";

export function encodeToken(obj: unknown) {
	const json = JSON.stringify(obj);
	return Buffer.from(json, "utf8").toString("base64url");
}

export function decodeToken(token: string) {
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
	stateKey?: string;
	filePath?: string;
};

/** A decoded token as a workflow resume payload, or null when it is another kind of token. */
export function decodeWorkflowResumePayload(payload: unknown): WorkflowResumePayload | null {
	if (
		!payload ||
		typeof payload !== "object" ||
		(payload as WorkflowResumePayload).kind !== "workflow-file"
	)
		return null;
	const value = payload as WorkflowResumePayload;
	if (value.protocolVersion !== 1 || value.v !== 1) throw new Error("Unsupported token version");
	return value;
}
