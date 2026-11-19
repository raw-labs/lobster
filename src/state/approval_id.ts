// Short approval IDs: 8 lowercase hex characters, easy to copy in a chat. One rule for the
// IDs Lobster hands out and the ones it accepts back.
import { randomBytes } from "node:crypto";

const APPROVAL_ID = /^[0-9a-f]{8}$/;

export function isApprovalId(value: unknown): value is string {
	return typeof value === "string" && APPROVAL_ID.test(value);
}

/** A new approval ID. */
export function generateApprovalId(): string {
	return randomBytes(4).toString("hex");
}
