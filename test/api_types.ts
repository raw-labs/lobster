// Compile-only public contract checks; this function is never executed.
import {
	Lobster,
	type SdkResult,
	type ResumeDecision,
	type WaitResumeOptions,
	type StoreSession,
} from "../src/sdk/index.js";
export type {
	SdkApprovalRequest,
	SdkInputRequest,
	MigrationOptions,
	MigrationPhase,
	PersistenceOptions,
	RunMetadata,
	HistoryPage,
	RunEvent,
	Attempt,
	AttemptStatus,
	OperationReceipt,
} from "../src/sdk/index.js";
import { resumeToolRequest, type ResumeToolOptions } from "../src/core/index.js";

export function checkPublicTypes(store: StoreSession, workflow: Lobster, result: SdkResult) {
	const decision: ResumeDecision = { approved: false };
	const wait: WaitResumeOptions = { waitId: "wait", expectedRevision: 0, ...decision };
	const options: ResumeToolOptions = { ...wait, ctx: { storeSession: store } };
	void resumeToolRequest(options);
	void workflow.resume("token", decision);
	void workflow.resumeWait(wait);
	void store.getRun("run", { callerId: "owner" });
	void store.getWait("wait", { callerId: "owner" });
	// @ts-expect-error Wait IDs require a revision.
	void resumeToolRequest({ waitId: "wait", approved: true });
	// @ts-expect-error Targets are mutually exclusive.
	void resumeToolRequest({ token: "token", approvalId: "id", approved: true });
	// @ts-expect-error A decision is required.
	void resumeToolRequest({ token: "token" });
	// @ts-expect-error Decisions are mutually exclusive.
	void workflow.resumeWait({ ...wait, response: 1, approved: true });
	// @ts-expect-error Cancellation must be an explicit true.
	void workflow.resume("token", { cancel: false });
	if (result.status === "needs_approval") {
		const token: string = result.requiresApproval.resumeToken;
		void token;
	}
	if (result.status === "error") {
		const message: string = result.error.message;
		void message;
	}
}
