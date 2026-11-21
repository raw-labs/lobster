// Compile-only public contract checks; this function is never executed.
import {
	Lobster,
	type SdkResult,
	type ResumeDecision,
	type WaitResumeOptions,
	type StoreSession,
	InvalidAnswerError,
	ResumeRefusedError,
	runPipeline,
	type RunPipelineOptions,
	type RunPipelineResult,
} from "../src/sdk/index.js";
export type {
	SdkApprovalRequest,
	SdkInputRequest,
	PersistenceOptions,
	RunMetadata,
	HistoryPage,
	RunEvent,
	Attempt,
	AttemptStatus,
	OperationReceipt,
} from "../src/sdk/index.js";
import {
	advanceWorkflowRun,
	parsePipeline,
	runWorkflowFile,
	resumeToolRequest,
	runToolRequest,
	type ResumeToolOptions,
	type RunToolOptions,
} from "../src/core/index.js";

export function checkPublicTypes(store: StoreSession, workflow: Lobster, result: SdkResult) {
	const decision: ResumeDecision = { approved: false };
	const wait: WaitResumeOptions = { waitId: "wait", expectedRevision: 0, ...decision };
	const options: ResumeToolOptions = { ...wait, ctx: { storeSession: store } };
	void resumeToolRequest(options);
	const run: RunToolOptions = { filePath: "flow.lobster", args: { n: 1 }, ctx: { cwd: "." } };
	void runToolRequest(run);
	// The run entry points take the context runToolRequest takes.
	void advanceWorkflowRun({ runId: "run", ctx: { storeSession: store } });
	void runWorkflowFile({ filePath: "flow.lobster", ctx: { storeSession: store } });
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
	// Input is an iterable of items, null or undefined as no items, or any other value as one item (checked strictly by
	// tsconfig.api.json).
	void workflow.run([{ id: 1 }]);
	void workflow.run({ id: 1 });
	if (result.status === "needs_approval") {
		const token: string = result.requiresApproval.resumeToken;
		const approver: string | undefined = result.requiresApproval.requiredApprover;
		void token;
		void approver;
	}
	// The one-shot pipeline runner takes typed options and returns a typed result.
	const pipelineOptions: RunPipelineOptions = {
		pipeline: [],
		registry: { get: () => undefined },
		stdin: process.stdin,
		stdout: process.stdout,
		stderr: process.stderr,
		env: process.env,
	};
	// A pipeline parses with the same registry it runs with.
	void parsePipeline("exec --json echo 1", pipelineOptions.registry);
	void runPipeline(pipelineOptions).then((ran: RunPipelineResult) => {
		const items: unknown[] = ran.items;
		// @ts-expect-error Items are unknown, not any.
		const typed: string[] = ran.items;
		void [items, typed, ran.haltedAt?.index];
	});
	// A refused resume or a bad answer can be told apart from other failures.
	void ((error: unknown) =>
		error instanceof ResumeRefusedError || error instanceof InvalidAnswerError);
	if (result.status === "error") {
		const message: string = result.error.message;
		void message;
	}
}
