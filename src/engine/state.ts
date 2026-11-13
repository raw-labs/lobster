import type {
	WorkflowFile,
	WorkflowStep,
	WorkflowStepResult,
	WorkflowRunResult,
} from "../workflows/types.js";
import type { CostSummary } from "../core/cost_tracker.js";
import type { LlmOutstandingCharge } from "../core/llm_accounting.js";
import type { WorkflowPipelineInputResumeState } from "../activities/pipeline.js";
import type { WaitRequest } from "../persistence/waits.js";

export type Definition = {
	filePath: string;
	hash: string;
	workflow: WorkflowFile;
	children: Record<string, Definition>;
};

export type ActivityInput = {
	kind: "shell" | "pipeline" | "none";
	text?: string;
	input: unknown;
	env: Record<string, string | undefined>;
	cwd?: string;
};

export type Activation = {
	phase: "ready" | "started" | "waiting" | "completed" | "failed" | "abandoned";
	attempt: number;
	result?: WorkflowStepResult;
	error?: string;
	nonRetryable?: boolean;
	accounted?: boolean;
	stepId?: string;
	input?: ActivityInput;
	retryAt?: number;
	cursor?: number;
	items?: unknown[];
	iterations?: unknown[];
	results?: Record<string, WorkflowStepResult>;
	winner?: string;
	child?: Definition;
	childArgs?: Record<string, unknown>;
	pipelineInput?: WorkflowPipelineInputResumeState;
	response?: unknown;
	waitKey?: string;
	wait?: WaitRequest;
	approval?: WorkflowRunResult["requiresApproval"];
	deadline?: number;
};

export type ExecutionState = {
	version: 1;
	revision: number;
	definition: Definition;
	args: Record<string, unknown>;
	cwd?: string;
	activations: Record<string, Activation>;
	status: "running" | "waiting" | "completed" | "failed" | "cancelled" | "blocked";
	output?: unknown[];
	error?: string;
	cost?: CostSummary;
	llmSpend?: LlmOutstandingCharge[];
	llmBilled?: LlmOutstandingCharge[];
};

export type ExecutionLease = { owner: string; epoch: number; expiresAt: number };

export class InterruptedActivityError extends Error {
	readonly code = "LOBSTER_ACTIVITY_OUTCOME_UNKNOWN";
	constructor(readonly path: string) {
		super(
			`Activity ${path} was interrupted without a saved result; inspect its outcome before continuing`,
		);
		this.name = "InterruptedActivityError";
	}
}

export class InvocationRetiredError extends Error {
	constructor(cause: unknown) {
		super(cause instanceof Error ? cause.message : String(cause), { cause });
		this.name = "InvocationRetiredError";
	}
}

export class ExecutionConflictError extends Error {
	readonly code = "LOBSTER_EXECUTION_CONFLICT";
	constructor() {
		super("Workflow is owned by another executor or its saved state changed");
		this.name = "ExecutionConflictError";
	}
}

export function activation(state: ExecutionState, key: string): Activation {
	return (state.activations[key] ??= { phase: "ready", attempt: 1 });
}

export function occurrence(parent: string, id: string): string {
	return parent + "/" + encodeURIComponent(id);
}

export function nodeKind(step: WorkflowStep) {
	if (step.for_each !== undefined) return "for-each";
	if (step.parallel) return "parallel";
	if (step.workflow) return "child-workflow";
	if (step.input) return "input";
	return "activity";
}
