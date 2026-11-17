import type {
	WorkflowFile,
	WorkflowStep,
	WorkflowStepResult,
	WorkflowRunResult,
} from "../workflows/types.js";
import type { CostSummary } from "../llm/cost_tracker.js";
import type { LlmOutstandingCharge } from "../llm/accounting.js";
import type { WorkflowPipelineInputResumeState } from "./activities/pipeline.js";
import type { WaitRequest } from "../persistence/waits.js";

export type Definition = {
	filePath: string;
	hash: string;
	workflow: WorkflowFile;
	children: Record<string, Definition>;
	/**
	 * A standalone pipeline (`lobster '...'`, `runToolRequest({ pipeline })`) run as a one-step
	 * workflow: its output is the pipeline's items as they are, and in human mode renderers write
	 * to the terminal as the pipeline runs.
	 */
	standalone?: boolean;
	/** A standalone pipeline's input items, used as they are (never read as templates). */
	input?: unknown[];
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
	lastStepId?: string;
	/** A switch's chosen case: its number from 1, or "else". */
	case?: number | "else";
	dispatchAttempts?: number;
	items?: unknown[];
	iterations?: unknown[];
	results?: Record<string, WorkflowStepResult>;
	winner?: string;
	child?: Definition;
	childArgs?: Record<string, unknown>;
	costStart?: number;
	childEnv?: Record<string, string | undefined>;
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
	/** When the run started (epoch ms): event waits match events from then on. */
	startedAt?: number;
	/** Inbox events this run has consumed; each is consumed at most once. */
	events?: string[];
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
	if (step.switch) return "switch";
	if (step.input) return "input";
	if (step.sleep !== undefined) return "sleep";
	if (step.wait) return "event";
	return "activity";
}

/** A gate (approval, input, timer or event deadline) cancelled the run; already saved. */
export class RunCancelled extends Error {
	readonly code = "LOBSTER_RUN_CANCELLED";
	constructor(message: string) {
		super(message);
		this.name = "RunCancelled";
	}
}
