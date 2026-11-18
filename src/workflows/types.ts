import type { CostLimit, CostSummary } from "../llm/cost_tracker.js";

export type WorkflowFile = {
	name?: string;
	description?: string;
	args?: Record<string, { default?: unknown; description?: string }>;
	env?: Record<string, string>;
	cwd?: string;
	steps: WorkflowStep[];
	cost_limit?: CostLimit;
};

export type ParallelBranch = {
	id: string;
	run?: string;
	command?: string;
	pipeline?: string;
	env?: Record<string, string>;
	cwd?: string;
	stdin?: unknown;
};

export type SwitchCase = { when: string | boolean; steps: WorkflowStep[] };

export type SwitchConfig = { cases: SwitchCase[]; else?: WorkflowStep[] };

/** Run `steps` in rounds until `until` holds after one, at most `max` rounds. */
export type RepeatConfig = { until: string | boolean; max: number; on_max?: "fail" | "continue" };

export type ParallelConfig = {
	wait?: "all" | "any";
	timeout_ms?: number;
	branches: ParallelBranch[];
};

export type WorkflowStep = {
	recovery?: "repeat" | "manual";
	id: string;
	command?: string;
	run?: string;
	pipeline?: string;
	workflow?: string;
	workflow_args?: Record<string, unknown>;
	env?: Record<string, string>;
	cwd?: string;
	stdin?: unknown;
	approval?: WorkflowApproval;
	input?: WorkflowInputRequest;
	condition?: unknown;
	when?: unknown;
	parallel?: ParallelConfig;
	for_each?: string;
	/** Run the steps of the first case whose `when` holds, or `else`. */
	switch?: SwitchConfig;
	/** Run `steps` again and again until a condition holds. */
	repeat?: RepeatConfig;
	item_var?: string;
	index_var?: string;
	batch_size?: number;
	pause_ms?: number;
	steps?: WorkflowStep[];
	timeout_ms?: number;
	/** Abort the step when it shows no progress for this long (see LOBSTER_HEARTBEAT_FILE). */
	heartbeat_timeout_ms?: number;
	/** Pause the run for a duration ("30s", "2h", "3d", or ms). */
	sleep?: string | number;
	/** Pause the run until an external event arrives (`lobster signal`). */
	wait?: WorkflowEventWait;
	on_error?: "stop" | "continue" | "skip_rest";
	retry?: {
		max?: number;
		backoff?: "fixed" | "exponential";
		delay_ms?: number;
		max_delay_ms?: number;
		jitter?: boolean;
	};
};

/** How long a question or event wait may take, and what happens after that. */
export type WorkflowWaitTimeout = {
	timeout?: string | number;
	on_timeout?: "cancel" | "continue";
};

export type WorkflowEventWait = {
	event: string;
	/** Fields the event's data must contain (nested objects match as subsets). */
	match?: Record<string, unknown>;
} & WorkflowWaitTimeout;

export type WorkflowApproval =
	| boolean
	| "required"
	| string
	| ({
			prompt?: string;
			items?: unknown[];
			preview?: string;
			initiated_by?: string;
			initiatedBy?: string;
			required_approver?: string;
			requiredApprover?: string;
			require_different_approver?: boolean;
			requireDifferentApprover?: boolean;
			/** `continue`: a rejection completes the step with `approved: false` instead of cancelling the run. */
			on_reject?: "cancel" | "continue";
	  } & WorkflowWaitTimeout);

export type WorkflowApprovalIdentity = {
	initiatedBy?: string;
	requiredApprover?: string;
	requireDifferentApprover?: boolean;
};

export type WorkflowInputRequest = {
	prompt: string;
	responseSchema: unknown;
	defaults?: unknown;
} & WorkflowWaitTimeout;

export type WorkflowStepResult = {
	id: string;
	stdout?: string;
	json?: unknown;
	approved?: boolean;
	initiatedBy?: string;
	approvedBy?: string;
	subject?: unknown;
	response?: unknown;
	skipped?: boolean;
	/** For a switch: the case that ran, from 1, or "else". */
	case?: number | "else";
	/** How many rounds a repeat step ran. */
	rounds?: number;
	/** A question or event wait reached its timeout with `on_timeout: continue`. */
	timedOut?: boolean;
	/** A standalone pipeline's items as they are, and whether a renderer already showed them. */
	items?: unknown[];
	rendered?: boolean;
	error?: boolean;
	errorMessage?: string;
};

/** What a run with `status: "waiting"` waits for; `runs advance --due` or `signal` continue it. */
export type WorkflowWaitingFor =
	| { kind: "timer"; stepId: string; until: string }
	| { kind: "event"; stepId: string; event: string; until?: string };

export type WorkflowRunResult = {
	status: "ok" | "needs_approval" | "needs_input" | "waiting" | "cancelled";
	output: unknown[];
	/** A standalone pipeline whose renderer (`table`, `json`) already wrote its output. */
	rendered?: boolean;
	waitingFor?: WorkflowWaitingFor;
	requiresApproval?: {
		type: "approval_request";
		prompt: string;
		items: unknown[];
		preview?: string;
		initiatedBy?: string;
		requiredApprover?: string;
		requireDifferentApprover?: boolean;
		resumeToken?: string;
		approvalId?: string;
	};
	requiresInput?: {
		type: "input_request";
		prompt: string;
		responseSchema: unknown;
		defaults?: unknown;
		subject?: unknown;
		resumeToken?: string;
	};
	_meta?: {
		cost?: CostSummary;
	};
};
