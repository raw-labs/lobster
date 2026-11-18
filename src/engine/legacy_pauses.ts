// Pauses saved by Lobster versions before the run store, rebuilt as waiting runs of the
// engine: paused workflow files and paused standalone pipelines. A paused SDK step becomes a
// run the same way, the first time its host resumes it with its old token.
import { messageOf } from "../util/errors.js";
import { isPlainObject } from "../util/objects.js";
import path from "node:path";
import { pipelineDefinition, snapshotDefinition } from "../workflows/definition.js";
import { resolveWorkflowArgs } from "../workflows/args.js";
import type { Definition } from "../workflows/definition.js";
import { resolveTemplate } from "../workflows/expressions.js";
import { getStepExecution, isApprovalStep, isInputStep } from "../workflows/steps.js";
import { createRecord, mergeEnv, resolveCwd, resolveInputValue } from "../workflows/values.js";
import type { WorkflowStepResult } from "../workflows/types.js";
import { validateCommandInputState, type CommandInputState } from "../pipeline/input_request.js";
import {
	continuationText,
	keys,
	type Activation,
	type ExecutionState,
	type PipelineStage,
	type WaitRequest,
} from "./checkpoint.js";
import { approvalRequest } from "./nodes/approval.js";

export type OldWorkflowPause = {
	filePath: string;
	resumeAtIndex: number;
	steps: Record<string, WorkflowStepResult>;
	args?: Record<string, unknown>;
	approvalStepId?: string;
	approvalIdentity?: Record<string, unknown>;
	inputStepId?: string;
	inputKind?: "workflow_step" | "pipeline_command";
	inputSchema?: unknown;
	inputSubject?: unknown;
	pipelineInput?: {
		pipeline: PipelineStage[];
		resumeAtIndex: number;
		items?: unknown[];
		/** Untrusted until validated (validateCommandInputState). */
		commandInput: unknown;
	};
	cost?: ExecutionState["cost"];
	llmSpend?: ExecutionState["llmSpend"];
	llmBilled?: ExecutionState["llmBilled"];
};

export function isOldWorkflowPause(key: string, value: unknown): value is OldWorkflowPause {
	return (
		/^workflow[-_]resume_/.test(key) &&
		isPlainObject(value) &&
		value.type !== "lobster.consumed-resume-state.v1" &&
		value.machineVersion === undefined
	);
}

export type OldPipelinePause = {
	pipeline: { name: string; args: Record<string, unknown>; raw?: string }[];
	resumeAtIndex: number;
	items: unknown[];
	haltType?: "approval_request" | "input_request";
	resumeMode?: "next_stage" | "same_stage";
	inputSchema?: unknown;
	prompt?: string;
	/** Untrusted until validated (validateCommandInputState). */
	commandInput?: unknown;
};

export function isOldPipelinePause(key: string, value: unknown): value is OldPipelinePause {
	return (
		key.startsWith("pipeline_resume_") &&
		isPlainObject(value) &&
		value.type !== "lobster.consumed-resume-state.v1"
	);
}

/** Rebuild the state machine's view of a pause written by the old workflow executor. */
export async function convertWorkflowPause(key: string, old: OldWorkflowPause) {
	if (
		typeof old.filePath !== "string" ||
		!old.filePath ||
		!Number.isSafeInteger(old.resumeAtIndex) ||
		!old.steps ||
		typeof old.steps !== "object" ||
		(old.approvalStepId === undefined) === (old.inputStepId === undefined)
	) {
		throw new Error("Unrecognized saved workflow state");
	}
	let definition: ExecutionState["definition"];
	try {
		definition = await snapshotDefinition(path.resolve(old.filePath), old.args ?? {});
	} catch (error) {
		throw new Error(
			error?.code === "ENOENT"
				? `Workflow file not found: ${old.filePath}`
				: `Workflow file cannot be loaded: ${messageOf(error)}`,
		);
	}
	const steps = definition.workflow.steps;
	const args = resolveWorkflowArgs(definition.workflow.args, old.args ?? {});
	const results = createRecord<WorkflowStepResult>(structuredClone(old.steps));
	const stepId = (old.approvalStepId ?? old.inputStepId)!;
	const index = steps.findIndex((step) => step.id === stepId);
	const step = steps[index];
	// The old executor saved the index to continue from: past an approval or input
	// step, but at a pipeline step that is waiting inside one of its commands.
	const pipelineCommand = old.inputKind === "pipeline_command";
	if (!step || old.resumeAtIndex !== (pipelineCommand ? index : index + 1)) {
		throw new Error(`Workflow file changed since the pause: step '${stepId}' moved or was removed`);
	}

	const stepKey = keys.occurrence("root", step.id);
	const attemptKey = keys.attempt(stepKey, 1);
	const activations: Record<string, Activation> = {};
	let gateKey: string;
	let wait: Extract<WaitRequest, { kind: "approval" | "input" }>;

	if (old.approvalStepId !== undefined) {
		if (!isApprovalStep(step.approval))
			throw new Error(
				`Workflow file changed since the pause: step '${stepId}' is no longer an approval`,
			);
		const subject = results[step.id];
		if (!subject) throw new Error("Unrecognized saved workflow state");
		delete results[step.id];
		// Approver rules from the workflow file stay in force; the identity saved at the
		// pause (which also captured environment-provided values) takes precedence.
		const approval = { ...approvalRequest(step, subject, {}), ...old.approvalIdentity };
		gateKey = keys.approval(attemptKey);
		wait = approval;
		// The step already ran and was charged; only the decision is outstanding.
		activations[stepKey] = { phase: "started", attempt: 1, accounted: true };
		activations[gateKey] = {
			phase: "waiting",
			result: {
				...subject,
				...(approval.initiatedBy ? { initiatedBy: approval.initiatedBy } : {}),
			},
			wait: { request: wait, stepId: step.id, key },
		};
	} else if (!pipelineCommand) {
		if (!isInputStep(step.input))
			throw new Error(
				`Workflow file changed since the pause: step '${stepId}' is no longer an input`,
			);
		gateKey = attemptKey;
		wait = {
			kind: "input",
			prompt: step.input!.prompt,
			responseSchema: old.inputSchema ?? step.input!.responseSchema,
			...(step.input!.defaults !== undefined ? { defaults: step.input!.defaults } : {}),
			subject: old.inputSubject ?? null,
		};
		activations[stepKey] = { phase: "started", attempt: 1 };
		activations[gateKey] = {
			phase: "waiting",
			wait: { request: wait, stepId: step.id, key },
		};
	} else {
		const execution = getStepExecution(step);
		if (execution.kind !== "pipeline")
			throw new Error(
				`Workflow file changed since the pause: step '${stepId}' is no longer a pipeline`,
			);
		const saved = old.pipelineInput;
		if (!saved || !Number.isSafeInteger(saved.resumeAtIndex))
			throw new Error("Unrecognized saved workflow state");
		const commandInput = validCommandInput(saved.commandInput, "workflow");
		const pending = commandInput.pending;
		const request = pending.metadata;
		gateKey = keys.pipelineInput(attemptKey, saved.resumeAtIndex, pending.requestIndex);
		wait = {
			kind: "input",
			prompt: request.prompt,
			responseSchema: old.inputSchema ?? request.responseSchema,
			...(request.defaults !== undefined ? { defaults: request.defaults } : {}),
			subject: old.inputSubject ?? request.subject ?? null,
		};
		const cwd = resolveCwd(step.cwd ?? definition.workflow.cwd, args);
		activations[stepKey] = { phase: "started", attempt: 1, dispatches: 1 };
		activations[keys.activity(attemptKey)] = {
			phase: "waiting",
			activity: {
				dispatch: 1,
				question: wait,
				input: {
					kind: "pipeline",
					text: resolveTemplate(execution.value, args, results),
					input: resolveInputValue(step.stdin, args, results),
					env: mergeEnv({}, definition.workflow.env, step.env, args, results),
					...(cwd ? { cwd } : {}),
				},
				pause: {
					pipeline: saved.pipeline,
					resumeAtIndex: saved.resumeAtIndex,
					items: saved.items ?? [],
					mode: "same_stage",
					commandInput,
				},
			},
		};
		activations[gateKey] = {
			phase: "waiting",
			wait: { request: wait, stepId: step.id, key },
		};
	}

	let lastStepId: string | undefined;
	for (let i = index - 1; i >= 0; i--) {
		const result = results[steps[i].id];
		if (result && !result.skipped && !result.error) {
			lastStepId = steps[i].id;
			break;
		}
	}
	activations.root = {
		phase: "started",
		results,
		sequence: { cursor: index, ...(lastStepId ? { lastStepId } : {}) },
	};

	// No cwd: the old executor ran steps without one in the directory of whoever resumed.
	const execution: ExecutionState = {
		version: 1,
		revision: 1,
		definition,
		args,
		activations,
		status: "waiting",
		// Converted now: an event wait matches events from here on, as in a run started now.
		startedAt: Date.now(),
		...(old.cost ? { cost: old.cost } : {}),
		...(old.llmSpend?.length ? { llmSpend: old.llmSpend } : {}),
		...(old.llmBilled?.length ? { llmBilled: old.llmBilled } : {}),
	};
	const continuation = (runId: string) => continuationText(runId, gateKey, wait);
	return { execution, continuation };
}

/**
 * Rebuild a pause written by the old pipeline runtime as a waiting run of a standalone
 * pipeline: the same stages, items and position, so the run continues exactly where the
 * old one would have.
 */
export function convertPipelinePause(key: string, old: OldPipelinePause) {
	const stages = old.pipeline;
	const sameStage = old.resumeMode === "same_stage";
	const input = old.haltType === "input_request";
	if (
		!Array.isArray(stages) ||
		!stages.length ||
		!stages.every((stage) => typeof stage?.raw === "string" && stage.raw.length > 0) ||
		!Number.isSafeInteger(old.resumeAtIndex) ||
		old.resumeAtIndex < 0 ||
		old.resumeAtIndex > stages.length ||
		!Array.isArray(old.items) ||
		(input && (typeof old.prompt !== "string" || old.inputSchema === undefined)) ||
		(sameStage && !input)
	)
		throw new Error("Unrecognized saved pipeline state");
	const text = stages.map((stage) => stage.raw!).join(" | ");
	return waitingPipelineRun(
		key,
		pipelineDefinition(text),
		stages.map((stage) => ({ name: stage.name, args: stage.args, raw: stage.raw! })),
		sameStage
			? {
					gate: "same_stage",
					resumeAtIndex: old.resumeAtIndex,
					items: old.items,
					inputSchema: old.inputSchema,
					commandInput: validCommandInput(old.commandInput, "pipeline"),
				}
			: input
				? {
						gate: "input",
						resumeAtIndex: old.resumeAtIndex,
						prompt: old.prompt!,
						inputSchema: old.inputSchema,
					}
				: {
						gate: "approval",
						resumeAtIndex: old.resumeAtIndex,
						items: old.items,
						prompt: typeof old.prompt === "string" ? old.prompt : "Approve?",
					},
	);
}

/** Where a paused standalone pipeline stopped, and what it asked. */
export type PausedPipeline =
	| { gate: "approval"; resumeAtIndex: number; items: unknown[]; prompt: string }
	| {
			gate: "input";
			resumeAtIndex: number;
			prompt: string;
			inputSchema: unknown;
			subject?: unknown;
	  }
	| {
			gate: "same_stage";
			resumeAtIndex: number;
			items: unknown[];
			inputSchema: unknown;
			commandInput: CommandInputState;
	  };

/**
 * The state of a standalone pipeline run waiting at `pause`, as the engine would have saved
 * it, with its continuation published under `key` (which keeps an old token valid).
 */
export function waitingPipelineRun(
	key: string,
	definition: Definition,
	stages: PipelineStage[],
	pause: PausedPipeline,
) {
	const text = String(definition.workflow.steps[0].pipeline);
	const stepKey = keys.occurrence("root", "pipeline");
	const attemptKey = keys.attempt(stepKey, 1);
	let gateKey: string;
	let wait: Extract<WaitRequest, { kind: "approval" | "input" }>;
	const gate: Activation = { phase: "waiting" };
	if (pause.gate === "same_stage") {
		const pending = pause.commandInput.pending;
		gateKey = keys.pipelineInput(attemptKey, pause.resumeAtIndex, pending.requestIndex);
		wait = {
			kind: "input",
			prompt: pending.metadata.prompt,
			responseSchema: pause.inputSchema,
			...(pending.metadata.defaults !== undefined ? { defaults: pending.metadata.defaults } : {}),
			subject: pending.metadata.subject ?? null,
		};
	} else if (pause.gate === "input") {
		gateKey = keys.pipelineGate(attemptKey, pause.resumeAtIndex);
		wait = {
			kind: "input",
			prompt: pause.prompt,
			responseSchema: pause.inputSchema,
			subject: pause.subject ?? null,
		};
	} else {
		gateKey = keys.pipelineGate(attemptKey, pause.resumeAtIndex);
		wait = { kind: "approval", prompt: pause.prompt, items: pause.items };
		gate.result = { id: "pipeline" };
	}
	const activations: Record<string, Activation> = {
		root: { phase: "started", sequence: { cursor: 0 }, results: {} },
		[stepKey]: { phase: "started", attempt: 1, dispatches: 1 },
		[keys.activity(attemptKey)]: {
			phase: "waiting",
			activity: {
				dispatch: 1,
				question: wait,
				input: {
					kind: "pipeline",
					text,
					input: definition.input ?? null,
					env: mergeEnv({}, undefined, undefined, {}, {}),
				},
				pause: {
					pipeline: stages,
					resumeAtIndex: pause.resumeAtIndex,
					items: pause.gate === "input" ? [] : pause.items,
					...(pause.gate === "same_stage"
						? { mode: "same_stage", commandInput: pause.commandInput }
						: { mode: "next_stage" }),
				},
			},
		},
		[gateKey]: { ...gate, wait: { request: wait, stepId: "pipeline", key } },
	};
	// No cwd: the old runtime ran the rest of the pipeline in the directory of whoever resumed.
	const execution: ExecutionState = {
		version: 1,
		revision: 1,
		definition,
		args: {},
		activations,
		status: "waiting",
		// Converted now: an event wait matches events from here on, as in a run started now.
		startedAt: Date.now(),
	};
	const continuation = (runId: string) => continuationText(runId, gateKey, wait);
	return { execution, continuation };
}

/** A saved command's question state, checked: a pause that fails it cannot be converted. */
function validCommandInput(value: unknown, kind: "workflow" | "pipeline"): CommandInputState {
	try {
		return validateCommandInputState(value);
	} catch {
		throw new Error(`Unrecognized saved ${kind} state`);
	}
}
