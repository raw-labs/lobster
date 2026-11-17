import { PassThrough } from "node:stream";
import { isDeepStrictEqual } from "node:util";
import { parsePipeline } from "../../pipeline/parser.js";
import { runPipeline } from "../../pipeline/runtime.js";
import { RequestInputResumeError, type CommandInputState } from "../../pipeline/input_request.js";
import { recorderFromEnv, bindRecorder } from "../../persistence/context.js";
import { carryLlmProvenance } from "../../llm/accounting.js";
import type { LlmSpendLedger } from "../../llm/accounting.js";
import { stableStringify } from "../../state/store.js";
import type { WorkflowStepResult } from "../../workflows/types.js";
import type { RunContext } from "../context.js";
import { stepScopedLedger, attachPipelineSourceItems } from "../accounting.js";
import { parseJson, serializePipelineItemsToStdout, inputValueToPipelineItems } from "../values.js";

/**
 * Where a paused pipeline continues. `same_stage` (a command asked mid-run, `ctx.requestInput`)
 * re-runs that stage with `commandInput`; `next_stage` (it halted for `approve` or `ask --emit`)
 * continues with the next stage, fed `items` after an approval or `[response]` after an input.
 */
export type WorkflowPipelineInputResumeState = {
	pipeline: Array<{ name: string; args: Record<string, unknown>; raw: string }>;
	resumeAtIndex: number;
	items: unknown[];
	commandInput?: CommandInputState;
	mode?: "same_stage" | "next_stage";
	gate?: "approval" | "input";
};

/** A question a pipeline asked: the step pauses on it, and the pipeline resumes afterwards. */
export type PipelineGateRequest =
	| { kind: "approval"; prompt: string; items: unknown[]; preview?: string }
	| {
			kind: "input";
			prompt: string;
			responseSchema: unknown;
			defaults?: unknown;
			subject?: unknown;
	  };

export class WorkflowPipelineInputSuspension extends Error {
	stepId: string;
	request: PipelineGateRequest;
	pipelineInput: WorkflowPipelineInputResumeState;

	constructor({
		stepId,
		request,
		pipelineInput,
	}: {
		stepId: string;
		request: PipelineGateRequest;
		pipelineInput: WorkflowPipelineInputResumeState;
	}) {
		super(`Workflow step ${stepId} pipeline asked: ${request.prompt}`);
		this.name = "WorkflowPipelineInputSuspension";
		this.stepId = stepId;
		this.request = request;
		this.pipelineInput = pipelineInput;
	}
}

export async function runPipelineStep({
	stepId,
	pipelineText,
	inputValue,
	ctx,
	llmSpendLedger,
	env,
	cwd,
	resume,
	requestInputEnabled = true,
	onExecutionStart,
}: {
	stepId: string;
	pipelineText: string;
	inputValue: unknown;
	ctx: RunContext;
	llmSpendLedger?: LlmSpendLedger;
	env: Record<string, string | undefined>;
	cwd?: string;
	resume?: {
		pipelineInput: WorkflowPipelineInputResumeState;
		response: unknown;
		onConsumed?: () => Promise<void>;
	};
	requestInputEnabled?: boolean;
	onExecutionStart?: () => void | Promise<void>;
}) {
	let pipeline;
	try {
		const currentPipeline = parsePipeline(pipelineText, ctx.registry);
		if (resume) {
			if (!isDeepStrictEqual(currentPipeline, resume.pipelineInput.pipeline)) {
				throw new RequestInputResumeError("workflow pipeline changed since input request");
			}
			pipeline = resume.pipelineInput.pipeline;
		} else {
			pipeline = currentPipeline;
		}
	} catch (err: any) {
		if (err instanceof RequestInputResumeError) throw err;
		throw new Error(
			`Workflow step ${stepId} pipeline parse failed: ${err?.message ?? String(err)}`,
		);
	}

	const stdout = new PassThrough();
	let renderedStdout = "";
	stdout.setEncoding("utf8");
	stdout.on("data", (chunk) => {
		renderedStdout += String(chunk);
	});

	const pipelineStartIndex = resume ? resume.pipelineInput.resumeAtIndex : 0;
	const remainingPipeline = pipeline.slice(pipelineStartIndex);
	const pipelineRecorder = recorderFromEnv(env)?.child(`step/${stepId}`);
	if (pipelineRecorder) {
		pipelineRecorder.pipelineOffset = pipelineStartIndex;
	}
	const result = await runPipeline({
		pipeline: remainingPipeline,
		registry: ctx.registry,
		stdin: ctx.stdin,
		stdout,
		stderr: ctx.stderr,
		env: pipelineRecorder ? bindRecorder(env, pipelineRecorder) : env,
		mode: ctx.mode,
		cwd,
		signal: ctx.signal,
		forceTerminationSignal: ctx.forceTerminationSignal,
		haltAfterStageOnAbort: true,
		assertInvocationCurrent: ctx.assertInvocationCurrent,
		onUnsafeCommandDispatch: ctx._onUnsafeDispatch,
		llmAdapters: ctx.llmAdapters,
		// Scoped so a charge nothing bills can still name the step that opened it.
		llmSpendLedger: llmSpendLedger ? stepScopedLedger(llmSpendLedger, stepId) : undefined,
		input: resume
			? resume.pipelineInput.mode === "next_stage" && resume.pipelineInput.gate === "input"
				? [resume.response]
				: resume.pipelineInput.items
			: inputValueToPipelineItems(inputValue),
		requestInputEnabled,
		requestInputResume:
			resume && resume.pipelineInput.mode !== "next_stage"
				? {
						state: resume.pipelineInput.commandInput!,
						response: resume.response,
						onConsumed: resume.onConsumed,
					}
				: undefined,
		onExecutionStart,
		onNonRetryableSideEffect: ctx._onNonRetryableSideEffect,
		heartbeat: ctx._heartbeat,
	});
	stdout.end();
	ctx.signal?.throwIfAborted();

	if (result.halted) {
		const haltedName = result.haltedAt?.stage?.name ?? "unknown";
		const haltedAt = pipelineStartIndex + (result.haltedAt?.index ?? 0);
		const item =
			result.items.length === 1 && result.items[0] && typeof result.items[0] === "object"
				? (result.items[0] as Record<string, any>)
				: null;
		// A pipeline that halts to ask pauses the step; the run resumes it after the answer.
		// Gates are not supported in child workflows, so there the halt stays an error.
		if (item?.type === "approval_request" && requestInputEnabled) {
			throw new WorkflowPipelineInputSuspension({
				stepId,
				request: {
					kind: "approval",
					prompt: String(item.prompt ?? "Approve?"),
					items: Array.isArray(item.items) ? item.items : [],
					...(typeof item.preview === "string" ? { preview: item.preview } : null),
				},
				pipelineInput: {
					pipeline,
					resumeAtIndex: haltedAt + 1,
					items: Array.isArray(item.items) ? item.items : [],
					mode: "next_stage",
					gate: "approval",
				},
			});
		}
		if (item?.type === "approval_request") {
			throw new WorkflowPipelineHaltError(
				`Workflow step ${stepId} halted for approval inside pipeline stage ${haltedName}; approvals are not supported in child workflows.`,
			);
		}
		const request = item?.type === "input_request" ? item : null;
		if (request && (request.commandInput || requestInputEnabled)) {
			const question = {
				kind: "input" as const,
				prompt: String(request.prompt),
				responseSchema: request.responseSchema,
				...(request.defaults !== undefined ? { defaults: request.defaults } : null),
				...(request.subject !== undefined ? { subject: request.subject } : null),
			};
			throw new WorkflowPipelineInputSuspension({
				stepId,
				request: question,
				pipelineInput: request.commandInput
					? {
							pipeline,
							resumeAtIndex: haltedAt,
							items: Array.isArray(request.items) ? request.items : [],
							commandInput: request.commandInput,
							mode: "same_stage",
						}
					: { pipeline, resumeAtIndex: haltedAt + 1, items: [], mode: "next_stage", gate: "input" },
			});
		}
		throw new WorkflowPipelineHaltError(
			`Workflow step ${stepId} halted before completion at pipeline stage ${haltedName}`,
		);
	}

	const normalizedStdout = renderedStdout || serializePipelineItemsToStdout(result.items);
	const json = result.items.length
		? result.items.length === 1
			? result.items[0]
			: result.items
		: parseJson(renderedStdout);

	const stepResult: WorkflowStepResult = {
		id: stepId,
		stdout: normalizedStdout,
		json,
	};
	// Only when a renderer consumed the pipeline: otherwise `json` already is those items.
	if (!result.items.length && result.renderedItems.length) {
		// Only restore marks from the renderer's own matching objects, never from public JSON fields.
		const source = Array.isArray(json)
			? result.renderedItems
			: result.renderedItems.length === 1
				? result.renderedItems[0]
				: undefined;
		if (source !== undefined && stableStringify(source) === stableStringify(json)) {
			carryLlmProvenance(source, json);
		}
		attachPipelineSourceItems(stepResult, result.renderedItems);
	}
	return stepResult;
}

export class WorkflowPipelineHaltError extends Error {}
