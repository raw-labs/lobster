import { PassThrough } from "node:stream";
import { isDeepStrictEqual } from "node:util";
import { parsePipeline } from "../parser.js";
import { runPipeline } from "../runtime.js";
import { RequestInputResumeError, type CommandInputState } from "../input_request.js";
import { recorderFromEnv, bindRecorder } from "../persistence/context.js";
import { carryLlmProvenance } from "../core/llm_accounting.js";
import type { LlmSpendLedger } from "../core/llm_accounting.js";
import { stableStringify } from "../state/store.js";
import type { WorkflowStepResult } from "../workflows/types.js";
import type { RunContext } from "../engine/context.js";
import { stepScopedLedger, attachPipelineSourceItems } from "../engine/accounting.js";
import {
	parseJson,
	serializePipelineItemsToStdout,
	inputValueToPipelineItems,
} from "../engine/values.js";

export type WorkflowPipelineInputResumeState = {
	pipeline: Array<{ name: string; args: Record<string, unknown>; raw: string }>;
	resumeAtIndex: number;
	items: unknown[];
	commandInput: CommandInputState;
};

export class WorkflowPipelineInputSuspension extends Error {
	stepId: string;
	request: {
		prompt: string;
		responseSchema: unknown;
		defaults?: unknown;
		subject?: unknown;
	};
	pipelineInput: WorkflowPipelineInputResumeState;

	constructor({
		stepId,
		request,
		pipelineInput,
	}: {
		stepId: string;
		request: WorkflowPipelineInputSuspension["request"];
		pipelineInput: WorkflowPipelineInputResumeState;
	}) {
		super(`Workflow step ${stepId} pipeline requested input`);
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
		const currentPipeline = parsePipeline(pipelineText);
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
		input: resume ? resume.pipelineInput.items : inputValueToPipelineItems(inputValue),
		requestInputEnabled,
		requestInputResume: resume
			? {
					state: resume.pipelineInput.commandInput,
					response: resume.response,
					onConsumed: resume.onConsumed,
				}
			: undefined,
		onExecutionStart,
		onNonRetryableSideEffect: ctx._onNonRetryableSideEffect,
	});
	stdout.end();
	ctx.signal?.throwIfAborted();

	if (result.halted) {
		const haltedName = result.haltedAt?.stage?.name ?? "unknown";
		if (result.items.length === 1 && result.items[0]?.type === "approval_request") {
			throw new WorkflowPipelineHaltError(
				`Workflow step ${stepId} halted for approval inside pipeline stage ${haltedName}. Use a separate approval step in the workflow file.`,
			);
		}
		const request =
			result.items.length === 1 && result.items[0]?.type === "input_request"
				? (result.items[0] as Record<string, any>)
				: null;
		if (request?.commandInput) {
			throw new WorkflowPipelineInputSuspension({
				stepId,
				request: {
					prompt: String(request.prompt),
					responseSchema: request.responseSchema,
					...(request.defaults !== undefined ? { defaults: request.defaults } : null),
					...(request.subject !== undefined ? { subject: request.subject } : null),
				},
				pipelineInput: {
					pipeline,
					resumeAtIndex: pipelineStartIndex + (result.haltedAt?.index ?? 0),
					items: Array.isArray(request.items) ? request.items : [],
					commandInput: request.commandInput,
				},
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
