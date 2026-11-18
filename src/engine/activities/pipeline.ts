// Running a pipeline step: parse it, run its stages, and turn a halt into a pause (it asked
// a question) or an error. Its items become the step's result.
import { messageOf } from "../../util/errors.js";
import { PassThrough } from "node:stream";
import { isDeepStrictEqual } from "node:util";
import { parsePipeline } from "../../pipeline/parser.js";
import { runPipeline } from "../../pipeline/runtime.js";
import { RequestInputResumeError, type CommandInputState } from "../../pipeline/input_request.js";
import { isPlainObject } from "../../util/objects.js";
import { recorderFromEnv, bindRecorder } from "../../persistence/context.js";
import { carryLlmProvenance } from "../../llm/accounting.js";
import type { LlmSpendLedger } from "../../llm/accounting.js";
import { stableStringify } from "../../util/serialization.js";
import type { WorkflowStepResult } from "../../workflows/types.js";
import type { PipelinePause, PipelineStage, WaitRequest } from "../checkpoint.js";
import type { RunContext } from "../context.js";
import { PipelineHaltError } from "../errors.js";
import { stepScopedLedger } from "../accounting.js";
import {
	attachPipelineSourceItems,
	inputValueToPipelineItems,
	parseJson,
	serializePipelineItemsToStdout,
} from "../results.js";

/** A question a pipeline asked: the step pauses on it, and the pipeline resumes afterwards. */
type PipelineQuestion = Extract<WaitRequest, { kind: "approval" | "input" }>;

/** The pipeline paused to ask `question`; it continues from `pause` once answered. */
export class PipelinePausedError extends Error {
	constructor(
		readonly question: PipelineQuestion,
		readonly pause: PipelinePause,
		stepId: string,
	) {
		super(`Workflow step ${stepId} pipeline asked: ${question.prompt}`);
		this.name = "PipelinePausedError";
	}
}

/**
 * Run a pipeline step's pipeline, or continue it after the question it paused on. A pipeline
 * that halts to ask throws PipelinePausedError where steps may pause (`gatesRefused` unset),
 * and PipelineHaltError where they cannot.
 */
export async function runPipelineStep({
	stepId,
	pipelineText,
	inputValue,
	ctx,
	llmSpendLedger,
	env,
	cwd,
	resume,
	gatesRefused,
	onExecutionStart,
	standalone = false,
}: {
	stepId: string;
	pipelineText: string;
	inputValue: unknown;
	ctx: RunContext;
	llmSpendLedger?: LlmSpendLedger;
	env: Record<string, string | undefined>;
	cwd?: string;
	/** Continue after the question it paused on, with the answer (`response`, for an input). */
	resume?: { pause: PipelinePause; question: PipelineQuestion; response: unknown };
	/** Why the pipeline cannot pause here, if it cannot: the error when it tries to says so. */
	gatesRefused?: string;
	onExecutionStart?: () => void | Promise<void>;
	/** A standalone pipeline: render to the terminal in human mode and keep its items. */
	standalone?: boolean;
}) {
	const requestInputEnabled = !gatesRefused;
	let pipeline: PipelineStage[];
	try {
		const currentPipeline = parsePipeline(pipelineText, ctx.registry);
		if (resume && !isDeepStrictEqual(currentPipeline, resume.pause.pipeline))
			throw new RequestInputResumeError("workflow pipeline changed since input request");
		pipeline = resume ? resume.pause.pipeline : currentPipeline;
	} catch (err) {
		if (err instanceof RequestInputResumeError) throw err;
		throw new Error(`Workflow step ${stepId} pipeline parse failed: ${messageOf(err)}`);
	}

	// A standalone pipeline in human mode renders straight to the terminal, as it always did.
	const live = standalone && ctx.mode !== "tool";
	const stdout = new PassThrough();
	if (live) stdout.pipe(ctx.stdout, { end: false });
	let renderedStdout = "";
	stdout.setEncoding("utf8");
	stdout.on("data", (chunk) => {
		renderedStdout += String(chunk);
	});

	const pipelineStartIndex = resume ? resume.pause.resumeAtIndex : 0;
	const remainingPipeline = pipeline.slice(pipelineStartIndex);
	const pipelineRecorder = recorderFromEnv(env)?.child(`step/${stepId}`);
	if (pipelineRecorder) {
		pipelineRecorder.pipelineOffset = pipelineStartIndex;
	}
	const result = await runPipeline({
		pipeline: remainingPipeline,
		...(standalone ? { renderOutput: live } : {}),
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
		onUnsafeCommandDispatch: ctx.hooks?.onUnsafeDispatch,
		llmAdapters: ctx.llmAdapters,
		// Scoped so a charge nothing bills can still name the step that opened it.
		llmSpendLedger: llmSpendLedger ? stepScopedLedger(llmSpendLedger, stepId) : undefined,
		input: resume
			? resume.pause.mode === "next_stage" && resume.question.kind === "input"
				? [resume.response]
				: resume.pause.items
			: inputValueToPipelineItems(inputValue),
		requestInputEnabled,
		requestInputResume:
			resume?.pause.mode === "same_stage"
				? { state: resume.pause.commandInput, response: resume.response }
				: undefined,
		onExecutionStart,
		onNonRetryableSideEffect: ctx.hooks?.onNonRetryableSideEffect,
		heartbeat: ctx.hooks?.heartbeat,
	});
	stdout.end();
	ctx.signal?.throwIfAborted();

	if (result.halted) {
		const haltedName = result.haltedAt?.stage?.name ?? "unknown";
		const haltedAt = pipelineStartIndex + (result.haltedAt?.index ?? 0);
		// What it halted with: one request object (`approval_request` or `input_request`).
		const item =
			result.items.length === 1 && isPlainObject(result.items[0]) ? result.items[0] : undefined;
		// A pipeline that halts to ask pauses the step; the run resumes it after the answer.
		// Gates are not supported in child workflows, so there the halt stays an error.
		if (item?.type === "approval_request" && requestInputEnabled) {
			const items = Array.isArray(item.items) ? item.items : [];
			throw new PipelinePausedError(
				{
					kind: "approval",
					prompt: String(item.prompt ?? "Approve?"),
					items,
					...(typeof item.preview === "string" ? { preview: item.preview } : null),
				},
				{ pipeline, resumeAtIndex: haltedAt + 1, items, mode: "next_stage" },
				stepId,
			);
		}
		if (item?.type === "approval_request") {
			throw new PipelineHaltError(
				`Workflow step ${stepId} halted for approval inside pipeline stage ${haltedName}; ${gatesRefused ?? "approvals are not supported here"}.`,
			);
		}
		// Only a stage that may ask can produce a `commandInput` (ctx.requestInput); with
		// pausing refused, the field in an item is just output.
		const request = item?.type === "input_request" ? item : undefined;
		if (request && requestInputEnabled) {
			throw new PipelinePausedError(
				{
					kind: "input",
					prompt: String(request.prompt),
					responseSchema: request.responseSchema,
					...(request.defaults !== undefined ? { defaults: request.defaults } : null),
					...(request.subject !== undefined ? { subject: request.subject } : null),
				},
				request.commandInput
					? {
							pipeline,
							resumeAtIndex: haltedAt,
							items: Array.isArray(request.items) ? request.items : [],
							mode: "same_stage",
							commandInput: request.commandInput as CommandInputState,
						}
					: { pipeline, resumeAtIndex: haltedAt + 1, items: [], mode: "next_stage" },
				stepId,
			);
		}
		throw new PipelineHaltError(
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
		...(standalone ? { items: result.items, rendered: Boolean(result.rendered) } : {}),
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
