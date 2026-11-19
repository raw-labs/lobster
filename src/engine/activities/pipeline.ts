// Running a pipeline step: parse it, run its stages, and turn a halt into a pause (it asked
// a question) or an error. Its items become the step's result.
import { messageOf } from "../../util/errors.js";
import { PassThrough, Readable } from "node:stream";
import { parsePipeline } from "../../pipeline/parser.js";
import { runPipeline } from "../../pipeline/runtime.js";
import {
	RequestInputResumeError,
	validateCommandInputState,
} from "../../pipeline/input_request.js";
import { isPlainObject } from "../../util/objects.js";
import { recorderFromEnv, bindRecorder } from "../../persistence/context.js";
import type { ModelCall } from "../../llm/cost_tracker.js";
import { assertSavable } from "../../util/serialization.js";
import type { WorkflowStepResult } from "../../workflows/types.js";
import type { PipelinePause, WaitRequest } from "../checkpoint.js";
import type { RunContext } from "../context.js";
import { GateRefusedError, PipelineHaltError } from "../errors.js";
import {
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
 * and GateRefusedError where they cannot.
 */
export async function runPipelineStep({
	stepId,
	pipelineText,
	inputValue,
	ctx,
	recordModelCall,
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
	/** Counts a model call the pipeline made in the run's cost, under the step. */
	recordModelCall?: (call: ModelCall) => void;
	env: Record<string, string | undefined>;
	cwd: string;
	/** Continue after the question it paused on, with the answer (`response`, for an input). */
	resume?: { pause: PipelinePause; question: PipelineQuestion; response: unknown };
	/** Why the pipeline cannot pause here, if it cannot: the error when it tries to says so. */
	gatesRefused?: string;
	onExecutionStart?: () => void | Promise<void>;
	/** A standalone pipeline: render to the terminal in human mode and keep its items. */
	standalone?: boolean;
}) {
	// A question always halts the pipeline (requestInput is never disabled here), so it reaches
	// the checks below: where steps cannot pause, they refuse it, naming why, in every mode.
	const mayPause = !gatesRefused;
	let pipeline: ReturnType<typeof parsePipeline>;
	try {
		pipeline = parsePipeline(pipelineText, ctx.registry);
		// A pause continues the pipeline it paused, compared by its stages' text: their flags are
		// read by today's parser, which may read them differently than the one that paused.
		const paused = resume?.pause.stages;
		if (
			paused &&
			(paused.length !== pipeline.length ||
				paused.some((raw, index) => raw !== pipeline[index].raw))
		)
			throw new RequestInputResumeError("workflow pipeline changed since input request");
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
	// Its stages are recorded under the activity that runs them.
	const pipelineRecorder = recorderFromEnv(env)?.child("activity");
	if (pipelineRecorder) {
		pipelineRecorder.pipelineOffset = pipelineStartIndex;
	}
	const result = await runPipeline({
		pipeline: remainingPipeline,
		...(standalone ? { renderOutput: live } : {}),
		registry: ctx.registry,
		// A stage never asks at the terminal itself: its question (`approve`, `ask`,
		// requestInput) pauses the pipeline, and the engine asks it as a gate, which shows the
		// prompt, holds the step's clocks, and cancels the run on a "no".
		stdin: Readable.from([]),
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
		recordModelCall,
		input: resume
			? resume.pause.mode === "next_stage" && resume.question.kind === "input"
				? [resume.response]
				: resume.pause.items
			: inputValueToPipelineItems(inputValue),
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
	// What it ended with (its output, or the question it halted to ask) is saved next, so it is
	// checked first: a value that cannot be saved fails the step instead of every later save.
	assertSavable(result.items, `${standalone ? "Pipeline" : `Workflow step ${stepId}`} output`);

	if (result.halted) {
		const stageTexts = pipeline.map((stage) => stage.raw);
		const haltedName = result.haltedAt?.stage?.name ?? "unknown";
		const haltedAt = pipelineStartIndex + (result.haltedAt?.index ?? 0);
		// What it halted with: one request object (`approval_request` or `input_request`).
		const item =
			result.items.length === 1 && isPlainObject(result.items[0]) ? result.items[0] : undefined;
		// A pipeline that halts to ask pauses the step; the run resumes it after the answer.
		// Where steps cannot pause (`gatesRefused` says why), the halt is an error.
		if (item?.type === "approval_request" && mayPause) {
			const items = Array.isArray(item.items) ? item.items : [];
			throw new PipelinePausedError(
				{
					kind: "approval",
					prompt: String(item.prompt ?? "Approve?"),
					items,
					...(typeof item.preview === "string" ? { preview: item.preview } : null),
				},
				{ stages: stageTexts, resumeAtIndex: haltedAt + 1, items, mode: "next_stage" },
				stepId,
			);
		}
		if (item?.type === "approval_request") {
			throw new GateRefusedError(
				`Workflow step ${stepId} halted for approval inside pipeline stage ${haltedName}; ${gatesRefused ?? "approvals are not supported here"}.`,
			);
		}
		// A command that asked mid-run (ctx.requestInput) halted "in place": it runs again with
		// its saved state once answered. Otherwise the stage halted after emitting a request.
		const request = item?.type === "input_request" ? item : undefined;
		const inPlace = result.haltedAt?.inPlace === true;
		if (request && mayPause) {
			throw new PipelinePausedError(
				{
					kind: "input",
					prompt: String(request.prompt),
					responseSchema: request.responseSchema,
					...(request.defaults !== undefined ? { defaults: request.defaults } : null),
					...(request.subject !== undefined ? { subject: request.subject } : null),
				},
				inPlace
					? {
							stages: stageTexts,
							resumeAtIndex: haltedAt,
							items: Array.isArray(request.items) ? request.items : [],
							mode: "same_stage",
							commandInput: validateCommandInputState(request.commandInput),
						}
					: { stages: stageTexts, resumeAtIndex: haltedAt + 1, items: [], mode: "next_stage" },
				stepId,
			);
		}
		if (request) {
			throw new GateRefusedError(
				`Workflow step ${stepId} halted for input inside pipeline stage ${haltedName}; ${gatesRefused ?? "input requests are not supported here"}.`,
			);
		}
		throw new PipelineHaltError(
			`${standalone ? "Pipeline" : `Workflow step ${stepId}`} halted before completion at pipeline stage ${haltedName}`,
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
	return stepResult;
}
