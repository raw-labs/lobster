// Tool mode: a host (an agent's tool call, `lobster --mode tool`) runs a pipeline or workflow
// file, or answers a wait, and gets one JSON envelope back: ok, the status, the output, and
// what it waits for.
import type { RunHooks } from "../engine/context.js";
import type { WorkflowRunResult, WorkflowWaitingFor } from "../workflows/types.js";
import {
	decisionCount,
	type Decision,
	type ResumeDecision,
	type ResumeTarget,
} from "../resume/options.js";
import { discardStream } from "../io/streams.js";
import { approvalIndexKey, findStateKeyByApprovalId } from "../state/store.js";
import { isApprovalId } from "../state/approval_id.js";
import { createDefaultRegistry } from "../commands/registry.js";
import { checkStages, parsePipeline } from "../pipeline/parser.js";
import { runtimeErrorDetails } from "../persistence/backend.js";
import { bindResumeGuard, sessionFromEnv } from "../persistence/context.js";
import {
	findWaitAtRevision,
	callerId,
	checkWaitReference,
	visibleTo,
} from "../persistence/waits.js";
import { withExecution, withStoreContext } from "../persistence/execution.js";
import type { PersistenceOptions } from "../persistence/context.js";
import { decodeResumeToken } from "../resume/resume.js";
import { workflowResumePayload } from "../resume/token.js";
import { runWorkflowFile, runPipelineText, runIdOf } from "../engine/runner.js";
import { InvalidAnswerError, UpgradeRequiredError } from "../engine/errors.js";
import { savedByOlderVersion } from "../engine/upgrade.js";
import { resolveWorkflowFile } from "../workflows/load.js";
import { messageOf } from "../util/errors.js";
import { isPlainObject } from "../util/objects.js";
import type { CommandLookup } from "../commands/types.js";

export type ToolRunContext = PersistenceOptions & {
	cwd?: string;
	env?: Record<string, string | undefined>;
	stdin?: NodeJS.ReadableStream;
	stdout?: NodeJS.WritableStream;
	stderr?: NodeJS.WritableStream;
	signal?: AbortSignal;
	forceTerminationSignal?: AbortSignal;
	assertInvocationCurrent?: () => void;
	/** The commands pipelines may use (default: the built-in packs). */
	registry?: CommandLookup;
	llmAdapters?: Record<string, unknown>;
	/** Called as the run works (see RunHooks). */
	hooks?: RunHooks;
};

export type ToolEnvelope = {
	protocolVersion: 1;
	runId?: string;
	ok: boolean;
	status?: WorkflowRunResult["status"];
	output?: unknown[];
	/** With `status: "waiting"`: the timer or event the run waits for. */
	waitingFor?: WorkflowWaitingFor;
	/** The question a paused run asks, as the run reports it. */
	requiresApproval?: NonNullable<WorkflowRunResult["requiresApproval"]> | null;
	requiresInput?: NonNullable<WorkflowRunResult["requiresInput"]> | null;
	error?: {
		type: string;
		message: string;
		operationId?: string;
		runId?: string;
	};
};

/** What a tool request runs: a pipeline, or a workflow file with its args. */
export type RunToolOptions = {
	pipeline?: string;
	filePath?: string;
	args?: Record<string, unknown>;
	ctx?: ToolRunContext;
};

export async function runToolRequest(options: RunToolOptions): Promise<ToolEnvelope> {
	const ctx = createToolContext(options.ctx);
	// The request is checked before a run is created: a refusal writes nothing.
	const target = await toolRunTarget(options, ctx);
	if ("protocolVersion" in target) return target;
	try {
		return await withExecution(
			ctx,
			"filePath" in target ? "workflow" : "pipeline",
			async (bound) => {
				try {
					return runEnvelope(
						"filePath" in target
							? await runWorkflowFile({ filePath: target.filePath, args: options.args, ctx: bound })
							: await runPipelineText({ pipeline: target.pipeline, ctx: bound }),
					);
				} catch (err) {
					return errorEnvelope("runtime_error", err);
				}
			},
		);
	} catch (error) {
		return errorEnvelope("runtime_error", error);
	}
}

/**
 * What a tool request runs, checked without the store: one workflow file that exists (with
 * `args`, an object, as the CLI's --args-json) or one pipeline that parses. A malformed
 * request is a `parse_error` envelope.
 */
async function toolRunTarget(
	{ pipeline, filePath, args }: RunToolOptions,
	ctx: ToolContext,
): Promise<{ filePath: string } | { pipeline: string } | ToolEnvelope> {
	const hasPipeline = typeof pipeline === "string" && pipeline.trim().length > 0;
	const hasFile = typeof filePath === "string" && filePath.trim().length > 0;
	if (!hasPipeline && !hasFile)
		return errorEnvelope("parse_error", "run requires either pipeline or filePath");
	if (hasPipeline && hasFile)
		return errorEnvelope("parse_error", "run accepts either pipeline or filePath, not both");
	if (args !== undefined && !isPlainObject(args))
		return errorEnvelope("parse_error", "args must be a JSON object");
	if (args !== undefined && hasPipeline)
		return errorEnvelope(
			"parse_error",
			"args applies to a workflow file (filePath), not a pipeline",
		);
	try {
		if (hasFile) return { filePath: await resolveWorkflowFile(filePath!, ctx.cwd) };
		checkStages(parsePipeline(pipeline!, ctx.registry), ctx.registry);
		return { pipeline: pipeline! };
	} catch (err) {
		return errorEnvelope("parse_error", messageOf(err));
	}
}

/** The tool envelope for a workflow or pipeline run result. */
export function runEnvelope(output: WorkflowRunResult & { runId?: string }): ToolEnvelope {
	const runId = output.runId ? { runId: output.runId } : {};
	if (output.status === "needs_approval")
		return { ...okEnvelope("needs_approval", [], output.requiresApproval ?? null, null), ...runId };
	if (output.status === "needs_input")
		return { ...okEnvelope("needs_input", [], null, output.requiresInput ?? null), ...runId };
	if (output.status === "waiting")
		return { ...okEnvelope("waiting", [], null, null), waitingFor: output.waitingFor, ...runId };
	if (output.status === "cancelled")
		return { ...okEnvelope("cancelled", [], null, null), ...runId };
	return { ...okEnvelope("ok", output.output, null, null), ...runId };
}

export type ResumeToolOptions = ResumeTarget & ResumeDecision & { ctx?: ToolRunContext };

/**
 * Answer a wait, named by a resume token, a short approval ID, or a wait ID with the revision
 * the answer is for. The reference is resolved once, to the key the wait is published under.
 */
export function resumeToolRequest(options: ResumeToolOptions): Promise<ToolEnvelope>;
export async function resumeToolRequest(
	options: {
		token?: string;
		approvalId?: string;
		waitId?: string;
		expectedRevision?: number;
		ctx?: ToolRunContext;
	} & Decision,
): Promise<ToolEnvelope> {
	// The request is checked before the store is opened: a refusal writes nothing.
	const reference = resumeReference(options);
	if ("protocolVersion" in reference) return reference;
	try {
		return await withStoreContext(createToolContext(options.ctx), async (ctx) => {
			const store = sessionFromEnv(ctx.env)!;
			let stateKey: string;
			if ("waitId" in reference) {
				const { waitId, expectedRevision } = reference;
				stateKey = (
					await findWaitAtRevision(store, waitId, expectedRevision, callerId(ctx.runMetadata))
				).key;
				ctx = { ...ctx, env: bindResumeGuard(ctx.env, { key: stateKey, expectedRevision }) };
			} else if ("stateKey" in reference) {
				stateKey = reference.stateKey;
			} else {
				const { approvalId } = reference;
				const notFound = () =>
					errorEnvelope("parse_error", `Approval ID "${approvalId}" not found or expired`);
				const found = await findStateKeyByApprovalId({ env: ctx.env, approvalId });
				if (!found) {
					const indexKey = approvalIndexKey(approvalId);
					if (indexKey && (await savedByOlderVersion(store.directory, indexKey)))
						return errorEnvelope("upgrade_required", new UpgradeRequiredError());
					return notFound();
				}
				// Another caller's approval reads like an unknown one: it tells nothing about that run.
				const owner = (await store.read(found)).runId;
				const run = owner ? await store.getRunState(owner) : null;
				if (owner && (!run || !visibleTo(run, callerId(ctx.runMetadata)))) return notFound();
				stateKey = found;
			}
			// Still in a file an older Lobster wrote: it has to be imported first.
			if (
				(await store.read(stateKey)).text === null &&
				(await savedByOlderVersion(store.directory, stateKey))
			)
				return errorEnvelope("upgrade_required", new UpgradeRequiredError());
			return answerWait(ctx, stateKey, options);
		});
	} catch (error) {
		// Every failure is an envelope, a host's stop included, wherever it lands.
		return errorEnvelope("runtime_error", error);
	}
}

/**
 * What a resume request names, checked without the store: a wait at a revision, the key a
 * token encodes, or a short approval ID. A malformed request is a `parse_error` envelope.
 */
function resumeReference(
	options: {
		token?: string;
		approvalId?: string;
		waitId?: string;
		expectedRevision?: number;
	} & Decision,
):
	| { waitId: string; expectedRevision: number }
	| { stateKey: string }
	| { approvalId: string }
	| ToolEnvelope {
	const { token, approvalId, waitId, expectedRevision } = options;
	// The same rule and words as the engine's (waits.answer), checked here before any store work.
	// No decision at all is left to the question, which says what it needs.
	if (decisionCount(options) > 1)
		return errorEnvelope("parse_error", "resume takes only one of approved, response, or cancel");
	if (waitId !== undefined) {
		if (token || approvalId || expectedRevision === undefined)
			return errorEnvelope(
				"parse_error",
				"waitId requires expectedRevision and cannot be combined with token or approvalId",
			);
		try {
			checkWaitReference(waitId, expectedRevision);
		} catch (err) {
			return errorEnvelope("parse_error", messageOf(err));
		}
		return { waitId, expectedRevision };
	}
	if (expectedRevision !== undefined)
		return errorEnvelope("parse_error", "expectedRevision requires waitId");
	if (token && approvalId)
		return errorEnvelope("parse_error", "resume takes a token or an approvalId, not both");
	if (token) {
		try {
			return { stateKey: decodeResumeToken(token).stateKey };
		} catch (err) {
			return errorEnvelope("parse_error", messageOf(err));
		}
	}
	if (approvalId) {
		if (!isApprovalId(approvalId))
			return errorEnvelope("parse_error", "Invalid approval ID (8 lowercase hex characters)");
		return { approvalId };
	}
	return errorEnvelope("parse_error", "resume requires token or approvalId");
}

/**
 * Answer the wait published under `stateKey`. Pipelines and workflow files are both runs: one
 * resume path for every reference. Answering consumes the wait and withdraws its approval ID.
 */
async function answerWait(
	ctx: ToolContext,
	stateKey: string,
	{ approved, response, cancel }: Decision,
): Promise<ToolEnvelope> {
	try {
		return runEnvelope(
			await runWorkflowFile({
				ctx,
				resume: workflowResumePayload(stateKey),
				approved,
				response,
				cancel,
			}),
		);
	} catch (err) {
		if (err instanceof InvalidAnswerError) return errorEnvelope("parse_error", err.message);
		return errorEnvelope("runtime_error", err);
	}
}

/** A tool request's context, with every default filled in. */
type ToolContext = ReturnType<typeof createToolContext>;

export function createToolContext(ctx: ToolRunContext = {}) {
	return {
		cwd: ctx.cwd ?? process.cwd(),
		env: { ...process.env, ...ctx.env },
		mode: "tool" as const,
		stdin: ctx.stdin ?? process.stdin,
		stdout: ctx.stdout ?? discardStream(),
		stderr: ctx.stderr ?? discardStream(),
		signal: ctx.signal,
		forceTerminationSignal: ctx.forceTerminationSignal,
		assertInvocationCurrent: ctx.assertInvocationCurrent,
		registry: ctx.registry ?? createDefaultRegistry(),
		llmAdapters: ctx.llmAdapters,
		storeSession: ctx.storeSession,
		history: ctx.history,
		runMetadata: ctx.runMetadata,
		hooks: ctx.hooks,
	};
}

function okEnvelope(
	status: WorkflowRunResult["status"],
	output: unknown[],
	requiresApproval: ToolEnvelope["requiresApproval"],
	requiresInput: ToolEnvelope["requiresInput"],
) {
	return {
		protocolVersion: 1 as const,
		ok: true,
		status,
		output,
		requiresApproval,
		requiresInput,
	};
}

/** The envelope of a request that failed, naming its run when it has one. */
export function errorEnvelope(type: string, error: unknown): ToolEnvelope {
	// A run that failed names itself, so the caller can retry it with `runs advance`.
	const runId = runIdOf(error);
	return {
		protocolVersion: 1,
		ok: false,
		error: runtimeErrorDetails(error, type),
		...(runId ? { runId } : {}),
	};
}
