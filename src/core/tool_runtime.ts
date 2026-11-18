import type { WorkflowRunResult, WorkflowWaitingFor } from "../workflows/types.js";
import type { ResumeDecision, ResumeTarget } from "../resume/options.js";
import { Writable } from "node:stream";
import { createDefaultRegistry } from "../commands/registry.js";
import { parsePipeline } from "../pipeline/parser.js";
import { runtimeErrorDetails } from "../persistence/backend.js";
import { bindResumeGuard, sessionFromEnv } from "../persistence/context.js";
import { resolveWait, callerId } from "../persistence/waits.js";
import { encodeToken } from "../resume/token.js";
import { withExecution, withStoreContext } from "../persistence/execution.js";
import type { PersistenceOptions } from "../persistence/context.js";
import { decodeResumeToken, resolveApprovalId } from "../resume/resume.js";
import { deleteApprovalId, cleanupApprovalIndexByStateKey } from "../state/store.js";
import {
	WorkflowResumeArgumentError,
	UpgradeRequiredError,
	runWorkflowFile,
	runPipelineText,
	runIdOf,
} from "../workflows/file.js";
import { savedByOlderVersion } from "../workflows/upgrade.js";
import { resolveWorkflowFile } from "../workflows/load.js";

export type ToolRunContext = PersistenceOptions & {
	_resumeSignalAborted?: boolean;
	cwd?: string;
	env?: Record<string, string | undefined>;
	mode?: "tool" | "human" | "sdk";
	stdin?: NodeJS.ReadableStream;
	stdout?: NodeJS.WritableStream;
	stderr?: NodeJS.WritableStream;
	signal?: AbortSignal;
	forceTerminationSignal?: AbortSignal;
	assertInvocationCurrent?: () => void;
	registry?: any;
	llmAdapters?: Record<string, any>;
};

export type ToolEnvelope = {
	protocolVersion: 1;
	runId?: string;
	ok: boolean;
	status?: "ok" | "needs_approval" | "needs_input" | "waiting" | "cancelled";
	output?: unknown[];
	/** With `status: "waiting"`: the timer or event the run waits for. */
	waitingFor?: WorkflowWaitingFor;
	requiresApproval?: {
		type?: "approval_request";
		prompt: string;
		items: unknown[];
		preview?: string;
		resumeToken?: string;
		approvalId?: string;
	} | null;
	requiresInput?: {
		type?: "input_request";
		prompt: string;
		responseSchema: unknown;
		defaults?: unknown;
		subject?: unknown;
		resumeToken?: string;
	} | null;
	error?: {
		type: string;
		message: string;
		operationId?: string;
		runId?: string;
	};
};

export async function runToolRequest(
	options: Parameters<typeof runToolRequestInternal>[0],
): Promise<ToolEnvelope> {
	try {
		// Every run is recorded: pipelines run on the workflow engine as one-step workflows.
		return await withExecution(
			createToolContext({ ...options.ctx, trackRuns: true }),
			options.filePath ? "workflow" : "pipeline",
			(ctx) => runToolRequestInternal({ ...options, ctx }),
		);
	} catch (error) {
		return errorEnvelope("runtime_error", error);
	}
}

async function runToolRequestInternal({
	pipeline,
	filePath,
	args,
	ctx = {},
}: {
	pipeline?: string;
	filePath?: string;
	args?: Record<string, unknown>;
	ctx?: ToolRunContext;
}): Promise<ToolEnvelope> {
	const runtime = createToolContext(ctx);
	const hasPipeline = typeof pipeline === "string" && pipeline.trim().length > 0;
	const hasFile = typeof filePath === "string" && filePath.trim().length > 0;

	if (!hasPipeline && !hasFile) {
		return errorEnvelope("parse_error", "run requires either pipeline or filePath");
	}
	if (hasPipeline && hasFile) {
		return errorEnvelope("parse_error", "run accepts either pipeline or filePath, not both");
	}

	if (hasFile) {
		let resolvedFilePath: string;
		try {
			resolvedFilePath = await resolveWorkflowFile(filePath!, runtime.cwd);
		} catch (err: any) {
			return errorEnvelope("parse_error", err?.message ?? String(err));
		}

		try {
			const output = await runWorkflowFile({
				filePath: resolvedFilePath,
				args,
				ctx: runtime,
			});

			if (output.status === "needs_approval") {
				return okEnvelope("needs_approval", [], output.requiresApproval ?? null, null);
			}
			if (output.status === "needs_input") {
				return okEnvelope("needs_input", [], null, output.requiresInput ?? null);
			}
			if (output.status === "waiting") {
				return {
					...okEnvelope("waiting", [], null, null),
					waitingFor: output.waitingFor,
				};
			}
			if (output.status === "cancelled") {
				return okEnvelope("cancelled", [], null, null);
			}
			return okEnvelope("ok", output.output, null, null);
		} catch (err: any) {
			return errorEnvelope("runtime_error", err);
		}
	}

	try {
		parsePipeline(String(pipeline), runtime.registry);
	} catch (err: any) {
		return errorEnvelope("parse_error", err?.message ?? String(err));
	}

	try {
		return runEnvelope(
			await runPipelineText({
				pipeline: String(pipeline),
				ctx: { ...runtime, mode: "tool" },
			}),
		);
	} catch (err: any) {
		return errorEnvelope("runtime_error", err);
	}
}

/** The tool envelope for a workflow or pipeline run result. */
function runEnvelope(output: WorkflowRunResult & { runId?: string }): ToolEnvelope {
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

export function resumeToolRequest(options: ResumeToolOptions): Promise<ToolEnvelope>;
export async function resumeToolRequest(
	options: Parameters<typeof resumeToolRequestInternal>[0] & {
		waitId?: string;
		expectedRevision?: number;
	},
): Promise<ToolEnvelope> {
	const entry = createToolContext(options.ctx);
	entry._resumeSignalAborted = entry.signal?.aborted === true;
	if (options.waitId !== undefined) entry.trackRuns = true;
	try {
		return await withStoreContext(entry, async (ctx) => {
			let resolved = options;
			if (options.waitId !== undefined) {
				if (options.token || options.approvalId || options.expectedRevision === undefined) {
					return errorEnvelope(
						"parse_error",
						"waitId requires expectedRevision and cannot be combined with token or approvalId",
					);
				}
				const decisions =
					Number(typeof options.approved === "boolean") +
					Number(options.response !== undefined) +
					Number(options.cancel === true);
				if (
					decisions !== 1 ||
					!Number.isSafeInteger(options.expectedRevision) ||
					options.expectedRevision < 0
				) {
					return errorEnvelope(
						"parse_error",
						"wait resume requires a non-negative revision and exactly one of approved, response, or cancel",
					);
				}
				const store = sessionFromEnv(ctx.env);
				if (!store) throw new Error("Wait resume requires a persistence session");
				const wait = await resolveWait(
					store,
					options.waitId,
					options.expectedRevision,
					callerId(ctx.runMetadata),
				);
				ctx = {
					...ctx,
					env: bindResumeGuard(ctx.env, {
						key: wait.key,
						expectedRevision: options.expectedRevision,
						admitted: false,
					}),
				};
				resolved = {
					...options,
					token: encodeToken({
						protocolVersion: 1,
						v: 1,
						kind: "workflow-file",
						stateKey: wait.key,
					}),
				};
			} else if (options.expectedRevision !== undefined) {
				return errorEnvelope("parse_error", "expectedRevision requires waitId");
			}
			let key: string | undefined;
			try {
				const payload = resolved.token
					? decodeResumeToken(resolved.token)
					: resolved.approvalId
						? decodeResumeToken(await resolveApprovalId(resolved.approvalId, ctx.env))
						: undefined;
				key = payload?.stateKey;
			} catch {
				/* normal parser reports the error */
			}
			// Still in a file an older Lobster wrote: it has to be imported first.
			const store = sessionFromEnv(ctx.env)!;
			const unimported =
				key !== undefined
					? (await store.read(key)).text === null && key
					: resolved.approvalId &&
						`approval_${String(resolved.approvalId).replace(/[^a-f0-9]/g, "")}`;
			if (unimported && (await savedByOlderVersion(store.directory, unimported)))
				return errorEnvelope("upgrade_required", new UpgradeRequiredError());
			return resumeToolRequestInternal({ ...resolved, ctx });
		});
	} catch (error) {
		if (
			entry.signal?.aborted &&
			(options.cancel === true || (options.approved === false && !entry._resumeSignalAborted))
		) {
			throw error;
		}
		return errorEnvelope("runtime_error", error);
	}
}

async function resumeToolRequestInternal({
	token,
	approvalId,
	approved,
	response,
	cancel,
	ctx = {},
}: {
	token?: string;
	approvalId?: string;
	approved?: boolean;
	response?: unknown;
	cancel?: boolean;
	ctx?: ToolRunContext;
}): Promise<ToolEnvelope> {
	const runtime = createToolContext(ctx);
	let payload: ReturnType<typeof decodeResumeToken>;
	try {
		let resolvedToken: string;
		if (approvalId) {
			resolvedToken = await resolveApprovalId(approvalId, runtime.env);
		} else if (token) {
			resolvedToken = token;
		} else {
			return errorEnvelope("parse_error", "resume requires token or approvalId");
		}
		payload = decodeResumeToken(resolvedToken);
	} catch (err: any) {
		return errorEnvelope("parse_error", err?.message ?? String(err));
	}

	// Pipelines and workflow files are both runs: one resume path for every token.
	let stateKey = payload.stateKey;
	try {
		const output = await runWorkflowFile({
			filePath: payload.filePath,
			ctx: {
				...runtime,
				_onResumeStateResolved: (key) => {
					stateKey = key;
				},
			},
			resume: payload,
			approved,
			response,
			cancel,
		});
		if (output.status !== "needs_approval" && output.status !== "needs_input") {
			if (approvalId) await deleteApprovalId({ env: runtime.env, approvalId });
			else if (stateKey) await cleanupApprovalIndexByStateKey({ env: runtime.env, stateKey });
		}
		return runEnvelope(output);
	} catch (err: any) {
		if (err instanceof WorkflowResumeArgumentError) {
			return errorEnvelope("parse_error", err.message);
		}
		// Non-abort failures and cancellations before step execution remain retryable.
		return errorEnvelope("runtime_error", err);
	}
}

export function createToolContext(ctx: ToolRunContext = {}) {
	return {
		cwd: ctx.cwd ?? process.cwd(),
		env: { ...process.env, ...ctx.env },
		mode: "tool" as const,
		stdin: ctx.stdin ?? process.stdin,
		stdout: ctx.stdout ?? createCaptureStream(),
		stderr: ctx.stderr ?? createCaptureStream(),
		signal: ctx.signal,
		forceTerminationSignal: ctx.forceTerminationSignal,
		assertInvocationCurrent: ctx.assertInvocationCurrent,
		registry: ctx.registry ?? createDefaultRegistry(),
		llmAdapters: ctx.llmAdapters,
		storeSession: ctx.storeSession,
		history: ctx.history,
		trackRuns: ctx.trackRuns,
		runMetadata: ctx.runMetadata,
		_resumeSignalAborted: ctx._resumeSignalAborted,
	};
}

export function createCaptureStream() {
	return new Writable({
		write(_chunk, _encoding, callback) {
			callback();
		},
	});
}

function okEnvelope(
	status: "ok" | "needs_approval" | "needs_input" | "waiting" | "cancelled",
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

function errorEnvelope(type: string, error: unknown): ToolEnvelope {
	// A run that failed names itself, so the caller can retry it with `runs advance`.
	const runId = runIdOf(error);
	return {
		protocolVersion: 1,
		ok: false,
		error: runtimeErrorDetails(error, type),
		...(runId ? { runId } : {}),
	};
}
