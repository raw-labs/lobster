import path from "node:path";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { listRuns, getRun, getWait } from "./core/runs.js";
import { forceTerminateAbortableProcesses } from "./process/abortable_process.js";
import { createDefaultRegistry } from "./commands/registry.js";
import type { CommandRegistry } from "./commands/types.js";
import { resumeToolRequest, createToolContext } from "./core/tool_runtime.js";
import { parsePipeline } from "./pipeline/parser.js";
import { runtimeErrorDetails } from "./persistence/backend.js";
import { recorderFromEnv } from "./persistence/context.js";
import { withExecution } from "./persistence/execution.js";
import { openStoreSession } from "./persistence/session.js";
import { parseResumeArgs } from "./resume/resume.js";
import { readLineFromStream } from "./io/read_line.js";
import { defaultStateDir } from "./state/store.js";
import { pendingUpgrade, upgradeStore, type UpgradeReport } from "./workflows/upgrade.js";
import { runPipeline } from "./pipeline/runtime.js";
import {
	loadWorkflowFile,
	resolveWorkflowArgs,
	runWorkflowFile,
	advanceWorkflowRun,
	abandonWorkflowRun,
	advanceDueRuns,
	signalEvent,
	runPipelineText,
} from "./engine/runner.js";
import { renderWorkflowGraph } from "./workflows/graph.js";
import { parseWorkflowArgsJson } from "./workflows/values.js";
import type { WorkflowGraphFormat } from "./workflows/graph.js";
import { resolveWorkflowFile, WORKFLOW_FILE_EXTENSIONS } from "./workflows/load.js";
import { parseArgv, UsageError } from "./util/argv.js";

// CLI commands without help of their own show the usage, which covers all of them.
const USAGE_TOPICS = new Set([
	"run",
	"resume",
	"signal",
	"runs",
	"waits",
	"history",
	"storage",
	"upgrade",
	"doctor",
	"version",
	"help",
]);

export async function runCli(argv) {
	const cancellation = createCliCancellation();
	try {
		await runCliWithSignal(argv, cancellation.signal, cancellation.signal);
	} catch (error) {
		// Whatever reaches here is reported as one line, not a stack trace; LOBSTER_DEBUG shows it.
		if (cancellation.signal.aborted) throw error;
		process.stderr.write(
			`${process.env.LOBSTER_DEBUG ? (error?.stack ?? error) : `Error: ${error?.message ?? String(error)}`}\n`,
		);
		process.exitCode = error instanceof UsageError ? 2 : 1;
	} finally {
		if (cancellation.exitCode !== undefined) {
			process.exitCode = cancellation.exitCode;
		}
		cancellation.dispose();
	}
}

async function runCliWithSignal(
	argv,
	signal: AbortSignal,
	forceTerminationSignal: AbortSignal = signal,
) {
	const registry = createDefaultRegistry();

	if (argv[0] === "graph") {
		await handleGraph({ argv: argv.slice(1) });
		return;
	}

	if (argv.length === 0 || argv.includes("-h") || argv.includes("--help")) {
		process.stdout.write(helpText(registry));
		return;
	}

	if (argv[0] === "help") {
		const topic = argv[1];
		if (!topic) {
			process.stdout.write(helpText(registry));
			return;
		}
		if (topic === "graph") {
			process.stdout.write(graphHelpText());
			return;
		}
		// These built-ins are documented in the top-level usage, which `<topic> --help` prints too.
		if (USAGE_TOPICS.has(topic)) {
			process.stdout.write(helpText(registry));
			return;
		}
		const cmd = registry.get(topic);
		if (!cmd) {
			process.stderr.write(`Unknown command: ${topic}\n`);
			process.exitCode = 2;
			return;
		}
		process.stdout.write(cmd.help(topic));
		return;
	}

	if (argv[0] === "version" || argv[0] === "--version" || argv[0] === "-v") {
		process.stdout.write(`${await readVersion()}\n`);
		return;
	}

	if (argv[0] === "upgrade") {
		await handleUpgrade(argv.slice(1));
		return;
	}

	// Everything that can run or resume a workflow checks for pauses from an older Lobster.
	if (
		!["runs", "waits", "history", "storage", "doctor"].includes(argv[0]) ||
		(argv[0] === "runs" && argv[1] === "advance")
	) {
		await offerUpgrade(argv, signal);
	}

	if (argv[0] === "runs" && argv[1] === "advance" && argv.includes("--due")) {
		try {
			const { flags, positional } = parseArgv(
				argv.slice(2),
				{
					due: { type: "boolean" },
					limit: { type: "integer", min: 1 },
					concurrency: { type: "integer", min: 1 },
					"dry-run": { type: "boolean" },
				},
				{ usage: "runs advance --due [--limit <n>] [--concurrency <n>] [--dry-run]" },
			);
			if (positional.length) throw new UsageError("runs advance --due takes no run ID");
			const result = await advanceDueRuns({
				ctx: createToolContext({ signal, forceTerminationSignal }),
				limit: flags.limit as number | undefined,
				concurrency: flags.concurrency as number | undefined,
				dryRun: flags["dry-run"] === true,
			});
			process.stdout.write(JSON.stringify(result) + "\n");
		} catch (error) {
			if (error instanceof UsageError) throw error;
			process.stderr.write(`runs advance --due: ${error?.message ?? String(error)}\n`);
			process.exitCode = 1;
		}
		return;
	}

	if (argv[0] === "signal") {
		try {
			const usage = "signal <event> [--data '{...}'] [--id <id>]";
			const { flags, positional } = parseArgv(
				argv.slice(1),
				{ data: { type: "string" }, id: { type: "string" } },
				{ usage },
			);
			const [name] = positional;
			if (!name || positional.length > 1) throw new UsageError(`Usage: ${usage}`);
			let data: unknown = null;
			if (flags.data !== undefined) {
				try {
					data = JSON.parse(flags.data as string);
				} catch {
					throw new UsageError("signal --data must be valid JSON");
				}
			}
			const id = flags.id as string | undefined;
			const result = await signalEvent({
				name,
				data,
				id,
				ctx: createToolContext({ signal, forceTerminationSignal }),
			});
			process.stdout.write(JSON.stringify(result) + "\n");
		} catch (error) {
			if (error instanceof UsageError) throw error;
			process.stderr.write(`signal: ${error?.message ?? String(error)}\n`);
			process.exitCode = 1;
		}
		return;
	}

	if (argv[0] === "runs" && argv[1] === "advance") {
		try {
			const runId = oneId(argv.slice(2), "runs advance <runId>");
			const result = await advanceWorkflowRun({
				runId,
				ctx: createToolContext({ signal, forceTerminationSignal }),
			});
			writeToolEnvelope({
				ok: true,
				status: result.status,
				output: result.output,
				requiresApproval: result.requiresApproval ?? null,
				requiresInput: result.requiresInput ?? null,
				...(result.waitingFor ? { waitingFor: result.waitingFor } : {}),
				runId: result.runId,
			});
		} catch (error) {
			// A command line it does not understand is reported like any other (exit code 2).
			if (error instanceof UsageError) throw error;
			writeToolEnvelope({
				ok: false,
				status: "error",
				output: [],
				requiresApproval: null,
				requiresInput: null,
				error: runtimeErrorDetails(error),
			});
			process.exitCode = 1;
		}
		return;
	}

	if (argv[0] === "runs" && argv[1] === "abandon") {
		try {
			const runId = oneId(argv.slice(2), "runs abandon <runId>");
			const result = await abandonWorkflowRun({
				runId,
				ctx: createToolContext({ signal, forceTerminationSignal }),
			});
			process.stdout.write(JSON.stringify(result) + "\n");
		} catch (error) {
			if (error instanceof UsageError) throw error;
			process.stderr.write(`runs abandon: ${error?.message ?? String(error)}\n`);
			process.exitCode = 1;
		}
		return;
	}

	if (argv[0] === "runs" || argv[0] === "waits") {
		await handleRuns(argv, signal);
		return;
	}

	if (argv[0] === "history" || argv[0] === "storage") {
		await handlePersistence(argv);
		return;
	}

	if (argv[0] === "doctor") {
		await handleDoctor({ argv: argv.slice(1), registry, signal, forceTerminationSignal });
		return;
	}

	if (argv[0] === "run") {
		await handleRun({ argv: argv.slice(1), registry, signal, forceTerminationSignal });
		return;
	}

	if (argv[0] === "resume") {
		await handleResume({ argv: argv.slice(1), registry, signal, forceTerminationSignal });
		return;
	}

	// Default: treat argv as a pipeline string.
	await handleRun({ argv, registry, signal, forceTerminationSignal });
}

function createCliCancellation() {
	const controller = new AbortController();
	let received: NodeJS.Signals | undefined;
	const terminatingSignals: NodeJS.Signals[] = ["SIGINT", "SIGTERM", "SIGHUP", "SIGQUIT"];
	const abort = (receivedSignal: NodeJS.Signals) => {
		if (received) {
			forceTerminateAbortableProcesses(controller.signal);
			return;
		}
		received = receivedSignal;
		controller.abort(new Error(`Received ${receivedSignal}`));
	};
	const listeners = new Map<NodeJS.Signals, () => void>();
	for (const terminatingSignal of terminatingSignals) {
		const listener = () => abort(terminatingSignal);
		listeners.set(terminatingSignal, listener);
		process.on(terminatingSignal, listener);
	}

	return {
		signal: controller.signal,
		get exitCode() {
			if (!received) return undefined;
			return (
				{
					SIGINT: 130,
					SIGHUP: 129,
					SIGQUIT: 131,
					SIGTERM: 143,
				} as const
			)[received];
		},
		dispose() {
			for (const [terminatingSignal, listener] of listeners) {
				process.removeListener(terminatingSignal, listener);
			}
		},
	};
}

async function handleGraph({ argv }) {
	const parsed = parseGraphArgs(argv);
	if (parsed.help) {
		process.stdout.write(graphHelpText());
		return;
	}

	if (!parsed.filePath) {
		process.stderr.write("graph requires a workflow file path (use --file <path>)\n");
		process.exitCode = 2;
		return;
	}

	if (!isWorkflowGraphFormat(parsed.format)) {
		process.stderr.write("graph --format must be one of: mermaid, dot, ascii\n");
		process.exitCode = 2;
		return;
	}

	let argsJson: Record<string, unknown> = {};
	if (parsed.argsJson !== null) {
		try {
			argsJson = parseWorkflowArgsJson(parsed.argsJson, "graph");
		} catch (err) {
			process.stderr.write(`${err.message}\n`);
			process.exitCode = 2;
			return;
		}
	}

	let filePath: string;
	try {
		filePath = await resolveWorkflowFile(parsed.filePath);
	} catch (err) {
		process.stderr.write(`Error: ${err?.message ?? String(err)}\n`);
		process.exitCode = 1;
		return;
	}

	try {
		const workflow = await loadWorkflowFile(filePath);
		const args = resolveWorkflowArgs(workflow.args, argsJson);
		const graph = renderWorkflowGraph({ workflow, format: parsed.format, args });
		process.stdout.write(graph);
		process.stdout.write("\n");
	} catch (err) {
		process.stderr.write(`Error: ${err?.message ?? String(err)}\n`);
		process.exitCode = 1;
	}
}

function isWorkflowGraphFormat(value: string): value is WorkflowGraphFormat {
	return value === "mermaid" || value === "dot" || value === "ascii";
}

type ToolPayload = Omit<Awaited<ReturnType<typeof resumeToolRequest>>, "protocolVersion">;

type RunOptions = {
	argv: string[];
	registry: ReturnType<typeof createDefaultRegistry>;
	signal: AbortSignal;
	forceTerminationSignal: AbortSignal;
};

async function handleRun(options: RunOptions) {
	let parsed: ReturnType<typeof parseRunArgs>;
	try {
		parsed = parseRunArgs(options.argv);
	} catch (error) {
		// Reported like any parse error of the run: an envelope in tool mode.
		const tool = options.argv.some(
			(arg, index) =>
				arg === "--mode=tool" || (arg === "--mode" && options.argv[index + 1] === "tool"),
		);
		reportRunError(tool ? "tool" : "human", "parse_error", error);
		return { ok: false };
	}
	const prepared = await prepareRunTarget(parsed).catch(() => undefined);
	if (!prepared) {
		return handleRunInternal({ ...options, env: process.env });
	}
	let envelope: ToolPayload | undefined;
	try {
		const result = await withExecution(
			{
				env: process.env,
				signal: options.signal,
				dryRun: prepared.dryRun,
				// Every run is recorded: pipelines run on the engine as one-step workflows.
				...(prepared.dryRun ? {} : { trackRuns: true }),
			},
			prepared.workflowFile ? "cli" : "pipeline",
			(ctx) =>
				handleRunInternal({
					...options,
					env: ctx.env,
					onEnvelope: (value) => {
						envelope = value;
					},
				}),
		);
		if (envelope) {
			writeToolEnvelope(envelope);
		}
		return result;
	} catch (error) {
		reportRunError(normalizeMode(parsed.mode), "runtime_error", error);
		return { ok: false };
	}
}

async function handleRunInternal({
	argv,
	registry,
	signal,
	forceTerminationSignal,
	onEnvelope,
	env,
}: RunOptions & {
	env: NodeJS.ProcessEnv;
	onEnvelope?: (envelope: ToolPayload) => void;
}) {
	const emit = (envelope: ToolPayload) =>
		(onEnvelope ?? writeToolEnvelope)({
			...envelope,
			...(recorderFromEnv(env) ? { runId: recorderFromEnv(env).runId } : {}),
		});
	const parsed = parseRunArgs(argv);
	const { mode, argsJson } = parsed;
	const normalizedMode = normalizeMode(mode);
	let target: Awaited<ReturnType<typeof prepareRunTarget>>;
	try {
		target = await prepareRunTarget(parsed);
	} catch (err) {
		reportRunError(normalizedMode, "parse_error", err, undefined, emit);
		return { ok: false };
	}
	const { rest, workflowFile, dryRun } = target;
	if (workflowFile) {
		let parsedArgs = {};
		if (argsJson !== null) {
			try {
				parsedArgs = parseWorkflowArgsJson(argsJson, "run");
			} catch (err) {
				reportRunError(normalizedMode, "parse_error", err, "", emit);
				return { ok: false };
			}
		}

		try {
			const output = await runWorkflowFile({
				filePath: workflowFile,
				args: parsedArgs,
				ctx: {
					stdin: process.stdin,
					stdout: process.stdout,
					stderr: process.stderr,
					env: env,
					mode: normalizedMode,
					registry,
					dryRun,
					signal,
					forceTerminationSignal,
				},
			});

			if (normalizedMode === "tool") {
				if (output.status === "needs_approval") {
					emit({
						ok: true,
						status: "needs_approval",
						output: [],
						requiresApproval: output.requiresApproval ?? null,
						requiresInput: null,
					});
					return output;
				}

				if (output.status === "needs_input") {
					emit({
						ok: true,
						status: "needs_input",
						output: [],
						requiresApproval: null,
						requiresInput: output.requiresInput ?? null,
					});
					return output;
				}

				if (output.status === "waiting") {
					emit({
						ok: true,
						status: "waiting",
						output: [],
						requiresApproval: null,
						requiresInput: null,
						waitingFor: output.waitingFor,
					});
					return output;
				}

				emit({
					ok: true,
					status: "ok",
					output: output.output,
					requiresApproval: null,
					requiresInput: null,
				});
				return output;
			}

			if (output.status === "waiting") {
				const waiting = output.waitingFor!;
				process.stderr.write(
					waiting.kind === "timer"
						? `Run ${output.runId ?? ""} sleeps until ${waiting.until}; \`lobster runs advance --due\` continues it then.\n`
						: `Run ${output.runId ?? ""} waits for the event "${waiting.event}"${waiting.until ? ` until ${waiting.until}` : ""}; deliver it with \`lobster signal ${waiting.event}\`.\n`,
				);
				return output;
			}

			if (output.status === "needs_approval" || output.status === "needs_input") {
				process.stdout.write(
					JSON.stringify(
						{
							status: output.status,
							output: [],
							requiresApproval: output.requiresApproval ?? null,
							requiresInput: output.requiresInput ?? null,
						},
						null,
						2,
					),
				);
				process.stdout.write("\n");
				return output;
			}

			if (output.status === "ok" && output.output.length) {
				process.stdout.write(JSON.stringify(output.output, null, 2));
				process.stdout.write("\n");
			}
			return output;
		} catch (err) {
			reportRunError(normalizedMode, "runtime_error", err, undefined, emit);
			return { ok: false, error: runtimeErrorDetails(err) };
		}
	}

	const pipelineString = rest.join(" ");

	let pipeline;
	try {
		pipeline = parsePipeline(pipelineString, registry);
	} catch (err) {
		reportRunError(normalizedMode, "parse_error", err, undefined, emit);
		return { ok: false };
	}

	if (dryRun) {
		try {
			const output = await runPipeline({
				pipeline,
				registry,
				input: [],
				stdin: process.stdin,
				stdout: process.stdout,
				stderr: process.stderr,
				env: env,
				mode: normalizedMode,
				renderOutput: normalizedMode !== "tool",
				dryRun,
				signal,
				forceTerminationSignal,
				haltAfterStageOnAbort: true,
			});
			if (normalizedMode === "tool") emit({ ok: true, status: "ok", output: output.items });
			return { status: "ok", output: output.items };
		} catch (err) {
			reportRunError(normalizedMode, "runtime_error", err, undefined, emit);
			return { ok: false, error: runtimeErrorDetails(err) };
		}
	}

	try {
		const output = await runPipelineText({
			pipeline: pipelineString,
			ctx: {
				stdin: process.stdin,
				stdout: process.stdout,
				stderr: process.stderr,
				env: env,
				mode: normalizedMode,
				registry,
				signal,
				forceTerminationSignal,
			},
		});
		if (normalizedMode === "tool") {
			emit({
				ok: true,
				status: output.status,
				output: output.status === "ok" ? output.output : [],
				requiresApproval: output.requiresApproval ?? null,
				requiresInput: output.requiresInput ?? null,
				...(output.waitingFor ? { waitingFor: output.waitingFor } : {}),
			});
			return output;
		}
		if (output.status === "needs_approval" || output.status === "needs_input") {
			process.stdout.write(
				JSON.stringify(
					{
						status: output.status,
						output: [],
						requiresApproval: output.requiresApproval ?? null,
						requiresInput: output.requiresInput ?? null,
					},
					null,
					2,
				) + "\n",
			);
			return output;
		}
		// Human mode: if the last command didn't render, print JSON.
		if (output.status === "ok" && !output.rendered) {
			process.stdout.write(JSON.stringify(output.output, null, 2));
			process.stdout.write("\n");
		}
		return output;
	} catch (err) {
		reportRunError(normalizedMode, "runtime_error", err, undefined, emit);
		return { ok: false, error: runtimeErrorDetails(err) };
	}
}

function reportRunError(
	mode: "human" | "tool",
	type: "parse_error" | "runtime_error",
	error: unknown,
	prefix = type === "parse_error" ? "Parse error: " : "Error: ",
	emit = writeToolEnvelope,
) {
	const message = (error as { message?: string })?.message ?? String(error);
	if (mode === "tool") {
		emit({ ok: false, error: runtimeErrorDetails(error, type) });
	} else {
		process.stderr.write(`${prefix}${message}\n`);
	}
	process.exitCode = type === "parse_error" ? 2 : 1;
}

async function prepareRunTarget(parsed: ReturnType<typeof parseRunArgs>) {
	const { rest, filePath, dryRun } = await resolveRunTarget(parsed);
	const workflowFile = filePath
		? await resolveWorkflowFile(filePath)
		: await detectWorkflowFile(rest);
	return { rest, workflowFile, dryRun };
}

/**
 * `lobster run`'s own flags. Everything else is the pipeline (or workflow file), whose words may
 * be its commands' own flags, so unknown flags pass through. `--dry-run` is Lobster's only before
 * the pipeline starts; after that it may belong to a command.
 */
function parseRunArgs(argv: string[]) {
	const { flags, positional } = parseArgv(
		argv,
		{ mode: { type: "string" }, file: { type: "string" }, "args-json": { type: "string" } },
		{
			usage:
				"lobster run [--mode tool] [--file <workflow>] [--args-json '{...}'] [--dry-run] <pipeline>",
			passthrough: true,
		},
	);
	const lead = positional.findIndex((token) => token !== "--dry-run");
	const leading = lead === -1 ? positional.length : lead;
	return {
		mode: (flags.mode as string | undefined) || "human",
		rest: positional.slice(leading),
		filePath: (flags.file as string | undefined) ?? null,
		argsJson: (flags["args-json"] as string | undefined) ?? null,
		dryRun: leading > 0,
	};
}
function parseGraphArgs(argv: string[]) {
	const { flags, positional } = parseArgv(
		argv,
		{
			file: { type: "string" },
			format: { type: "string" },
			"args-json": { type: "string" },
			help: { type: "boolean", aliases: ["h"] },
		},
		{
			usage: "lobster graph <workflow.lobster> [--format mermaid|dot|ascii] [--args-json '{...}']",
		},
	);
	return {
		filePath: (flags.file as string | undefined) ?? positional[0] ?? null,
		format: (flags.format as string | undefined) || "mermaid",
		argsJson: (flags["args-json"] as string | undefined) ?? null,
		help: flags.help === true || positional.includes("-h"),
	};
}

async function resolveRunTarget(parsed: {
	rest: string[];
	filePath: string | null;
	dryRun: boolean;
}) {
	if (parsed.filePath) return parsed;
	const restWithoutDryRun = parsed.rest.filter((token) => token !== "--dry-run");
	if (restWithoutDryRun.length === 1 && restWithoutDryRun.length !== parsed.rest.length) {
		try {
			const workflowFile = await resolveWorkflowFile(restWithoutDryRun[0]);
			return { ...parsed, filePath: workflowFile, rest: [], dryRun: true };
		} catch (err) {
			if (isWorkflowPathLike(restWithoutDryRun[0])) throw err;
			return parsed;
		}
	}
	return parsed;
}

function normalizeMode(mode) {
	return mode === "tool" ? "tool" : "human";
}

async function detectWorkflowFile(rest) {
	if (rest.length !== 1) return null;
	const candidate = rest[0];
	if (!candidate || candidate.includes("|")) return null;
	try {
		return await resolveWorkflowFile(candidate);
	} catch (err) {
		if (isWorkflowPathLike(candidate)) throw err;
		return null;
	}
}

// Command names never contain a path separator or end in a workflow extension.
function isWorkflowPathLike(token: string) {
	if (/[\s|]/.test(token) || token.startsWith("-")) return false;
	const lower = token.toLowerCase();
	return /[\\/]/.test(token) || WORKFLOW_FILE_EXTENSIONS.some((ext) => lower.endsWith(ext));
}

async function handleResume({
	argv,
	registry,
	signal,
	forceTerminationSignal,
}: {
	argv;
	registry;
	signal: AbortSignal;
	forceTerminationSignal: AbortSignal;
}) {
	let parsed;
	try {
		parsed = parseResumeArgs(argv);
	} catch (err) {
		writeToolEnvelope({
			ok: false,
			error: { type: "parse_error", message: err?.message ?? String(err) },
		});
		process.exitCode = 2;
		return;
	}

	let envelope;
	try {
		// A wait ID without --expected-revision answers the question as it stands now, as a
		// token or a short approval ID does; --expected-revision adds the stale-question guard.
		let expectedRevision = parsed.expectedRevision;
		if (parsed.waitId && expectedRevision === null) {
			const wait = await getWait({ waitId: parsed.waitId, ctx: { env: process.env } });
			if (!wait) throw new Error(`Wait ${parsed.waitId} not found`);
			expectedRevision = wait.revision;
		}
		envelope = await resumeToolRequest({
			token: parsed.token ?? undefined,
			approvalId: parsed.approvalId ?? undefined,
			waitId: parsed.waitId,
			expectedRevision: parsed.waitId ? expectedRevision : undefined,
			approved: parsed.approved,
			response: parsed.response,
			cancel: parsed.cancel,
			ctx: {
				cwd: process.cwd(),
				env: process.env,
				mode: "tool",
				stdin: process.stdin,
				stdout: process.stdout,
				stderr: process.stderr,
				registry,
				signal,
				forceTerminationSignal,
			},
		});
	} catch (err: any) {
		envelope = {
			ok: false,
			error: runtimeErrorDetails(err),
		};
	}
	writeToolEnvelope(envelope);
	if (!envelope.ok) {
		process.exitCode = envelope.error?.type === "parse_error" ? 2 : 1;
	}
}

async function readVersion() {
	const here = path.dirname(fileURLToPath(import.meta.url));
	const pkg = JSON.parse(await readFile(path.join(here, "..", "..", "package.json"), "utf8"));
	return pkg.version ?? "0.0.0";
}

async function handleDoctor({
	argv,
	registry,
	signal,
	forceTerminationSignal,
}: {
	argv;
	registry;
	signal: AbortSignal;
	forceTerminationSignal: AbortSignal;
}) {
	const mode = "tool";
	const pipeline = "exec --json --shell 'echo [1]'";
	const output: any = await (async () => {
		try {
			const parsed = parsePipeline(pipeline, registry);
			return await runPipeline({
				pipeline: parsed,
				registry,
				input: [],
				stdin: process.stdin,
				stdout: process.stdout,
				stderr: process.stderr,
				env: process.env,
				mode,
				signal,
				forceTerminationSignal,
				haltAfterStageOnAbort: true,
			});
		} catch (err: any) {
			return { error: err };
		}
	})();

	if (output?.error) {
		writeToolEnvelope({
			ok: false,
			error: { type: "doctor_error", message: output.error?.message ?? String(output.error) },
		});
		process.exitCode = 1;
		return;
	}

	writeToolEnvelope({
		ok: true,
		status: "ok",
		output: [
			{
				toolMode: true,
				protocolVersion: 1,
				version: await readVersion(),
				notes: argv.length ? argv : undefined,
			},
		],
		requiresApproval: null,
		requiresInput: null,
	});
}

function writeToolEnvelope(payload) {
	const envelope = {
		protocolVersion: 1,
		...payload,
	};
	process.stdout.write(JSON.stringify(envelope, null, 2));
	process.stdout.write("\n");
}

function helpText(registry: CommandRegistry) {
	return (
		`lobster — OpenClaw-native typed shell\n\n` +
		`Usage:\n\n` +
		`Start and answer:\n` +
		`  lobster '<pipeline>'                                run a pipeline\n` +
		`  lobster run [--mode tool] '<pipeline>'\n` +
		`  lobster run <workflow.lobster> [--args-json '{...}']  start a workflow run\n` +
		`  lobster run --dry-run <workflow.lobster | pipeline>  show the plan only\n` +
		`  lobster resume <ref> --approve yes|no               answer a question\n` +
		`  lobster resume <ref> --response-json '{...}'\n` +
		`  lobster resume <ref> --cancel\n` +
		`      <ref> is a resume token, a short approval ID or a wait ID.\n` +
		`  lobster signal <event> [--data '{...}'] [--id <id>]   deliver an event to waiting runs\n\n` +
		`Manage runs:\n` +
		`  lobster runs list [--status waiting] [--limit 20] [--after <cursor>]\n` +
		`  lobster runs show <runId>\n` +
		`  lobster runs advance <runId>                       continue after a crash\n` +
		`  lobster runs advance --due [--limit n] [--concurrency n] [--dry-run]  advance every run that is due\n` +
		`  lobster runs abandon <runId>                       stop a run for good\n` +
		`  lobster waits show <waitId>\n\n` +
		`Diagnostics:\n` +
		`  lobster graph <workflow.lobster> [--format mermaid|dot|ascii]\n` +
		`  lobster history list | show <runId> [afterSequence] | prune <before> [--apply]\n` +
		`  lobster storage export\n` +
		`  lobster doctor\n\n` +
		`Setup:\n` +
		`  lobster upgrade [--dry-run] [--json]                import state from older versions\n` +
		`  lobster version\n` +
		`  lobster help <command>\n\n` +
		`Flags:\n` +
		`  --dry-run  Validate and print the execution plan without running anything\n\n` +
		`Modes:\n` +
		`  - human (default): renderers can write to stdout\n` +
		`  - tool: prints a single JSON envelope for easy integration\n\n` +
		`Examples:\n` +
		`  lobster 'exec --json "echo [1,2,3]" | json'\n` +
		`  lobster run --mode tool 'exec --json "echo [1]" | approve --prompt "ok?"'\n\n` +
		`Commands:\n` +
		`  ${registry.list().join(", ")}\n`
	);
}

function graphHelpText() {
	return (
		`lobster graph — render workflow step graphs\n\n` +
		`Usage:\n` +
		`  lobster graph --file path/to/workflow.lobster [--format mermaid|dot|ascii] [--args-json '{...}']\n` +
		`  lobster graph path/to/workflow.lobster [--format mermaid|dot|ascii]\n\n` +
		`Flags:\n` +
		`  --file       Workflow file path (.lobster, .yaml, .yml, .json)\n` +
		`  --format     Output format: mermaid (default), dot, ascii\n` +
		`  --args-json  JSON object used to resolve workflow args for labels\n`
	);
}

async function handlePersistence(argv: string[]) {
	const usage =
		"history list [after]|show <runId> [afterSequence]|prune <before> [--apply]; storage export";
	const [command, action = "list"] = argv;
	const { flags, positional } = parseArgv(argv.slice(2), { apply: { type: "boolean" } }, { usage });
	const known =
		(command === "history" && action === "list" && positional.length <= 1) ||
		(command === "history" &&
			action === "show" &&
			positional.length >= 1 &&
			positional.length <= 2) ||
		(command === "history" && action === "prune" && positional.length === 1) ||
		(command === "storage" && action === "export" && positional.length === 0);
	if (!known || (flags.apply !== undefined && action !== "prune")) throw new UsageError(usage);
	const afterSequence = positional[1] === undefined ? 0 : Number(positional[1]);
	if (!Number.isSafeInteger(afterSequence) || afterSequence < 0)
		throw new UsageError(`history show afterSequence must be a whole number. Usage: ${usage}`);
	const session = await openStoreSession({
		env: process.env,
		readOnly: !(command === "history" && action === "prune" && flags.apply === true),
	});
	try {
		let result: unknown;
		if (command === "storage") result = await session.backend.export();
		else if (action === "list") result = await session.listRuns({ after: positional[0] ?? "" });
		else if (action === "show") result = await session.inspect(positional[0], { afterSequence });
		else result = await session.retain({ before: positional[0], dryRun: flags.apply !== true });
		process.stdout.write(JSON.stringify(result) + "\n");
	} finally {
		await session.close();
	}
}

async function handleRuns(argv: string[], signal: AbortSignal) {
	const [command, action, ...rest] = argv;
	const ctx = { env: process.env, signal };
	const usage =
		"runs list [--status <status>] [--limit <limit>] [--after <cursor>]; runs show <runId>; waits show <waitId>";
	let result: unknown;
	if (command === "runs" && (!action || action === "list")) {
		const { flags, positional } = parseArgv(
			rest,
			{
				status: { type: "string" },
				limit: { type: "integer", min: 1 },
				after: { type: "string" },
			},
			{ usage },
		);
		if (positional.length) throw new UsageError(`runs list takes no arguments. Usage: ${usage}`);
		result = await listRuns({
			ctx,
			...(flags.status !== undefined
				? { status: flags.status as NonNullable<Parameters<typeof listRuns>[0]["status"]> }
				: {}),
			...(flags.limit !== undefined ? { limit: flags.limit as number } : {}),
			...(flags.after !== undefined ? { after: flags.after as string } : {}),
		});
	} else if (action === "show") {
		const id = oneId(rest, command === "runs" ? "runs show <runId>" : "waits show <waitId>");
		result =
			command === "runs" ? await getRun({ runId: id, ctx }) : await getWait({ waitId: id, ctx });
		if (result === null)
			throw new Error(
				command === "runs" ? `Run ${id} not found` : `Wait ${id} not found (or no longer pending)`,
			);
	} else {
		throw new UsageError(usage);
	}
	process.stdout.write(JSON.stringify(result) + "\n");
}

/**
 * Paused workflows saved by an older Lobster must be converted once before they can
 * resume. Ask on an interactive terminal; otherwise say what to run.
 */
async function offerUpgrade(argv: string[], signal: AbortSignal) {
	const directory = defaultStateDir(process.env);
	const pending = await pendingUpgrade(directory);
	if (!pending.length) return;
	const toolMode = argv.some(
		(arg, index) => arg === "--mode=tool" || (arg === "--mode" && argv[index + 1] === "tool"),
	);
	const what = `Found ${pending.length} saved item${pending.length === 1 ? "" : "s"} (paused runs, approval IDs, stored values) from an older version of Lobster. ${pending.length === 1 ? "It" : "They"} must be imported once before ${pending.length === 1 ? "it" : "they"} can be used.`;
	if (toolMode || argv[0] === "resume" || !process.stdin.isTTY || !process.stderr.isTTY) {
		process.stderr.write(`lobster: ${what} Run \`lobster upgrade\`.\n`);
		return;
	}
	process.stderr.write(`${what}\nImport now? [y/N] `);
	const answer = String((await readLineFromStream(process.stdin, { signal })) ?? "").trim();
	if (!/^y(es)?$/i.test(answer)) {
		process.stderr.write("Not imported. Run `lobster upgrade` when ready.\n");
		return;
	}
	try {
		process.stderr.write(formatUpgradeReport(await upgradeStore({ directory })));
	} catch (error) {
		process.stderr.write(`Upgrade failed: ${error?.message ?? String(error)}\n`);
	}
}

async function handleUpgrade(argv: string[]) {
	const usage = "lobster upgrade [--dry-run] [--json]";
	const { flags, positional } = parseArgv(
		argv,
		{ "dry-run": { type: "boolean" }, json: { type: "boolean" } },
		{ usage },
	);
	if (positional.length) throw new UsageError(`Usage: ${usage}`);
	let report: UpgradeReport;
	try {
		report = await upgradeStore({
			directory: defaultStateDir(process.env),
			dryRun: flags["dry-run"] === true,
		});
	} catch (error) {
		process.stderr.write(`Upgrade failed: ${error?.message ?? String(error)}\n`);
		process.exitCode = 1;
		return;
	}
	process.stdout.write(
		flags.json === true ? JSON.stringify(report) + "\n" : formatUpgradeReport(report),
	);
}

function formatUpgradeReport(report: UpgradeReport): string {
	const count = (n: number, what: string) => `${n} ${what}${n === 1 ? "" : "s"}`;
	if (!report.converted.length && !report.imported.length && !report.skipped.length)
		return "Nothing to upgrade.\n";
	const would = (done: string, planned: string) => (report.dryRun ? planned : done);
	const lines: string[] = [];
	if (report.converted.length)
		lines.push(
			`${would("Converted", "Would convert")} ${count(report.converted.length, "paused workflow")}.`,
		);
	if (report.imported.length)
		lines.push(
			`${would("Imported", "Would import")} ${count(report.imported.length, "other saved item")} (paused pipelines, approval IDs, stored values).`,
		);
	if (!report.dryRun && (report.converted.length || report.imported.length))
		lines.push("Existing resume tokens and approval IDs still work.");
	if (report.skipped.length) {
		lines.push(
			`${would("Skipped", "Would skip")} ${count(report.skipped.length, "item")} that cannot be used:`,
		);
		for (const item of report.skipped) lines.push(`  ${item.checkpointId}: ${item.reason}`);
		const moved = report.skipped.find((item) => item.movedTo);
		if (moved)
			lines.push(
				`Moved to ${path.dirname(moved.movedTo!)}. To retry one, fix the cause (for example its workflow file), move it back and run \`lobster upgrade\` again.`,
			);
	}
	if (report.backup) lines.push(`The original files were moved to ${report.backup}`);
	return lines.join("\n") + "\n";
}

/** The one ID a command takes, or a UsageError naming its usage. */
function oneId(args: string[], usage: string) {
	const { positional } = parseArgv(args, {}, { usage });
	if (positional.length !== 1) throw new UsageError(`Usage: ${usage}`);
	return positional[0];
}
