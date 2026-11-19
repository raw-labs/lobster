import path from "node:path";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { listRuns, getRun, getWait } from "./core/runs.js";
import { forceTerminateAbortableProcesses } from "./process/abortable_process.js";
import { createDefaultRegistry } from "./commands/registry.js";
import type { CommandRegistry } from "./commands/types.js";
import {
	resumeToolRequest,
	createToolContext,
	runEnvelope,
	errorEnvelope,
	type ToolEnvelope,
} from "./core/tool_runtime.js";
import { checkStages, parsePipeline } from "./pipeline/parser.js";
import { runtimeErrorDetails, WaitConflictError } from "./persistence/backend.js";
import { withExecution } from "./persistence/execution.js";
import { isRunId, type RunStatus } from "./persistence/model.js";
import { isRunStatus, RUN_STATUSES } from "./engine/checkpoint.js";
import { openStoreSession } from "./persistence/session.js";
import { parseResumeArgs } from "./resume/resume.js";
import type { ResumeTarget } from "./resume/options.js";
import { readLineFromStream } from "./io/read_line.js";
import { yesOrNoPrompt } from "./io/terminal.js";
import { defaultStateDir } from "./state/store.js";
import { pendingUpgrade, upgradeStore, type UpgradeReport } from "./engine/upgrade.js";
import { runPipeline } from "./pipeline/runtime.js";
import {
	runWorkflowFile,
	advanceWorkflowRun,
	abandonWorkflowRun,
	runPipelineText,
	runIdOf,
} from "./engine/runner.js";
import { advanceDueRuns, signalEvent } from "./engine/due.js";
import { checkArgNames, resolveWorkflowArgs } from "./workflows/args.js";
import { renderWorkflowGraph } from "./workflows/graph.js";
import { parseWorkflowArgsJson } from "./workflows/values.js";
import type { WorkflowGraphFormat } from "./workflows/graph.js";
import type { WorkflowRunResult } from "./workflows/types.js";
import {
	loadWorkflowFile,
	resolveWorkflowFile,
	WORKFLOW_FILE_EXTENSIONS,
} from "./workflows/load.js";
import { parseArgv, UsageError } from "./util/argv.js";
import { messageOf } from "./util/errors.js";
import { closestMatch } from "./util/suggest.js";

// The CLI's own commands: `help <command>` and `<command> --help` show commandHelp.
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

export async function runCli(argv: string[]) {
	const cancellation = createCliCancellation();
	try {
		await runCliWithSignal(argv, cancellation.signal);
	} catch (error) {
		// Whatever reaches here is reported as one line, not a stack trace; LOBSTER_DEBUG shows it.
		if (cancellation.signal.aborted) throw error;
		process.stderr.write(
			`${process.env.LOBSTER_DEBUG ? (error?.stack ?? error) : `Error: ${messageOf(error)}`}\n`,
		);
		process.exitCode = error instanceof UsageError ? 2 : 1;
	} finally {
		if (cancellation.exitCode !== undefined) {
			process.exitCode = cancellation.exitCode;
		}
		cancellation.dispose();
	}
}

/**
 * The context of a run the CLI continues, abandons or signals: the run's messages (retries,
 * cost warnings) reach stderr, as they do for `lobster run`.
 */
function cliRunContext(signal: AbortSignal) {
	return createToolContext({ signal, forceTerminationSignal: signal, stderr: process.stderr });
}

/**
 * `signal` is the CLI's cancellation. Every context also passes it as `forceTerminationSignal`:
 * a step's process watches a narrower signal of its own, and a second Ctrl-C must reach it.
 */
async function runCliWithSignal(argv: string[], signal: AbortSignal) {
	const registry = createDefaultRegistry();

	// Every command takes `--mode tool|human`, as `run` does; only `run` reads it (the others always
	// print an envelope, or text that is their output: graph, help, version).
	if (DATA_COMMANDS.has(argv[0]) || argv[0] === "graph") {
		try {
			argv = withoutMode(argv);
		} catch (error) {
			writeEnvelope(errorEnvelope("parse_error", error));
			process.exitCode = 2;
			return;
		}
	}

	if (argv[0] === "graph") {
		await handleGraph({ argv: argv.slice(1) });
		return;
	}

	if (argv.length === 0) {
		process.stdout.write(helpText(registry));
		return;
	}

	// `<command> --help` is that command's help; anything else asking for help gets the overview.
	if (argv.includes("-h") || argv.includes("--help")) {
		process.stdout.write(USAGE_TOPICS.has(argv[0]) ? commandHelp(argv[0]) : helpText(registry));
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
		if (USAGE_TOPICS.has(topic)) {
			process.stdout.write(commandHelp(topic));
			return;
		}
		const cmd = registry.get(topic);
		if (!cmd) {
			const near = closestMatch(topic, [...USAGE_TOPICS, "graph", ...registry.list()]);
			process.stderr.write(
				`Unknown command: ${topic}${near ? ` (did you mean ${near}?)` : ""}. \`lobster help\` lists them.\n`,
			);
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
		await answer(() => upgradeResult(argv.slice(1)));
		return;
	}

	// Everything that can run or resume a workflow checks for pauses from an older Lobster.
	if (
		!["runs", "waits", "history", "storage", "doctor"].includes(argv[0]) ||
		(argv[0] === "runs" && argv[1] === "advance")
	) {
		await offerUpgrade(argv, signal);
	}

	if (argv[0] === "runs" && argv[1] === "advance") {
		await answer(() => advanceResult(argv.slice(2), signal));
		return;
	}

	if (argv[0] === "signal") {
		await answer(async () => {
			const usage = COMMAND_USAGE.signal;
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
			return signalEvent({
				name,
				data,
				id: flags.id as string | undefined,
				ctx: cliRunContext(signal),
			});
		});
		return;
	}

	if (argv[0] === "runs" && argv[1] === "abandon") {
		await answer(async () =>
			abandonWorkflowRun({
				runId: runIdArg(oneId(argv.slice(2), COMMAND_USAGE["runs abandon"])),
				ctx: cliRunContext(signal),
			}),
		);
		return;
	}

	if (argv[0] === "runs" || argv[0] === "waits") {
		await answer(() => runsResult(argv, signal));
		return;
	}

	if (argv[0] === "history" || argv[0] === "storage") {
		await answer(() => persistenceResult(argv));
		return;
	}

	if (argv[0] === "doctor") {
		await handleDoctor({ argv: argv.slice(1), registry, signal });
		return;
	}

	if (argv[0] === "run") {
		await handleRun({ argv: argv.slice(1), registry, signal });
		return;
	}

	if (argv[0] === "resume") {
		await handleResume({ argv: argv.slice(1), registry, signal });
		return;
	}

	// Default: treat argv as a pipeline string.
	await handleRun({ argv, registry, signal });
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
			const codes: Partial<Record<NodeJS.Signals, number>> = {
				SIGINT: 130,
				SIGHUP: 129,
				SIGQUIT: 131,
				SIGTERM: 143,
			};
			return codes[received];
		},
		dispose() {
			for (const [terminatingSignal, listener] of listeners) {
				process.removeListener(terminatingSignal, listener);
			}
		},
	};
}

async function handleGraph({ argv }: { argv: string[] }) {
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
		// An input error, as for `lobster run`: exit 2.
		process.stderr.write(`Error: ${messageOf(err)}\n`);
		process.exitCode = 2;
		return;
	}

	try {
		const workflow = await loadWorkflowFile(filePath);
		// Misspelled names are refused as `run` refuses them; a graph needs no required arg.
		checkArgNames(workflow.args, argsJson, `Workflow ${filePath}`);
		const args = resolveWorkflowArgs(workflow.args, argsJson);
		const graph = renderWorkflowGraph({ workflow, format: parsed.format, args });
		process.stdout.write(graph);
		process.stdout.write("\n");
	} catch (err) {
		process.stderr.write(`Error: ${messageOf(err)}\n`);
		process.exitCode = 1;
	}
}

function isWorkflowGraphFormat(value: string): value is WorkflowGraphFormat {
	return value === "mermaid" || value === "dot" || value === "ascii";
}

type ToolPayload = Omit<ToolEnvelope, "protocolVersion">;

type RunOptions = {
	argv: string[];
	registry: CommandRegistry;
	signal: AbortSignal;
};

/** Whether a command line asks for tool mode (read before, or without, parsing it). */
function isToolMode(argv: string[]) {
	return argv.some(
		(arg, index) => arg === "--mode=tool" || (arg === "--mode" && argv[index + 1] === "tool"),
	);
}

async function handleRun(options: RunOptions): Promise<void> {
	let parsed: ReturnType<typeof parseRunArgs>;
	try {
		parsed = parseRunArgs(options.argv);
	} catch (error) {
		// Reported like any parse error of the run: an envelope in tool mode.
		reportRunError(isToolMode(options.argv) ? "tool" : "human", "parse_error", error);
		return;
	}
	const { mode } = parsed;
	let target: RunTarget;
	let args: Record<string, unknown> = {};
	try {
		target = await prepareRunTarget(parsed, options.registry);
	} catch (error) {
		reportRunError(mode, "parse_error", error);
		return;
	}
	try {
		// Only a workflow file takes --args-json (prepareRunTarget refuses it otherwise).
		if (parsed.argsJson !== null) args = parseWorkflowArgsJson(parsed.argsJson, "run");
	} catch (error) {
		reportRunError(mode, "parse_error", error, "");
		return;
	}
	let envelope: ToolPayload | undefined;
	try {
		const result = await withExecution(
			{ env: process.env, signal: options.signal, dryRun: target.dryRun },
			target.kind,
			(ctx) =>
				runTarget({
					...options,
					env: ctx.env,
					mode,
					args,
					target,
					emit: (value) => {
						envelope = value;
					},
				}),
		);
		// The run's ID is known only now: one that never started has been removed.
		const runId = result.runId;
		if (envelope) writeEnvelope({ ...envelope, ...(runId ? { runId } : {}) });
	} catch (error) {
		reportRunError(mode, "runtime_error", error);
	}
}

/** Run what `lobster run` named, a workflow file or a pipeline, and report its result. */
async function runTarget({
	registry,
	signal,
	env,
	mode,
	args,
	target,
	emit,
}: RunOptions & {
	env: NodeJS.ProcessEnv;
	mode: "human" | "tool";
	args: Record<string, unknown>;
	target: RunTarget;
	emit: (envelope: ToolPayload) => void;
}) {
	const { dryRun } = target;
	const ctx = {
		stdin: process.stdin,
		stdout: process.stdout,
		stderr: process.stderr,
		env,
		mode,
		registry,
		signal,
		forceTerminationSignal: signal,
	};
	if (target.kind === "workflow") {
		try {
			const output = await runWorkflowFile({
				filePath: target.workflowFile,
				args,
				ctx: { ...ctx, dryRun },
			});
			if (mode === "tool") emit(runEnvelope(output));
			else printRunResult(output, output.output.length > 0);
			return output;
		} catch (err) {
			reportRunError(mode, "runtime_error", err, runPrefix(err), emit);
			return { ok: false, error: runtimeErrorDetails(err) };
		}
	}

	try {
		if (dryRun) {
			const output = await runPipeline({
				...ctx,
				pipeline: target.pipeline,
				input: [],
				renderOutput: mode !== "tool",
				dryRun,
				haltAfterStageOnAbort: true,
			});
			const plan = "plan" in output ? (output.plan as string) : undefined;
			const planned = { status: "ok" as const, output: output.items, plan };
			if (mode === "tool") emit(runEnvelope(planned));
			return planned;
		}
		const output = await runPipelineText({ pipeline: target.text, ctx });
		if (mode === "tool") emit(runEnvelope(output));
		// If the last command did not render its output, print it as JSON.
		else printRunResult(output, !output.rendered);
		return output;
	} catch (err) {
		reportRunError(mode, "runtime_error", err, runPrefix(err), emit);
		return { ok: false, error: runtimeErrorDetails(err) };
	}
}

/**
 * A run's result for a person: what it waits on, why it stopped, the question it asks, or (when
 * `printOutput`) its output as JSON.
 */
function printRunResult(output: WorkflowRunResult & { runId?: string }, printOutput: boolean) {
	if (output.status === "waiting") {
		const waiting = output.waitingFor!;
		process.stderr.write(
			waiting.kind === "timer"
				? `Run ${output.runId ?? ""} sleeps until ${waiting.until}; \`lobster runs advance --due\` continues it then.\n`
				: `Run ${output.runId ?? ""} waits for the event "${waiting.event}"${waiting.until ? ` until ${waiting.until}` : ""}; deliver it with \`lobster signal ${waiting.event} --data '{...}'\` (the data must contain the fields its wait matches).\n`,
		);
	} else if (output.status === "cancelled") {
		reportCancelled(output);
	} else if (output.status === "needs_approval" || output.status === "needs_input") {
		const question = {
			status: output.status,
			output: [],
			requiresApproval: output.requiresApproval ?? null,
			requiresInput: output.requiresInput ?? null,
		};
		process.stdout.write(JSON.stringify(question, null, 2) + "\n");
	} else if (output.status === "ok" && printOutput) {
		process.stdout.write(JSON.stringify(output.output, null, 2) + "\n");
	}
}

/**
 * A run that ended in an error is named, so it can be looked at (`runs show`) and continued.
 * Its status is not stated: a stopped run is blocked or still running, not failed.
 */
function runPrefix(error: unknown) {
	const runId = runIdOf(error);
	return runId ? `Run ${runId}: ` : undefined;
}

/** A run that ended cancelled (a "no", a deadline): it did not complete, so exit 1. */
function reportCancelled(output: { runId?: string; reason?: string }) {
	const run = output.runId ? `Run ${output.runId}` : "The run";
	process.stderr.write(`${run} was cancelled${output.reason ? `: ${output.reason}` : ""}.\n`);
	process.exitCode = 1;
}

function reportRunError(
	mode: "human" | "tool",
	type: "parse_error" | "runtime_error",
	error: unknown,
	prefix = type === "parse_error" ? "Parse error: " : "Error: ",
	emit: (payload: ToolPayload) => void = writeEnvelope,
) {
	const message = messageOf(error);
	if (mode === "tool") {
		emit({ ok: false, error: runtimeErrorDetails(error, type) });
	} else {
		process.stderr.write(`${prefix}${message}\n`);
	}
	process.exitCode = type === "parse_error" ? 2 : 1;
}

/**
 * What `lobster run` names: a workflow file (`--file`, or the one word of the command line) or
 * a pipeline. A `--dry-run` after a lone workflow file is Lobster's too; after a pipeline it
 * may belong to the last command, so it stays in the pipeline. Words or flags the target would
 * not use are refused: running with other inputs than the ones typed is worse than not running.
 */
async function prepareRunTarget(
	parsed: ReturnType<typeof parseRunArgs>,
	registry: CommandRegistry,
): Promise<RunTarget> {
	const { rest, dryRun } = parsed;
	const words = rest.filter((token) => token !== "--dry-run");
	if (parsed.filePath) {
		if (words.length)
			throw new Error(
				`run --file takes no pipeline (got "${words.join(" ")}"); give workflow args with --args-json, or quote the pipeline to give a command its own --file`,
			);
		return { kind: "workflow", workflowFile: await resolveWorkflowFile(parsed.filePath), dryRun };
	}
	const workflowFile = words.length === 1 ? await workflowFileNamed(words[0]) : null;
	if (workflowFile)
		return { kind: "workflow", workflowFile, dryRun: dryRun || words.length !== rest.length };
	if (parsed.argsJson !== null)
		throw new Error(
			"run --args-json applies to a workflow file; quote the pipeline to give a command its own --args-json",
		);
	// Parsed here, before a run exists: a pipeline that does not parse writes nothing.
	const text = rest.join(" ");
	const pipeline = parsePipeline(text, registry);
	checkStages(pipeline, registry);
	return { kind: "pipeline", text, pipeline, dryRun };
}

/** What `lobster run` runs: a workflow file, or a pipeline (its text, and its parsed stages). */
type RunTarget = { dryRun: boolean } & (
	| { kind: "workflow"; workflowFile: string }
	| { kind: "pipeline"; text: string; pipeline: ReturnType<typeof parsePipeline> }
);

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
		mode: runMode(flags.mode as string | undefined),
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
			help: { type: "boolean" },
		},
		{
			usage: "lobster graph <workflow.lobster> [--format mermaid|dot|ascii] [--args-json '{...}']",
		},
	);
	const words = positional.filter((word) => word !== "-h");
	const file = flags.file as string | undefined;
	// One workflow file, by --file or by name: other words would be dropped.
	if (words.length > (file === undefined ? 1 : 0))
		throw new UsageError(
			"Usage: lobster graph <workflow.lobster> [--format mermaid|dot|ascii] [--args-json '{...}']",
		);
	return {
		filePath: file ?? words[0] ?? null,
		format: (flags.format as string | undefined) || "mermaid",
		argsJson: (flags["args-json"] as string | undefined) ?? null,
		help: flags.help === true || positional.includes("-h"),
	};
}

/** `--mode`: `human` (the default) or `tool`. A misspelled mode is refused, not read as human. */
function runMode(mode: string | undefined): "human" | "tool" {
	if (mode === undefined || mode === "human") return "human";
	if (mode === "tool") return "tool";
	throw new UsageError(`--mode must be human or tool (got "${mode}")`);
}

/** The workflow file `word` names, or null when it is a pipeline. */
async function workflowFileNamed(word: string) {
	if (word.includes("|")) return null;
	try {
		return await resolveWorkflowFile(word);
	} catch (err) {
		if (isWorkflowPathLike(word)) throw err;
		return null;
	}
}

// Command names never contain a path separator or end in a workflow extension.
function isWorkflowPathLike(token: string) {
	if (/[\s|]/.test(token) || token.startsWith("-")) return false;
	const lower = token.toLowerCase();
	return /[\\/]/.test(token) || WORKFLOW_FILE_EXTENSIONS.some((ext) => lower.endsWith(ext));
}

async function handleResume({ argv, registry, signal }: RunOptions) {
	let parsed;
	try {
		parsed = parseResumeArgs(argv);
	} catch (err) {
		writeEnvelope({
			ok: false,
			error: { type: "parse_error", message: messageOf(err) },
		});
		process.exitCode = 2;
		return;
	}

	let envelope;
	try {
		// A wait ID without --expected-revision answers the question as it stands now, as a
		// token or a short approval ID does; --expected-revision adds the stale-question guard.
		const parsedTarget = parsed.target;
		let target: ResumeTarget;
		if ("waitId" in parsedTarget) {
			let expectedRevision = parsedTarget.expectedRevision;
			if (expectedRevision === undefined) {
				const wait = await getWait({ waitId: parsedTarget.waitId, ctx: { env: process.env } });
				// Gone (answered, withdrawn) or not this caller's: the same conflict as a stale revision.
				if (!wait)
					throw new WaitConflictError(
						`Wait ${parsedTarget.waitId} is not pending: it was answered or withdrawn, or is not yours`,
					);
				expectedRevision = wait.revision;
			}
			target = { waitId: parsedTarget.waitId, expectedRevision };
		} else target = parsedTarget;
		envelope = await resumeToolRequest({
			...target,
			...parsed.decision,
			ctx: {
				cwd: process.cwd(),
				env: process.env,
				stdin: process.stdin,
				stdout: process.stdout,
				stderr: process.stderr,
				registry,
				signal,
				forceTerminationSignal: signal,
			},
		});
	} catch (err) {
		envelope = errorEnvelope("runtime_error", err);
	}
	writeEnvelope(envelope);
	if (!envelope.ok) {
		process.exitCode = envelope.error?.type === "parse_error" ? 2 : 1;
	}
}

async function readVersion() {
	const here = path.dirname(fileURLToPath(import.meta.url));
	const pkg = JSON.parse(await readFile(path.join(here, "..", "..", "package.json"), "utf8"));
	return pkg.version ?? "0.0.0";
}

async function handleDoctor({ argv, registry, signal }: RunOptions) {
	try {
		await runPipeline({
			pipeline: parsePipeline("exec --json --shell 'echo [1]'", registry),
			registry,
			input: [],
			stdin: process.stdin,
			stdout: process.stdout,
			stderr: process.stderr,
			env: process.env,
			mode: "tool",
			signal,
			forceTerminationSignal: signal,
			haltAfterStageOnAbort: true,
		});
	} catch (error) {
		writeEnvelope({ ok: false, error: { type: "doctor_error", message: messageOf(error) } });
		process.exitCode = 1;
		return;
	}

	writeEnvelope({
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

/** The envelope every command answers with on stdout: `{ protocolVersion, ok, ... }`. */
function writeEnvelope(payload: object) {
	process.stdout.write(JSON.stringify({ protocolVersion: 1, ...payload }, null, 2) + "\n");
}

/**
 * Run a command and answer with its result in the envelope, or with its error: a mistake in the
 * command line is a `parse_error` (exit 2), anything else a `runtime_error` (exit 1). An error
 * may carry `details`, what the command still has to report (the runs a batch did advance).
 */
async function answer(task: () => Promise<object>) {
	try {
		writeEnvelope({ ok: true, ...(await task()) });
	} catch (error) {
		const input = error instanceof UsageError;
		const details = (error as { details?: object })?.details;
		writeEnvelope({
			...errorEnvelope(input ? "parse_error" : "runtime_error", error),
			...details,
		});
		process.exitCode = input ? 2 : 1;
	}
}

/** The commands that answer with an envelope whatever `--mode` says (see DATA_COMMANDS). */
const DATA_COMMANDS = new Set([
	"resume",
	"signal",
	"runs",
	"waits",
	"history",
	"storage",
	"upgrade",
	"doctor",
]);

/** `argv` without `--mode tool|human`, which every command accepts; a bad mode is refused. */
function withoutMode(argv: string[]) {
	const rest: string[] = [];
	for (let index = 0; index < argv.length; index++) {
		const arg = argv[index];
		if (arg === "--mode") {
			const value = argv[++index];
			if (value === undefined) throw new UsageError("--mode takes tool or human");
			runMode(value);
		} else if (arg.startsWith("--mode=")) runMode(arg.slice("--mode=".length));
		else rest.push(arg);
	}
	return rest;
}

/** How each command is used: its help, and the usage its errors name. */
const COMMAND_USAGE: Record<string, string> = {
	run: "lobster run [--mode tool] [--dry-run] (<workflow.lobster> | --file <workflow> [--args-json '{...}'] | '<pipeline>')",
	resume:
		"lobster resume <token | approval ID | wait ID> (--approve yes|no | --response-json '{...}' | --cancel) [--expected-revision <n>]",
	signal: "lobster signal <event> [--data '{...}'] [--id <delivery id>]",
	"runs list": `lobster runs list [--status ${RUN_STATUSES.join("|")}] [--limit <n>] [--after <runId>]`,
	"runs show": "lobster runs show <runId>",
	"runs advance":
		"lobster runs advance <runId> | lobster runs advance --due [--limit <n>] [--concurrency <n>] [--dry-run]",
	"runs abandon": "lobster runs abandon <runId>",
	"waits show": "lobster waits show <waitId>",
	upgrade: "lobster upgrade [--dry-run]",
};

function helpText(registry: CommandRegistry) {
	return (
		`lobster — workflows with approvals, waits and LLM calls, for agents\n\n` +
		`Usage: lobster <command> [options]    (lobster help <command> for one command)\n\n` +
		`Every command prints one JSON envelope on stdout: {"protocolVersion":1,"ok":true,...} or\n` +
		`{"ok":false,"error":{"type","message"}}, exit 2 for a mistake in the command line, 1 otherwise.\n` +
		`\`run\` prints the envelope with --mode tool; without it, the run's output. stderr carries\n` +
		`messages for a person (retries, warnings). \`lobster help <command>\` explains each one.\n\n` +
		`Start and answer:\n` +
		`  lobster run --mode tool <workflow.lobster> [--args-json '{...}']   start a workflow run\n` +
		`  lobster run --mode tool '<pipeline>'                 run a pipeline (also: lobster '<pipeline>')\n` +
		`  lobster run --mode tool --dry-run <workflow | pipeline>   the plan, nothing runs\n` +
		`  lobster resume <ref> --approve yes|no | --response-json '{...}' | --cancel\n` +
		`      <ref>: a resume token, a short approval ID or a wait ID\n` +
		`  lobster signal <event> [--data '{...}'] [--id <delivery id>]   deliver an event\n\n` +
		`Runs:\n` +
		`  lobster runs list [--status <status>] [--limit <n>] [--after <runId>]\n` +
		`  lobster runs show <runId>              a run, and the questions it waits on\n` +
		`  lobster runs advance <runId>           continue a stopped run, or retry a failed one\n` +
		`  lobster runs advance --due             continue every run whose time has come (cron)\n` +
		`  lobster runs abandon <runId>           stop a run for good\n` +
		`  lobster waits show <waitId>            a question in full\n\n` +
		`Diagnostics and setup:\n` +
		`  lobster history list | show <runId> | prune <date> [--apply]\n` +
		`  lobster storage export\n` +
		`  lobster graph <workflow.lobster> [--format mermaid|dot|ascii]   (prints the graph)\n` +
		`  lobster upgrade [--dry-run]            import state saved by an older version\n` +
		`  lobster doctor | version | help <command>\n\n` +
		`Environment:\n` +
		`  LOBSTER_STATE_DIR   where runs are saved (default ~/.lobster/state)\n` +
		`  LOBSTER_HISTORY=1   record each step's attempts, for \`history show\`\n\n` +
		`The commands a pipeline runs (\`lobster help <command>\` explains one):\n\n` +
		`Commands:\n` +
		`  ${registry.list().join(", ")}\n`
	);
}

/** One CLI command's help: its usage, what it does, and what its envelope holds. */
function commandHelp(topic: string) {
	const text: Record<string, string> = {
		run:
			`${COMMAND_USAGE.run}\n\n` +
			`Starts a workflow file, or runs a pipeline of Lobster commands.\n` +
			`  --mode tool       print the envelope: status (ok, needs_approval, needs_input, waiting,\n` +
			`                    cancelled), output, requiresApproval / requiresInput (with a resumeToken),\n` +
			`                    waitingFor, runId. Without it, the output only.\n` +
			`  --args-json '{}'  the workflow's args (a workflow file only)\n` +
			`  --dry-run         check and plan without running; the plan is in "plan" (and on stderr)\n`,
		resume:
			`${COMMAND_USAGE.resume}\n\n` +
			`Answers a question a run asked, then continues the run. Prints the run's envelope, as run does.\n` +
			`  --approve yes|no          an approval\n` +
			`  --response-json '{...}'   an input request; it must fit the question's schema\n` +
			`  --cancel                  cancel the run instead of answering\n` +
			`  --expected-revision <n>   with a wait ID: refuse if the question changed since revision n\n`,
		signal:
			`${COMMAND_USAGE.signal}\n\n` +
			`Stores an event, then advances the runs waiting for it (a wait: step whose match it fits).\n` +
			`Envelope: event { id, name, inserted }, advanced [{ runId, status }].\n` +
			`  --data '{...}'   the event's data (JSON)\n` +
			`  --id <id>        a delivery ID: the same ID again is ignored (with other data, refused)\n`,
		runs:
			[
				COMMAND_USAGE["runs list"],
				COMMAND_USAGE["runs show"],
				COMMAND_USAGE["runs advance"],
				COMMAND_USAGE["runs abandon"],
			].join("\n") +
			`\n\nlist: runs, in run-ID order, with nextCursor for --after. show: run, with its pending waits.\n` +
			`advance: the run's envelope, as run prints it; --due: due and advanced [{ runId, status }],\n` +
			`not ok when a run failed or blocked. abandon: runId, status, abandoned (what was withdrawn).\n`,
		waits: `${COMMAND_USAGE["waits show"]}\n\nA pending question in full: wait { id, runId, revision, request }.\n`,
		history:
			`lobster history list [after] | show <runId> [afterSequence] | prune <date> [--apply]\n\n` +
			`list: runs, as runs list. show: history, a run's events and attempts (record them with\n` +
			`LOBSTER_HISTORY=1). prune: runs finished before <date>; without --apply nothing is removed\n` +
			`(dryRun: true), with it they are.\n`,
		storage: `lobster storage export\n\nThe whole store as JSON: archive { states, runs }.\n`,
		upgrade:
			`${COMMAND_USAGE.upgrade}\n\n` +
			`Imports what an older Lobster saved (paused runs, approval IDs, stored values), once.\n` +
			`Envelope: upgrade (the report) and summary (in words). --dry-run reports without changing.\n`,
		doctor: `lobster doctor\n\nChecks that commands run, and prints the version.\n`,
		version: `lobster version\n\nPrints the version.\n`,
		help: `lobster help [command]\n\nThe overview, or one command's help (CLI or pipeline command).\n`,
	};
	return text[topic];
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

/** The usage of each `history` and `storage` command, so an error names the one typed. */
const PERSISTENCE_USAGE: Record<string, string> = {
	"history list": "history list [after]",
	"history show": "history show <runId> [afterSequence]",
	"history prune": "history prune <before> [--apply]",
	"storage export": "storage export",
};

/** `history` and `storage`: what they read (or, with `prune --apply`, removed). */
async function persistenceResult(argv: string[]): Promise<Record<string, unknown>> {
	const [command, action = "list"] = argv;
	const usage =
		PERSISTENCE_USAGE[`${command} ${action}`] ?? Object.values(PERSISTENCE_USAGE).join(" | ");
	const prune = command === "history" && action === "prune";
	const { flags, positional } = parseArgv(
		argv.slice(2),
		prune ? { apply: { type: "boolean" } } : {},
		{ usage },
	);
	const known =
		(command === "history" && action === "list" && positional.length <= 1) ||
		(command === "history" &&
			action === "show" &&
			positional.length >= 1 &&
			positional.length <= 2) ||
		(prune && positional.length === 1) ||
		(command === "storage" && action === "export" && positional.length === 0);
	if (!known) throw new UsageError(`Usage: ${usage}`);
	// The same run-ID rules as `runs show` and `runs list --after`.
	if (action === "show") runIdArg(positional[0]);
	if (action === "list" && positional[0] !== undefined && !isRunId(positional[0]))
		throw new UsageError(`Invalid run cursor "${positional[0]}": use a run ID from the list`);
	const afterSequence = positional[1] === undefined ? 0 : Number(positional[1]);
	if (!Number.isSafeInteger(afterSequence) || afterSequence < 0)
		throw new UsageError(`history show afterSequence must be a whole number. Usage: ${usage}`);
	if (prune && !Number.isFinite(Date.parse(positional[0])))
		throw new UsageError(
			`history prune takes a date or time (runs that finished before it), such as 2026-01-31; got "${positional[0]}"`,
		);
	const apply = prune && flags.apply === true;
	const session = await openStoreSession({ env: process.env, readOnly: !apply });
	try {
		if (command === "storage") return { archive: await session.backend.export() };
		if (action === "list") return { ...(await session.listRuns({ after: positional[0] ?? "" })) };
		if (action === "show") {
			const history = await session.inspect(positional[0], { afterSequence });
			if (history === null) throw new Error(`Run ${positional[0]} not found`);
			return { history };
		}
		// Without --apply nothing is removed: the runs that would be.
		const runs = await session.retain({ before: positional[0], dryRun: !apply });
		return { dryRun: !apply, runs };
	} finally {
		await session.close();
	}
}

/** `runs list`, `runs show` and `waits show`. */
async function runsResult(argv: string[], signal: AbortSignal): Promise<Record<string, unknown>> {
	const [command, action, ...rest] = argv;
	const ctx = { env: process.env, signal };
	if (command === "runs" && (!action || action === "list")) {
		const usage = COMMAND_USAGE["runs list"];
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
		const status = flags.status as string | undefined;
		if (status !== undefined && !isRunStatus(status))
			throw new UsageError(
				`runs list --status takes one of ${RUN_STATUSES.join(", ")}; got "${status}"`,
			);
		const after = flags.after as string | undefined;
		if (after !== undefined && !isRunId(after))
			throw new UsageError(`Invalid run cursor "${after}": use a run ID from the list`);
		return {
			...(await listRuns({
				ctx,
				...(status !== undefined ? { status: status as RunStatus } : {}),
				...(flags.limit !== undefined ? { limit: flags.limit as number } : {}),
				...(after !== undefined ? { after } : {}),
			})),
		};
	}
	if (action === "show" && (command === "runs" || command === "waits")) {
		const id = oneId(rest, COMMAND_USAGE[`${command} show`]);
		if (command === "runs") {
			runIdArg(id);
			const run = await getRun({ runId: id, ctx });
			if (run === null) throw new Error(`Run ${id} not found`);
			return { run };
		}
		const wait = await getWait({ waitId: id, ctx });
		if (wait === null) throw new Error(`Wait ${id} not found (or no longer pending)`);
		return { wait };
	}
	throw new UsageError(
		`Usage: ${
			command === "waits"
				? COMMAND_USAGE["waits show"]
				: Object.entries(COMMAND_USAGE)
						.filter(([name]) => name.startsWith("runs "))
						.map(([, usage]) => usage)
						.join(" | ")
		}`,
	);
}

/**
 * Paused workflows saved by an older Lobster must be converted once before they can
 * resume. Ask on an interactive terminal; otherwise say what to run.
 */
async function offerUpgrade(argv: string[], signal: AbortSignal) {
	const directory = defaultStateDir(process.env);
	const pending = await pendingUpgrade(directory);
	if (!pending.length) return;
	const what = `Found ${pending.length} saved item${pending.length === 1 ? "" : "s"} (paused runs, approval IDs, stored values) from an older version of Lobster. ${pending.length === 1 ? "It" : "They"} must be imported once before ${pending.length === 1 ? "it" : "they"} can be used.`;
	if (isToolMode(argv) || argv[0] === "resume" || !process.stdin.isTTY || !process.stderr.isTTY) {
		process.stderr.write(`lobster: ${what} Run \`lobster upgrade\`.\n`);
		return;
	}
	process.stderr.write(`${what}\nImport now? [y/N] `);
	const answer = (await readLineFromStream(process.stdin, { signal })).trim();
	// Anything but yes declines: nothing is lost, and `lobster upgrade` imports later.
	if (yesOrNoPrompt(answer) !== true) {
		process.stderr.write("Not imported. Run `lobster upgrade` when ready.\n");
		return;
	}
	try {
		process.stderr.write(formatUpgradeReport(await upgradeStore({ directory })));
	} catch (error) {
		process.stderr.write(`Upgrade failed: ${messageOf(error)}\n`);
	}
}

/** `lobster upgrade`: the report as an envelope (`--json` is accepted, as before). */
async function upgradeResult(argv: string[]): Promise<Record<string, unknown>> {
	const usage = COMMAND_USAGE.upgrade;
	const { flags, positional } = parseArgv(
		argv,
		{ "dry-run": { type: "boolean" }, json: { type: "boolean" } },
		{ usage },
	);
	if (positional.length) throw new UsageError(`Usage: ${usage}`);
	const upgrade = await upgradeStore({
		directory: defaultStateDir(process.env),
		dryRun: flags["dry-run"] === true,
	});
	return { upgrade, summary: formatUpgradeReport(upgrade).trimEnd() };
}

function formatUpgradeReport(report: UpgradeReport): string {
	const count = (n: number, what: string) => `${n} ${what}${n === 1 ? "" : "s"}`;
	// A file an interrupted upgrade had already imported is only moved: the report has its backup.
	if (
		!report.converted.length &&
		!report.imported.length &&
		!report.skipped.length &&
		!report.backup
	)
		return "Nothing to upgrade.\n";
	const would = (done: string, planned: string) => (report.dryRun ? planned : done);
	const lines: string[] = [];
	if (report.converted.length)
		lines.push(
			`${would("Converted", "Would convert")} ${count(report.converted.length, "paused run")} (workflows and pipelines).`,
		);
	if (report.imported.length)
		lines.push(
			`${would("Imported", "Would import")} ${count(report.imported.length, "other saved item")} (paused SDK steps, approval IDs, stored values).`,
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

/** `runs advance <runId>` continues one run; `runs advance --due`, every run whose time has come. */
async function advanceResult(
	argv: string[],
	signal: AbortSignal,
): Promise<Record<string, unknown>> {
	const usage = COMMAND_USAGE["runs advance"];
	const { flags, positional } = parseArgv(
		argv,
		{
			due: { type: "boolean" },
			limit: { type: "integer", min: 1 },
			concurrency: { type: "integer", min: 1 },
			"dry-run": { type: "boolean" },
		},
		{ usage },
	);
	const ctx = cliRunContext(signal);
	if (flags.due === true) {
		if (positional.length) throw new UsageError("runs advance --due takes no run ID");
		const result = await advanceDueRuns({
			ctx,
			limit: flags.limit as number | undefined,
			concurrency: flags.concurrency as number | undefined,
			dryRun: flags["dry-run"] === true,
		});
		// A run that failed, or blocked, needs someone: said on stderr too (a scheduler that
		// throws stdout away still shows it), and in the envelope and the exit code.
		const troubled = result.advanced.filter((run) => run.error !== undefined);
		for (const run of troubled)
			process.stderr.write(`lobster: run ${run.runId} is ${run.status}: ${run.error}\n`);
		if (!troubled.length) return result;
		throw Object.assign(
			new Error(`${troubled.length} of ${result.advanced.length} due runs did not advance`),
			{ details: result },
		);
	}
	for (const flag of ["limit", "concurrency", "dry-run"])
		if (flags[flag] !== undefined)
			throw new UsageError(`runs advance --${flag} applies only with --due`);
	if (positional.length !== 1) throw new UsageError(`Usage: ${usage}`);
	return runEnvelope(await advanceWorkflowRun({ runId: runIdArg(positional[0]), ctx }));
}

/** A run ID typed on the command line; a malformed one is a mistake in the command line. */
function runIdArg(id: string) {
	if (!isRunId(id))
		throw new UsageError(`Invalid run ID "${id}": use one from \`lobster runs list\``);
	return id;
}

/** The one ID a command takes, or a UsageError naming its usage. */
function oneId(args: string[], usage: string) {
	const { positional } = parseArgv(args, {}, { usage });
	if (positional.length !== 1) throw new UsageError(`Usage: ${usage}`);
	return positional[0];
}
