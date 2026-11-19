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
import { checkRunId, isRunId, type RunStatus } from "./persistence/model.js";
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
 * `signal` is the CLI's cancellation. Every context also passes it as `forceTerminationSignal`:
 * a step's process watches a narrower signal of its own, and a second Ctrl-C must reach it.
 */
async function runCliWithSignal(argv: string[], signal: AbortSignal) {
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

	if (argv[0] === "runs" && argv[1] === "advance") {
		await handleAdvance(argv.slice(2), signal);
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
				ctx: createToolContext({ signal, forceTerminationSignal: signal }),
			});
			process.stdout.write(JSON.stringify(result) + "\n");
		} catch (error) {
			if (error instanceof UsageError) throw error;
			process.stderr.write(`signal: ${messageOf(error)}\n`);
			process.exitCode = 1;
		}
		return;
	}

	if (argv[0] === "runs" && argv[1] === "abandon") {
		try {
			const runId = oneId(argv.slice(2), "runs abandon <runId>");
			const result = await abandonWorkflowRun({
				runId,
				ctx: createToolContext({ signal, forceTerminationSignal: signal }),
			});
			process.stdout.write(JSON.stringify(result) + "\n");
		} catch (error) {
			if (error instanceof UsageError) throw error;
			process.stderr.write(`runs abandon: ${messageOf(error)}\n`);
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
		if (envelope) writeToolEnvelope({ ...envelope, ...(runId ? { runId } : {}) });
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
			if (mode === "tool") emit(runEnvelope({ status: "ok", output: output.items }));
			return { status: "ok", output: output.items };
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
				: `Run ${output.runId ?? ""} waits for the event "${waiting.event}"${waiting.until ? ` until ${waiting.until}` : ""}; deliver it with \`lobster signal ${waiting.event}\`.\n`,
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
	emit: (payload: ToolPayload) => void = writeToolEnvelope,
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
		writeToolEnvelope({
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
				if (!wait) throw new WaitConflictError();
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
		writeToolEnvelope({ ok: false, error: { type: "doctor_error", message: messageOf(error) } });
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

function writeToolEnvelope(payload: ToolPayload) {
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
		`  lobster runs advance <runId>                       continue a stopped run, or retry a failed one\n` +
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

/** The usage of each `history` and `storage` command, so an error names the one typed. */
const PERSISTENCE_USAGE: Record<string, string> = {
	"history list": "history list [after]",
	"history show": "history show <runId> [afterSequence]",
	"history prune": "history prune <before> [--apply]",
	"storage export": "storage export",
};

async function handlePersistence(argv: string[]) {
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
	if (action === "show") checkRunId(positional[0]);
	if (action === "list" && positional[0] !== undefined && !isRunId(positional[0]))
		throw new Error("Invalid run cursor");
	const afterSequence = positional[1] === undefined ? 0 : Number(positional[1]);
	if (!Number.isSafeInteger(afterSequence) || afterSequence < 0)
		throw new UsageError(`history show afterSequence must be a whole number. Usage: ${usage}`);
	const session = await openStoreSession({
		env: process.env,
		readOnly: !(prune && flags.apply === true),
	});
	try {
		let result: unknown;
		if (command === "storage") result = await session.backend.export();
		else if (action === "list") result = await session.listRuns({ after: positional[0] ?? "" });
		else if (action === "show") {
			result = await session.inspect(positional[0], { afterSequence });
			if (result === null) throw new Error(`Run ${positional[0]} not found`);
		} else result = await session.retain({ before: positional[0], dryRun: flags.apply !== true });
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
				? {
						status: flags.status as RunStatus,
					}
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
		process.stderr.write(`Upgrade failed: ${messageOf(error)}\n`);
		process.exitCode = 1;
		return;
	}
	process.stdout.write(
		flags.json === true ? JSON.stringify(report) + "\n" : formatUpgradeReport(report),
	);
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
async function handleAdvance(argv: string[], signal: AbortSignal) {
	const usage =
		"runs advance <runId> | runs advance --due [--limit <n>] [--concurrency <n>] [--dry-run]";
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
	const ctx = createToolContext({ signal, forceTerminationSignal: signal });
	if (flags.due === true) {
		if (positional.length) throw new UsageError("runs advance --due takes no run ID");
		try {
			const result = await advanceDueRuns({
				ctx,
				limit: flags.limit as number | undefined,
				concurrency: flags.concurrency as number | undefined,
				dryRun: flags["dry-run"] === true,
			});
			process.stdout.write(JSON.stringify(result) + "\n");
		} catch (error) {
			process.stderr.write(`runs advance --due: ${messageOf(error)}\n`);
			process.exitCode = 1;
		}
		return;
	}
	for (const flag of ["limit", "concurrency", "dry-run"])
		if (flags[flag] !== undefined)
			throw new UsageError(`runs advance --${flag} applies only with --due`);
	if (positional.length !== 1) throw new UsageError(`Usage: ${usage}`);
	try {
		writeToolEnvelope(runEnvelope(await advanceWorkflowRun({ runId: positional[0], ctx })));
	} catch (error) {
		writeToolEnvelope(errorEnvelope("runtime_error", error));
		process.exitCode = 1;
	}
}

/** The one ID a command takes, or a UsageError naming its usage. */
function oneId(args: string[], usage: string) {
	const { positional } = parseArgv(args, {}, { usage });
	if (positional.length !== 1) throw new UsageError(`Usage: ${usage}`);
	return positional[0];
}
