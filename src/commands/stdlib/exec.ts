import { defineCommand } from "../define.js";
import { runAbortableProcess } from "../../process/abortable_process.js";
import { streamOf } from "../../io/streams.js";
import type { RunAbortableOptions } from "../../process/abortable_process.js";
import { messageOf } from "../../util/errors.js";

/** The formats `--stdin` writes the stage's input in. */
const STDIN_FORMATS = ["raw", "json", "jsonl"] as const;
export type StdinFormat = (typeof STDIN_FORMATS)[number];

export const execCommand = defineCommand({
	name: "exec",
	meta: {
		description: "Run an OS command",
		argsSchema: {
			type: "object",
			properties: {
				json: { type: "boolean", description: "Parse stdout as JSON (single value)." },
				shell: { type: "string", description: "Run via the system shell with this command line." },
				stdin: {
					type: "string",
					enum: STDIN_FORMATS,
					description: "Write pipeline input to the command's stdin in this format.",
				},
				_: { type: "array", items: { type: "string" }, description: "Command + args." },
			},
			required: ["_"],
		},
		sideEffects: ["local_exec"],
		repeatable: true,
	},
	help() {
		return (
			`exec — run an OS command\n\n` +
			`Usage:\n` +
			`  exec <command...>\n` +
			`  exec --stdin ${STDIN_FORMATS.join("|")} <command...>\n` +
			`  exec --json <command...>\n` +
			`  exec --shell "<command line>"\n\n` +
			`Notes:\n` +
			`  - With --json, parses stdout as JSON (single value).\n` +
			`  - With --stdin, writes pipeline input to stdin.\n` +
			`  - With --shell (or a single arg containing spaces), runs via the system shell.\n`
		);
	},
	async run({ input, args, ctx }) {
		const cmd: string[] = args._;
		// A bare `--shell` would run only the first word as the shell line.
		if (args.shell !== undefined && typeof args.shell !== "string")
			throw new Error(
				'exec --shell takes the command line as its value: exec --shell "<command line>"',
			);
		const shellLine: string | null = args.shell ?? null;
		// A bare `--stdin` would drop the input instead of writing it.
		if (args.stdin !== undefined && typeof args.stdin !== "string")
			throw new Error(`exec --stdin takes a format: ${STDIN_FORMATS.join(", ")}`);
		if (!cmd.length && !shellLine) throw new Error("exec requires a command");
		// The whole command line is --shell's value: words after it would be dropped.
		if (shellLine !== null && cmd.length)
			throw new Error(
				`exec --shell takes the whole command line as its value; got extra words: ${cmd.join(" ")}`,
			);
		// `exec "ls -la"` is one word with a space: it runs as a shell line, like --shell.
		const useShell = shellLine !== null || (cmd.length === 1 && /\s/.test(cmd[0]));
		const target: ExecTarget = useShell ? { shellLine: shellLine ?? cmd[0] } : { argv: cmd };
		return runExec(target, { json: Boolean(args.json), stdin: args.stdin }, input, ctx);
	},
});

/** What exec runs: an executable with its arguments, or a line for the system shell. */
export type ExecTarget = { argv: string[] } | { shellLine: string };

/**
 * Run `target` as the exec command does: pipeline input goes to stdin in the `stdin` format (or
 * is drained), and stdout becomes items, parsed as one JSON value with `json` or else one item
 * per line. The SDK's `exec()` runs through here too.
 */
export async function runExec(
	target: ExecTarget,
	options: { json?: boolean; stdin?: string },
	input: AsyncIterable<unknown>,
	ctx: Omit<ProcessContext, "stdin" | "cwd"> & { cwd?: string },
) {
	// Checked before the input is read: reading it runs the stages before this one.
	const stdinMode = stdinFormat(options.stdin);
	let stdinPayload = null;
	if (stdinMode !== null) {
		const items = [];
		for await (const item of input) items.push(item);
		stdinPayload = encodeStdin(items, stdinMode);
	} else {
		// Drain input to avoid dangling streams.
		for await (const _item of input) {
			// no-op
		}
	}

	const stdout = await spawnExec(target, {
		env: ctx.env,
		cwd: ctx.cwd ?? process.cwd(),
		stdin: stdinPayload,
		signal: ctx.signal,
		assertInvocationCurrent: ctx.assertInvocationCurrent,
		forceTerminationSignal: ctx.forceTerminationSignal,
	});

	if (options.json) {
		let parsed;
		try {
			parsed = JSON.parse(stdout.trim() || "null");
		} catch (err) {
			throw new Error(`exec --json could not parse stdout as JSON: ${messageOf(err)}`);
		}
		return { output: streamOf(Array.isArray(parsed) ? parsed : [parsed]) };
	}

	const lines = stdout.split(/\r?\n/).filter(Boolean);
	return { output: streamOf(lines) };
}

/** What a spawned command gets from the stage that runs it. */
type ProcessContext = Pick<
	RunAbortableOptions,
	"env" | "stdin" | "signal" | "assertInvocationCurrent" | "forceTerminationSignal"
> & { cwd: string };

/** Run `target` with `ctx` and return its stdout; a non-zero exit is an error. */
async function spawnExec(target: ExecTarget, ctx: ProcessContext) {
	// Spawning reports a missing working directory as a missing command or shell: name both.
	const { stdout, stderr, code } = await runAbortableProcess(
		"shellLine" in target
			? {
					...ctx,
					shellCommand: target.shellLine,
					notFoundMessage: `exec shell or working directory (${ctx.cwd}) not found; check LOBSTER_SHELL or ComSpec, or the cwd`,
				}
			: {
					...ctx,
					command: target.argv[0],
					argv: target.argv.slice(1),
					notFoundMessage: `exec command or working directory (${ctx.cwd}) not found: ${target.argv[0]}`,
				},
	);
	if (code === 0) return stdout;
	const what = "shellLine" in target ? target.shellLine : target.argv[0];
	throw new Error(`exec failed (${code}): ${stderr.trim() || stdout.trim() || what}`);
}

/**
 * `--stdin`'s format, or null without it. Pipelines refuse another value before any stage runs
 * (the flag's declared choices); this guards direct callers too.
 */
function stdinFormat(value: string | undefined): StdinFormat | null {
	if (value === undefined) return null;
	if (!isStdinFormat(value))
		throw new Error(`exec --stdin must be one of ${STDIN_FORMATS.join(", ")} (got "${value}")`);
	return value;
}

function isStdinFormat(mode: string): mode is StdinFormat {
	return (STDIN_FORMATS as readonly string[]).includes(mode);
}

function encodeStdin(items: unknown[], mode: StdinFormat) {
	if (mode === "json") return JSON.stringify(items);
	if (mode === "jsonl")
		return items.map((item) => JSON.stringify(item)).join("\n") + (items.length ? "\n" : "");
	return items.map((item) => (typeof item === "string" ? item : JSON.stringify(item))).join("\n");
}
