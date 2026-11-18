import { defineCommand } from "../define.js";
import { runAbortableProcess } from "../../process/abortable_process.js";
import { streamOf } from "../../io/terminal.js";
import type { RunAbortableOptions } from "../../process/abortable_process.js";

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
					enum: ["raw", "json", "jsonl"],
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
			`  exec --stdin raw|json|jsonl <command...>\n` +
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
		const shellLine = typeof args.shell === "string" ? args.shell : null;
		if (!cmd.length && !shellLine) throw new Error("exec requires a command");
		// `exec "ls -la"` is one word with a space: it runs as a shell line, like --shell.
		const useShell = Boolean(args.shell) || (cmd.length === 1 && /\s/.test(cmd[0]));
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
	ctx: any,
) {
	const stdinMode = typeof options.stdin === "string" ? options.stdin.toLowerCase() : null;
	let stdinPayload = null;
	if (stdinMode) {
		const items = [];
		for await (const item of input) items.push(item);
		stdinPayload = encodeStdin(items, stdinMode);
	} else {
		// Drain input to avoid dangling streams.
		for await (const _item of input) {
			// no-op
		}
	}

	const processContext: ProcessContext = {
		env: ctx.env,
		cwd: ctx.cwd ?? process.cwd(),
		stdin: stdinPayload,
		signal: ctx.signal,
		assertInvocationCurrent: ctx.assertInvocationCurrent,
		forceTerminationSignal: ctx.forceTerminationSignal,
	};
	const result =
		"shellLine" in target
			? await runShellLine(target.shellLine, processContext)
			: await runProcess(target.argv[0], target.argv.slice(1), processContext);

	if (options.json) {
		let parsed;
		try {
			parsed = JSON.parse(result.stdout.trim() || "null");
		} catch (err: any) {
			throw new Error(`exec --json could not parse stdout as JSON: ${err?.message ?? String(err)}`);
		}
		return { output: streamOf(Array.isArray(parsed) ? parsed : [parsed]) };
	}

	const lines = result.stdout.split(/\r?\n/).filter(Boolean);
	return { output: streamOf(lines) };
}

/** What a spawned command gets from the stage that runs it. */
type ProcessContext = Pick<
	RunAbortableOptions,
	"env" | "cwd" | "stdin" | "signal" | "assertInvocationCurrent" | "forceTerminationSignal"
>;

async function runProcess(
	command: string,
	argv: string[],
	{ env, cwd, stdin, signal, assertInvocationCurrent, forceTerminationSignal }: ProcessContext,
) {
	const { stdout, stderr, code } = await runAbortableProcess({
		command,
		argv,
		env,
		cwd,
		stdin,
		signal,
		assertInvocationCurrent,
		forceTerminationSignal,
		notFoundMessage: `exec command not found: ${command}`,
	});
	if (code === 0) return { stdout, stderr };
	throw new Error(`exec failed (${code}): ${stderr.trim() || stdout.trim() || command}`);
}

function runShellLine(
	commandLine: string,
	{ env, cwd, stdin, signal, assertInvocationCurrent, forceTerminationSignal }: ProcessContext,
) {
	return runAbortableProcess({
		shellCommand: commandLine,
		env,
		cwd,
		stdin,
		signal,
		assertInvocationCurrent,
		forceTerminationSignal,
		notFoundMessage: "exec shell not found; check LOBSTER_SHELL or ComSpec",
	}).then(({ stdout, stderr, code }) => {
		if (code === 0) return { stdout, stderr };
		throw new Error(`exec failed (${code}): ${stderr.trim() || stdout.trim() || commandLine}`);
	});
}

function encodeStdin(items: unknown[], mode: string) {
	if (mode === "json") return JSON.stringify(items);
	if (mode === "jsonl") {
		return items.map((item) => JSON.stringify(item)).join("\n") + (items.length ? "\n" : "");
	}
	if (mode === "raw") {
		return items.map((item) => (typeof item === "string" ? item : JSON.stringify(item))).join("\n");
	}
	throw new Error(`exec --stdin must be raw, json, or jsonl (got ${mode})`);
}
