// The SDK's ready-made stages. Each runs a built-in command, so it behaves exactly as that
// command does in a pipeline (same checks, errors and host-authority calls) and carries the
// command's meta, which tells the engine how to recover and resume it.
import { approveCommand } from "../commands/stdlib/approve.js";
import { diffLastCommand } from "../commands/stdlib/diff_last.js";
import { execCommand, runExec, type ExecTarget } from "../commands/stdlib/exec.js";
import { stateGetCommand, stateSetCommand } from "../commands/stdlib/state.js";
import type { CommandMeta, DefinedCommand } from "../commands/types.js";
import { tokenizeCommand } from "../pipeline/command_tokens.js";
import type { SdkStageArgs } from "./runtime.js";

/** What a command's run returns: its items, and `halt` when it stops the run (approve). */
type StageResult = { output: AsyncIterable<unknown> | Iterable<unknown>; halt?: boolean };

/** An SDK stage that runs a built-in command: its name, flags and meta, and how it runs. */
type CommandStage = {
	type: string;
	args: Record<string, unknown>;
	meta: CommandMeta;
	run(stage: SdkStageArgs): Promise<StageResult>;
};

/** A stage that runs `command` with `args`; `run` replaces the command's own run when given. */
function commandStage(
	command: DefinedCommand,
	args: Record<string, unknown>,
	run: (stage: SdkStageArgs) => Promise<StageResult> = ({ input, ctx }) =>
		command.run({ input, args, ctx }),
): CommandStage {
	return { type: command.name, args, meta: command.meta, run };
}

export type ExecOptions = {
	/** Parse stdout as one JSON value (default). `false` yields one item per output line. */
	json?: boolean;
	/** Run `command` as a line for the system shell instead of splitting it into arguments. */
	shell?: boolean;
	/** Working directory (default: the run's). */
	cwd?: string;
	/** Write the stage's input to the command's stdin in this format. */
	stdin?: "raw" | "json" | "jsonl";
};

/**
 * Run an OS command, as the `exec` command does. The command line is split into arguments
 * (quotes group words) and run without a shell, unless `shell: true`.
 *
 * @example new Lobster().pipe(exec("gh pr view 123 --repo owner/repo --json title,url"))
 */
export function exec(command: string, options: ExecOptions = {}) {
	const target: ExecTarget = options.shell
		? { shellLine: command }
		: { argv: tokenizeCommand(command, "sdk") };
	if ("argv" in target && !target.argv.length) throw new Error("exec requires a command");
	const flags = { json: options.json !== false, stdin: options.stdin };
	const args =
		"argv" in target ? { ...flags, _: target.argv } : { ...flags, shell: command, _: [] };
	// Through runExec, not the command's run: a single argument containing a space (a quoted
	// path) stays one executable instead of becoming a shell line.
	return commandStage(execCommand, args, ({ input, ctx }) =>
		runExec(target, flags, input, options.cwd ? { ...ctx, cwd: options.cwd } : ctx),
	);
}

export type ApproveOptions = {
	/** The question (default "Approve?"). */
	prompt?: string;
	/** Add a text preview of the first `limit` items to the request (they are always in it). */
	preview?: boolean;
	/** Items in the preview (default 5). */
	limit?: number;
};

/**
 * Stop the run for a yes/no decision on the items so far, as the `approve` command does
 * outside a terminal. After a yes the items go on to the next stage.
 *
 * @example new Lobster().pipe(fetchReplies).pipe(approve({ prompt: "Send these?" })).pipe(send)
 */
export function approve(options: ApproveOptions = {}) {
	return commandStage(approveCommand, {
		prompt: options.prompt ?? "Approve?",
		emit: true,
		...(options.preview ? { "preview-from-stdin": true } : null),
		...(options.limit !== undefined ? { limit: options.limit } : null),
	});
}

/** Read a JSON value from Lobster state, as `state.get <key>` does. */
export function stateGet(key: string) {
	if (!key) throw new Error("stateGet requires a key");
	return commandStage(stateGetCommand, { _: [key] });
}

/** Store the stage's input as one JSON value (a single item, or the list), as `state.set` does. */
export function stateSet(key: string) {
	if (!key) throw new Error("stateSet requires a key");
	return commandStage(stateSetCommand, { _: [key] });
}

/** `state.get` and `state.set` under one name. */
export const state = { get: stateGet, set: stateSet };

export type DiffLastOptions = {
	/** When nothing changed, yield `{ kind, key, changed: false, suppressed: true }` instead. */
	changesOnly?: boolean;
};

/**
 * Compare the stage's input with the value stored under `key` last time, store it, and yield
 * `{ kind: "diff.last", key, changed, before, after }`, as the `diff.last` command does.
 */
export function diffLast(key: string, options: DiffLastOptions = {}) {
	if (!key) throw new Error("diffLast requires a key");
	const args = { key };
	if (!options.changesOnly) return commandStage(diffLastCommand, args);
	return commandStage(diffLastCommand, args, async ({ input, ctx }) => {
		const result = await diffLastCommand.run({ input, args, ctx });
		const diffs: Array<{ changed: boolean }> = [];
		for await (const diff of result.output) diffs.push(diff as { changed: boolean });
		return {
			output: diffs.map((diff) =>
				diff.changed ? diff : { kind: "diff.last", key, changed: false, suppressed: true },
			),
		};
	});
}
