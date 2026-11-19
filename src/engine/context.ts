// What a run executes with, passed down from the host (CLI, tool mode, SDK) to every step.
import path from "node:path";
import type { PersistenceOptions } from "../persistence/context.js";
import type { CommandLookup } from "../commands/types.js";

/**
 * `dir` (default: the run's directory itself) as an absolute path. A relative directory is the
 * run's directory's, whoever runs or resumes the run, from wherever: never the process's.
 */
export function inRunDirectory(ctx: { cwd?: string }, dir = ".") {
	return path.resolve(ctx.cwd ?? process.cwd(), dir);
}

/** Callbacks a host or an enclosing step passes down to observe a run. */
export type RunHooks = {
	/**
	 * Work that may act is about to start: before a shell step's command, and once per
	 * pipeline dispatch, before its first command that may act.
	 */
	onExecutionStarted?: () => void | Promise<void>;
	/** A command whose effect cannot be undone by restoring a checkpoint started. */
	onUnsafeDispatch?: () => void;
	/** A command that may not be repeated (`repeatable: false`) reported dispatch. */
	onNonRetryableSideEffect?: () => void;
	/**
	 * Progress a command reports (a pipeline command's `ctx.heartbeat()`, whether or not its
	 * step has a limit). It reaches every enclosing step with `heartbeat_timeout_ms`, and the
	 * host's hook, if the host set one.
	 */
	heartbeat?: () => void;
};

/** What a run executes with: its streams, environment, commands, and the host's controls. */
export type RunContext = PersistenceOptions & {
	stdin: NodeJS.ReadableStream;
	stdout: NodeJS.WritableStream;
	stderr: NodeJS.WritableStream;
	env: Record<string, string | undefined>;
	mode: "human" | "tool" | "sdk";
	cwd?: string;
	signal?: AbortSignal;
	forceTerminationSignal?: AbortSignal;
	assertInvocationCurrent?: () => void;
	registry?: CommandLookup;
	/** Adapters `llm.invoke` calls by provider name, passed through to commands. */
	llmAdapters?: Record<string, unknown>;
	dryRun?: boolean;
	hooks?: RunHooks;
};
