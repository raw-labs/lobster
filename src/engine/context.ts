// What a run executes with, passed down from the host (CLI, tool mode, SDK) to every step.
import type { PersistenceOptions } from "../persistence/context.js";
import type { CommandLookup } from "../commands/types.js";

/** Callbacks a host or an enclosing step passes down to observe a run. */
export type RunHooks = {
	/** A resume token was resolved to the wait it answers. */
	onResumeStateResolved?: (stateKey: string) => void;
	/** A command is about to start. */
	onExecutionStarted?: () => void | Promise<void>;
	/** A command whose effect cannot be undone by restoring a checkpoint started. */
	onUnsafeDispatch?: () => void;
	/** A command that may not be repeated (`repeatable: false`) reported dispatch. */
	onNonRetryableSideEffect?: () => void;
	/** Progress for a step with `heartbeat_timeout_ms`. */
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
