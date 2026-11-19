// The one-shot pipeline runner the package exports: it runs parsed stages once, without saving
// a run. `Lobster` instances run pipelines as saved runs instead.
import { runPipeline as runCorePipeline } from "./runtime.js";
import type { parsePipeline } from "./parser.js";
import type { CommandInputResume } from "./input_request.js";
import type { CommandLookup } from "../commands/types.js";

/** What `runPipeline` runs, and with what. */
export type RunPipelineOptions = {
	/** The stages, as `parsePipeline` returns them. */
	pipeline: ReturnType<typeof parsePipeline>;
	registry: CommandLookup;
	stdin: NodeJS.ReadableStream;
	stdout: NodeJS.WritableStream;
	stderr: NodeJS.WritableStream;
	env: Record<string, string | undefined>;
	mode?: "human" | "tool" | "sdk";
	/** The first stage's input items. */
	input?: Iterable<unknown> | AsyncIterable<unknown>;
	signal?: AbortSignal;
	requestInputResume?: CommandInputResume;
	requestInputEnabled?: boolean;
};

/** What a pipeline run once produced. */
export type RunPipelineResult = {
	/** The last stage's output items (or, when it halted, what it halted with). */
	items: unknown[];
	/** Whether the last command rendered its own output. */
	rendered: boolean;
	/** Whether a stage halted the pipeline (to ask, or for approval). */
	halted: boolean;
	/** Where it halted: `inPlace` when a command asked mid-run and runs again once answered. */
	haltedAt: {
		index: number;
		stage: RunPipelineOptions["pipeline"][number];
		inPlace?: boolean;
	} | null;
	/** Whether any command started. */
	executionStarted: boolean;
};

/**
 * Run a parsed pipeline once, without saving it as a run: part of the SDK's public API
 * (`@clawdbot/lobster` exports it). `Lobster` instances run pipelines as saved runs.
 */
export async function runPipeline({
	pipeline,
	registry,
	stdin,
	stdout,
	stderr,
	env,
	mode,
	input,
	signal,
	requestInputResume,
	requestInputEnabled,
}: RunPipelineOptions): Promise<RunPipelineResult> {
	return runCorePipeline({
		pipeline,
		registry,
		stdin,
		stdout,
		stderr,
		env,
		mode,
		input,
		signal,
		requestInputResume,
		requestInputEnabled,
	});
}
