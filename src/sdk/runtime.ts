import { runPipeline as runCorePipeline } from "../pipeline/runtime.js";

/**
 * SDK Runtime - Executes Lobster pipelines
 *
 * This adapts SDK stages to the core runtime so command-level suspension rules
 * stay identical across CLI, tool mode, and SDK entry points.
 */

/**
 * What a stage object's `run` gets: the stream of items, and the command context (the same
 * one built-in commands get, with `requestInput`, `env`, `signal` and the rest).
 */
export type SdkStageArgs = { input: AsyncIterable<unknown>; ctx: any };

/**
 * A stage of an SDK pipeline: a function of the stage's items (a generator function gets the
 * stream instead), or an object with a command-like `run`.
 */
export type SdkStage = ((items: any, ctx: any) => unknown) | { run(args: SdkStageArgs): unknown };

async function collectItems(iterable: AsyncIterable<unknown> | Iterable<unknown>) {
	const items: unknown[] = [];
	for await (const item of iterable) {
		items.push(item);
	}
	return items;
}

function normalizeSdkOutput(output: any): unknown[] | AsyncIterable<unknown> | Iterable<unknown> {
	if (output === null || output === undefined) return [];
	if (Array.isArray(output)) return output;
	if (
		typeof output?.[Symbol.asyncIterator] === "function" ||
		typeof output?.[Symbol.iterator] === "function"
	) {
		return output;
	}
	return [output];
}

/** The pipeline text naming SDK stages: `sdk.stage.0 | sdk.stage.1 | ...`. */
export function sdkPipelineText(count: number) {
	return Array.from({ length: count }, (_stage, index) => `sdk.stage.${index}`).join(" | ");
}

/**
 * Commands named `sdk.stage.<n>` that run an SDK instance's stages. A run saves only these
 * names, so the instance that resumes it supplies the stages again.
 */
export function sdkStageRegistry(stages: SdkStage[], runtimeCtx: Record<string, unknown> = {}) {
	const commands = new Map(
		stages.map((stage, index) => [
			`sdk.stage.${index}`,
			{
				meta: { description: `SDK stage ${index}`, argsSchema: { type: "object" } },
				async run({ input, ctx }: { input: AsyncIterable<unknown>; ctx: Record<string, unknown> }) {
					const stageCtx = { ...runtimeCtx, ...ctx };
					if (typeof stage === "function") {
						const isGenerator =
							stage.constructor?.name === "AsyncGeneratorFunction" ||
							stage.constructor?.name === "GeneratorFunction";

						if (isGenerator) {
							return { output: normalizeSdkOutput(stage(input, stageCtx)) };
						}

						const items = await collectItems(input);
						return { output: normalizeSdkOutput(await stage(items, stageCtx)) };
					}

					if (typeof stage?.run === "function") {
						const result: any = await stage.run({ input, ctx: stageCtx });
						return result && "output" in result
							? { ...result, output: normalizeSdkOutput(result.output) }
							: result;
					}

					throw new Error(
						`Invalid stage at index ${index}: must be a function or have run() method`,
					);
				},
			},
		]),
	);
	return {
		get(name: string) {
			return commands.get(name);
		},
		list() {
			return [...commands.keys()];
		},
	};
}

/** SDK input as the items a run saves: iterables are read once, a single value is one item. */
export async function sdkInputItems(input: unknown): Promise<unknown[]> {
	const normalized = normalizeSdkOutput(input);
	if (Array.isArray(normalized)) return normalized;
	return collectItems(normalized);
}

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
	mode = "human",
	input,
	signal = undefined,
	requestInputResume = undefined,
	requestInputEnabled = true,
}: Pick<
	Parameters<typeof runCorePipeline>[0],
	| "pipeline"
	| "registry"
	| "stdin"
	| "stdout"
	| "stderr"
	| "env"
	| "mode"
	| "input"
	| "signal"
	| "requestInputResume"
	| "requestInputEnabled"
>) {
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
