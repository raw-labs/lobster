import { runPipeline as runCorePipeline } from "../pipeline/runtime.js";

/**
 * SDK Runtime - Executes Lobster pipelines
 *
 * This adapts SDK stages to the core runtime so command-level suspension rules
 * stay identical across CLI, tool mode, and SDK entry points.
 */

/**
 * Collect async iterable to array
 * @param {AsyncIterable} iterable
 * @returns {Promise<any[]>}
 */
async function collectItems(iterable) {
	const items = [];
	for await (const item of iterable) {
		items.push(item);
	}
	return items;
}

function normalizeSdkOutput(output) {
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

/**
 * Run a pipeline of stages
 *
 * @param {Object} options
 * @param {Array<Function|Object>} options.stages - Pipeline stages
 * @param {Object} options.ctx - Execution context
 * @param {any[]} [options.input] - Initial input items
 * @returns {Promise<PipelineResult>}
 */
/** The pipeline text naming SDK stages: `sdk.stage.0 | sdk.stage.1 | ...`. */
export function sdkPipelineText(count: number) {
	return Array.from({ length: count }, (_stage, index) => `sdk.stage.${index}`).join(" | ");
}

/**
 * Commands named `sdk.stage.<n>` that run an SDK instance's stages. A run saves only these
 * names, so the instance that resumes it supplies the stages again.
 */
export function sdkStageRegistry(stages, runtimeCtx: Record<string, unknown> = {}) {
	const commands = new Map(
		stages.map((stage, index) => [
			`sdk.stage.${index}`,
			{
				meta: { description: `SDK stage ${index}`, argsSchema: { type: "object" } },
				async run({ input, ctx }) {
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
						const result = await stage.run({ input, ctx: stageCtx });
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
 * Re-export for compatibility with CLI runtime
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
}) {
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
