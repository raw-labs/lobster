// SDK stages as commands: each stage of a `Lobster` instance becomes the command
// `sdk.stage.<n>`, so an SDK pipeline runs, pauses and resumes on the engine like any other;
// and the items a run starts from.
import type { CommandMeta, LobsterCommand } from "../commands/types.js";
import { isPlainObject } from "../util/objects.js";

/**
 * What a stage object's `run` gets: the stream of items, and the command context (the same
 * one built-in commands get, with `requestInput`, `env`, `signal` and the rest).
 */
export type SdkStageArgs = { input: AsyncIterable<unknown>; ctx: any };

/**
 * A stage object: a command-like `run`, and the `meta` of the command it runs, if any (the
 * SDK's own stages set it, so a resumed run treats them as it treats the command).
 */
export type SdkStageObject = { run(args: SdkStageArgs): unknown; meta?: CommandMeta };

/**
 * A stage of an SDK pipeline: a function of the stage's items (a generator function gets the
 * stream instead), or a stage object.
 */
export type SdkStage = ((items: any, ctx: any) => unknown) | SdkStageObject;

async function collectItems(iterable: AsyncIterable<unknown> | Iterable<unknown>) {
	const items: unknown[] = [];
	for await (const item of iterable) {
		items.push(item);
	}
	return items;
}

/** A stage's output as items: nothing, an array or iterable as it is, or one value. */
function normalizeSdkOutput(
	output: unknown,
): unknown[] | AsyncIterable<unknown> | Iterable<unknown> {
	if (output === null || output === undefined) return [];
	if (Array.isArray(output)) return output;
	const iterable = output as Partial<AsyncIterable<unknown> & Iterable<unknown>>;
	if (
		typeof iterable[Symbol.asyncIterator] === "function" ||
		typeof iterable[Symbol.iterator] === "function"
	)
		return iterable as AsyncIterable<unknown> | Iterable<unknown>;
	return [output];
}

/** The pipeline text naming SDK stages: `sdk.stage.0 | sdk.stage.1 | ...`. */
export function sdkPipelineText(count: number) {
	return Array.from({ length: count }, (_stage, index) => `sdk.stage.${index}`).join(" | ");
}

/** The parts of a stage's meta the engine acts on (recovery and resume safety). */
function engineMeta(meta: CommandMeta | undefined) {
	if (!meta) return {};
	const { sideEffects, repeatable, resumeSafeBeforeInput, resumeSafeAfterInput } = meta;
	return { sideEffects, repeatable, resumeSafeBeforeInput, resumeSafeAfterInput };
}

/**
 * Commands named `sdk.stage.<n>` that run an SDK instance's stages. A run saves only these
 * names, so the instance that resumes it supplies the stages again.
 */
export function sdkStageRegistry(stages: SdkStage[]) {
	const commands = new Map<string, LobsterCommand>(
		stages.map((stage, index) => [
			`sdk.stage.${index}`,
			{
				name: `sdk.stage.${index}`,
				help: () => `SDK stage ${index}`,
				meta: {
					...engineMeta(typeof stage === "function" ? undefined : stage?.meta),
					description: `SDK stage ${index}`,
					argsSchema: { type: "object" },
				},
				async run({ input, ctx }: SdkStageArgs) {
					if (typeof stage === "function") {
						const isGenerator =
							stage.constructor?.name === "AsyncGeneratorFunction" ||
							stage.constructor?.name === "GeneratorFunction";

						if (isGenerator) {
							return { output: normalizeSdkOutput(stage(input, ctx)) };
						}

						const items = await collectItems(input);
						return { output: normalizeSdkOutput(await stage(items, ctx)) };
					}

					// A stage object (`pipe` accepts nothing else).
					const result = await stage.run({ input, ctx });
					return isPlainObject(result) && "output" in result
						? { ...result, output: normalizeSdkOutput(result.output) }
						: result;
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

/**
 * SDK input as the items a run saves: an iterable is read once (a string by character), null
 * or undefined is no items, any other value is one item.
 */
export async function sdkInputItems(input: unknown): Promise<unknown[]> {
	const normalized = normalizeSdkOutput(input);
	if (Array.isArray(normalized)) return normalized;
	return collectItems(normalized);
}
