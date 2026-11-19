import { createDefaultRegistry } from "../commands/registry.js";
import { parsePipeline as parseWith } from "../pipeline/parser.js";
import type { CommandLookup, CommandRegistry } from "../commands/types.js";

let defaultRegistry: CommandRegistry | undefined;

/**
 * Parse a pipeline the way `runPipeline` will run it. Without a registry the built-in
 * commands are used, so their boolean flags (`exec --json`) never take the next word.
 */
export function parsePipeline(input: string, registry?: CommandLookup) {
	return parseWith(input, registry ?? (defaultRegistry ??= createDefaultRegistry()));
}
