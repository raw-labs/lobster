import { tokenizeCommand } from "./command_tokens.js";
import { booleanFlags } from "../commands/define.js";
import type { CommandLookup } from "../commands/types.js";

function splitPipes(input: string) {
	const parts: string[] = [];
	let current = "";
	let quote: string | null = null;

	for (let i = 0; i < input.length; i++) {
		const ch = input[i];

		if (quote) {
			if (ch === "\\") {
				const next = input[i + 1];
				if (next) {
					current += ch + next;
					i++;
					continue;
				}
			}
			current += ch;
			if (ch === quote) {
				quote = null;
			}
			continue;
		}

		if (ch === '"' || ch === "'") {
			quote = ch;
			current += ch;
			continue;
		}

		if (ch === "|") {
			parts.push(current.trim());
			current = "";
			continue;
		}

		current += ch;
	}

	if (quote) throw new Error("Unclosed quote");
	if (current.trim().length > 0) parts.push(current.trim());
	return parts;
}

function parseArgs(tokens: string[], booleanFlags: readonly string[] = []) {
	const args: { _: string[]; [flag: string]: unknown } = { _: [] };

	for (let i = 0; i < tokens.length; i++) {
		const tok = tokens[i];

		if (tok.startsWith("--")) {
			const eq = tok.indexOf("=");
			if (eq !== -1) {
				const key = tok.slice(2, eq);
				const value = tok.slice(eq + 1);
				// `--flag=false` and `--flag false` mean the same for a declared boolean.
				args[key] =
					booleanFlags.includes(key) && (value === "true" || value === "false")
						? value === "true"
						: value;
				continue;
			}

			const key = tok.slice(2);
			const next = tokens[i + 1];
			if (booleanFlags.includes(key)) {
				// A boolean flag never takes the next word, except an explicit true or false.
				if (next === "true" || next === "false") {
					args[key] = next === "true";
					i++;
				} else args[key] = true;
				continue;
			}
			if (next === undefined || next.startsWith("--")) {
				args[key] = true;
				continue;
			}
			args[key] = next;
			i++;
			continue;
		}

		args._.push(tok);
	}

	return args;
}

/**
 * Split a pipeline into stages. With a registry, each command's boolean flags come from its
 * schema, so `exec --json node -e ...` keeps `node` as the program rather than the flag value.
 */
export function parsePipeline(input: string, registry?: CommandLookup) {
	const stages = splitPipes(input);
	if (stages.length === 0) throw new Error("Empty pipeline");

	return stages.map((stage) => {
		const tokens = tokenizeCommand(stage);
		if (tokens.length === 0) throw new Error("Empty command stage");
		const name = tokens[0];
		const args = parseArgs(tokens.slice(1), booleanFlags(registry?.get(name)));
		return { name, args, raw: stage };
	});
}
