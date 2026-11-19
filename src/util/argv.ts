/** A command line Lobster does not understand: reported with exit code 2, like other parse errors. */
export class UsageError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "UsageError";
	}
}

export type FlagSpec = {
	/** A string, a whole number, or a switch (`--x`, `--x=true|false`, `--x true|false`). */
	type: "string" | "integer" | "boolean";
	/** For integers: the smallest value accepted. */
	min?: number;
	/** Other spellings of the flag (without dashes). */
	aliases?: string[];
};

export type ParsedArgs = {
	flags: Record<string, string | number | boolean>;
	positional: string[];
};

/**
 * Parse a command line against the flags a command declares. `--flag value` and
 * `--flag=value` both work; `--` ends the flags. A missing value, an integer that is not
 * one, or an unknown flag is a UsageError naming `usage`, unless `passthrough` keeps unknown
 * flags as positional words (for `lobster run`, whose pipeline text carries its commands'
 * own flags).
 */
export function parseArgv(
	argv: string[],
	spec: Record<string, FlagSpec>,
	options: { usage: string; passthrough?: boolean },
): ParsedArgs {
	const names = new Map<string, string>();
	for (const [name, flag] of Object.entries(spec)) {
		names.set(name, name);
		for (const alias of flag.aliases ?? []) names.set(alias, name);
	}
	const fail = (message: string): never => {
		throw new UsageError(`${message}. Usage: ${options.usage}`);
	};
	const flags: ParsedArgs["flags"] = {};
	const positional: string[] = [];
	for (let i = 0; i < argv.length; i++) {
		const token = argv[i];
		if (token === "--") {
			positional.push(...argv.slice(i + 1));
			break;
		}
		const match = /^--([^=]+)(?:=(.*))?$/s.exec(token);
		const name = match && names.get(match[1]);
		if (!match || !name) {
			if (match && !options.passthrough)
				fail(`${commandOf(options.usage)} has no flag --${match[1]}`);
			positional.push(token);
			continue;
		}
		const flag = spec[name];
		let value = match[2];
		if (flag.type === "boolean") {
			if (value === undefined && /^(true|false)$/i.test(argv[i + 1] ?? "")) value = argv[++i];
			if (value === undefined) flags[name] = true;
			else if (/^(true|false)$/i.test(value)) flags[name] = value.toLowerCase() === "true";
			else fail(`--${match[1]} takes true or false`);
			continue;
		}
		if (value === undefined) {
			const next = argv[i + 1];
			if (next === undefined || next.startsWith("--")) fail(`--${match[1]} needs a value`);
			value = next;
			i++;
		}
		if (flag.type === "integer") {
			const number = Number(value);
			if (
				!/^-?\d+$/.test(value!) ||
				!Number.isSafeInteger(number) ||
				number < (flag.min ?? -Infinity)
			)
				fail(
					`--${match[1]} must be a whole number${flag.min !== undefined ? ` of at least ${flag.min}` : ""}`,
				);
			flags[name] = number;
		} else flags[name] = value!;
	}
	return { flags, positional };
}

/** The command a usage line is for: its words before the first placeholder or flag. */
function commandOf(usage: string) {
	const words: string[] = [];
	for (const word of usage.replace(/^lobster /, "").split(" ")) {
		if (/^[<[-]/.test(word) || word.includes("|")) break;
		words.push(word);
	}
	return words.join(" ") || "this command";
}
