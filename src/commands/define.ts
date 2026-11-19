import { closestMatch } from "../util/suggest.js";
import type { CommandArgsSchema, CommandLike, DefinedCommand } from "./types.js";

const NAME = /^[A-Za-z][A-Za-z0-9_]*(?:\.[A-Za-z][A-Za-z0-9_]*)*$/;

/**
 * Declare a command. The type requires a description, a flag schema, the side-effect labels
 * and whether the command may be repeated; the checks below catch what types cannot.
 */
export function defineCommand<T extends DefinedCommand>(command: T): T {
	const where = `Command ${command.name}`;
	for (const name of [command.name, ...(command.meta.aliases ?? [])])
		if (!NAME.test(name)) throw new Error(`${where}: invalid name "${name}"`);
	if (!command.meta.description.trim()) throw new Error(`${where}: description is required`);
	const schema = command.meta.argsSchema;
	if (schema?.type !== "object" || !schema.properties || typeof schema.properties !== "object")
		throw new Error(`${where}: argsSchema must be an object schema with properties`);
	for (const key of schema.required ?? [])
		if (!(key in schema.properties))
			throw new Error(`${where}: required flag "${key}" is not declared`);
	for (const [key, spec] of Object.entries(schema.properties)) {
		const target = spec.aliasOf;
		if (target !== undefined && (typeof target !== "string" || !(target in schema.properties)))
			throw new Error(`${where}: flag "${key}" is an alias of undeclared flag "${target}"`);
	}
	return command;
}

/** Flags declared as booleans, which never take the next word as their value (except true/false). */
export function booleanFlags(command: CommandLike | undefined): string[] {
	const properties = (command?.meta?.argsSchema as CommandArgsSchema | undefined)?.properties;
	if (!properties) return [];
	return Object.entries(properties)
		.filter(([, spec]) => spec.type === "boolean")
		.map(([name]) => name);
}

/**
 * Reject flags a command does not declare, so a misspelled flag fails instead of being ignored;
 * a flag given with its alias (`aliasOf`); a string or number flag given no value (a bare
 * `--flag` reads as true); a number flag without a number; a value outside the flag's choices. Checked before any stage runs. Commands without a schema (custom host
 * commands) are not checked.
 */
export function assertKnownFlags(
	command: CommandLike,
	invokedAs: string,
	args: Record<string, unknown>,
) {
	const properties = (command.meta?.argsSchema as CommandArgsSchema | undefined)?.properties;
	if (!properties) return;
	const allowed = Object.keys(properties).filter((key) => key !== "_");
	// The spelling given for each flag that has aliases, by the flag it stands for.
	const spelled = new Map<string, string>();
	for (const [key, value] of Object.entries(args)) {
		if (key === "_") continue;
		if (!allowed.includes(key)) {
			const near = closestMatch(key, allowed);
			throw new Error(
				`${invokedAs} has no flag --${key}${near ? ` (did you mean --${near}?)` : ""}. Flags: ${
					allowed.map((flag) => `--${flag}`).join(", ") || "none"
				}`,
			);
		}
		if (value === undefined) continue;
		const spec = properties[key];
		const flag = spec.aliasOf ?? key;
		const other = spelled.get(flag);
		if (other && other !== key)
			throw new Error(`${invokedAs} takes --${other} or --${key}, not both`);
		spelled.set(flag, key);
		const choices = spec.enum;
		if ((spec.type === "string" || spec.type === "number") && value === true)
			throw new Error(
				`${invokedAs} --${key} takes ${Array.isArray(choices) ? `one of ${choices.join(", ")}` : "a value"}`,
			);
		if (spec.type === "number" && !isNumber(value))
			throw new Error(`${invokedAs} --${key} takes a number (got ${JSON.stringify(value)})`);
		if (Array.isArray(choices) && typeof value === "string" && !choices.includes(value))
			throw new Error(
				`${invokedAs} --${key} must be one of ${choices.join(", ")} (got "${value}")`,
			);
	}
}

/** A number, or text that reads as one (flags arrive as text). */
function isNumber(value: unknown) {
	if (typeof value === "number") return Number.isFinite(value);
	return typeof value === "string" && value.trim() !== "" && Number.isFinite(Number(value));
}
