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
 * Reject flags a command does not declare, so a misspelled flag fails instead of being ignored.
 * Commands without a schema (custom host commands) are not checked.
 */
export function assertKnownFlags(
	command: CommandLike,
	invokedAs: string,
	args: Record<string, unknown>,
) {
	const properties = (command.meta?.argsSchema as CommandArgsSchema | undefined)?.properties;
	if (!properties) return;
	const allowed = Object.keys(properties).filter((key) => key !== "_");
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
		// A value outside the flag's declared choices is refused before any stage runs.
		const choices = properties[key].enum;
		if (Array.isArray(choices) && typeof value === "string" && !choices.includes(value))
			throw new Error(
				`${invokedAs} --${key} must be one of ${choices.join(", ")} (got "${value}")`,
			);
	}
}
