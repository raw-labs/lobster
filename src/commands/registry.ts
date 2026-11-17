import type { CommandPack, CommandRegistry, LobsterCommand, NamedWorkflow } from "./types.js";
import { stdlibPack } from "./stdlib/index.js";
import { openclawPack } from "../packs/openclaw/index.js";
import { googlePack } from "../packs/google/index.js";
import { githubPack } from "../packs/github/index.js";

/** Every pack Lobster ships. The default registry enables all of them. */
export const builtinPacks: CommandPack[] = [stdlibPack, openclawPack, googlePack, githubPack];

/** Build a registry from packs. A name or alias claimed twice is an error, not an override. */
export function createRegistry({ packs }: { packs: CommandPack[] }): CommandRegistry {
	const commands = new Map<string, LobsterCommand>();
	const owners = new Map<string, string>();
	const workflows = new Map<string, NamedWorkflow>();
	const claim = (name: string, owner: string) => {
		const previous = owners.get(name);
		if (previous) throw new Error(`"${name}" is defined by both ${previous} and ${owner}`);
		owners.set(name, owner);
	};
	for (const pack of packs) {
		for (const command of pack.commands) {
			for (const name of [command.name, ...(command.meta.aliases ?? [])]) {
				claim(name, `pack ${pack.name}`);
				commands.set(name, command);
			}
		}
		for (const workflow of pack.workflows ?? []) {
			// workflows.run is declared repeatable, which only holds if every workflow is.
			if (workflow.repeatable !== true)
				throw new Error(`Workflow ${workflow.name} in pack ${pack.name} must be repeatable`);
			claim(workflow.name, `pack ${pack.name}`);
			workflows.set(workflow.name, workflow);
		}
	}
	return {
		get: (name) => commands.get(name),
		list: () => [...commands.keys()].sort(),
		workflows: () => [...workflows.values()],
	};
}

export function createDefaultRegistry(): CommandRegistry {
	return createRegistry({ packs: builtinPacks });
}

/** Named workflows of a registry. A registry that does not list any has none. */
export function registryWorkflows(registry: CommandRegistry | undefined): NamedWorkflow[] {
	return registry?.workflows?.() ?? [];
}
