import type { CommandRegistry, NamedWorkflow } from "./types.js";
import { githubPack } from "../packs/github/index.js";

/**
 * Named workflows of a registry. One that cannot list them (a host's `{ get, list }` wrapper,
 * the shape registries had before packs) sees the built-in ones, as `workflows.run` always did;
 * a host that must not run them leaves `workflows.run` out of its registry. The built-in named
 * workflows are the github pack's, the only built-in pack that has any.
 */
export function registryWorkflows(registry: CommandRegistry | undefined): NamedWorkflow[] {
	return registry?.workflows?.() ?? githubPack.workflows ?? [];
}
