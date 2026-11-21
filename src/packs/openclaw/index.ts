import type { CommandPack } from "../../commands/types.js";
import { openclawAgentCommand } from "./agent.js";
import { openclawInvokeCommand } from "./invoke.js";
import { llmTaskInvokeCommand } from "./llm_task.js";

/** Calls into an OpenClaw gateway: tools, agent turns and the older LLM command name. */
export const openclawPack: CommandPack = {
	name: "openclaw",
	description: "Call OpenClaw tools and agents.",
	commands: [openclawInvokeCommand, openclawAgentCommand, llmTaskInvokeCommand],
};
