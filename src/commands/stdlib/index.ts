import type { CommandPack } from "../types.js";
import { approveCommand } from "./approve.js";
import { askCommand } from "./ask.js";
import { dedupeCommand } from "./dedupe.js";
import { diffLastCommand } from "./diff_last.js";
import { execCommand } from "./exec.js";
import { groupByCommand } from "./group_by.js";
import { headCommand } from "./head.js";
import { jsonCommand } from "./json.js";
import { llmInvokeCommand } from "./llm_invoke.js";
import { mapCommand } from "./map.js";
import { pickCommand } from "./pick.js";
import { sortCommand } from "./sort.js";
import { stateGetCommand, stateSetCommand } from "./state.js";
import { tableCommand } from "./table.js";
import { templateCommand } from "./template.js";
import { whereCommand } from "./where.js";
import { commandsListCommand } from "./commands_list.js";
import { workflowsListCommand } from "./workflows_list.js";
import { workflowsRunCommand } from "./workflows_run.js";

/** The generic commands: data shaping, output, processes, state, people, models and discovery. */
export const stdlibPack: CommandPack = {
	name: "stdlib",
	description: "Generic pipeline commands that every Lobster install has.",
	commands: [
		// Data
		whereCommand,
		pickCommand,
		mapCommand,
		sortCommand,
		headCommand,
		dedupeCommand,
		groupByCommand,
		templateCommand,
		diffLastCommand,
		// Output
		jsonCommand,
		tableCommand,
		// Processes and state
		execCommand,
		stateGetCommand,
		stateSetCommand,
		// People
		approveCommand,
		askCommand,
		// Models
		llmInvokeCommand,
		// Discovery
		commandsListCommand,
		workflowsListCommand,
		workflowsRunCommand,
	],
};
