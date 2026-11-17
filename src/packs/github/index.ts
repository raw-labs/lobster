import type { CommandPack } from "../../commands/types.js";
import { runGithubPrMonitorNotifyWorkflow, runGithubPrMonitorWorkflow } from "./workflows.js";

const target = {
	repo: { type: "string", description: "owner/repo (e.g. openclaw/openclaw)" },
	pr: { type: "number", description: "Pull request number" },
	key: { type: "string", description: "Optional state key override." },
};

/** GitHub pull request monitoring through the `gh` CLI, as named workflows and SDK recipes. */
export const githubPack: CommandPack = {
	name: "github",
	description: "Watch GitHub pull requests through the gh CLI.",
	commands: [],
	workflows: [
		{
			name: "github.pr.monitor",
			description: "Fetch PR state via gh, diff against last run, emit only on change.",
			argsSchema: {
				type: "object",
				properties: {
					...target,
					changesOnly: {
						type: "boolean",
						description: "If true, suppress output when unchanged.",
					},
					summaryOnly: {
						type: "boolean",
						description: "If true, return only a compact change summary (smaller output).",
					},
				},
				required: ["repo", "pr"],
			},
			examples: [
				{
					args: { repo: "openclaw/openclaw", pr: 1152 },
					description: "Monitor a PR and report when it changes.",
				},
			],
			sideEffects: [],
			repeatable: true,
			run: runGithubPrMonitorWorkflow,
		},
		{
			name: "github.pr.monitor.notify",
			description: "Monitor a PR and emit a single human-friendly message when it changes.",
			argsSchema: { type: "object", properties: target, required: ["repo", "pr"] },
			examples: [
				{
					args: { repo: "openclaw/openclaw", pr: 1152 },
					description: 'Emit "PR updated" message only when changed.',
				},
			],
			sideEffects: [],
			repeatable: true,
			run: runGithubPrMonitorNotifyWorkflow,
		},
	],
};
