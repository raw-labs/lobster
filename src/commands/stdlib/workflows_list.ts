import { defineCommand } from "../define.js";
import { registryWorkflows } from "../named_workflows.js";
import { streamOf } from "../../io/terminal.js";

export const workflowsListCommand = defineCommand({
	name: "workflows.list",
	meta: {
		description: "List available Lobster workflows",
		argsSchema: { type: "object", properties: {}, required: [] },
		sideEffects: [],
		repeatable: true,
	},
	help() {
		return `workflows.list — list available Lobster workflows\n\nUsage:\n  workflows.list\n\nNotes:\n  - Intended for OpenClaw to discover workflows dynamically.\n`;
	},
	async run({ input, ctx }) {
		// Drain input.
		for await (const _item of input) {
			// no-op
		}

		const workflows = registryWorkflows(ctx?.registry).map((w) => ({
			name: w.name,
			description: w.description,
			argsSchema: w.argsSchema,
			examples: w.examples,
			sideEffects: w.sideEffects,
		}));
		return { output: streamOf(workflows) };
	},
});
