import { parseWorkflowArgsJson } from "../../workflows/values.js";
import { defineCommand } from "../define.js";
import { registryWorkflows } from "../named_workflows.js";
import { streamOf } from "../../io/terminal.js";

export const workflowsRunCommand = defineCommand({
	name: "workflows.run",
	meta: {
		description: "Run a named Lobster workflow",
		argsSchema: {
			type: "object",
			properties: {
				name: { type: "string", description: "Workflow name" },
				"args-json": { type: "string", description: "JSON string of workflow args" },
				_: { type: "array", items: { type: "string" } },
			},
			required: ["name"],
		},
		sideEffects: [],
		// Named workflows must be safe to repeat (see NamedWorkflow.repeatable).
		repeatable: true,
	},
	help() {
		return `workflows.run — run a named Lobster workflow\n\nUsage:\n  workflows.run --name <workflow> [--args-json '{...}']\n\nExample:\n  workflows.run --name github.pr.monitor.notify --args-json '{"repo":"openclaw/openclaw","pr":1152}'\n`;
	},
	async run({ input, args, ctx }) {
		// Drain input.
		for await (const _item of input) {
			// no-op
		}

		const name = args.name ?? args._[0];
		if (!name) throw new Error("workflows.run requires --name");

		const workflow = registryWorkflows(ctx?.registry).find((w) => w.name === name);
		if (!workflow) throw new Error(`Unknown workflow: ${name}`);

		let workflowArgs = {};
		if (args["args-json"] !== undefined) {
			workflowArgs = parseWorkflowArgsJson(String(args["args-json"]), "workflows.run");
		}

		const result = await workflow.run({ args: workflowArgs, ctx });
		return { output: streamOf([result]) };
	},
});
