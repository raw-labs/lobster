import { defineCommand } from "../define.js";
import { readLineFromStream } from "../../io/read_line.js";
import { approvalInputTimeoutMs, isInteractive, yesOrNoPrompt } from "../../io/terminal.js";
import { streamOf } from "../../io/streams.js";

export const approveCommand = defineCommand({
	name: "approve",
	meta: {
		description: "Require confirmation to continue",
		argsSchema: {
			type: "object",
			properties: {
				prompt: { type: "string", description: "Approval prompt text", default: "Approve?" },
				emit: { type: "boolean", description: "Force emit approval request + halt" },
				"preview-from-stdin": {
					type: "boolean",
					description: "Include a preview of the input items in the request",
				},
				previewFromStdin: {
					type: "boolean",
					aliasOf: "preview-from-stdin",
					description: "Alias for preview-from-stdin",
				},
				limit: { type: "number", description: "Items in the preview (default 5)" },
				"preview-limit": { type: "number", aliasOf: "limit", description: "Alias for limit" },
				previewLimit: { type: "number", aliasOf: "limit", description: "Alias for limit" },
				_: { type: "array", items: { type: "string" } },
			},
			required: [],
		},
		sideEffects: [],
		repeatable: true,
		resumeSafeBeforeInput: true,
	},
	help() {
		return `approve — require confirmation to continue\n\nUsage:\n  ... | approve --prompt "Send these emails?"\n  ... | approve --emit --prompt "Send these emails?"\n  ... | approve --emit --preview-from-stdin --limit 5 --prompt "Proceed?"\n\nModes:\n  - Interactive (default): prompts on TTY and passes items through if approved.\n  - Emit (--emit): returns an approval request object and stops the pipeline.\n\nNotes:\n  - In tool mode (or non-interactive), this emits an approval request and halts.\n`;
	},
	async run({ input, args, ctx }) {
		const prompt = args.prompt ?? "Approve?";
		// One spelling of each at most: the others are refused with the stage's flags.
		const previewFromStdin = Boolean(args["preview-from-stdin"] ?? args.previewFromStdin);
		const previewLimitRaw = args.limit ?? args["preview-limit"] ?? args.previewLimit;
		const previewLimit = Number.isFinite(Number(previewLimitRaw)) ? Number(previewLimitRaw) : 5;

		const items = [];
		for await (const item of input) items.push(item);

		const emit = Boolean(args.emit) || ctx.mode === "tool" || !isInteractive(ctx.stdin);

		if (emit) {
			const preview = previewFromStdin
				? buildPreview(items.slice(0, Math.max(0, previewLimit)))
				: undefined;
			return {
				halt: true,
				output: (async function* () {
					yield {
						type: "approval_request",
						prompt,
						items,
						...(preview ? { preview } : null),
					};
				})(),
			};
		}

		ctx.stdout.write(`${prompt} [y/N] `);
		const answer = await readLineFromStream(ctx.stdin, {
			timeoutMs: approvalInputTimeoutMs(ctx.env),
			signal: ctx.signal,
		});

		const text = answer.trim();
		const approved = yesOrNoPrompt(text);
		if (approved === undefined) throw new Error(`Answer yes or no (got ${JSON.stringify(text)})`);
		if (!approved) throw new Error("Not approved");

		return { output: streamOf(items) };
	},
});

function buildPreview(items: unknown[]) {
	if (!items.length) return "";
	if (items.every((item) => typeof item === "string")) {
		return items.join("\n");
	}
	return JSON.stringify(items, null, 2);
}
