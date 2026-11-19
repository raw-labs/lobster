import { defineCommand } from "../define.js";
import { responseChecker } from "../../util/validation.js";
import { readLineFromStream } from "../../io/read_line.js";
import {
	approvalInputTimeoutMs,
	firstAccepted,
	isInteractive,
	PREVIEW_CHARS,
	textAnswerCandidates,
} from "../../io/terminal.js";
import { streamOf } from "../../io/streams.js";

export const askCommand = defineCommand({
	name: "ask",
	meta: {
		description: "Pause and request structured input from the user",
		argsSchema: {
			type: "object",
			properties: {
				prompt: {
					type: "string",
					description: "Question or instruction to show",
					default: "Input required",
				},
				schema: { type: "string", description: "JSON Schema string for the expected response" },
				"subject-from-stdin": {
					type: "boolean",
					description: "Use stdin content as the subject (preview text)",
				},
				subjectFromStdin: { type: "boolean", description: "Alias for subject-from-stdin" },
				emit: { type: "boolean", description: "Force emit mode" },
				_: { type: "array", items: { type: "string" } },
			},
			required: [],
		},
		sideEffects: [],
		repeatable: true,
		resumeSafeBeforeInput: true,
		resumeSafeAfterInput: true,
	},
	help() {
		return [
			"ask — pause and request structured input from the user",
			"",
			"Usage:",
			'  ... | ask --prompt "Approve, reject, or send feedback:"',
			'  ... | ask --prompt "Feedback?" --schema \'{"type":"object","properties":{"decision":{"type":"string"},"feedback":{"type":"string"}},"required":["decision"]}\'',
			'  ... | ask --subject-from-stdin --prompt "Review this draft:"',
			"",
			"Notes:",
			"  - In tool mode (or non-interactive), emits a needs_input envelope and halts.",
			"  - Use --schema to constrain the response shape (JSON Schema).",
			"  - Use --subject-from-stdin to embed the current pipeline value as preview text.",
		].join("\n");
	},
	async run({ input, args, ctx }) {
		const prompt = typeof args.prompt === "string" ? args.prompt : "Input required";
		const subjectFromStdin = Boolean(args["subject-from-stdin"] ?? args.subjectFromStdin);
		const schemaRaw = typeof args.schema === "string" ? args.schema : null;

		const defaultSchema = {
			type: "object",
			properties: {
				decision: { type: "string", enum: ["approve", "reject", "redraft"] },
				feedback: { type: "string", description: "Feedback for redraft" },
			},
			required: ["decision"],
		};

		let responseSchema = defaultSchema;
		if (schemaRaw) {
			let parsedSchema;
			try {
				parsedSchema = JSON.parse(schemaRaw);
			} catch {
				throw new Error("ask --schema must be valid JSON");
			}
			if (!parsedSchema || typeof parsedSchema !== "object" || Array.isArray(parsedSchema)) {
				throw new Error("ask --schema must decode to a JSON schema object");
			}
			responseSchema = parsedSchema;
		}
		const checkResponse = responseChecker(responseSchema, "ask");

		const forceEmit = Boolean(args.emit);
		const emit = forceEmit || ctx.mode === "tool" || !isInteractive(ctx.stdin);
		const canRequestInput =
			!forceEmit && ctx.mode === "tool" && typeof ctx.requestInput === "function";
		const restoredState = canRequestInput ? ctx.requestInput.getSuspendedState?.() : undefined;
		const restoredAskState =
			restoredState && typeof restoredState === "object" && restoredState.type === "ask"
				? restoredState
				: null;

		const items = [];
		if (!restoredAskState) {
			for await (const item of input) items.push(item);
		}

		let subject = restoredAskState?.subject;
		if (subjectFromStdin && items.length > 0) {
			const preview = items
				.map((item) => (typeof item === "string" ? item : JSON.stringify(item)))
				.join("\n")
				.slice(0, PREVIEW_CHARS);
			subject = { text: preview };
		}

		if (emit) {
			if (canRequestInput) {
				const response = await ctx.requestInput({
					prompt,
					responseSchema,
					...(subject ? { subject } : null),
					suspendedState: { type: "ask", ...(subject ? { subject } : null) },
				});
				return {
					output: streamOf([response]),
				};
			}
			return {
				halt: true,
				output: (async function* () {
					yield {
						type: "input_request",
						prompt,
						responseSchema,
						...(subject ? { subject } : null),
						items,
					};
				})(),
			};
		}

		ctx.stdout.write(`${prompt}\n> `);
		const raw = await readLineFromStream(ctx.stdin, {
			timeoutMs: approvalInputTimeoutMs(ctx.env),
			signal: ctx.signal,
		});
		const text = raw.trim();

		const response = firstAccepted(textAnswerCandidates(text), checkResponse);
		return { output: streamOf([response]) };
	},
});
