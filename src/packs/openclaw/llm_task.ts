import { createLlmInvokeCommand } from "../../commands/stdlib/llm_invoke.js";

/**
 * The older name for `llm.invoke`, fixed to the OpenClaw adapter. It is a separate variant,
 * not an alias: its cache namespace, run-state type and item kind are its own, so answers and
 * paused runs saved under this name keep resuming.
 */
export const llmTaskInvokeCommand = createLlmInvokeCommand({
	name: "llm_task.invoke",
	itemKind: "llm_task.invoke",
	stateType: "llm_task.invoke",
	cacheNamespace: "llm_task.invoke",
	defaultProvider: "openclaw",
	description: "Backward-compatible alias for llm.invoke using the OpenClaw adapter",
	helpTitle: "llm_task.invoke — backward-compatible alias for llm.invoke using OpenClaw",
	helpConfig: [
		"Requires OPENCLAW_URL (or CLAWD_URL) and optionally OPENCLAW_TOKEN.",
		"Use llm.invoke for new workflows and non-OpenClaw adapters.",
	],
	helpExamples: [
		"llm_task.invoke --prompt 'Write summary'",
		"llm_task.invoke --model claude-3-sonnet --prompt 'Write summary'",
		"cat artifacts.json | llm_task.invoke --prompt 'Score each item'",
	],
	sourceForProvider() {
		return "clawd";
	},
	legacyEnvCompat: true,
});
