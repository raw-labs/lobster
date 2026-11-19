import test from "node:test";
import assert from "node:assert/strict";
import { promises as fsp } from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";

import { renderWorkflowGraph } from "../src/workflows/graph.js";

function runCli(args: string[], env?: Record<string, string | undefined>) {
	const bin = path.join(process.cwd(), "bin", "lobster.js");
	return spawnSync(process.execPath, [bin, ...args], {
		encoding: "utf8",
		env: { ...process.env, ...env },
	});
}

test("workflow graph renderer outputs mermaid nodes and labeled edges", () => {
	const workflow = {
		args: { city: { default: "Phoenix" } },
		steps: [
			{ id: "fetch", run: "weather --json ${city}" },
			{ id: "confirm", approval: "Proceed?", stdin: "$fetch.json" },
			{
				id: "advice",
				pipeline: 'llm.invoke --prompt "Summarize this weather"',
				stdin: "$fetch.stdout",
				when: "$confirm.approved && $fetch.json.temp > 70",
			},
		],
	};

	const output = renderWorkflowGraph({ workflow, format: "mermaid", args: { city: "Seattle" } });
	assert.match(output, /^flowchart TD/m);
	assert.match(output, /fetch\["fetch\\nrun: weather --json Seattle"\]/);
	assert.match(output, /confirm\{"confirm\\napproval gate"\}/);
	assert.match(
		output,
		/advice\["advice\\npipeline: llm\.invoke --prompt &quot;Summarize this weather&quot;"\]/,
	);
	assert.match(output, /s_fetch -->\|stdin\| s_confirm/);
	assert.match(output, /s_fetch -->\|stdin\| s_advice/);
	assert.match(
		output,
		/s_confirm -->\|when: \$confirm\.approved &amp;&amp; \$fetch\.json\.temp &gt; 70\| s_advice/,
	);
});

test("a step named like a Mermaid keyword still makes a graph Mermaid reads", () => {
	const output = renderWorkflowGraph({
		workflow: {
			steps: [
				{ id: "end", run: "echo a" },
				{ id: "style", run: "echo b", stdin: "$end.stdout" },
			],
		},
		format: "mermaid",
	});
	assert.match(output, /^  s_end\["end/m);
	assert.match(output, /s_end -->\|stdin\| s_style/);
	assert.doesNotMatch(output, /^  (end|style)\[/m);
});

test("workflow graph renderer contains Mermaid metacharacters in labels", () => {
	const output = renderWorkflowGraph({
		workflow: {
			steps: [{ id: "unsafe", run: 'echo \\"quoted" | next <tag>' }],
		},
		format: "mermaid",
	});

	assert.match(output, /echo \\&quot;quoted&quot; &#124; next &lt;tag&gt;/);
	assert.doesNotMatch(output, /"quoted"/);
});

test("workflow graph renderer outputs dot with approval shape", () => {
	const workflow = {
		steps: [
			{ id: "fetch", run: "echo hello" },
			{ id: "confirm", approval: "Proceed?", stdin: "$fetch.stdout" },
		],
	};

	const output = renderWorkflowGraph({ workflow, format: "dot" });
	assert.match(output, /^digraph workflow \{/m);
	assert.match(output, /"confirm" \[shape=diamond,label="confirm\\\\napproval gate"\];/);
	assert.match(output, /"fetch" -> "confirm" \[label="stdin"\];/);
});

test("cli graph defaults to mermaid and resolves --args-json values", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-graph-cli-"));
	const filePath = path.join(tmpDir, "workflow.lobster");
	const workflow = [
		"name: weather-check",
		"args:",
		"  city:",
		"    default: Phoenix",
		"steps:",
		"  - id: fetch",
		"    run: weather --json ${city}",
		"  - id: confirm",
		"    approval: Proceed?",
		"    stdin: $fetch.json",
	].join("\n");
	await fsp.writeFile(filePath, workflow, "utf8");

	const result = runCli(["graph", "--file", filePath, "--args-json", '{"city":"Seattle"}']);
	assert.equal(result.status, 0, `stderr=${result.stderr}`);
	assert.match(result.stdout, /^flowchart TD/m);
	assert.match(result.stdout, /run: weather --json Seattle/);
	assert.match(result.stdout, /confirm\{"confirm\\napproval gate"\}/);
});

test("cli graph refuses an arg the workflow does not declare, as run does", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-graph-args-"));
	const filePath = path.join(tmpDir, "workflow.lobster");
	await fsp.writeFile(
		filePath,
		[
			"args:",
			"  location:",
			"    default: here",
			"steps:",
			"  - id: a",
			"    run: echo ${location}",
		].join("\n"),
		"utf8",
	);
	const result = runCli(["graph", "--file", filePath, "--args-json", '{"locaton":"x"}']);
	assert.equal(result.status, 1);
	assert.match(result.stderr, /takes no arg "locaton" \(did you mean "location"\?\)/);
});

test("cli graph supports --format dot", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-graph-dot-"));
	const filePath = path.join(tmpDir, "workflow.lobster");
	const workflow = [
		"steps:",
		"  - id: fetch",
		"    run: echo hello",
		"  - id: gate",
		"    approval: Proceed?",
		"    stdin: $fetch.stdout",
	].join("\n");
	await fsp.writeFile(filePath, workflow, "utf8");

	const result = runCli(["graph", "--file", filePath, "--format", "dot"]);
	assert.equal(result.status, 0, `stderr=${result.stderr}`);
	assert.match(result.stdout, /^digraph workflow \{/m);
	assert.match(result.stdout, /"gate" \[shape=diamond/);
});

test("cli graph rejects unsupported formats", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-graph-bad-format-"));
	const filePath = path.join(tmpDir, "workflow.lobster");
	await fsp.writeFile(filePath, "steps:\n  - id: s\n    run: echo ok\n", "utf8");

	const result = runCli(["graph", "--file", filePath, "--format", "svg"]);
	assert.equal(result.status, 2);
	assert.match(result.stderr, /graph --format must be one of: mermaid, dot, ascii/);
});

test("an edge from a step inside a repeat or a parallel step starts at that step's node", () => {
	const workflow = {
		steps: [
			{
				id: "poll",
				repeat: { until: "$check.json.done", max: 3 },
				steps: [{ id: "check", run: "echo '{\"done\":true}'" }],
			},
			{ id: "fan", parallel: { branches: [{ id: "a", run: "echo a" }] } },
			{ id: "report", run: "cat", stdin: "$check.json", when: "$a.stdout == a" },
		],
	};
	const output = renderWorkflowGraph({ workflow: workflow as any, format: "dot" });
	assert.match(output, /"poll" -> "report" \[label="stdin"\]/);
	assert.match(output, /"fan" -> "report" \[label="when: \$a\.stdout == a"\]/);
	assert.doesNotMatch(output, /"poll" -> "poll"/, "a repeat reading its own step draws no loop");
});

test("what a compound step's inside reads is drawn as edges into that step", () => {
	const workflow = {
		steps: [
			{ id: "fetch", run: "echo '[1]'" },
			{ id: "par", parallel: { branches: [{ id: "b", run: "cat", stdin: "$fetch.stdout" }] } },
			{
				id: "poll",
				repeat: { until: "$check.json.done", max: 2 },
				steps: [{ id: "check", run: "cat", stdin: "$fetch.json" }],
			},
			{
				id: "each",
				for_each: "[1]",
				steps: [
					{ id: "use", run: "cat", stdin: "$item.json" },
					{ id: "again", run: "cat", stdin: "$use.stdout", when: "$fetch.json" },
				],
			},
		],
	};
	const output = renderWorkflowGraph({ workflow: workflow as any, format: "dot" });
	assert.match(output, /"fetch" -> "par" \[label="stdin"\]/);
	assert.match(output, /"fetch" -> "poll" \[label="stdin"\]/);
	assert.match(output, /"fetch" -> "each" \[label="when: \$fetch\.json"\]/);
	assert.doesNotMatch(output, /"(use|item|check)" ->/, "names local to a node draw no edge");
});
