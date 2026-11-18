import test from "node:test";
import assert from "node:assert/strict";
import { promises as fsp } from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";

import { createDefaultRegistry } from "../src/commands/registry.js";
import { loadWorkflowFile, runWorkflowFile } from "../src/workflows/file.js";

async function runWorkflow(workflow: unknown) {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-foreach-"));
	const stateDir = path.join(tmpDir, "state");
	const filePath = path.join(tmpDir, "workflow.lobster");
	await fsp.writeFile(filePath, JSON.stringify(workflow, null, 2), "utf8");

	return runWorkflowFile({
		filePath,
		ctx: {
			stdin: process.stdin,
			stdout: process.stdout,
			stderr: process.stderr,
			env: { ...process.env, LOBSTER_STATE_DIR: stateDir },
			mode: "tool",
			registry: createDefaultRegistry(),
		},
	});
}

test("for_each iterates items and collects per-iteration results", async () => {
	const result = await runWorkflow({
		steps: [
			{
				id: "data",
				command: 'node -e "process.stdout.write(JSON.stringify([{name:\\"a\\"},{name:\\"b\\"}]))"',
			},
			{
				id: "loop",
				for_each: "$data.json",
				steps: [
					{
						id: "transform",
						command:
							'node -e "process.stdout.write(JSON.stringify({upper: process.env.NAME.toUpperCase()}))"',
						env: { NAME: "$item.json.name" },
					},
				],
			},
		],
	});
	assert.equal(result.status, "ok");
	const output = result.output as any[];
	assert.equal(output.length, 2);
	assert.equal(output[0].index, 0);
	assert.equal(output[1].index, 1);
	assert.equal(output[0].transform.upper, "A");
	assert.equal(output[1].transform.upper, "B");
});

test("for_each supports custom item_var and index_var", async () => {
	const result = await runWorkflow({
		steps: [
			{ id: "vals", command: 'node -e "process.stdout.write(JSON.stringify([10,20]))"' },
			{
				id: "loop",
				for_each: "$vals.json",
				item_var: "num",
				index_var: "idx",
				steps: [
					{
						id: "emit",
						command:
							'node -e "process.stdout.write(JSON.stringify({num:$num.json,idx:$idx.json}))"',
					},
				],
			},
		],
	});
	assert.equal(result.status, "ok");
	assert.deepEqual(result.output, [
		{ num: 10, idx: 0, emit: { num: 10, idx: 0 } },
		{ num: 20, idx: 1, emit: { num: 20, idx: 1 } },
	]);
});

test("a pipeline that asks inside a for_each pauses the run for that item", async () => {
	const loop = (extra: object = {}) => ({
		steps: [
			{ id: "vals", command: 'node -e "process.stdout.write(JSON.stringify([1]))"' },
			{
				id: "loop",
				for_each: "$vals.json",
				...extra,
				steps: [{ id: "review", pipeline: "ask --prompt 'Review?'" }],
			},
		],
	});
	const paused = await runWorkflow(loop());
	assert.equal(paused.status, "needs_input");
	// A retry would run the loop again and ask again, so a retried loop cannot ask.
	await assert.rejects(
		() => runWorkflow(loop({ retry: { max: 2 } })),
		/halted for approval inside pipeline|requestInput is not supported in this pipeline context/,
	);
});

test("for_each throws when source is not an array", async () => {
	await assert.rejects(
		() =>
			runWorkflow({
				steps: [
					{ id: "data", command: 'node -e "process.stdout.write(JSON.stringify({x:1}))"' },
					{ id: "loop", for_each: "$data.json", steps: [{ id: "x", command: "echo hi" }] },
				],
			}),
		/for_each: expected array/,
	);
});

test("for_each validation rejects empty sub-step list", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-foreach-"));
	const filePath = path.join(tmpDir, "bad.lobster");
	await fsp.writeFile(
		filePath,
		JSON.stringify({
			steps: [{ id: "loop", for_each: "$x.json", steps: [] }],
		}),
		"utf8",
	);
	await assert.rejects(
		() => loadWorkflowFile(filePath),
		/for_each requires a non-empty steps array/,
	);
});

test("for_each validation rejects run/command/pipeline/workflow/parallel on loop step", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-foreach-"));
	const filePath = path.join(tmpDir, "bad.lobster");
	await fsp.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{
					id: "loop",
					for_each: "$x.json",
					run: "echo no",
					steps: [{ id: "s", command: "echo hi" }],
				},
			],
		}),
		"utf8",
	);
	await assert.rejects(
		() => loadWorkflowFile(filePath),
		/for_each cannot also define run, command, pipeline, workflow, parallel, repeat, or switch/,
	);
});

test("for_each validation rejects a retry around an approval in its sub-steps", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-foreach-"));
	const filePath = path.join(tmpDir, "bad.lobster");
	await fsp.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{
					id: "loop",
					for_each: "$x.json",
					retry: { max: 2 },
					steps: [{ id: "s", command: "echo hi", approval: true }],
				},
			],
		}),
		"utf8",
	);
	await assert.rejects(
		() => loadWorkflowFile(filePath),
		/for_each with retry cannot contain approval, input, sleep or wait steps: a retry would run the loop again and ask again/,
	);
});

test("for_each validation rejects duplicate sub-step ids", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-foreach-"));
	const filePath = path.join(tmpDir, "bad.lobster");
	await fsp.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{
					id: "loop",
					for_each: "$x.json",
					steps: [
						{ id: "dup", command: "echo a" },
						{ id: "dup", command: "echo b" },
					],
				},
			],
		}),
		"utf8",
	);
	await assert.rejects(() => loadWorkflowFile(filePath), /duplicate for_each sub-step id/);
});

test("for_each validation rejects item_var/index_var collisions", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-foreach-"));
	const filePath = path.join(tmpDir, "bad.lobster");
	await fsp.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{
					id: "loop",
					for_each: "$x.json",
					item_var: "x",
					index_var: "x",
					steps: [{ id: "s", command: "echo hi" }],
				},
			],
		}),
		"utf8",
	);
	await assert.rejects(
		() => loadWorkflowFile(filePath),
		/item_var and index_var cannot be the same/,
	);
});

test("for_each pause_ms and batch_size are accepted and executable", async () => {
	const result = await runWorkflow({
		steps: [
			{ id: "vals", command: 'node -e "process.stdout.write(JSON.stringify([1,2,3]))"' },
			{
				id: "loop",
				for_each: "$vals.json",
				batch_size: 2,
				pause_ms: 10,
				steps: [
					{ id: "emit", command: 'node -e "process.stdout.write(JSON.stringify({v:$item.json}))"' },
				],
			},
		],
	});
	assert.equal(result.status, "ok");
	assert.equal((result.output as any[]).length, 3);
});

test("for_each dry-run renders loop structure", async () => {
	const workflow = {
		steps: [
			{ id: "vals", command: 'node -e "process.stdout.write(JSON.stringify([1,2]))"' },
			{
				id: "loop",
				for_each: "$vals.json",
				batch_size: 2,
				steps: [{ id: "emit", command: "echo hi" }],
			},
		],
	};

	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-foreach-"));
	const stateDir = path.join(tmpDir, "state");
	const filePath = path.join(tmpDir, "workflow.lobster");
	await fsp.writeFile(filePath, JSON.stringify(workflow, null, 2), "utf8");

	const stderr = new PassThrough();
	let out = "";
	stderr.on("data", (d: Buffer | string) => {
		out += String(d);
	});

	await runWorkflowFile({
		filePath,
		ctx: {
			stdin: process.stdin,
			stdout: process.stdout,
			stderr,
			env: { ...process.env, LOBSTER_STATE_DIR: stateDir },
			mode: "tool",
			dryRun: true,
			registry: createDefaultRegistry(),
		},
	});

	assert.match(out, /\[for_each\]/);
	assert.match(out, /sub-steps: 1/);
	assert.match(out, /batch_size: 2/);
});

async function writeNodeCommand(tmpDir: string, name: string, source: string) {
	const filePath = path.join(tmpDir, name);
	await fsp.writeFile(filePath, source, "utf8");
	return `node ${filePath.split(path.sep).join("/")}`;
}

async function runWorkflowWithIo(workflow: unknown) {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-foreach-"));
	const stateDir = path.join(tmpDir, "state");
	const filePath = path.join(tmpDir, "workflow.lobster");
	await fsp.writeFile(filePath, JSON.stringify(workflow, null, 2), "utf8");

	const stderr = new PassThrough();
	const chunks: string[] = [];
	stderr.on("data", (chunk: Buffer | string) => chunks.push(String(chunk)));

	const result = await runWorkflowFile({
		filePath,
		ctx: {
			stdin: process.stdin,
			stdout: process.stdout,
			stderr,
			env: { ...process.env, LOBSTER_STATE_DIR: stateDir },
			mode: "tool",
			registry: createDefaultRegistry(),
		},
	});
	return { result, stderrOutput: chunks.join("") };
}

for (const pipeline of [false, true]) {
	test(`for_each timeout_ms aborts a hanging ${pipeline ? "pipeline" : "shell"} child`, async (t) => {
		const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-foreach-timeout-"));
		t.after(() => fsp.rm(tmpDir, { recursive: true, force: true }));
		const dataCmd = await writeNodeCommand(
			tmpDir,
			"data.js",
			"process.stdout.write(JSON.stringify([1]));\n",
		);
		const hangCmd = await writeNodeCommand(tmpDir, "hang.js", "setTimeout(() => {}, 5000);\n");

		await assert.rejects(
			() =>
				runWorkflow({
					steps: [
						{ id: "data", command: dataCmd },
						{
							id: "loop",
							for_each: "$data.json",
							timeout_ms: 200,
							steps: [
								pipeline
									? { id: "slow", pipeline: `exec ${hangCmd}` }
									: { id: "slow", command: hangCmd },
							],
						},
					],
				}),
			/timed out after 200ms/,
		);
	});
}

test("for_each timeout_ms with on_error continue records error and continues", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-foreach-timeout-continue-"));
	const dataCmd = await writeNodeCommand(
		tmpDir,
		"data.js",
		"process.stdout.write(JSON.stringify([1]));\n",
	);
	const hangCmd = await writeNodeCommand(tmpDir, "hang.js", "setTimeout(() => {}, 5000);\n");
	const checkCmd = await writeNodeCommand(
		tmpDir,
		"check.js",
		"process.stdout.write(JSON.stringify({saw: process.env.SAW}));\n",
	);

	const result = await runWorkflow({
		steps: [
			{ id: "data", command: dataCmd },
			{
				id: "loop",
				for_each: "$data.json",
				timeout_ms: 200,
				on_error: "continue",
				steps: [{ id: "slow", command: hangCmd }],
			},
			{
				id: "check",
				command: checkCmd,
				env: { SAW: "$loop.error" },
			},
		],
	});
	assert.equal(result.status, "ok");
	assert.deepEqual(result.output, [{ saw: "true" }]);
});

test("for_each on_error continue records a child command failure and continues", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-foreach-onerror-"));
	const dataCmd = await writeNodeCommand(
		tmpDir,
		"data.js",
		"process.stdout.write(JSON.stringify([1]));\n",
	);
	const failCmd = await writeNodeCommand(tmpDir, "fail.js", "process.exit(1);\n");
	const checkCmd = await writeNodeCommand(
		tmpDir,
		"check.js",
		"process.stdout.write(JSON.stringify({saw: process.env.SAW}));\n",
	);

	const result = await runWorkflow({
		steps: [
			{ id: "data", command: dataCmd },
			{
				id: "loop",
				for_each: "$data.json",
				on_error: "continue",
				steps: [{ id: "fail", command: failCmd }],
			},
			{
				id: "check",
				command: checkCmd,
				env: { SAW: "$loop.error" },
			},
		],
	});
	assert.equal(result.status, "ok");
	assert.deepEqual(result.output, [{ saw: "true" }]);
});

test("for_each retry retries a failing child and then succeeds", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-foreach-retry-"));
	const counterFile = path.join(tmpDir, "counter");
	await fsp.writeFile(counterFile, "0", "utf8");
	const dataCmd = await writeNodeCommand(
		tmpDir,
		"data.js",
		"process.stdout.write(JSON.stringify([1]));\n",
	);
	const flakyCmd = await writeNodeCommand(
		tmpDir,
		"flaky.js",
		[
			'const fs = require("fs");',
			`const p = ${JSON.stringify(counterFile)};`,
			'const c = Number(fs.readFileSync(p, "utf8")) + 1;',
			"fs.writeFileSync(p, String(c));",
			"if (c < 3) process.exit(1);",
			"process.stdout.write(JSON.stringify({ attempt: c }));",
			"",
		].join("\n"),
	);

	const { result, stderrOutput } = await runWorkflowWithIo({
		steps: [
			{ id: "data", command: dataCmd },
			{
				id: "loop",
				for_each: "$data.json",
				retry: { max: 3, delay_ms: 20 },
				steps: [{ id: "flaky", command: flakyCmd }],
			},
		],
	});
	assert.equal(result.status, "ok");
	assert.deepEqual(result.output, [{ item: 1, index: 0, flaky: { attempt: 3 } }]);
	assert.ok(stderrOutput.includes("[RETRY]"), "should log retry attempts");
});

test("for_each cost_limit stop does not retry a child that already exceeded the budget", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-foreach-cost-stop-"));
	const counterFile = path.join(tmpDir, "counter");
	await fsp.writeFile(counterFile, "0", "utf8");
	const dataCmd = await writeNodeCommand(
		tmpDir,
		"data.js",
		"process.stdout.write(JSON.stringify([1, 2]));\n",
	);
	const spendCmd = await writeNodeCommand(
		tmpDir,
		"spend.js",
		[
			'const fs = require("fs");',
			`const p = ${JSON.stringify(counterFile)};`,
			'const c = Number(fs.readFileSync(p, "utf8")) + 1;',
			"fs.writeFileSync(p, String(c));",
			"process.stdout.write(JSON.stringify({model:'gpt-4o',usage:{inputTokens:1000,outputTokens:1000}}));",
			"",
		].join("\n"),
	);

	await assert.rejects(
		() =>
			runWorkflowWithIo({
				cost_limit: { max_usd: 0.01, action: "stop" },
				steps: [
					{ id: "data", command: dataCmd },
					{
						id: "loop",
						for_each: "$data.json",
						retry: { max: 3, delay_ms: 20 },
						steps: [{ id: "spend", command: spendCmd }],
					},
				],
			}),
		/Cost limit exceeded/,
	);
	assert.equal(await fsp.readFile(counterFile, "utf8"), "1");
});

test("for_each dry-run renders timeout, retry, and on_error", async () => {
	const workflow = {
		steps: [
			{ id: "vals", command: 'node -e "process.stdout.write(JSON.stringify([1]))"' },
			{
				id: "loop",
				for_each: "$vals.json",
				timeout_ms: 1500,
				on_error: "continue",
				retry: { max: 3, backoff: "fixed", delay_ms: 100 },
				steps: [{ id: "emit", command: "echo hi" }],
			},
		],
	};

	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-foreach-"));
	const stateDir = path.join(tmpDir, "state");
	const filePath = path.join(tmpDir, "workflow.lobster");
	await fsp.writeFile(filePath, JSON.stringify(workflow, null, 2), "utf8");

	const stderr = new PassThrough();
	let out = "";
	stderr.on("data", (d: Buffer | string) => {
		out += String(d);
	});

	await runWorkflowFile({
		filePath,
		ctx: {
			stdin: process.stdin,
			stdout: process.stdout,
			stderr,
			env: { ...process.env, LOBSTER_STATE_DIR: stateDir },
			mode: "tool",
			dryRun: true,
			registry: createDefaultRegistry(),
		},
	});

	assert.match(out, /\[for_each\]/);
	assert.match(out, /timeout: 1500ms/);
	assert.match(out, /on_error: continue/);
	assert.match(out, /retry: up to 3 attempts/);
});

test("for_each retries restart at the first item after a later item fails", async (t) => {
	const dir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-foreach-restart-"));
	t.after(() => fsp.rm(dir, { recursive: true, force: true }));
	const history = path.join(dir, "history.json");
	const command = await writeNodeCommand(
		dir,
		"flaky.cjs",
		`
const fs = require("node:fs");
const file = ${JSON.stringify(history)};
const history = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, "utf8")) : [];
const item = Number(process.env.ITEM);
history.push(item);
fs.writeFileSync(file, JSON.stringify(history));
if (history.length === 2) process.exit(1);
console.log(item);
`,
	);
	const result = await runWorkflow({
		steps: [
			{ id: "data", run: "node -e \"console.log('[1,2]')\"" },
			{
				id: "loop",
				for_each: "$data.json",
				retry: { max: 2, delay_ms: 1 },
				steps: [{ id: "child", run: command, env: { ITEM: "$item.json" } }],
			},
		],
	});
	assert.equal(result.status, "ok");
	assert.deepEqual(JSON.parse(await fsp.readFile(history, "utf8")), [1, 2, 1, 2]);
	assert.deepEqual(result.output, [
		{ item: 1, index: 0, child: 1 },
		{ item: 2, index: 1, child: 2 },
	]);
});

for (const customNames of [false, true]) {
	test(`for_each resolves loop environment per item with ${customNames ? "custom" : "default"} names`, async () => {
		const itemVar = customNames ? "value" : "item";
		const indexVar = customNames ? "position" : "index";
		const result = await runWorkflow({
			steps: [
				{ id: "data", run: "node -e \"console.log('[10,20]')\"" },
				{
					id: "loop",
					for_each: "$data.json",
					item_var: itemVar,
					index_var: indexVar,
					env: { ITEM: `$${itemVar}.json`, INDEX: `$${indexVar}.json` },
					steps: [
						{
							id: "read",
							run: 'node -e "console.log(JSON.stringify({item:process.env.ITEM,index:process.env.INDEX}))"',
						},
					],
				},
			],
		});
		assert.deepEqual(result.output, [
			{ [itemVar]: 10, [indexVar]: 0, read: { item: "10", index: "0" } },
			{ [itemVar]: 20, [indexVar]: 1, read: { item: "20", index: "1" } },
		]);
	});
}
