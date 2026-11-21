import test from "node:test";
import assert from "node:assert/strict";
import { promises as fsp } from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { setTimeout as delay } from "node:timers/promises";

import { builtinPacks, createRegistry } from "../src/commands/registry.js";
import { defineCommand } from "../src/commands/define.js";
import { CostTracker } from "../src/llm/cost_tracker.js";
import { runWorkflowFile } from "../src/engine/runner.js";
import { decodeResumeToken } from "../src/resume/resume.js";

test("CostTracker records usage and computes totals", () => {
	const tracker = new CostTracker();
	tracker.recordUsage("step1", "gpt-4o", { inputTokens: 1000, outputTokens: 500 });
	const summary = tracker.getSummary();
	assert.equal(summary.totalInputTokens, 1000);
	assert.equal(summary.totalOutputTokens, 500);
	assert.equal(summary.estimatedCostUsd, 0.0075);
	assert.equal(summary.byStep.length, 1);
	assert.equal(summary.byStep[0].stepId, "step1");
});

test("CostTracker handles OpenAI token field names", () => {
	const tracker = new CostTracker();
	tracker.recordUsage("step1", "gpt-4o", { prompt_tokens: 1000, completion_tokens: 500 });
	const summary = tracker.getSummary();
	assert.equal(summary.totalInputTokens, 1000);
	assert.equal(summary.totalOutputTokens, 500);
});

function captureWritable() {
	const stream = new PassThrough();
	let output = "";
	stream.on("data", (d: Buffer | string) => {
		output += String(d);
	});
	return { stream, output: () => output };
}

test("CostTracker uses zero cost for unknown models and warns once", () => {
	const stderr = captureWritable();
	const tracker = new CostTracker(undefined, stderr.stream);
	tracker.recordUsage("step1", "unknown-model", { inputTokens: 1000, outputTokens: 500 });
	tracker.recordUsage("step2", "unknown-model", { inputTokens: 1000, outputTokens: 500 });
	const summary = tracker.getSummary();
	assert.equal(summary.estimatedCostUsd, 0);
	assert.equal(
		stderr.output().match(/No LLM pricing configured for model "unknown-model"/g)?.length,
		1,
	);
});

test("CostTracker warns when usage omits the model id", () => {
	const stderr = captureWritable();
	const tracker = new CostTracker({ "": { input: 100, output: 100 } }, stderr.stream);
	tracker.recordUsage("step1", null, { inputTokens: 1000, outputTokens: 500 });
	tracker.recordUsage("step2", "", { inputTokens: 1000, outputTokens: 500 });
	tracker.recordUsage("step3", "   ", { inputTokens: 1000, outputTokens: 500 });
	const summary = tracker.getSummary();
	assert.equal(summary.estimatedCostUsd, 0);
	assert.equal(stderr.output().match(/model "<missing>"/g)?.length, 1);
});

test("CostTracker treats inherited object keys as unknown model ids", () => {
	const stderr = captureWritable();
	const tracker = new CostTracker(undefined, stderr.stream);
	tracker.recordUsage("step1", "constructor", { inputTokens: 1000, outputTokens: 500 });
	const summary = tracker.getSummary();
	assert.equal(summary.estimatedCostUsd, 0);
	assert.equal(Number.isNaN(summary.byStep[0].costUsd), false);
	assert.match(stderr.output(), /No LLM pricing configured for model "constructor"/);
});

test("CostTracker warns when pricing env json is invalid", () => {
	const stderr = captureWritable();
	const pricing = CostTracker.parsePricingFromEnv(
		{
			LOBSTER_LLM_PRICING_JSON: "{not-json",
		},
		stderr.stream,
	);
	assert.equal(pricing, undefined);
	assert.match(stderr.output(), /Ignoring invalid LOBSTER_LLM_PRICING_JSON/);
});

test("CostTracker rejects structurally invalid pricing env json", () => {
	const stderr = captureWritable();
	const pricing = CostTracker.parsePricingFromEnv(
		{
			LOBSTER_LLM_PRICING_JSON: '{"my-model":{"input":1.0}}',
		},
		stderr.stream,
	);
	assert.equal(pricing, undefined);
	assert.match(stderr.output(), /Ignoring invalid LOBSTER_LLM_PRICING_JSON/);
});

test("CostTracker rejects blank pricing model keys", () => {
	const stderr = captureWritable();
	const pricing = CostTracker.parsePricingFromEnv(
		{
			LOBSTER_LLM_PRICING_JSON: '{"":{"input":1.0,"output":2.0}}',
		},
		stderr.stream,
	);
	assert.equal(pricing, undefined);
	assert.match(stderr.output(), /Ignoring invalid LOBSTER_LLM_PRICING_JSON/);
});

test("CostTracker supports custom pricing from env json", () => {
	const pricing = CostTracker.parsePricingFromEnv({
		LOBSTER_LLM_PRICING_JSON: '{"my-model":{"input":1.0,"output":2.0}}',
	});
	const tracker = new CostTracker(pricing);
	tracker.recordUsage("step1", "my-model", { inputTokens: 1_000_000, outputTokens: 1_000_000 });
	assert.equal(tracker.getSummary().estimatedCostUsd, 3);
});

test("CostTracker checkLimit throws when action=stop and limit exceeded", () => {
	const tracker = new CostTracker();
	tracker.recordUsage("step1", "gpt-4o", { inputTokens: 10_000_000, outputTokens: 10_000_000 });
	assert.throws(() => tracker.checkLimit({ max_usd: 0.01, action: "stop" }), /Cost limit exceeded/);
});

test("CostTracker checkLimit warns when action=warn and limit exceeded", () => {
	const tracker = new CostTracker();
	tracker.recordUsage("step1", "gpt-4o", { inputTokens: 10_000_000, outputTokens: 10_000_000 });
	const stderr = new PassThrough();
	let out = "";
	stderr.on("data", (d: Buffer | string) => {
		out += String(d);
	});
	tracker.checkLimit({ max_usd: 0.01, action: "warn" }, stderr);
	assert.match(out, /\[WARN\] Cost/);
});

test("CostTracker escapes unknown model ids in warnings", () => {
	const stderr = captureWritable();
	const tracker = new CostTracker(undefined, stderr.stream);
	tracker.recordUsage("step1", "bad\n\u001b[31m\u009b31m\u2028next\u2029line", {
		inputTokens: 1,
		outputTokens: 1,
	});
	assert.ok(stderr.output().includes('"bad\\n\\u001b[31m\\u009b31m\\u2028next\\u2029line"'));
	assert.equal(stderr.output().includes("\u009b"), false);
	assert.equal(stderr.output().includes("\u2028"), false);
	assert.equal(stderr.output().includes("\u2029"), false);
});

// Holds its items for --ms (or until the step is stopped), then passes them on.
const hold = defineCommand({
	name: "test.hold",
	meta: {
		description: "Passes items on after a delay",
		argsSchema: { type: "object", properties: { ms: { type: "number" } } },
		sideEffects: [],
		repeatable: true,
	},
	help: () => "test.hold",
	async run({ input, args, ctx }) {
		const items = [];
		for await (const item of input) items.push(item);
		await delay(Number(args.ms), undefined, { signal: ctx.signal });
		return {
			output: (async function* () {
				yield* items;
			})(),
		};
	},
});
const registry = createRegistry({
	packs: [...builtinPacks, { name: "test", description: "Test", commands: [hold] }],
});

async function runWorkflow(
	workflow: unknown,
	envOverride?: Record<string, string>,
	llmAdapters?: Record<string, unknown>,
) {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-cost-"));
	const stateDir = path.join(tmpDir, "state");
	const filePath = path.join(tmpDir, "workflow.lobster");
	await fsp.writeFile(filePath, JSON.stringify(workflow, null, 2), "utf8");
	const stderr = new PassThrough();
	let stderrOutput = "";
	stderr.on("data", (d: Buffer | string) => {
		stderrOutput += String(d);
	});

	try {
		const result = await runWorkflowFile({
			filePath,
			ctx: {
				stdin: process.stdin,
				stdout: process.stdout,
				stderr,
				env: { ...process.env, LOBSTER_STATE_DIR: stateDir, ...envOverride },
				mode: "tool",
				registry,
				llmAdapters,
			},
		});

		return { result, stderrOutput };
	} finally {
		stderr.destroy();
		await fsp.rm(tmpDir, { recursive: true, force: true });
	}
}

// The step payloads are written to files rather than passed with `node -e`, because
// `cmd /s /c` hands the inline quoting to node verbatim and the JSON never reaches stdout.
async function emitJsonCommand(payload: unknown) {
	const dir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-cost-emit-"));
	const filePath = path.join(dir, "emit.mjs");
	const literal = JSON.stringify(JSON.stringify(payload));
	await fsp.writeFile(filePath, `process.stdout.write(${literal});\n`, "utf8");
	return `node ${filePath}`;
}

// Use the HTTP adapter's usage-bearing envelope; OpenClaw llm-task does not report usage.
async function startFakeProvider(
	holdUntil = 1,
	usageFor: (seen: number) => Record<string, number> | null = () => ({
		inputTokens: 1000,
		outputTokens: 500,
	}),
	// Lets a case fix the order two concurrent calls reach the ledger in.
	delayMsFor: (prompt: string) => number = () => 0,
	dataFor: (seen: number) => unknown = () => ({ summary: "hello" }),
) {
	let requests = 0;
	const held: Array<() => void> = [];
	const releaseHeld = () => {
		while (held.length) held.shift()?.();
	};
	const server = http.createServer((req, res) => {
		let body = "";
		req.setEncoding("utf8");
		req.on("data", (chunk) => (body += chunk));
		req.on("end", () => {
			requests += 1;
			const seen = requests;
			const parsed = JSON.parse(body || "{}");
			const answer = () => {
				res.writeHead(200, { "content-type": "application/json" });
				res.end(
					JSON.stringify({
						ok: true,
						result: {
							runId: `invoke_${seen}`,
							model: parsed.model,
							prompt: parsed.prompt,
							output: { data: dataFor(seen) },
							...(usageFor(seen) ? { usage: usageFor(seen) } : null),
						},
					}),
				);
			};
			const enqueue = () => {
				held.push(answer);
				if (requests >= holdUntil) releaseHeld();
			};
			const delayMs = delayMsFor(String(parsed.prompt ?? ""));
			if (delayMs > 0) setTimeout(enqueue, delayMs);
			else enqueue();
		});
	});
	await new Promise<void>((resolve) => server.listen(0, resolve));
	const address = server.address();
	const port = typeof address === "object" && address ? address.port : 0;
	return {
		url: `http://localhost:${port}`,
		requests: () => requests,
		close: () => new Promise<void>((resolve) => server.close(() => resolve())),
	};
}

async function runComposedWorkflow(
	parent: Record<string, unknown>,
	child: unknown,
	envOverride?: Record<string, string>,
) {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-cost-"));
	const stateDir = path.join(tmpDir, "state");
	const parentPath = path.join(tmpDir, "workflow.lobster");
	const childPath = path.join(tmpDir, "child.lobster");
	await fsp.writeFile(parentPath, JSON.stringify(parent, null, 2), "utf8");
	await fsp.writeFile(childPath, JSON.stringify(child, null, 2), "utf8");
	const stderr = new PassThrough();
	let stderrOutput = "";
	stderr.on("data", (d: Buffer | string) => {
		stderrOutput += String(d);
	});

	const result = await runWorkflowFile({
		filePath: parentPath,
		ctx: {
			stdin: process.stdin,
			stdout: process.stdout,
			stderr,
			env: { ...process.env, LOBSTER_STATE_DIR: stateDir, ...envOverride },
			mode: "tool",
			registry,
		},
	});

	return { result, stderrOutput };
}

type FakeProvider = Awaited<ReturnType<typeof startFakeProvider>>;

/** Run `body` against `provider` (a default fake one) with a fresh answer cache. */
async function withProvider(
	body: (env: Record<string, string>, provider: FakeProvider) => Promise<void>,
	provider?: FakeProvider,
) {
	const fake = provider ?? (await startFakeProvider());
	const cacheDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-cost-cache-"));
	try {
		await body({ LOBSTER_LLM_ADAPTER_URL: fake.url, LOBSTER_CACHE_DIR: cacheDir }, fake);
	} finally {
		await fake.close();
		await fsp.rm(cacheDir, { recursive: true, force: true });
	}
}

const invoke = (prompt = "Summarize", model = "gpt-4o") =>
	`llm.invoke --model ${model} --prompt ${prompt}`;
const stepIds = (result: { _meta?: { cost?: { byStep: Array<{ stepId: string }> } } }) =>
	result._meta?.cost?.byStep.map((entry) => entry.stepId).sort();

test("a model call is counted under the step that made it", async () => {
	await withProvider(async (env, provider) => {
		const { result } = await runWorkflow({ steps: [{ id: "llm", pipeline: invoke() }] }, env);
		assert.equal(result.status, "ok");
		assert.equal(provider.requests(), 1);
		assert.equal(result._meta?.cost?.totalInputTokens, 1000);
		assert.equal(result._meta?.cost?.totalOutputTokens, 500);
		assert.equal(result._meta?.cost?.estimatedCostUsd, 0.0075);
		assert.deepEqual(
			result._meta?.cost?.byStep.map(({ stepId, model }) => ({ stepId, model })),
			[{ stepId: "llm", model: "gpt-4o" }],
		);
	});
});

test("workflow result omits _meta.cost when no model call was made", async () => {
	const { result } = await runWorkflow({ steps: [{ id: "plain", command: 'echo "hello"' }] });
	assert.equal(result.status, "ok");
	assert.equal(result._meta, undefined);
});

test("usage a command prints is not a call, and is not counted", async () => {
	const printed = await emitJsonCommand({
		kind: "llm.invoke",
		model: "gpt-4o",
		source: "http",
		usage: { inputTokens: 1000, outputTokens: 500 },
	});
	const { result } = await runWorkflow({
		cost_limit: { max_usd: 0.00001, action: "stop" },
		steps: [{ id: "printed", command: printed }],
	});
	assert.equal(result.status, "ok");
	assert.equal(result._meta, undefined);
});

for (const [replay, flags, env] of [
	["a cached answer", "", {}],
	["a run-state replay", " --disable-cache", { LOBSTER_RUN_STATE_KEY: "cost-tracker-replay" }],
] as const)
	test(`${replay} is not a call, and is not counted`, async () => {
		await withProvider(async (providerEnv, provider) => {
			const { result } = await runWorkflow(
				{
					cost_limit: { max_usd: 0.01, action: "stop" },
					steps: [
						{ id: "live", pipeline: invoke() + flags },
						{ id: "replayed", pipeline: invoke() + flags },
						{ id: "copy", pipeline: "head --n 1", stdin: "$live.json" },
					],
				},
				{ ...providerEnv, ...env },
			);
			assert.equal(result.status, "ok");
			assert.equal(provider.requests(), 1);
			assert.equal(result._meta?.cost?.estimatedCostUsd, 0.0075);
			assert.deepEqual(stepIds(result), ["live"]);
		});
	});

test("cost_limit warn logs a warning and the run continues", async () => {
	await withProvider(async (env) => {
		const { result, stderrOutput } = await runWorkflow(
			{
				cost_limit: { max_usd: 0.00001, action: "warn" },
				steps: [
					{ id: "llm", pipeline: invoke() },
					{ id: "after", command: "echo done" },
				],
			},
			env,
		);
		assert.equal(result.status, "ok");
		assert.match(stderrOutput, /\[WARN\] Cost/);
		assert.deepEqual(result.output, ["done\n"]);
	});
});

test("cost_limit stop ends the run once the calls made pass it", async () => {
	await withProvider(async (env, provider) => {
		await assert.rejects(
			runWorkflow(
				{
					cost_limit: { max_usd: 0.01, action: "stop" },
					steps: [
						{ id: "first", pipeline: invoke("One") },
						{ id: "second", pipeline: invoke("Two") },
						{ id: "third", pipeline: invoke("Three") },
					],
				},
				env,
			),
			/Cost limit exceeded/,
		);
		assert.equal(provider.requests(), 2);
	});
});

test("a call to a model with no price is counted at zero, with a warning", async () => {
	await withProvider(async (env) => {
		const { result, stderrOutput } = await runWorkflow(
			{ steps: [{ id: "llm", pipeline: invoke("Summarize", "unknown-model") }] },
			env,
		);
		assert.equal(result.status, "ok");
		assert.equal(result._meta?.cost?.totalInputTokens, 1000);
		assert.equal(result._meta?.cost?.estimatedCostUsd, 0);
		assert.match(stderrOutput, /No LLM pricing configured for model "unknown-model"/);
	});
});

test("workflow cost tracking warns for invalid pricing env json", async () => {
	await withProvider(async (env) => {
		const { result, stderrOutput } = await runWorkflow(
			{ steps: [{ id: "llm", pipeline: invoke() }] },
			{ ...env, LOBSTER_LLM_PRICING_JSON: "{not-json" },
		);
		assert.equal(result.status, "ok");
		assert.match(stderrOutput, /Ignoring invalid LOBSTER_LLM_PRICING_JSON/);
	});
});

test("every provider call is counted, one the schema validator rejected too", async () => {
	const provider = await startFakeProvider(
		1,
		(seen) =>
			seen === 1
				? { inputTokens: 1000, outputTokens: 500 }
				: { inputTokens: 3000, outputTokens: 100 },
		() => 0,
		(seen) => (seen === 1 ? {} : { summary: "hello" }),
	);
	const schema = JSON.stringify({
		type: "object",
		required: ["summary"],
		properties: { summary: { type: "string" } },
	});
	await withProvider(async (env) => {
		const { result } = await runWorkflow(
			{ steps: [{ id: "validated", pipeline: `${invoke()} --output-schema '${schema}' | json` }] },
			env,
		);
		assert.equal(result.status, "ok");
		assert.equal(provider.requests(), 2, "the first answer failed the schema and was asked again");
		assert.equal(result._meta?.cost?.totalInputTokens, 4000);
		assert.equal(result._meta?.cost?.totalOutputTokens, 600);
	}, provider);
});

test("what a run spent before an approval is still counted after the resume", async () => {
	await withProvider(async (providerEnv, provider) => {
		const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-cost-resume-"));
		const filePath = path.join(tmpDir, "workflow.lobster");
		await fsp.writeFile(
			filePath,
			JSON.stringify({
				steps: [
					{ id: "live", pipeline: invoke() },
					{ id: "gate", run: "echo gate", approval: "Continue?" },
					{ id: "replay", pipeline: invoke() },
					{ id: "after", pipeline: invoke("Again") },
				],
			}),
		);
		const ctx = () => ({
			stdin: process.stdin,
			stdout: process.stdout,
			stderr: new PassThrough(),
			env: { ...process.env, ...providerEnv, LOBSTER_STATE_DIR: path.join(tmpDir, "state") },
			mode: "tool" as const,
			registry,
		});
		try {
			const paused = await runWorkflowFile({ filePath, ctx: ctx() });
			assert.equal(paused.status, "needs_approval");
			const resumed = await runWorkflowFile({
				filePath,
				ctx: ctx(),
				resume: decodeResumeToken(paused.requiresApproval!.resumeToken!),
				approved: true,
			});
			assert.equal(resumed.status, "ok");
			assert.equal(provider.requests(), 2);
			assert.equal(resumed._meta?.cost?.totalInputTokens, 2000);
			assert.deepEqual(stepIds(resumed), ["after", "live"]);
		} finally {
			await fsp.rm(tmpDir, { recursive: true, force: true });
		}
	});
});

test("a child workflow's cost_limit counts only its own calls, with siblings running at once", async () => {
	// Each call costs $0.00035; each child is allowed $0.0005, so only another child's call
	// could take it over. Both calls are made before either child's step ends.
	const provider = await startFakeProvider(1, () => ({ inputTokens: 100, outputTokens: 10 }));
	await withProvider(async (env) => {
		const { result } = await runComposedWorkflow(
			{
				steps: [
					{ id: "items", run: "echo '[1, 2]'" },
					{
						id: "loop",
						for_each: "$items.json",
						concurrency: 2,
						steps: [{ id: "kid", workflow: "child.lobster" }],
					},
				],
			},
			{
				cost_limit: { max_usd: 0.0005, action: "stop" },
				steps: [{ id: "call", pipeline: `${invoke()} --refresh | test.hold --ms 300` }],
			},
			env,
		);
		assert.equal(result.status, "ok", JSON.stringify(result));
		assert.equal(provider.requests(), 2);
	}, provider);
});

test("a child workflow's calls count under its workflow step, a loop's as loop.step", async () => {
	await withProvider(async (env, provider) => {
		const { result } = await runComposedWorkflow(
			{
				steps: [
					{ id: "kid", workflow: "child.lobster" },
					{ id: "items", run: "echo '[1]'" },
					{
						id: "loop",
						for_each: "$items.json",
						steps: [{ id: "inloop", pipeline: invoke("Loop") }],
					},
				],
			},
			{ steps: [{ id: "llm", pipeline: invoke("Child") + " | json" }] },
			env,
		);
		assert.equal(result.status, "ok", JSON.stringify(result));
		assert.equal(provider.requests(), 2);
		assert.deepEqual(stepIds(result), ["kid", "loop.inloop"]);
	});
});

test("every branch's calls count, a wait:any race's losers too", async () => {
	// Slow's call answers at once; Fast's answer comes later, but its step ends first.
	const provider = await startFakeProvider(1, undefined, (prompt) => (prompt === "Fast" ? 200 : 0));
	await withProvider(async (env) => {
		const { result } = await runWorkflow(
			{
				steps: [
					{
						id: "race",
						parallel: {
							wait: "any",
							branches: [
								{ id: "fast", pipeline: invoke("Fast") },
								{ id: "slow", pipeline: invoke("Slow") + " | test.hold --ms 10000" },
							],
						},
					},
				],
			},
			env,
		);
		assert.equal(result.status, "ok", JSON.stringify(result));
		assert.equal(provider.requests(), 2);
		assert.equal(result._meta?.cost?.totalInputTokens, 2000);
		assert.deepEqual(stepIds(result), ["fast", "slow"]);
	}, provider);
});

test("items running at once each have their calls counted", async () => {
	await withProvider(async (env, provider) => {
		const { result } = await runWorkflow(
			{
				steps: [
					{ id: "items", run: "echo '[0, 300]'" },
					{
						id: "loop",
						for_each: "$items.json",
						concurrency: 2,
						steps: [
							{
								id: "call",
								pipeline:
									"llm.invoke --model gpt-4o --prompt 'hi $item.json' | test.hold --ms $item.json",
							},
						],
					},
				],
			},
			env,
		);
		assert.equal(result.status, "ok", JSON.stringify(result));
		assert.equal(provider.requests(), 2);
		assert.equal(result._meta?.cost?.totalInputTokens, 2000);
		assert.deepEqual(stepIds(result), ["loop.call", "loop.call"]);
	});
});

for (const loop of [false, true]) {
	test(`cost_limit stops ${loop ? "for_each" : "pipeline"} retries once a failed attempt's call passed it`, async () => {
		let calls = 0;
		const step = {
			id: "spend",
			pipeline:
				'llm.invoke --provider budget-test --model gpt-4o --prompt test --refresh --disable-cache | exec --stdin json node -e "process.exit(1)"',
		};
		const retry = { max: 3, delay_ms: 1 };
		await assert.rejects(
			() =>
				runWorkflow(
					{
						cost_limit: { max_usd: 0.001, action: "stop" },
						steps: loop
							? [
									{ id: "data", pipeline: "exec --json node -e \"console.log('[1,2]')\"" },
									{
										id: "loop",
										for_each: "$data.json",
										retry,
										on_error: "continue",
										steps: [step],
									},
								]
							: [{ ...step, retry, on_error: "continue" }],
					},
					undefined,
					{
						"budget-test": async () => {
							calls++;
							return {
								ok: true,
								result: {
									output: { text: "paid" },
									model: "gpt-4o",
									usage: { inputTokens: 1000, outputTokens: 500 },
								},
							};
						},
					},
				),
			/Cost limit exceeded/,
		);
		assert.equal(calls, 1, "a failed attempt's call counts before the step is retried");
	});
}

test("cost limit messages show a limit under a cent as written", () => {
	const tracker = new CostTracker();
	tracker.recordUsage("step", "gpt-4o", { inputTokens: 100, outputTokens: 100 });
	assert.throws(
		() => tracker.checkLimit({ max_usd: 0.001, action: "stop" }),
		/Cost limit exceeded: \$0\.001\d > \$0\.001 limit/,
	);
	const stderr = captureWritable();
	tracker.checkLimit({ max_usd: 0.001, action: "warn" }, stderr.stream);
	assert.match(stderr.output(), /exceeds limit \$0\.001\n/);
});
