// Regression tests for findings of the review of the engine, loader and SDK work.
import test from "node:test";
import assert from "node:assert/strict";
import { PassThrough } from "node:stream";
import { evaluateCondition } from "../src/workflows/conditions.js";
import { runIdOf, runWorkflowFile } from "../src/engine/runner.js";
import { workflowFixture } from "./helpers/workflow_runner.js";

test("a backslash in a quoted pattern means what it means in a regular expression", () => {
	const results = { a: { id: "a", json: { id: "123", t: "axb", s: "a.b", q: 'say "hi"' } } } as any;
	assert.equal(evaluateCondition(String.raw`$a.json.id matches "^\d+$"`, results), true);
	assert.equal(evaluateCondition(String.raw`$a.json.t matches "a\.b"`, results), false);
	assert.equal(evaluateCondition(String.raw`$a.json.s matches "a\.b"`, results), true);
	// Quotes and backslashes are still escaped.
	assert.equal(evaluateCondition(String.raw`$a.json.q == "say \"hi\""`, results), true);
});

test("the steps inside a skipped switch, repeat or parallel count as skipped", async (t) => {
	const fx = await workflowFixture(t);
	const result = await fx.run({
		steps: [
			{
				id: "sw",
				when: false,
				switch: {
					cases: [{ when: true, steps: [{ id: "inner", pipeline: "fake --name inner" }] }],
				},
			},
			{
				id: "rp",
				when: false,
				repeat: { until: true, max: 1 },
				steps: [{ id: "body", pipeline: "fake --name body" }],
			},
			{
				id: "par",
				when: false,
				parallel: { branches: [{ id: "branch", pipeline: "fake --name branch" }] },
			},
			{
				id: "after",
				pipeline: "fake --name after",
				when: "$inner.skipped && $body.skipped && $branch.skipped",
			},
		],
	});
	assert.equal(result.status, "ok");
	assert.equal(fx.fake.calls("after"), 1);
	assert.equal(fx.fake.calls("inner") + fx.fake.calls("body") + fx.fake.calls("branch"), 0);
});

test("dry-run of a switch that may not run, and of steps inside an undecided step", async (t) => {
	const fx = await workflowFixture(t);
	await fx.run(
		{
			steps: [
				{ id: "seed", pipeline: "fake --name seed" },
				{
					id: "sw",
					when: "$seed.json.go == true",
					switch: {
						cases: [{ when: false, steps: [{ id: "never", pipeline: "fake --name never" }] }],
					},
				},
				{ id: "after", pipeline: "fake --name after", when: "$never.skipped" },
			],
		},
		{ dryRun: true },
	);
	const plan = fx.stderr();
	assert.match(plan, /case 1: when false {2}\[condition: false\]/);
	// Whether "never" was skipped depends on whether the switch ran.
	assert.match(
		plan,
		/after {2}\[pipeline\]\n\s+when: \$never\.skipped {2}\[depends on step output/,
	);
});

test("loop-only keys, oversized delays and a NUL in a branch cwd are refused", async (t) => {
	const fx = await workflowFixture(t);
	for (const [step, error] of [
		[{ id: "w", run: "true", pause_ms: 5000 }, /pause_ms applies only to for_each/],
		[{ id: "w", run: "true", item_var: "x" }, /item_var applies only to for_each/],
		[
			{ id: "w", run: "true", retry: { max: 2, delay_ms: 3e9 } },
			/delay_ms must be a number from 0/,
		],
		[
			{ id: "p", parallel: { branches: [{ id: "b", run: "true", cwd: "a\u0000b" }] } },
			/parallel branch b cwd cannot contain a NUL character/,
		],
		[
			{
				id: "sw",
				retry: { max: 2 },
				switch: { cases: [{ when: true, steps: [{ id: "ok", approval: "Go?" }] }] },
			},
			/switch with retry cannot contain approval, input, sleep or wait steps/,
		],
	] as const)
		await assert.rejects(fx.run({ steps: [step] }), error, JSON.stringify(step));
});

test("a failure names its own run, never an earlier run that failed with the same error", async (t) => {
	const fx = await workflowFixture(t);
	const controller = new AbortController();
	const ctx = () => ({ ...(fx.ctx() as any), stdin: new PassThrough(), signal: controller.signal });
	const slow = await fx.write("slow.lobster", { steps: [{ id: "w", run: "sleep 5" }] });
	setTimeout(() => controller.abort(new Error("stopped")), 300);
	const first = await runWorkflowFile({ filePath: slow, ctx: ctx() }).catch((error) => error);
	const firstRun = runIdOf(first);
	assert.ok(firstRun, "the first run names itself");
	// The same signal, already aborted: the same error object comes back from another call.
	const second = await runWorkflowFile({ filePath: slow, ctx: ctx() }).catch((error) => error);
	assert.equal(second, first, "the same error object");
	assert.notEqual(runIdOf(second), firstRun);
});
