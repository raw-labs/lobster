// Regression tests for bugs found by the node contract matrix (node_contract.test.ts).
import test from "node:test";
import assert from "node:assert/strict";
import { workflowFixture } from "./helpers/workflow_runner.js";

test("a when of the wrong type is rejected before any step runs", async (t) => {
	const fx = await workflowFixture(t);
	for (const when of [5, { eq: 1 }, ["$a.json"]]) {
		await assert.rejects(
			fx.run({
				steps: [
					{ id: "first", pipeline: "fake --name first" },
					{ id: "n", pipeline: "fake --name n", when },
				],
			}),
			/Workflow step n when must be a boolean or an expression string/,
		);
	}
	await assert.rejects(
		fx.run({ steps: [{ id: "n", pipeline: "fake --name n", condition: 1 }] }),
		/Workflow step n condition must be a boolean or an expression string/,
	);
	assert.equal(fx.fake.calls("first"), 0);
});

test("a for_each sub-step's timeout_ms, retry, on_error and when are validated", async (t) => {
	const fx = await workflowFixture(t);
	fx.fake.script({ items: [{ output: ["a"] }] });
	const loop = (sub: Record<string, unknown>) => ({
		steps: [
			{ id: "items", pipeline: "fake --name items" },
			{
				id: "loop",
				for_each: "$items.json",
				steps: [{ id: "w", pipeline: "fake --name w", ...sub }],
			},
		],
	});
	// A quoted timeout was added to the clock as text and expired at once.
	await assert.rejects(
		fx.run(loop({ timeout_ms: "5000" })),
		/Workflow step loop for_each sub-step w timeout_ms must be a positive integer/,
	);
	await assert.rejects(
		fx.run(loop({ retry: { max: 0 } })),
		/sub-step w retry\.max must be a positive integer/,
	);
	await assert.rejects(fx.run(loop({ on_error: "ignore" })), /sub-step w on_error must be/);
	await assert.rejects(fx.run(loop({ when: 5 })), /sub-step w when must be a boolean/);
	assert.equal(fx.fake.calls("items"), 0);
	const result = await fx.run(loop({ timeout_ms: 5000, retry: { max: 2 }, on_error: "continue" }));
	assert.equal(result.status, "ok");
});
