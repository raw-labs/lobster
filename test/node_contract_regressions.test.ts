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
