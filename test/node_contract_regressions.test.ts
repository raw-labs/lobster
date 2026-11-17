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

test("an approval of the wrong type is rejected, not dropped", async (t) => {
	const fx = await workflowFixture(t);
	const gated = (approval: unknown) => ({
		steps: [{ id: "deploy", pipeline: "fake --name deploy", approval }],
	});
	// A number or a list used to mean "no approval": the step ran unasked.
	for (const approval of [5, ["yes"], null])
		await assert.rejects(
			fx.run(gated(approval)),
			/Workflow step deploy approval must be true, a prompt string, or an object/,
		);
	for (const approval of ["", "  "])
		await assert.rejects(fx.run(gated(approval)), /approval prompt cannot be blank/);
	await assert.rejects(fx.run(gated({ prompt: 5 })), /approval\.prompt must be a string/);
	await assert.rejects(fx.run(gated({ preview: {} })), /approval\.preview must be a string/);
	await assert.rejects(fx.run(gated({ items: "all" })), /approval\.items must be a list/);
	assert.equal(fx.fake.calls("deploy"), 0);
	for (const approval of [true, "Deploy?", { prompt: "Deploy?", items: [1], preview: "diff" }])
		assert.equal((await fx.run(gated(approval))).status, "needs_approval");
	assert.equal((await fx.run(gated(false))).status, "ok");
});

test("dry-run does not claim a step is skipped when its condition reads a step output", async (t) => {
	const fx = await workflowFixture(t);
	fx.fake.script({ items: [{ output: [1] }] });
	await fx.run(
		{
			steps: [
				{ id: "seed", pipeline: "fake --name seed" },
				{ id: "n", pipeline: "fake --name n", when: "$seed.json.call == 1" },
				{ id: "ask", input: { prompt: "Go?", responseSchema: {} }, when: "$seed.stdout != ''" },
				{
					id: "loop",
					for_each: "$seed.json",
					when: "$ask.response.go == true",
					steps: [{ id: "sub", pipeline: "fake --name sub", when: "$item.json.ok == true" }],
				},
				{ id: "never", pipeline: "fake --name never", when: false },
				{ id: "gated", pipeline: "fake --name gated", approval: true },
				{ id: "ifApproved", pipeline: "fake --name a", when: "$gated.approved == true" },
				{ id: "ifFailed", pipeline: "fake --name f", when: "$seed.error ==\n true" },
			],
		},
		{ dryRun: true },
	);
	const plan = fx.stderr();
	// The plan used to say "[skipped — condition: false]" for all of these, which a run would execute.
	assert.match(
		plan,
		/2\. n {2}\[pipeline\]\n {5}when: \$seed\.json\.call == 1 {2}\[depends on step output — unknown at plan time\]/,
	);
	assert.match(
		plan,
		/3\. ask {2}\[input\]\n {5}when: \$seed\.stdout != '' {2}\[depends on step output/,
	);
	assert.match(
		plan,
		/4\. loop {2}\[for_each\]\n {5}when: \$ask\.response\.go == true {2}\[depends/,
	);
	assert.match(
		plan,
		/1\. sub {2}\[pipeline\] pipeline: fake --name sub\n {10}when: \$item\.json\.ok == true/,
	);
	// Gates are planned as granted, so a condition on `approved` is decided; one on any
	// other field is not, and is shown on one line.
	assert.match(plan, /7\. ifApproved {2}\[pipeline\]\n {5}pipeline: fake --name a\n/);
	assert.match(plan, /8\. ifFailed {2}\[pipeline\]\n {5}when: \$seed\.error == true {2}\[depends/);
	await assert.rejects(
		fx.run(
			{
				steps: [
					{ id: "a", run: "true" },
					{ id: "x", run: "true", when: "$a.json == == 1" },
				],
			},
			{ dryRun: true },
		),
		/condition|expected|unexpected/i,
	);
	assert.match(plan, /5\. never {2}\[skipped — condition: false\]/);
	assert.equal(fx.fake.calls("seed"), 0);
});

test("an approval gate in a child workflow is refused before its command runs", async (t) => {
	const fx = await workflowFixture(t);
	await fx.write("child.lobster", {
		steps: [
			{ id: "skipped", pipeline: "fake --name skipped", approval: true, when: false },
			{ id: "deploy", pipeline: "fake --name deploy", approval: true },
		],
	});
	await assert.rejects(
		fx.run({ steps: [{ id: "child", workflow: "child.lobster" }] }),
		/Sub-workflow approval\/input gates are not supported in composition/,
	);
	// The command used to run, and only then was the step refused.
	assert.equal(fx.fake.calls("deploy"), 0);
	// A gate that its condition skips is still allowed.
	assert.equal(fx.fake.calls("skipped"), 0);
	assert.equal((await fx.execution()).status, "failed");
});
