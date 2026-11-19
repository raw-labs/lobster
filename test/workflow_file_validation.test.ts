import test from "node:test";
import assert from "node:assert/strict";
import { promises as fsp } from "node:fs";
import os from "node:os";
import path from "node:path";

import { loadWorkflowFile } from "../src/workflows/load.js";

async function load(t: test.TestContext, workflow: Record<string, unknown>) {
	const dir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-file-validation-"));
	t.after(() => fsp.rm(dir, { recursive: true, force: true }));
	const filePath = path.join(dir, "flow.json");
	await fsp.writeFile(filePath, JSON.stringify({ steps: [{ id: "a", run: "true" }], ...workflow }));
	return loadWorkflowFile(filePath);
}

test("workflow args are name → { default, description }, and nothing else loads", async (t) => {
	await load(t, { args: { name: { default: "world", description: "who to greet" }, bare: {} } });
	for (const [args, message] of [
		[
			{ name: { defualt: "x" } },
			/Workflow arg name has unknown key "defualt" \(did you mean "default"\?\)/,
		],
		[{ name: "world" }, /Workflow arg name must be an object/],
		[{ name: null }, /Workflow arg name must be an object/],
		[["name"], /Workflow args must be an object/],
		[{ name: { description: 3 } }, /Workflow arg name description must be a string/],
	] as const) {
		await assert.rejects(load(t, { args }), message);
	}
});

test("cost_limit.max_usd must be a number, not a string or an empty value", async (t) => {
	await load(t, { cost_limit: { max_usd: 0.5, action: "stop" } });
	for (const max_usd of ["0.5", null, "", true, -1, undefined]) {
		await assert.rejects(
			load(t, { cost_limit: { max_usd, action: "stop" } }),
			/Workflow cost_limit.max_usd must be a non-negative number/,
			String(max_usd),
		);
	}
});

test("a workflow's name and description are strings", async (t) => {
	await load(t, { name: "greet", description: "Says hello" });
	await assert.rejects(load(t, { name: 5 }), /Workflow name must be a string/);
	await assert.rejects(load(t, { description: [1, 2] }), /Workflow description must be a string/);
});

test("a setting given under two names is refused, not half dropped", async (t) => {
	for (const [steps, message] of [
		[
			[{ id: "a", run: "echo run", command: "echo command" }],
			/Workflow step a takes run or command, not both/,
		],
		[
			[{ id: "p", parallel: { branches: [{ id: "b", run: "echo 1", command: "echo 2" }] } }],
			/parallel branch b takes run or command, not both/,
		],
		[
			[{ id: "a", approval: { required_approver: "alice", requiredApprover: "bob" } }],
			/approval takes required_approver or requiredApprover, not both/,
		],
		[
			[{ id: "a", approval: { initiated_by: "alice", initiatedBy: "alice" } }],
			/approval takes initiated_by or initiatedBy, not both/,
		],
		[
			[
				{
					id: "a",
					approval: { require_different_approver: true, requireDifferentApprover: false },
				},
			],
			/approval takes require_different_approver or requireDifferentApprover, not both/,
		],
	] as const) {
		await assert.rejects(load(t, { steps }), message);
	}
});

test("no step inside a loop, however nested, may take a name in scope and hide it", async (t) => {
	const shadowing = (id: string) => ({ id, run: "echo shadow" });
	for (const [steps, name] of [
		// A loop variable, from a switch case in the body.
		[
			[
				{
					id: "loop",
					for_each: "[1]",
					steps: [{ id: "route", switch: { cases: [{ when: true, steps: [shadowing("item")] }] } }],
				},
			],
			"item",
		],
		// An outer loop's variable, from an inner loop's body.
		[
			[
				{
					id: "outer",
					for_each: "[1]",
					item_var: "x",
					steps: [{ id: "inner", for_each: "[2]", item_var: "y", steps: [shadowing("x")] }],
				},
			],
			"x",
		],
		// A step before the loop.
		[
			[
				{ id: "list", run: "echo '[1]'" },
				{ id: "loop", for_each: "$list.json", steps: [shadowing("list")] },
			],
			"list",
		],
	] as const) {
		await assert.rejects(load(t, { steps }), new RegExp(`Duplicate workflow step id: ${name} `));
	}
	// A step after the loop may reuse a body id: the body's ids are its own.
	await load(t, {
		steps: [
			{ id: "loop", for_each: "[1]", steps: [shadowing("each")] },
			{ id: "each", run: "true" },
		],
	});
});

test("no step inside a switch case or a repeat may take the id of the step around it", async (t) => {
	for (const step of [
		{ id: "s", switch: { cases: [{ when: true, steps: [{ id: "s", run: "true" }] }] } },
		{ id: "s", repeat: { until: true, max: 1 }, steps: [{ id: "s", run: "true" }] },
	]) {
		await assert.rejects(load(t, { steps: [step] }), /Duplicate workflow step id: s /);
	}
});

test("an unknown key is matched to the nearest known key, not the first near one", async (t) => {
	await assert.rejects(
		load(t, { steps: [{ id: "a", run: "true", whn: "true" }] }),
		/unknown key "whn" \(did you mean "when"\?\)/,
	);
});

test("a misspelled recovery names its step or branch", async (t) => {
	await assert.rejects(
		load(t, { steps: [{ id: "a", run: "true", recovery: "repet" }] }),
		/Workflow step a recovery must be "repeat" or "manual"/,
	);
	await assert.rejects(
		load(t, {
			steps: [{ id: "p", parallel: { branches: [{ id: "b", run: "true", recovery: "x" }] } }],
		}),
		/Workflow step p parallel branch b recovery must be/,
	);
});
