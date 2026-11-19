import test from "node:test";
import assert from "node:assert/strict";
import { promises as fsp } from "node:fs";
import os from "node:os";
import path from "node:path";

import { runWorkflowFile } from "../src/engine/runner.js";

async function runWorkflow(workflow: unknown) {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-cond-"));
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
		},
	});
}

test("condition > works with numbers", async () => {
	const result = await runWorkflow({
		steps: [
			{ id: "data", command: 'node -e "process.stdout.write(JSON.stringify({count:5}))"' },
			{ id: "check", command: 'echo "big"', when: "$data.json.count > 3" },
		],
	});
	assert.equal(result.status, "ok");
	assert.deepEqual(result.output, ["big\n"]);
});

test("condition > skips when false", async () => {
	const result = await runWorkflow({
		steps: [
			{ id: "data", command: 'node -e "process.stdout.write(JSON.stringify({count:1}))"' },
			{ id: "check", command: 'echo "big"', when: "$data.json.count > 3" },
			{ id: "fallback", command: 'echo "small"' },
		],
	});
	assert.equal(result.status, "ok");
	assert.deepEqual(result.output, ["small\n"]);
});

test("condition < works", async () => {
	const result = await runWorkflow({
		steps: [
			{ id: "data", command: 'node -e "process.stdout.write(JSON.stringify({val:2}))"' },
			{ id: "check", command: 'echo "low"', when: "$data.json.val < 10" },
		],
	});
	assert.equal(result.status, "ok");
	assert.deepEqual(result.output, ["low\n"]);
});

test("condition >= works at boundary", async () => {
	const result = await runWorkflow({
		steps: [
			{ id: "data", command: 'node -e "process.stdout.write(JSON.stringify({val:5}))"' },
			{ id: "check", command: 'echo "yes"', when: "$data.json.val >= 5" },
		],
	});
	assert.equal(result.status, "ok");
	assert.deepEqual(result.output, ["yes\n"]);
});

test("condition <= works at boundary", async () => {
	const result = await runWorkflow({
		steps: [
			{ id: "data", command: 'node -e "process.stdout.write(JSON.stringify({val:5}))"' },
			{ id: "check", command: 'echo "yes"', when: "$data.json.val <= 5" },
		],
	});
	assert.equal(result.status, "ok");
	assert.deepEqual(result.output, ["yes\n"]);
});

test("comparison operators combine with boolean operators", async () => {
	const result = await runWorkflow({
		steps: [
			{ id: "data", command: 'node -e "process.stdout.write(JSON.stringify({a:5,b:20}))"' },
			{ id: "check", command: 'echo "in range"', when: "$data.json.a >= 1 && $data.json.b < 100" },
		],
	});
	assert.equal(result.status, "ok");
	assert.deepEqual(result.output, ["in range\n"]);
});

for (const [name, data, when] of [
	["a non-numeric string", { val: "hello" }, "$data.json.val > 3"],
	["a boolean", { val: true }, "$data.json.val > 0"],
	["null", { val: null }, "$data.json.val >= 0"],
	["a missing value", { val: 1 }, "$data.json.nope < 2"],
	["two strings", { val: "b", other: "a" }, "$data.json.val > $data.json.other"],
] as const) {
	test(`ordering ${name} is an error, not a silent false`, async () => {
		await assert.rejects(
			runWorkflow({
				steps: [
					{ id: "data", command: `echo '${JSON.stringify(data)}'` },
					{ id: "check", command: 'echo "yes"', when },
					{ id: "fallback", command: 'echo "no"' },
				],
			}),
			/cannot compare .* only numbers can be ordered/,
		);
	});
}

test("numeric strings still compare as numbers", async () => {
	const result = await runWorkflow({
		steps: [
			{ id: "data", command: `echo '{"val":"0.9"}'` },
			{ id: "check", command: 'echo "yes"', when: "$data.json.val >= 0.8" },
		],
	});
	assert.deepEqual(result.output, ["yes\n"]);
});

test("a guard before && or || keeps a comparison from failing", async () => {
	const result = await runWorkflow({
		steps: [
			{ id: "data", command: `echo '{"val":null}'` },
			{ id: "a", command: 'echo "a"', when: "$data.json.val != null && $data.json.val > 1" },
			{ id: "b", command: 'echo "b"', when: "$data.json.val == null || $data.json.val > 1" },
		],
	});
	assert.deepEqual(result.output, ["b\n"]);
});

test("existing == and != still work with new operators", async () => {
	const result = await runWorkflow({
		steps: [
			{ id: "data", command: 'node -e "process.stdout.write(JSON.stringify({status:\\"ok\\"}))"' },
			{ id: "check", command: 'echo "good"', when: '$data.json.status == "ok"' },
		],
	});
	assert.equal(result.status, "ok");
	assert.deepEqual(result.output, ["good\n"]);
});

test("a misspelled key is rejected instead of ignored", async () => {
	await assert.rejects(
		runWorkflow({
			steps: [
				{ id: "data", command: "echo '{\"ok\":false}'" },
				{ id: "danger", command: "echo ran", wehn: "$data.json.ok == true" },
			],
		}),
		/Workflow step danger has unknown key "wehn" \(did you mean "when"\?\)/,
	);
	for (const [workflow, message] of [
		[{ steps: [{ id: "a", run: "echo a", next: "a" }] }, /step a has unknown key "next"/],
		[{ step: [], steps: [{ id: "a", run: "echo a" }] }, /Workflow file has unknown key "step"/],
		[
			{ steps: [{ id: "a", run: "echo a", retry: { maximum: 2 } }] },
			/step a retry has unknown key "maximum"/,
		],
		[
			{ steps: [{ id: "a", approval: { promt: "ok?" } }] },
			/step a approval has unknown key "promt" \(did you mean "prompt"\?\)/,
		],
		[
			{
				steps: [{ id: "l", for_each: "$x.json", steps: [{ id: "s", run: "echo", wehn: "true" }] }],
			},
			/step l sub-step s has unknown key "wehn"/,
		],
		[
			{ steps: [{ id: "p", parallel: { branches: [{ id: "b", run: "echo", when: "true" }] } }] },
			/parallel branch b has unknown key "when"/,
		],
	] as const) {
		await assert.rejects(runWorkflow(workflow), message);
	}
});

test("a guard works for a missing field as well as for null", async () => {
	const result = await runWorkflow({
		steps: [
			{ id: "data", command: `echo '{}'` },
			{ id: "a", command: 'echo "a"', when: "$data.json.val != null && $data.json.val > 1" },
			{ id: "b", command: 'echo "b"', when: "$data.json.val == null" },
		],
	});
	assert.deepEqual(result.output, ["b\n"]);
});

test("condition syntax is checked when the workflow loads, before any step runs", async () => {
	await assert.rejects(
		runWorkflow({
			steps: [
				{ id: "first", command: "echo first" },
				{ id: "x", command: "echo x", when: "$first.json.ok = 1" },
			],
		}),
		/x when is not a valid condition/,
	);
});

test("dry-run plans a comparison on step output instead of failing on its placeholder", async () => {
	const result = await runWorkflowFile({
		filePath: await (async () => {
			const dir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-cond-dry-"));
			const file = path.join(dir, "w.lobster");
			await fsp.writeFile(
				file,
				JSON.stringify({
					steps: [
						{ id: "s", command: "echo '{\"v\":1}'" },
						{ id: "big", command: "echo big", when: "$s.json.v > 0.8" },
					],
				}),
			);
			return file;
		})(),
		ctx: {
			stdin: process.stdin,
			stdout: process.stdout,
			stderr: new (await import("node:stream")).PassThrough(),
			env: { ...process.env },
			mode: "tool",
			dryRun: true,
		} as any,
	});
	assert.equal(result.status, "ok");
});

test("a malformed approval or input in a for_each sub-step is rejected, not run unguarded", async () => {
	for (const gate of [{ approval: 1 }, { approval: [] }, { approval: "  " }, { input: "name?" }])
		await assert.rejects(
			runWorkflow({
				steps: [
					{ id: "items", command: "echo '[1]'" },
					{ id: "loop", for_each: "$items.json", steps: [{ id: "s", command: "echo s", ...gate }] },
				],
			}),
			/approval must be true, a prompt string, or an object|approval prompt cannot be blank|input must be an object/,
			JSON.stringify(gate),
		);
});

test("two missing values are not equal to each other", async () => {
	const result = await runWorkflow({
		steps: [
			{ id: "a", command: `echo '{}'` },
			{ id: "b", command: `echo '{}'` },
			{ id: "same", command: 'echo "same"', when: "$a.json.token == $b.json.token" },
			{ id: "end", command: 'echo "end"' },
		],
	});
	assert.deepEqual(result.output, ["end\n"]);
});

test("a parenthesised null guards a missing field too", async () => {
	const result = await runWorkflow({
		steps: [
			{ id: "data", command: `echo '{}'` },
			{ id: "a", command: 'echo "a"', when: "$data.json.val != (null) && $data.json.val > 1" },
			{ id: "b", command: 'echo "b"', when: "($data.json.val) == (null)" },
		],
	});
	assert.deepEqual(result.output, ["b\n"]);
});

test("comparing two fields never matches on a missing and a null value", async () => {
	const result = await runWorkflow({
		steps: [
			{ id: "a", command: `echo '{"token":null}'` },
			{ id: "b", command: `echo '{}'` },
			{ id: "same", command: 'echo "same"', when: "$a.json.token == $b.json.token" },
			{ id: "end", command: 'echo "end"' },
		],
	});
	assert.deepEqual(result.output, ["end\n"]);
});

test("a null written in a list matches a missing field, as == null does", async () => {
	const result = await runWorkflow({
		steps: [
			{ id: "data", command: `echo '{}'` },
			{ id: "a", command: 'echo "a"', when: '$data.json.val in [null, ""]' },
			{ id: "b", command: 'echo "b"', when: "[null] contains $data.json.val" },
			{ id: "c", command: 'echo "c"', when: '$data.json.val in ["x"]' },
			{
				id: "report",
				command: 'echo "matched"',
				when: "$a.skipped != true && $b.skipped != true && $c.skipped == true",
			},
		],
	});
	assert.equal(result.status, "ok");
	assert.deepEqual(result.output, ["matched\n"]);
});
