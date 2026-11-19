import test from "node:test";
import assert from "node:assert/strict";
import { promises as fsp } from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";

import { resumeToolRequest, runToolRequest } from "../src/core/index.js";
import { runWorkflowFile } from "../src/engine/runner.js";

async function setup(t: test.TestContext, workflow: unknown) {
	const dir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-switch-"));
	t.after(() => fsp.rm(dir, { recursive: true, force: true }));
	const filePath = path.join(dir, "flow.lobster");
	await fsp.writeFile(filePath, JSON.stringify(workflow, null, 2), "utf8");
	const env = { ...process.env, LOBSTER_STATE_DIR: path.join(dir, "state") };
	return { dir, filePath, env };
}

const json = (value: unknown) => `echo '${JSON.stringify(value)}'`;

/** Classify, route on the label, then report what the route produced and what was skipped. */
const router = (label: string) => ({
	steps: [
		{ id: "classify", command: json({ label, score: 0.9 }) },
		{
			id: "route",
			switch: {
				cases: [
					{
						when: "$classify.json.label == refund && $classify.json.score >= 0.8",
						steps: [{ id: "refund", command: json({ action: "refund" }) }],
					},
					{
						when: "$classify.json.label in [spam, junk]",
						steps: [
							{ id: "spam", command: json({ action: "drop" }) },
							{ id: "log", command: json({ action: "drop", logged: true }) },
						],
					},
					{
						when: '$classify.json.label matches "^urgent-"',
						steps: [{ id: "page", command: json({ action: "page" }) }],
					},
				],
				else: [{ id: "triage", command: json({ action: "triage" }) }],
			},
		},
		{
			id: "report",
			command: "cat",
			stdin: "$route.json",
		},
	],
});

test("a switch runs the first case that holds, and its result is that case's output", async (t) => {
	for (const [label, expected] of [
		["refund", { action: "refund" }],
		["junk", { action: "drop", logged: true }],
		["urgent-outage", { action: "page" }],
		["question", { action: "triage" }],
	] as const) {
		const { filePath, env } = await setup(t, router(label));
		const result = await runToolRequest({ filePath, ctx: { env } });
		assert.equal(result.status, "ok", JSON.stringify(result));
		assert.deepEqual(result.output, [expected], label);
	}
});

test("a switch records which case ran", async (t) => {
	const { filePath, env } = await setup(t, {
		steps: [
			{ id: "n", command: json({ v: 2 }) },
			{
				id: "route",
				switch: {
					cases: [
						{ when: "$n.json.v == 1", steps: [{ id: "one", command: "echo one" }] },
						{ when: "$n.json.v == 2", steps: [{ id: "two", command: "echo two" }] },
					],
					else: [],
				},
			},
			{ id: "which", command: "echo case-2", when: "$route.case == 2 && $one.skipped" },
		],
	});
	const result = await runToolRequest({ filePath, ctx: { env } });
	assert.deepEqual(result.output, ["case-2\n"]);
});

test("a switch with no matching case fails unless it has an else, which may be empty", async (t) => {
	const cases = [{ when: false, steps: [{ id: "never", command: "echo never" }] }];
	const failing = await setup(t, { steps: [{ id: "route", switch: { cases } }] });
	const failed = await runToolRequest({ filePath: failing.filePath, ctx: { env: failing.env } });
	assert.equal(failed.ok, false);
	assert.match(failed.error!.message, /route switch: no case matched and there is no else/);

	const empty = await setup(t, {
		steps: [
			{ id: "route", switch: { cases, else: [] } },
			{ id: "after", command: "echo after" },
		],
	});
	const ok = await runToolRequest({ filePath: empty.filePath, ctx: { env: empty.env } });
	assert.deepEqual(ok.output, ["after\n"]);
});

test("an approval inside a case pauses the run and resumes in the same case", async (t) => {
	const { filePath, env } = await setup(t, {
		steps: [
			{ id: "classify", command: json({ label: "refund" }) },
			{
				id: "route",
				switch: {
					cases: [
						{
							when: "$classify.json.label == refund",
							steps: [
								{ id: "confirm", command: json({ amount: 10 }), approval: "Refund 10?" },
								{ id: "pay", command: json({ paid: true }), when: "$confirm.approved" },
							],
						},
					],
					else: [{ id: "ignore", command: "echo ignored" }],
				},
			},
			{ id: "done", command: "cat", stdin: "$route.json" },
		],
	});
	const paused = await runToolRequest({ filePath, ctx: { env } });
	assert.equal(paused.status, "needs_approval", JSON.stringify(paused));
	const resumed = await resumeToolRequest({
		token: paused.requiresApproval!.resumeToken,
		approved: true,
		ctx: { env },
	});
	assert.equal(resumed.status, "ok", JSON.stringify(resumed));
	assert.deepEqual(resumed.output, [{ paid: true }]);
});

test("switches nest, and steps of every case are known to later steps", async (t) => {
	const { filePath, env } = await setup(t, {
		steps: [
			{ id: "n", command: json({ a: 1, b: 2 }) },
			{
				id: "outer",
				switch: {
					cases: [
						{
							when: "$n.json.a == 1",
							steps: [
								{
									id: "inner",
									switch: {
										cases: [
											{ when: "$n.json.b == 1", steps: [{ id: "b1", command: "echo b1" }] },
											{ when: "$n.json.b == 2", steps: [{ id: "b2", command: json({ b: 2 }) }] },
										],
									},
								},
							],
						},
					],
					else: [{ id: "a_other", command: "echo other" }],
				},
			},
			{
				id: "check",
				command: "echo ok",
				when: "$b2.json.b == 2 && $b1.skipped && $a_other.skipped && $inner.case == 2",
			},
		],
	});
	const result = await runToolRequest({ filePath, ctx: { env } });
	assert.deepEqual(result.output, ["ok\n"]);
});

test("a case cannot read the steps of another case", async (t) => {
	const { filePath, env } = await setup(t, {
		steps: [
			{
				id: "route",
				switch: {
					cases: [
						{ when: true, steps: [{ id: "a", command: "echo a" }] },
						{ when: "$a.skipped", steps: [{ id: "b", command: "echo b" }] },
					],
				},
			},
		],
	});
	const result = await runToolRequest({ filePath, ctx: { env } });
	assert.equal(result.ok, false);
	assert.match(
		result.error!.message,
		/case 2 when is not a valid condition: .*no step "a" runs before it/,
	);
});

test("dry-run follows a case it can decide and lists every case it cannot", async (t) => {
	const plan = async (workflow: unknown) => {
		const { filePath, env } = await setup(t, workflow);
		const stderr = new PassThrough();
		let text = "";
		stderr.on("data", (chunk) => (text += chunk));
		await runWorkflowFile({
			filePath,
			ctx: {
				stdin: process.stdin,
				stdout: process.stdout,
				stderr,
				env,
				mode: "tool",
				dryRun: true,
			} as any,
		});
		return text;
	};
	const decided = await plan({
		steps: [
			{ id: "gate", command: "echo hi", approval: "go?" },
			{
				id: "route",
				switch: {
					cases: [
						{ when: "!$gate.approved", steps: [{ id: "stop", command: "echo stop" }] },
						{ when: "$gate.approved", steps: [{ id: "go", command: "echo go" }] },
					],
					else: [{ id: "neither", command: "echo neither" }],
				},
			},
		],
	});
	assert.match(decided, /case 1: when !\$gate\.approved {2}\[condition: false\]/);
	assert.match(decided, /case 2: when \$gate\.approved {2}\[runs\]\n\s+1\. go {2}\[shell\]/);
	assert.match(decided, /else {2}\[not reached\]/);

	const open = await plan({
		steps: [
			{ id: "classify", command: json({ label: "x" }) },
			{
				id: "route",
				switch: {
					cases: [{ when: "$classify.json.label == x", steps: [{ id: "x", command: "echo x" }] }],
					else: [{ id: "y", command: "echo y" }],
				},
			},
			{ id: "after", command: "echo after", when: "$x.skipped" },
		],
	});
	assert.match(open, /case 1: .* {2}\[may run — depends on step output\]/);
	assert.match(open, /else {2}\[may run — depends on step output\]/);
	assert.match(open, /after {2}\[shell\]\n\s+when: \$x\.skipped {2}\[depends on step output/);
});

test("a switch runs its case with the switch's env and cwd, as other compound steps do", async (t) => {
	const { dir, filePath, env } = await setup(t, {
		steps: [
			{
				id: "route",
				env: { GREETING: "hello" },
				switch: {
					cases: [{ when: "true", steps: [{ id: "say", command: 'echo "$GREETING $(pwd)"' }] }],
				},
			},
		],
	});
	const sub = path.join(dir, "sub");
	await fsp.mkdir(sub);
	const workflow = JSON.parse(await fsp.readFile(filePath, "utf8"));
	workflow.steps[0].cwd = sub;
	await fsp.writeFile(filePath, JSON.stringify(workflow), "utf8");
	const result = await runToolRequest({ filePath, ctx: { env } });
	assert.equal(result.status, "ok", JSON.stringify(result));
	assert.deepEqual(result.output, [`hello ${await fsp.realpath(sub)}\n`]);
});

test("steps skip_rest left unrun are skipped, so later steps can read them", async (t) => {
	const { filePath, env } = await setup(t, {
		steps: [
			{
				id: "route",
				switch: {
					cases: [
						{
							when: "true",
							steps: [
								{ id: "a", run: "exit 1", on_error: "skip_rest" },
								{ id: "b", run: "echo b" },
							],
						},
					],
				},
			},
			{ id: "c", when: "$b.skipped", run: "echo b-was-skipped" },
		],
	});
	const result = await runToolRequest({ filePath, ctx: { env } });
	assert.equal(result.status, "ok", JSON.stringify(result));
	assert.deepEqual(result.output, ["b-was-skipped\n"]);
});

test("a typo in a later pipeline step is refused before any step acts", async (t) => {
	const { dir, filePath, env } = await setup(t, { steps: [] });
	const marker = path.join(dir, "acted");
	await fsp.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{ id: "first", run: `echo acted > ${JSON.stringify(marker)}` },
				{
					id: "route",
					switch: {
						cases: [{ when: "true", steps: [{ id: "bad", pipeline: "exec --jsn echo 1" }] }],
					},
				},
			],
		}),
	);
	const result = await runToolRequest({ filePath, ctx: { env } });
	assert.equal(result.ok, false);
	assert.match(result.error?.message ?? "", /exec has no flag --jsn/);
	await assert.rejects(fsp.access(marker), "the first step did not run");
});

test("a compound step whose failure on_error tolerates leaves its inner steps skipped", async (t) => {
	const failing: Record<string, unknown> = {
		switch: {
			id: "s",
			on_error: "continue",
			switch: { cases: [{ when: "true", steps: [{ id: "inner", run: "exit 3" }] }] },
		},
		parallel: {
			id: "s",
			on_error: "continue",
			parallel: {
				branches: [
					{ id: "inner", run: "echo ok" },
					{ id: "bad", run: "exit 3" },
				],
			},
		},
		repeat: {
			id: "s",
			on_error: "continue",
			repeat: { until: "true", max: 2 },
			steps: [{ id: "inner", run: "exit 3" }],
		},
	};
	for (const [kind, step] of Object.entries(failing)) {
		const { filePath, env } = await setup(t, {
			steps: [step, { id: "after", when: "$inner.skipped", run: "echo skipped" }],
		});
		const result = await runToolRequest({ filePath, ctx: { env } });
		assert.equal(result.status, "ok", `${kind}: ${JSON.stringify(result)}`);
		assert.deepEqual(result.output, ["skipped\n"], kind);
	}
});
