import test from "node:test";
import assert from "node:assert/strict";
import { promises as fsp } from "node:fs";
import os from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

import { resumeToolRequest, runToolRequest } from "../src/core/index.js";
import { advanceDueRuns } from "../src/engine/due.js";

async function setup(t: test.TestContext, workflow: unknown, env: Record<string, string> = {}) {
	const dir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-loops-"));
	t.after(() => fsp.rm(dir, { recursive: true, force: true }));
	const filePath = path.join(dir, "flow.lobster");
	await fsp.writeFile(filePath, JSON.stringify(workflow, null, 2), "utf8");
	return {
		dir,
		filePath,
		env: { ...process.env, LOBSTER_STATE_DIR: path.join(dir, "state"), ...env },
	};
}

const json = (value: unknown) => `echo '${JSON.stringify(value)}'`;

type Envelope = Awaited<ReturnType<typeof runToolRequest>>;

/** Answer each approval the run asks for with the next decision, until it stops asking. */
async function answer(first: Envelope, decisions: boolean[], env: NodeJS.ProcessEnv) {
	let result = first;
	const prompts: unknown[] = [];
	for (const approved of decisions) {
		assert.equal(result.status, "needs_approval", JSON.stringify(result));
		prompts.push(result.requiresApproval!.items);
		result = await resumeToolRequest({
			token: result.requiresApproval!.resumeToken!,
			approved,
			ctx: { env },
		});
	}
	return { result, prompts };
}

/** One approval per draft; a "no" skips that draft instead of cancelling the run. */
const drafts = (onReject?: "continue") => ({
	steps: [
		{ id: "drafts", command: json([{ to: "a" }, { to: "b" }, { to: "c" }]) },
		{
			id: "each",
			for_each: "$drafts.json",
			steps: [
				{
					id: "confirm",
					command: "cat",
					stdin: "$item.json",
					approval: { prompt: "Send it?", ...(onReject ? { on_reject: onReject } : {}) },
				},
				{ id: "send", command: json({ sent: true }), when: "$confirm.approved" },
			],
		},
	],
});

test("an approval inside for_each asks once per item, and on_reject: continue skips the item", async (t) => {
	const { filePath, env } = await setup(t, drafts("continue"));
	const { result, prompts } = await answer(
		await runToolRequest({ filePath, ctx: { env } }),
		[true, false, true],
		env,
	);
	assert.deepEqual(prompts, [[{ to: "a" }], [{ to: "b" }], [{ to: "c" }]]);
	assert.equal(result.status, "ok", JSON.stringify(result));
	assert.deepEqual(
		(result.output as any[]).map((entry) => [entry.item.to, entry.send ?? null]),
		[
			["a", { sent: true }],
			["b", null],
			["c", { sent: true }],
		],
	);
});

test("without on_reject, a no inside a loop cancels the run", async (t) => {
	const { filePath, env } = await setup(t, drafts());
	const { result } = await answer(
		await runToolRequest({ filePath, ctx: { env } }),
		[true, false],
		env,
	);
	assert.equal(result.status, "cancelled");
});

test("a rejection that lets the run go on is checked against the required approver", async (t) => {
	const { filePath, env } = await setup(t, {
		steps: [
			{
				id: "gate",
				approval: { prompt: "Ok?", on_reject: "continue", required_approver: "alice" },
			},
			{ id: "after", command: "echo after" },
		],
	});
	const asked = await runToolRequest({ filePath, ctx: { env } });
	const refused = await resumeToolRequest({
		token: asked.requiresApproval!.resumeToken!,
		approved: false,
		ctx: { env: { ...env, LOBSTER_APPROVAL_APPROVED_BY: "mallory" } },
	});
	assert.equal(refused.ok, false);
	assert.match(refused.error!.message, /alice/);
	const decided = await resumeToolRequest({
		token: asked.requiresApproval!.resumeToken!,
		approved: false,
		ctx: { env: { ...env, LOBSTER_APPROVAL_APPROVED_BY: "alice" } },
	});
	assert.equal(decided.status, "ok", JSON.stringify(decided));
	assert.deepEqual(decided.output, ["after\n"]);
});

test("a loop's timeout covers its work, not the time a person takes to answer", async (t) => {
	const { filePath, env } = await setup(t, {
		steps: [
			{ id: "items", command: json([1, 2]) },
			{
				id: "each",
				for_each: "$items.json",
				timeout_ms: 300,
				steps: [{ id: "ok", approval: "Go on?" }],
			},
			{ id: "after", command: "echo after" },
		],
	});
	let result = await runToolRequest({ filePath, ctx: { env } });
	for (let i = 0; i < 2; i++) {
		await sleep(450);
		result = await resumeToolRequest({
			token: result.requiresApproval!.resumeToken!,
			approved: true,
			ctx: { env },
		});
	}
	assert.equal(result.status, "ok", JSON.stringify(result));
	assert.deepEqual(result.output, ["after\n"]);
});

test("repeat polls until a condition holds, sleeping between rounds", async (t) => {
	const { dir, filePath, env } = await setup(t, {}, { LOBSTER_INLINE_SLEEP_MS: "0" });
	const counter = path.join(dir, "count");
	// The third check reports done. Every sleep pauses the run until it is due.
	await fsp.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{
					id: "poll",
					repeat: { until: "$check.json.state == done", max: 10 },
					timeout_ms: 200,
					steps: [
						{
							id: "check",
							run: `n=$(( $(cat '${counter}' 2>/dev/null || echo 0) + 1 )); echo $n > '${counter}'; if [ $n -ge 3 ]; then echo '{"state":"done"}'; else echo '{"state":"running"}'; fi`,
						},
						{ id: "pause", sleep: "250ms", when: "$check.json.state == running" },
					],
				},
				{
					id: "report",
					run: `echo "$ROUNDS $STATE $PAUSED" > '${path.join(dir, "report")}'`,
					env: { ROUNDS: "$poll.rounds", STATE: "$check.json.state", PAUSED: "$pause.skipped" },
				},
			],
		}),
	);
	const ctx = { env, stdin: process.stdin, stdout: process.stdout, stderr: process.stderr };
	let result = await runToolRequest({ filePath, ctx: { env } });
	assert.equal(result.status, "waiting", JSON.stringify(result));
	for (let wakes = 0; wakes < 2; wakes++) {
		await sleep(300);
		const { advanced } = await advanceDueRuns({ ctx: { ...ctx, mode: "tool" } as any });
		assert.equal(advanced.length, 1);
		assert.equal(advanced[0].status, wakes === 0 ? "waiting" : "ok", JSON.stringify(advanced));
	}
	// Three rounds; the last one's steps are what later steps see.
	assert.equal(await fsp.readFile(path.join(dir, "report"), "utf8"), "3 done true\n");
});

test("each round sees the round before it; repeat fails after max rounds unless on_max: continue", async (t) => {
	const counting = (onMax?: "continue") => ({
		steps: [
			{
				id: "loop",
				repeat: { until: "$step.json.n >= 99", max: 3, ...(onMax ? { on_max: onMax } : {}) },
				steps: [
					// Reads its own result from the round before (none in the first round).
					{
						id: "step",
						run: `node -e 'const p = Number(process.env.PREV); console.log(JSON.stringify({ n: (Number.isFinite(p) ? p : 0) + 1 }))'`,
						env: { PREV: "$step.json.n" },
					},
				],
			},
			{ id: "after", command: "cat", stdin: "$loop.json" },
		],
	});
	const failing = await setup(t, counting());
	const failed = await runToolRequest({ filePath: failing.filePath, ctx: { env: failing.env } });
	assert.equal(failed.ok, false);
	assert.match(failed.error!.message, /loop repeat: until did not hold after 3 rounds/);

	const ending = await setup(t, counting("continue"));
	const ok = await runToolRequest({ filePath: ending.filePath, ctx: { env: ending.env } });
	assert.equal(ok.status, "ok", JSON.stringify(ok));
	assert.deepEqual(ok.output, [{ n: 3 }]);
});

test("an approval inside repeat is asked each round, and the answers decide the loop", async (t) => {
	const { filePath, env } = await setup(t, {
		steps: [
			{
				id: "rounds",
				repeat: { until: "!$more.approved", max: 5 },
				steps: [
					{ id: "work", command: "echo working" },
					{ id: "more", approval: { prompt: "Another round?", on_reject: "continue" } },
				],
			},
		],
	});
	const { result } = await answer(
		await runToolRequest({ filePath, ctx: { env } }),
		[true, true, false],
		env,
	);
	assert.equal(result.status, "ok", JSON.stringify(result));
});

test("repeat and for_each refuse a retry around a gate, and the until condition is checked at load", async (t) => {
	const load = async (step: object) => {
		const { filePath, env } = await setup(t, { steps: [{ id: "first", command: "echo 1" }, step] });
		return runToolRequest({ filePath, ctx: { env } });
	};
	const retried = await load({
		id: "loop",
		repeat: { until: true, max: 2 },
		retry: { max: 3 },
		steps: [{ id: "wait", sleep: "1s" }],
	});
	assert.match(
		retried.error!.message,
		/repeat with retry cannot contain approval, input, sleep or wait/,
	);
	const nested = await load({
		id: "loop",
		for_each: "$first.json",
		retry: { max: 3 },
		steps: [
			{ id: "pick", switch: { cases: [{ when: true, steps: [{ id: "ask", approval: "Ok?" }] }] } },
		],
	});
	assert.match(nested.error!.message, /for_each with retry cannot contain approval/);
	const typo = await load({
		id: "loop",
		repeat: { until: "$chek.json.done", max: 2 },
		steps: [{ id: "check", command: "echo '{}'" }],
	});
	assert.match(
		typo.error!.message,
		/until is not a valid condition: .*no step "chek" runs before it/,
	);
});
