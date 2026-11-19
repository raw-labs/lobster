import test from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { createDefaultRegistry, resumeToolRequest, runToolRequest } from "../src/core/index.js";
import { defineCommand } from "../src/commands/define.js";
import { Lobster } from "../src/sdk/Lobster.js";
import { abandonWorkflowRun, advanceWorkflowRun } from "../src/engine/runner.js";
import { unresolvedActivities } from "../src/engine/checkpoint.js";
import { openStoreSession } from "../src/persistence/session.js";

const repoRoot = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const cli = path.join(repoRoot, "bin", "lobster.js");

async function fixture(t: test.TestContext) {
	const dir = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-retry-"));
	t.after(() => fs.rm(dir, { recursive: true, force: true }));
	const env = { ...process.env, LOBSTER_STATE_DIR: path.join(dir, "state"), RETRY_DIR: dir };
	const ran = async (name: string) =>
		(await fs.readFile(path.join(dir, name), "utf8").catch(() => "")).split("\n").filter(Boolean)
			.length;
	const advance = (runId: string, registry = createDefaultRegistry()) =>
		advanceWorkflowRun({
			runId,
			ctx: {
				env,
				registry,
				mode: "tool",
				stdin: process.stdin,
				stdout: process.stdout,
				stderr: process.stderr,
			},
		});
	return { dir, env, ran, advance };
}

/** A step that fails until `$RETRY_DIR/<flag>` exists, counting its calls. */
const flaky = (name: string, flag: string) =>
	`echo x >> "$RETRY_DIR/${name}"; test -f "$RETRY_DIR/${flag}"`;

test("a failed workflow run continues from the step that failed", async (t) => {
	const { dir, env, ran, advance } = await fixture(t);
	const filePath = path.join(dir, "flow.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{ id: "one", run: 'echo x >> "$RETRY_DIR/one"; echo \'{"n":1}\'' },
				{ id: "two", run: flaky("two", "fixed") },
				{
					id: "three",
					run: 'echo x >> "$RETRY_DIR/three"; echo "$N"',
					env: { N: "$one.json.n" },
				},
			],
		}),
	);
	const first = await runToolRequest({ filePath, ctx: { env } });
	assert.equal(first.ok, false);
	assert.ok(first.runId);

	// Still failing: the run stays failed and can be retried again.
	await assert.rejects(advance(first.runId!), /workflow command failed/);
	assert.equal(await ran("two"), 2);

	await fs.writeFile(path.join(dir, "fixed"), "");
	const retried = await advance(first.runId!);
	assert.equal(retried.status, "ok");
	assert.deepEqual(retried.output, [1], "a later step still sees earlier results");
	assert.equal(await ran("one"), 1, "a completed step does not run again");
	assert.equal(await ran("two"), 3);
	assert.equal(await ran("three"), 1);

	// Advancing a completed run returns its result; nothing runs again.
	assert.deepEqual((await advance(first.runId!)).output, [1]);
	assert.equal(await ran("three"), 1);
});

test("a retried pipeline step asks its approval again: the items may differ", async (t) => {
	const { dir, env, ran, advance } = await fixture(t);
	const filePath = path.join(dir, "flow.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{
					id: "send",
					pipeline: `exec --json --shell 'echo x >> "$RETRY_DIR/count"; wc -l < "$RETRY_DIR/count"' | approve --prompt Send? | exec --shell 'test -f "$RETRY_DIR/fixed"'`,
				},
			],
		}),
	);
	const paused = await runToolRequest({ filePath, ctx: { env } });
	assert.equal(paused.status, "needs_approval", JSON.stringify(paused));
	assert.deepEqual(paused.requiresApproval!.items, [1]);
	const failed = await resumeToolRequest({
		token: paused.requiresApproval!.resumeToken,
		approved: true,
		ctx: { env },
	});
	assert.equal(failed.ok, false);

	await fs.writeFile(path.join(dir, "fixed"), "");
	const retried = await advance(paused.runId!);
	assert.equal(retried.status, "needs_approval", JSON.stringify(retried));
	assert.deepEqual(retried.requiresApproval!.items, [2], "the new items are asked about");
	assert.equal(await ran("count"), 2);
});

test("retrying a failed child workflow reruns only its failed step", async (t) => {
	const { dir, env, ran, advance } = await fixture(t);
	await fs.writeFile(
		path.join(dir, "child.lobster"),
		JSON.stringify({
			steps: [
				{ id: "prepare", run: 'echo x >> "$RETRY_DIR/prepare"' },
				{ id: "send", run: flaky("send", "fixed") },
			],
		}),
	);
	const filePath = path.join(dir, "parent.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({ steps: [{ id: "child", workflow: "child.lobster" }] }),
	);
	const first = await runToolRequest({ filePath, ctx: { env } });
	assert.equal(first.ok, false);
	await fs.writeFile(path.join(dir, "fixed"), "");
	const retried = await advance(first.runId!);
	assert.equal(retried.status, "ok");
	assert.equal(await ran("prepare"), 1);
	assert.equal(await ran("send"), 2);
});

test("a failed step that may have acted is not run again", async (t) => {
	const { dir, env, advance } = await fixture(t);
	let sends = 0;
	const send = defineCommand({
		name: "test.send",
		meta: {
			description: "Send something, then fail",
			argsSchema: { type: "object", properties: {} },
			sideEffects: ["sends_email"],
			repeatable: false,
		},
		help: () => "test.send",
		async run({ ctx }: any) {
			ctx.onNonRetryableSideEffect?.();
			sends++;
			throw new Error("the reply was lost");
		},
	});
	const base = createDefaultRegistry();
	const registry = { ...base, get: (name: string) => (name === send.name ? send : base.get(name)) };
	const filePath = path.join(dir, "send.lobster");
	await fs.writeFile(filePath, JSON.stringify({ steps: [{ id: "send", pipeline: "test.send" }] }));
	const first = await runToolRequest({ filePath, ctx: { env, registry } });
	assert.equal(first.ok, false);
	assert.equal(sends, 1);
	await assert.rejects(advance(first.runId!, registry), /may have acted before it failed/);
	assert.equal(sends, 1);
	const store = await openStoreSession({ directory: env.LOBSTER_STATE_DIR });
	try {
		assert.equal((await store.getRunState(first.runId!))!.status, "failed");
	} finally {
		await store.close();
	}
});

test("a resume that fails names its run, so the caller can retry it", async (t) => {
	const { dir, env, ran, advance } = await fixture(t);
	const filePath = path.join(dir, "flow.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{ id: "ok", approval: "Continue?" },
				{ id: "two", run: flaky("two", "fixed") },
			],
		}),
	);
	const paused = await runToolRequest({ filePath, ctx: { env } });
	assert.equal(paused.status, "needs_approval");
	const token = paused.requiresApproval!.resumeToken;
	const failed = await resumeToolRequest({ token, approved: true, ctx: { env } });
	assert.equal(failed.ok, false);
	assert.equal(failed.runId, paused.runId);
	// The answer stands: the token is spent, and the run is retried by its ID.
	const again = await resumeToolRequest({ token, approved: true, ctx: { env } });
	assert.equal(again.ok, false);
	assert.match(again.error!.message, /Resume state not found/);
	await fs.writeFile(path.join(dir, "fixed"), "");
	assert.equal((await advance(failed.runId!)).status, "ok");
	assert.equal(await ran("two"), 2);
});

test("a command cannot name another run in its failure", async (t) => {
	const { dir, env } = await fixture(t);
	const liar = defineCommand({
		name: "test.liar",
		meta: {
			description: "Fail with a made-up run ID",
			sideEffects: [],
			repeatable: true,
			argsSchema: { type: "object", properties: {} },
		},
		help: () => "test.liar",
		async run() {
			throw Object.assign(new Error("failed"), { runId: "someone-elses-run" });
		},
	});
	const base = createDefaultRegistry();
	const registry = { ...base, get: (name: string) => (name === liar.name ? liar : base.get(name)) };
	const filePath = path.join(dir, "liar.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{ id: "x", approval: "Go?" },
				{ id: "y", pipeline: "test.liar" },
			],
		}),
	);
	const paused = await runToolRequest({ filePath, ctx: { env, registry } });
	const failed = await resumeToolRequest({
		token: paused.requiresApproval!.resumeToken,
		approved: true,
		ctx: { env, registry },
	});
	assert.equal(failed.ok, false);
	assert.equal(failed.runId, paused.runId);
	const sdk = await new Lobster({ env })
		.pipe(() => {
			throw Object.assign(new Error("failed"), { runId: "someone-elses-run" });
		})
		.run();
	assert.ok(sdk.runId && sdk.runId !== "someone-elses-run");
});

test("an SDK host retries its failed run with Lobster.advance", async (t) => {
	const { env } = await fixture(t);
	let attempts = 0;
	let effects = 0;
	const pipeline = () =>
		new Lobster({ env })
			.pipe(() => [1])
			.pipe((items: number[]) => {
				attempts++;
				if (attempts === 1) throw new Error("temporary failure");
				return items.map((n) => n + 1);
			})
			.pipe((items: number[]) => {
				effects++;
				return items;
			});
	const first = await pipeline().run();
	assert.equal(first.ok, false);
	assert.ok(first.runId);
	const retried = await pipeline().advance(first.runId!);
	assert.equal(retried.status, "ok", JSON.stringify(retried));
	assert.deepEqual(retried.output, [2]);
	assert.equal(effects, 1);
});

test("CLI: `runs advance` retries a failed run", async (t) => {
	const { dir, env, ran } = await fixture(t);
	const filePath = path.join(dir, "flow.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{ id: "one", run: 'echo x >> "$RETRY_DIR/one"' },
				{ id: "two", run: flaky("two", "fixed") },
			],
		}),
	);
	const run = (...args: string[]) =>
		spawnSync(process.execPath, [cli, ...args], { encoding: "utf8", env });
	const first = JSON.parse(run("run", "--mode", "tool", "--file", filePath).stdout);
	assert.equal(first.ok, false);
	await fs.writeFile(path.join(dir, "fixed"), "");
	const advanced = run("runs", "advance", first.runId);
	assert.equal(advanced.status, 0, advanced.stderr);
	assert.equal(await ran("one"), 1);
	assert.equal(await ran("two"), 2);
});

test("a retried failed run gives the failed step its whole retry budget again", async (t) => {
	const { dir, env, ran, advance } = await fixture(t);
	const filePath = path.join(dir, "flow.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [{ id: "flaky", run: flaky("flaky", "fixed"), retry: { max: 3, delay_ms: 0 } }],
		}),
	);
	const first = await runToolRequest({ filePath, ctx: { env } });
	assert.equal(first.ok, false);
	assert.equal(await ran("flaky"), 3, "three attempts on the first run");

	await assert.rejects(advance(first.runId!), /workflow command failed/);
	assert.equal(await ran("flaky"), 6, "three more attempts after runs advance");

	await fs.writeFile(path.join(dir, "fixed"), "");
	assert.equal((await advance(first.runId!)).status, "ok");
	assert.equal(await ran("flaky"), 7);
});

test("a retried failed run gives the failed step a fresh timeout", async (t) => {
	const { dir, env, ran, advance } = await fixture(t);
	const filePath = path.join(dir, "flow.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({ steps: [{ id: "slow", run: flaky("slow", "fixed"), timeout_ms: 300 }] }),
	);
	const first = await runToolRequest({ filePath, ctx: { env } });
	assert.equal(first.ok, false);
	// The first attempt's deadline passes before the run is advanced.
	await new Promise((resolve) => setTimeout(resolve, 500));
	await fs.writeFile(path.join(dir, "fixed"), "");
	assert.equal((await advance(first.runId!)).status, "ok");
	assert.equal(await ran("slow"), 2);
});

test("a step that timed out failed: nothing is left with an unknown outcome, and it retries", async (t) => {
	const { dir, env, ran, advance } = await fixture(t);
	const filePath = path.join(dir, "flow.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{
					id: "slow",
					run: 'echo x >> "$RETRY_DIR/slow"; test -f "$RETRY_DIR/fixed" || sleep 5',
					timeout_ms: 300,
					retry: { max: 2, delay_ms: 0 },
				},
			],
		}),
	);
	const first = await runToolRequest({ filePath, ctx: { env } });
	assert.equal(first.ok, false);
	assert.equal(await ran("slow"), 2);
	const store = await openStoreSession({ env, readOnly: true });
	try {
		const run = await store.getRunState(first.runId!);
		assert.deepEqual(unresolvedActivities(run!.execution!), []);
	} finally {
		await store.close();
	}
	await fs.writeFile(path.join(dir, "fixed"), "");
	assert.equal((await advance(first.runId!)).status, "ok");
	assert.equal(await ran("slow"), 3);
});

test("a failed run whose parallel branch was cut off can be abandoned", async (t) => {
	const { dir, env } = await fixture(t);
	const filePath = path.join(dir, "flow.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{
					id: "both",
					parallel: {
						branches: [
							{ id: "fails", run: "sleep 0.2; exit 1" },
							{ id: "slow", run: "sleep 5" },
						],
					},
				},
			],
		}),
	);
	const first = await runToolRequest({ filePath, ctx: { env } });
	assert.equal(first.ok, false);
	const ctx = {
		env,
		mode: "tool" as const,
		stdin: process.stdin,
		stdout: process.stdout,
		stderr: process.stderr,
	};
	const abandoned = await abandonWorkflowRun({ runId: first.runId!, ctx });
	assert.equal(abandoned.status, "cancelled");
	assert.equal(abandoned.abandoned.length, 1);
	assert.match(abandoned.abandoned[0], /slow/);
});
