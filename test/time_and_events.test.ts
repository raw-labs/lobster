// Timers, deadlines, external events, due runs and heartbeats: the behaviour that lets a
// run-on-demand Lobster wait for time and the outside world without a server of its own.
import test from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promises as fs } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { setTimeout as sleep } from "node:timers/promises";
import { advanceWorkflowRun, runWorkflowFile } from "../src/engine/runner.js";
import { advanceDueRuns, signalEvent } from "../src/engine/due.js";
import { openStoreSession } from "../src/persistence/session.js";
import { decodeToken } from "../src/resume/token.js";
import { createDefaultRegistry } from "../src/commands/registry.js";
import { defineCommand } from "../src/commands/define.js";
import { interactiveStdin, workflowFixture } from "./helpers/workflow_runner.js";
import { crashAndRecover } from "./helpers/crash_recovery.js";

const exec = promisify(execFile);
const repoRoot = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const after = { id: "after", pipeline: "fake --name after" };

async function due(fx: Awaited<ReturnType<typeof workflowFixture>>) {
	return advanceDueRuns({ ctx: fx.ctx() as any });
}

test("an approval deadline cancels the run by default, once it is due", async (t) => {
	const fx = await workflowFixture(t);
	const asked = await fx.run({
		steps: [{ id: "ok", approval: { prompt: "Ship?", timeout: "1s" } }, after],
	});
	assert.equal(asked.status, "needs_approval");
	assert.deepEqual((await due(fx)).due, [], "not due before the deadline");
	await sleep(1050);
	const { advanced } = await due(fx);
	assert.deepEqual(
		advanced.map((a) => a.status),
		["cancelled"],
	);
	const execution = await fx.execution();
	assert.equal(execution.status, "cancelled");
	assert.match(execution.error ?? "", /timed out waiting/);
	assert.equal(fx.fake.calls("after"), 0);
});

test("on_timeout: continue lets the workflow branch on $step.timedOut", async (t) => {
	const fx = await workflowFixture(t);
	const asked = await fx.run({
		steps: [
			{ id: "ok", approval: { prompt: "Ship?", timeout: "200ms", on_timeout: "continue" } },
			{ id: "escalate", pipeline: "fake --name escalate", when: "$ok.timedOut" },
			{ id: "ship", pipeline: "fake --name ship", when: "$ok.approved" },
		],
	});
	await sleep(250);
	assert.deepEqual(
		(await due(fx)).advanced.map((a) => a.status),
		["ok"],
	);
	assert.equal(fx.fake.calls("escalate"), 1);
	assert.equal(fx.fake.calls("ship"), 0, "a timed-out approval is not an approval");
	assert.equal(asked.status, "needs_approval");
});

test("an answer after the deadline is refused; the timeout applies instead", async (t) => {
	const fx = await workflowFixture(t);
	const asked = await fx.run({
		steps: [{ id: "q", input: { prompt: "n?", responseSchema: {}, timeout: "150ms" } }, after],
	});
	await sleep(200);
	await assert.rejects(fx.resume(asked, { response: 1 }), /question expired at/);
	assert.equal((await fx.execution()).status, "waiting");
	await due(fx);
	assert.equal((await fx.execution()).status, "cancelled");
	assert.equal(fx.fake.calls("after"), 0);
});

test("an input deadline with continue completes the step without a response", async (t) => {
	const fx = await workflowFixture(t);
	await fx.run({
		steps: [
			{
				id: "q",
				input: { prompt: "n?", responseSchema: {}, timeout: "100ms", on_timeout: "continue" },
			},
			{ id: "fallback", pipeline: "fake --name fallback", when: "$q.timedOut" },
		],
	});
	await sleep(150);
	await due(fx);
	assert.equal(fx.fake.calls("fallback"), 1);
	assert.equal((await fx.execution()).status, "completed");
});

test("a sleep longer than the in-place limit pauses the run with its wake time", async (t) => {
	const fx = await workflowFixture(t);
	const result = await fx.run(
		{ steps: [{ id: "nap", sleep: "1500ms" }, after] },
		{ env: { LOBSTER_INLINE_SLEEP_MS: "50" } },
	);
	assert.equal(result.status, "waiting");
	assert.equal(result.waitingFor?.kind, "timer");
	const until = Date.parse((result.waitingFor as { until: string }).until);
	assert.ok(until > Date.now() && until - Date.now() <= 1500);
	// Advancing early pauses again rather than finishing ahead of time.
	const early = await advanceWorkflowRun({
		runId: result.runId!,
		ctx: fx.ctx({ env: { LOBSTER_INLINE_SLEEP_MS: "50" } }) as any,
	});
	assert.equal(early.status, "waiting");
	assert.equal(fx.fake.calls("after"), 0);
	await sleep(until - Date.now() + 20);
	assert.deepEqual(
		(await due(fx)).advanced.map((a) => a.status),
		["ok"],
	);
	assert.equal(fx.fake.calls("after"), 1);
});

test("an event delivered before the wait is reached is still consumed, once", async (t) => {
	const fx = await workflowFixture(t);
	const asked = await fx.run({
		steps: [
			{ id: "go", approval: "Go?" },
			{ id: "first", wait: { event: "deploy.done" } },
			{ id: "second", wait: { event: "deploy.done" } },
			after,
		],
	});
	const early = await signalEvent({ name: "deploy.done", data: { v: 1 }, ctx: fx.ctx() as any });
	assert.deepEqual(early.advanced, [], "nothing is waiting yet");
	const waiting = await fx.resume(asked, { approve: true });
	// `first` takes the buffered event; `second` needs another one.
	assert.equal(waiting.status, "waiting");
	assert.equal(waiting.waitingFor?.stepId, "second");
	const second = await signalEvent({ name: "deploy.done", data: { v: 2 }, ctx: fx.ctx() as any });
	assert.deepEqual(
		second.advanced.map((a) => a.status),
		["ok"],
	);
	assert.equal(fx.fake.calls("after"), 1);
});

test("event waits match on data, resolve references, and ignore events from before the run", async (t) => {
	const fx = await workflowFixture(t);
	await signalEvent({ name: "pr.merged", data: { pr: 7 }, ctx: fx.ctx() as any });
	fx.fake.script({ open: [{ output: { pr: 7 } }] });
	const waiting = await fx.run({
		steps: [
			{ id: "open", pipeline: "fake --name open" },
			{ id: "merged", wait: { event: "pr.merged", match: { pr: "$open.json.pr" } } },
			{ id: "report", pipeline: "fake --name after" },
		],
	});
	assert.equal(waiting.status, "waiting", "the event from before the run is not matched");
	const other = await signalEvent({ name: "pr.merged", data: { pr: 8 }, ctx: fx.ctx() as any });
	assert.deepEqual(other.advanced, []);
	const id = "delivery-1";
	const hit = await signalEvent({ name: "pr.merged", data: { pr: 7 }, id, ctx: fx.ctx() as any });
	assert.equal(hit.event.inserted, true);
	assert.deepEqual(
		hit.advanced.map((a) => a.status),
		["ok"],
	);
	const again = await signalEvent({ name: "pr.merged", data: { pr: 7 }, id, ctx: fx.ctx() as any });
	assert.equal(again.event.inserted, false, "a repeated delivery id is ignored");
	assert.equal(fx.fake.calls("after"), 1);
});

test("an event wait's timeout cancels or continues", async (t) => {
	for (const onTimeout of ["cancel", "continue"] as const) {
		const fx = await workflowFixture(t);
		await fx.run({
			steps: [
				{ id: "w", wait: { event: "never", timeout: "100ms", on_timeout: onTimeout } },
				{ id: "next", pipeline: "fake --name after", when: "$w.timedOut" },
			],
		});
		await sleep(150);
		await due(fx);
		assert.equal((await fx.execution()).status, onTimeout === "cancel" ? "cancelled" : "completed");
		assert.equal(fx.fake.calls("after"), onTimeout === "cancel" ? 0 : 1);
	}
});

test("resuming a timer or event wait points to advance and signal", async (t) => {
	const fx = await workflowFixture(t);
	const waiting = await fx.run({ steps: [{ id: "w", wait: { event: "x" } }] });
	const session = await openStoreSession({ env: fx.env });
	const run = (await session.getRunState(waiting.runId!))!;
	await session.close();
	const stateKey = Object.values(run.execution!.activations).find((f) => f.wait?.key)!.wait!.key!;
	await assert.rejects(
		runWorkflowFile({
			resume: decodeToken(
				Buffer.from(
					JSON.stringify({ protocolVersion: 1, v: 1, kind: "workflow-file", stateKey }),
				).toString("base64url"),
			),
			response: {},
			ctx: fx.ctx() as any,
		} as any),
		/deliver it with `lobster signal x`/,
	);
});

test("a crashed run becomes due when its lease lapses and continues where it stopped", async (t) => {
	const fx = await workflowFixture(t);
	fx.fake.script({ w: ["hang", "ok"] });
	const flowPath = await fx.write("flow.lobster", {
		steps: [
			{ id: "before", pipeline: "fake --name before" },
			{ id: "w", pipeline: "fake --name w", recovery: "repeat" },
			after,
		],
	});
	// The harness kills the worker mid-step and waits out its lease; recovery itself is ours.
	const { runId, killed } = await crashAndRecover(t, {
		dir: fx.dir,
		env: fx.env,
		flowPath,
		killWhen: ({ calls }) => calls("w") === 1,
		recover: false,
	});
	assert.ok(killed);
	const { due: dueRuns, advanced } = await due(fx);
	assert.deepEqual(dueRuns, [runId]);
	assert.deepEqual(
		advanced.map((a) => a.status),
		["ok"],
	);
	assert.equal(fx.fake.calls("before"), 1);
	assert.equal(fx.fake.calls("w"), 2, "the repeatable step ran again");
	assert.equal(fx.fake.calls("after"), 1);
});

test("blocked and finished runs are never due", async (t) => {
	const fx = await workflowFixture(t);
	fx.fake.script({ w: ["hang"] });
	const flowPath = await fx.write("flow.lobster", {
		steps: [{ id: "w", pipeline: "fake --name w" }, after],
	});
	await crashAndRecover(t, {
		dir: fx.dir,
		env: fx.env,
		flowPath,
		killWhen: ({ calls }) => calls("w") === 1,
		recover: false,
	});
	const first = await due(fx);
	assert.deepEqual(
		first.advanced.map((a) => a.status),
		["blocked"],
		"an uncertain step needs a person",
	);
	assert.deepEqual((await due(fx)).due, [], "and is not picked up again");
	assert.equal(fx.fake.calls("after"), 0);
});

test("a step without heartbeats is stopped; one that keeps beating finishes", async (t) => {
	const fx = await workflowFixture(t);
	const script = path.join(fx.dir, "beat.mjs");
	await fs.writeFile(
		script,
		`import fs from "node:fs";\nconst beat = process.argv[2] === "beat";\n` +
			`for (let i = 0; i < 8; i++) { if (beat) fs.writeFileSync(process.env.LOBSTER_HEARTBEAT_FILE, String(i)); await new Promise(r => setTimeout(r, 100)); }\n` +
			`process.stdout.write("done");\n`,
	);
	const step = (mode: string) => ({
		id: "s",
		run: `${JSON.stringify(process.execPath)} ${JSON.stringify(script)} ${mode}`,
		heartbeat_timeout_ms: 300,
	});
	await assert.rejects(fx.run({ steps: [step("quiet")] }), /showed no progress for 300ms/);
	const fx2 = await workflowFixture(t);
	const ok = await fx2.run({ steps: [step("beat")] });
	assert.deepEqual(ok.output, ["done"]);
});

test("a pipeline command shows progress with ctx.heartbeat()", async (t) => {
	const fx = await workflowFixture(t);
	const worker = (beat: boolean) =>
		defineCommand({
			name: "slow.work",
			meta: {
				description: "Works for a while",
				argsSchema: { type: "object", properties: {} },
				sideEffects: [],
				repeatable: true,
			},
			help: () => "slow.work\n",
			async run({ ctx }) {
				for (let i = 0; i < 6; i++) {
					if (beat) ctx.heartbeat();
					await sleep(100);
				}
				return {
					output: (async function* () {
						yield "worked";
					})(),
				};
			},
		});
	for (const beat of [true, false]) {
		const base = createDefaultRegistry();
		const command = worker(beat);
		const registry = { ...base, get: (n: string) => (n === "slow.work" ? command : base.get(n)) };
		const filePath = await fx.write(`p-${beat}.lobster`, {
			steps: [{ id: "p", pipeline: "slow.work", heartbeat_timeout_ms: 250 }],
		});
		const run = runWorkflowFile({ filePath, ctx: { ...(fx.ctx() as any), registry } });
		if (beat) assert.deepEqual((await run).output, ["worked"]);
		else await assert.rejects(run, /showed no progress for 250ms/);
	}
});

test("CLI: runs advance --due and signal", async (t) => {
	const fx = await workflowFixture(t);
	const bin = path.join(repoRoot, "bin", "lobster.js");
	const env = { ...fx.env, LOBSTER_INLINE_SLEEP_MS: "0" };
	const flow = await fx.write("cli.lobster", {
		steps: [
			{ id: "nap", sleep: "200ms" },
			{ id: "pr", wait: { event: "cli.event" } },
			{ id: "done", run: "echo finished $pr.json.n" },
		],
	});
	const started = JSON.parse(
		(await exec(process.execPath, [bin, "run", "--mode", "tool", "--file", flow], { env })).stdout,
	);
	assert.equal(started.status, "waiting");
	assert.equal(started.waitingFor.kind, "timer");
	await sleep(250);
	const advanced = JSON.parse(
		(await exec(process.execPath, [bin, "runs", "advance", "--due"], { env })).stdout,
	);
	assert.deepEqual(advanced.advanced, [
		{ runId: started.runId, status: "waiting", waitingFor: advanced.advanced[0].waitingFor },
	]);
	assert.equal(advanced.advanced[0].waitingFor.event, "cli.event");
	const signalled = JSON.parse(
		(await exec(process.execPath, [bin, "signal", "cli.event", "--data", '{"n":3}'], { env }))
			.stdout,
	);
	assert.deepEqual(signalled.advanced, [{ runId: started.runId, status: "ok" }]);
});

test("events are scoped to the caller that owns the run", async (t) => {
	const fx = await workflowFixture(t);
	const as = (callerId: string) => ({ ...(fx.ctx() as any), runMetadata: { callerId } });
	const filePath = await fx.write("scoped.lobster", {
		steps: [{ id: "w", wait: { event: "job.done" } }, after],
	});
	const waiting = await runWorkflowFile({ filePath, ctx: as("bob") });
	assert.equal(waiting.status, "waiting");
	const fromAlice = await signalEvent({ name: "job.done", id: "d1", ctx: as("alice") });
	assert.deepEqual(fromAlice.advanced, [], "another caller's event does not reach the run");
	const stillWaiting = await advanceWorkflowRun({ runId: waiting.runId!, ctx: as("bob") });
	assert.equal(stillWaiting.status, "waiting", "nor is it consumed when the run advances");
	const fromBob = await signalEvent({ name: "job.done", id: "d1", ctx: as("bob") });
	assert.equal(fromBob.event.inserted, true, "delivery IDs are per caller");
	assert.deepEqual(
		fromBob.advanced.map((a) => a.status),
		["ok"],
	);
	assert.equal(fx.fake.calls("after"), 1);
});

test("a match reference that resolves to nothing fails instead of matching every event", async (t) => {
	const fx = await workflowFixture(t);
	fx.fake.script({ send: [{ output: { other: 1 } }] });
	await assert.rejects(
		fx.run({
			steps: [
				{ id: "send", pipeline: "fake --name send" },
				{ id: "reply", wait: { event: "email.reply", match: { thread: "$send.json.thread" } } },
			],
		}),
		/wait\.match\.thread resolved to nothing/,
	);
});

test("an event received after the wait's deadline does not count", async (t) => {
	const fx = await workflowFixture(t);
	await fx.run({
		steps: [
			{ id: "w", wait: { event: "late", timeout: "100ms", on_timeout: "continue" } },
			{ id: "after", pipeline: "fake --name after", when: "$w.timedOut" },
		],
	});
	await sleep(150);
	// Delivered after the deadline, before anything advanced the run: the run times out.
	const { advanced } = await signalEvent({ name: "late", ctx: fx.ctx() as any });
	assert.deepEqual(
		advanced.map((a) => a.status),
		["ok"],
	);
	assert.equal(fx.fake.calls("after"), 1, "the step timed out instead of taking the late event");
});

test("an interactive deadline longer than 24.8 days does not fire at once", async (t) => {
	const fx = await workflowFixture(t);
	const stdin = interactiveStdin();
	const running = fx.run(
		{ steps: [{ id: "ok", approval: { prompt: "Ship?", timeout: "30d" } }, after] },
		{ mode: "human", stdin },
	);
	await sleep(150);
	stdin.write("y\n");
	const result = await running;
	assert.equal(result.status, "ok");
	assert.equal(fx.fake.calls("after"), 1);
});

test("answering a published question at a terminal consumes its wait", async (t) => {
	const fx = await workflowFixture(t);
	const asked = await fx.run({ steps: [{ id: "ok", approval: "Ship?" }, after] });
	assert.equal(asked.status, "needs_approval");
	const stdin = interactiveStdin();
	const running = advanceWorkflowRun({
		runId: asked.runId!,
		ctx: fx.ctx({ mode: "human", stdin }) as any,
	});
	await sleep(150);
	stdin.write("y\n");
	assert.equal((await running).status, "ok");
	const session = await openStoreSession({ env: fx.env });
	const run = (await session.getRunState(asked.runId!))!;
	await session.close();
	assert.deepEqual(
		Object.values(run.waits).filter((w) => w.status === "pending"),
		[],
		"no question is left pending",
	);
});
