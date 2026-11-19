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
import { abandonWorkflowRun, advanceWorkflowRun, runWorkflowFile } from "../src/engine/runner.js";
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
		/deliver it with `lobster signal x --data/,
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

test("a step's progress counts for the loop around it too", async (t) => {
	const fx = await workflowFixture(t);
	const script = path.join(fx.dir, "beat.mjs");
	await fs.writeFile(
		script,
		`import fs from "node:fs";\n` +
			`for (let i = 0; i < 10; i++) { fs.writeFileSync(process.env.LOBSTER_HEARTBEAT_FILE, String(i)); await new Promise(r => setTimeout(r, 80)); }\n` +
			`process.stdout.write("done");\n`,
	);
	// The loop's limit is shorter than the step's run; the step beats against its own limit.
	const result = await fx.run({
		steps: [
			{ id: "items", run: "echo '[1]'" },
			{
				id: "loop",
				for_each: "$items.json",
				heartbeat_timeout_ms: 400,
				steps: [
					{
						id: "work",
						run: `${JSON.stringify(process.execPath)} ${JSON.stringify(script)}`,
						heartbeat_timeout_ms: 300,
					},
				],
			},
		],
	});
	assert.equal(result.status, "ok");
});

test("a loop's deliberate waits are not a hang: batch pauses, short sleeps, retry delays", async (t) => {
	const loop = (extra: Record<string, unknown>, steps: unknown[]) => ({
		steps: [
			{ id: "items", run: "echo '[1, 2]'" },
			{ id: "loop", for_each: "$items.json", heartbeat_timeout_ms: 200, steps, ...extra },
		],
	});
	const pause = await (
		await workflowFixture(t)
	).run(loop({ pause_ms: 600 }, [{ id: "hi", run: "echo hi" }]));
	assert.equal(pause.status, "ok", "a pause between batches");

	const sleep = await (await workflowFixture(t)).run(loop({}, [{ id: "nap", sleep: "600ms" }]));
	assert.equal(sleep.status, "ok", "a sleep inside the loop, waited in place");

	const fx = await workflowFixture(t);
	const marker = path.join(fx.dir, "failed-once");
	const retry = await fx.run(
		loop({}, [
			{
				id: "flaky",
				run: `test -f ${JSON.stringify(marker)} || (touch ${JSON.stringify(marker)}; exit 1)`,
				retry: { max: 2, delay_ms: 600 },
			},
		]),
	);
	assert.equal(retry.status, "ok", "a retry delay of a step inside the loop");
});

test("a deliberate wait does not count against an enclosing step's timeout_ms", async (t) => {
	// The same as a pause: a sleep waited in place, or a retry delay, is not the step's work.
	const sleep = await (
		await workflowFixture(t)
	).run({
		steps: [
			{
				id: "sw",
				timeout_ms: 400,
				switch: { cases: [{ when: "true", steps: [{ id: "nap", sleep: "700ms" }] }] },
			},
			{ id: "after", run: "echo done" },
		],
	});
	assert.equal(sleep.status, "ok", JSON.stringify(sleep));

	const fx = await workflowFixture(t);
	const marker = path.join(fx.dir, "failed-once");
	const retry = await fx.run({
		steps: [
			{ id: "items", run: "echo '[1]'" },
			{
				id: "loop",
				for_each: "$items.json",
				timeout_ms: 400,
				steps: [
					{
						id: "flaky",
						run: `test -f ${JSON.stringify(marker)} || (touch ${JSON.stringify(marker)}; exit 1)`,
						retry: { max: 2, delay_ms: 700 },
					},
				],
			},
		],
	});
	assert.equal(retry.status, "ok", JSON.stringify(retry));
});

test("a crash or a stop during a deliberate wait leaves the steps around it a fresh budget", async (t) => {
	const savedDeadlines = async (fx: Awaited<ReturnType<typeof workflowFixture>>) =>
		Object.entries((await fx.execution()).activations)
			.filter(([, frame]) => frame.timeoutAt !== undefined)
			.map(([key]) => key);
	const loop = (extra: Record<string, unknown>, steps: unknown[]) => ({
		steps: [
			{ id: "items", run: "echo '[1, 2]'" },
			{ id: "loop", for_each: "$items.json", timeout_ms: 600, steps, ...extra },
		],
	});
	// Each case touches `started` just before its wait begins.
	const started = (dir: string) => JSON.stringify(path.join(dir, "started"));
	const cases: Array<[string, (dir: string) => unknown]> = [
		[
			"a batch pause",
			(dir) => loop({ pause_ms: 800 }, [{ id: "hi", run: `touch ${started(dir)}` }]),
		],
		[
			"a sleep waited in place",
			(dir) =>
				loop({}, [
					{ id: "go", run: `touch ${started(dir)}` },
					{ id: "nap", sleep: "800ms" },
				]),
		],
		[
			"a retry delay",
			(dir) =>
				loop({}, [
					{
						id: "flaky",
						run: `test -f ${started(dir)} || (touch ${started(dir)}; exit 1)`,
						retry: { max: 2, delay_ms: 800 },
					},
				]),
		],
	];
	for (const [what, workflow] of cases) {
		const fx = await workflowFixture(t);
		const controller = new AbortController();
		const running = fx.run(workflow(fx.dir), { signal: controller.signal });
		// Inside the wait, however long the run took to reach it.
		while (!(await fs.stat(path.join(fx.dir, "started")).catch(() => null))) await sleep(20);
		await sleep(150);
		// What a crash would leave: the wait saved, the enclosing deadline not.
		assert.deepEqual(await savedDeadlines(fx), [], `${what}: saved during the wait`);
		controller.abort(new Error("stopped by host"));
		await assert.rejects(running, /stopped by host/);
		assert.deepEqual(await savedDeadlines(fx), [], `${what}: saved by the stop`);
		await sleep(700);
		const continued = await advanceWorkflowRun({ runId: await fx.runId(), ctx: fx.ctx() as any });
		assert.equal(continued.status, "ok", `${what}: ${JSON.stringify(continued)}`);
	}
});

test("a wait in one of several items running at once does not hold the loop's timeout", async (t) => {
	const fx = await workflowFixture(t);
	const marker = JSON.stringify(path.join(fx.dir, "failed-once"));
	// Item 0 works past the loop's limit while item 1 waits to retry: the loop is still working.
	await assert.rejects(
		fx.run({
			steps: [
				{ id: "items", run: "echo '[0, 1]'" },
				{
					id: "each",
					for_each: "$items.json",
					concurrency: 2,
					timeout_ms: 500,
					steps: [
						{
							id: "work",
							run: `if [ "$LOBSTER_ARG_N" = 0 ]; then sleep 1.2; else test -f ${marker} || (touch ${marker}; exit 1); fi`,
							env: { LOBSTER_ARG_N: "$item.json" },
							retry: { max: 2, delay_ms: 1000 },
						},
					],
				},
			],
		}),
		/Step 'each' timed out after 500ms/,
	);
});

test("a stop while a timed-out loop waits to retry leaves a run that continues, not a blocked one", async (t) => {
	const fx = await workflowFixture(t);
	const controller = new AbortController();
	const running = fx.run(
		{
			steps: [
				{ id: "items", run: "echo '[1]'" },
				{
					id: "loop",
					for_each: "$items.json",
					timeout_ms: 300,
					retry: { max: 2, delay_ms: 2000 },
					steps: [{ id: "work", run: "sleep 5" }],
				},
			],
		},
		{ signal: controller.signal },
	);
	// The loop's timeout stops the command; the loop waits to try again. Nothing is running.
	await sleep(900);
	controller.abort(new Error("stopped by host"));
	await assert.rejects(running, /stopped by host/);
	const saved = await fx.execution();
	assert.equal(saved.status, "running", saved.error);
	assert.deepEqual(
		Object.entries(saved.activations)
			.map(([key]) => key)
			.filter((key) => key.includes("/attempt-1")),
		[],
		"the timed-out attempt is forgotten",
	);
});

test("a step waiting for an event inside a timed step gives that step a fresh budget", async (t) => {
	const fx = await workflowFixture(t);
	const waiting = await fx.run({
		steps: [
			{
				id: "sw",
				timeout_ms: 400,
				switch: {
					cases: [
						{
							when: "true",
							steps: [
								{ id: "go", wait: { event: "go" } },
								{ id: "after", run: "echo done" },
							],
						},
					],
				},
			},
		],
	});
	assert.equal(waiting.status, "waiting");
	// The pause clears the switch's deadline, in what it saves too.
	const saved = Object.values((await fx.execution()).activations);
	assert.deepEqual(
		saved.filter((frame) => frame.timeoutAt !== undefined),
		[],
	);
	await sleep(600);
	const delivered = await signalEvent({ name: "go", ctx: fx.ctx() as any });
	assert.deepEqual(
		delivered.advanced.map((a) => a.status),
		["ok"],
		JSON.stringify(delivered.advanced),
	);
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

test("signal and advance --due refuse bad settings before touching the store", async (t) => {
	const fx = await workflowFixture(t);
	await assert.rejects(
		signalEvent({ name: "go", leaseMs: 5, ctx: fx.ctx() as any }),
		/lease must be at least 100ms/,
	);
	// The refused signal stored nothing: a later run does not take that event.
	const waiting = await fx.run({ steps: [{ id: "go", wait: { event: "go" } }] });
	assert.equal(waiting.status, "waiting");
	for (const options of [{ limit: -1 }, { limit: 0 }, { leaseMs: 5 }])
		await assert.rejects(
			advanceDueRuns({ ctx: fx.ctx() as any, ...options }),
			/must be a positive integer|lease must be at least 100ms/,
		);
});

test("signal refuses a blank or non-string delivery ID before storing the event", async (t) => {
	const fx = await workflowFixture(t);
	for (const id of ["", "   ", 12, { a: 1 }])
		await assert.rejects(
			signalEvent({ name: "go", id: id as any, ctx: fx.ctx() as any }),
			/Event ID must be a non-empty string/,
		);
	const first = await signalEvent({ name: "go", id: "build-1", ctx: fx.ctx() as any });
	assert.equal(first.event.inserted, true);
});

test("signal refuses a non-string event name before storing anything", async (t) => {
	const fx = await workflowFixture(t);
	for (const name of [42, null, undefined])
		await assert.rejects(
			signalEvent({ name: name as any, ctx: fx.ctx() as any }),
			/Event names use/,
		);
});

test("a dry run is refused where it would not be honoured", async (t) => {
	const fx = await workflowFixture(t);
	const waiting = await fx.run({ steps: [{ id: "go", wait: { event: "go" } }] });
	await assert.rejects(
		advanceWorkflowRun({ runId: waiting.runId!, ctx: { ...(fx.ctx() as any), dryRun: true } }),
		/A dry run applies to starting a workflow file/,
	);
	await assert.rejects(
		runWorkflowFile({
			resume: { protocolVersion: 1, v: 1, kind: "workflow-file", stateKey: "x" },
			ctx: { ...(fx.ctx() as any), dryRun: true },
		}),
		/A dry run applies to starting a workflow file/,
	);
	assert.deepEqual(
		(await advanceDueRuns({ ctx: { ...(fx.ctx() as any), dryRun: true } })).advanced,
		[],
	);
	await assert.rejects(
		abandonWorkflowRun({ runId: waiting.runId!, ctx: { ...(fx.ctx() as any), dryRun: true } }),
		/A dry run applies to starting a workflow file/,
	);
	await assert.rejects(
		signalEvent({ name: "go", ctx: { ...(fx.ctx() as any), dryRun: true } }),
		/A dry run applies to starting a workflow file/,
	);
	// Nothing was stored: the run still waits, and a real signal finds it.
	const delivered = await signalEvent({ name: "go", ctx: fx.ctx() as any });
	assert.deepEqual(
		delivered.advanced.map((a) => a.status),
		["ok"],
	);
});

test("runWorkflowFile refuses a decision without a resume, and args with one", async (t) => {
	const fx = await workflowFixture(t);
	const filePath = await fx.write("flow.lobster", { steps: [{ id: "ask", approval: "Go?" }] });
	await assert.rejects(
		runWorkflowFile({ filePath, approved: true, ctx: fx.ctx() as any }),
		/apply to a resume/,
	);
	await assert.rejects(
		runWorkflowFile({
			filePath,
			args: { n: 1 },
			resume: { protocolVersion: 1, v: 1, kind: "workflow-file", stateKey: "x" },
			approved: true,
			ctx: fx.ctx() as any,
		}),
		/A resume takes no args or definition/,
	);
});
