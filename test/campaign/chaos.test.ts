// Chaos at the process level: real processes killed with SIGKILL at random checkpoints,
// frozen with SIGSTOP until another process takes their run over, clocks that are wrong,
// and a database locked by someone else. Lobster's state must stay consistent through all
// of it, and nothing that may not be repeated may run twice.
import test, { type TestContext } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { promises as fs } from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { openStoreSession } from "../../src/persistence/session.js";
import { crashAndRecover, workerSource } from "../helpers/crash_recovery.js";
import { workflowFixture, type WorkflowFixture } from "../helpers/workflow_runner.js";
import { BASE_SEED, cases, random } from "./settings.js";
import { drive, generate, normalize } from "./model.js";

// ---------------------------------------------------------------------------------------
// SIGKILL at a random checkpoint of a random workflow

const KILLS = cases(6, 120);
const killStats = { killed: 0, finishedFirst: 0, blocked: 0 };
for (let i = 0; i < KILLS; i++) {
	const seed = BASE_SEED * 10_000 + 5000 + i;
	test(`chaos: kill -9 at a random checkpoint, seed ${seed}`, { timeout: 120_000 }, async (t) => {
		const reference = await workflowFixture(t, "lobster-chaos-ref-");
		const g = generate(seed, (name) => reference.fake.shell(name), { gates: false });
		await setUp(reference, g);
		const expected = await drive(reference, seed);

		const fx = await workflowFixture(t, "lobster-chaos-");
		const crashed = generate(seed, (name) => fx.fake.shell(name), { gates: false });
		await setUp(fx, crashed);
		const killAt = random(seed).int(1, 12);
		const { recovered, killed, runId } = await crashAndRecover(t, {
			dir: fx.dir,
			env: fx.env,
			flowPath: path.join(fx.dir, "flow.lobster"),
			killWhen: ({ saves }) => saves >= killAt,
			leaseMs: 300,
			recover: "if-killed",
		});
		const where = `\nseed ${seed}, killed at save ${killAt} (${killed ? "killed" : "finished first"}), reference ${expected.status}\nrecovered: ${JSON.stringify(recovered ?? null)}\nworkflow: ${JSON.stringify({ ...g.workflow, children: g.children, scripts: g.scripts })}`;
		const run = await fx.execution(runId);
		killStats[killed ? "killed" : "finishedFirst"]++;
		if (run.status === "blocked") killStats.blocked++;
		if (run.status === "blocked") {
			assert.ok(killed, `blocked without a kill${where}`);
			for (const name of Object.keys(g.scripts))
				assert.ok(fx.fake.calls(name) <= reference.fake.calls(name) + 1, `${name}${where}`);
			return;
		}
		assert.equal(run.status, expected.status, `ended differently${where}`);
		if (expected.status === "completed")
			assert.deepEqual(normalize(run.output), normalize(expected.output), `output differs${where}`);
		for (const name of Object.keys(g.scripts)) {
			const ran = fx.fake.calls(name);
			const ref = reference.fake.calls(name);
			if (g.repeatable.has(name))
				assert.ok(ran >= ref && ran <= ref + 1, `${name} ran ${ran}, reference ${ref}${where}`);
			else assert.equal(ran, ref, `${name} (may not repeat) ran ${ran}, reference ${ref}${where}`);
		}
	});
}

test("chaos: kill statistics", { skip: KILLS < 10 && "too few cases" }, (t) => {
	t.diagnostic(`kills: ${JSON.stringify(killStats)}`);
	assert.ok(killStats.killed > KILLS / 3, "most workers should be killed mid-run");
});

async function setUp(fx: WorkflowFixture, g: ReturnType<typeof generate>) {
	fx.env.LOBSTER_INLINE_SLEEP_MS = "0";
	await fx.write("flow.lobster", g.workflow);
	for (const [file, child] of Object.entries(g.children)) await fx.write(file, child);
	fx.fake.script(g.scripts);
}

// ---------------------------------------------------------------------------------------
// Workers that are frozen or have a wrong clock

/** A worker process running `lobster`'s engine; it reports each save and its result. */
async function startWorker(
	t: TestContext,
	fx: WorkflowFixture,
	env: Record<string, string>,
	leaseMs: number,
) {
	const file = path.join(fx.dir, "chaos-worker.mjs");
	await fs.writeFile(file, workerSource());
	const child = spawn(process.execPath, [file], {
		env: { ...fx.env, FAKE_DIR: fx.dir, LEASE_MS: String(leaseMs), ...env },
		stdio: ["ignore", "pipe", "pipe"],
	});
	t.after(() => {
		if (child.exitCode === null) {
			child.kill("SIGCONT");
			child.kill("SIGKILL");
		}
	});
	const worker = {
		child,
		saves: 0,
		runId: undefined as string | undefined,
		result: undefined as any,
		stderr: "",
		exited: once(child, "exit"),
	};
	let buffer = "";
	child.stderr.setEncoding("utf8").on("data", (chunk) => (worker.stderr += chunk));
	child.stdout.setEncoding("utf8").on("data", (chunk) => {
		buffer += chunk;
		for (let at; (at = buffer.indexOf("\n")) >= 0;) {
			const line = buffer.slice(0, at);
			buffer = buffer.slice(at + 1);
			if (line.startsWith("SAVED ")) {
				worker.saves++;
				worker.runId ??= JSON.parse(line.slice(6)).runId;
			} else if (line.startsWith("RESULT ")) worker.result = JSON.parse(line.slice(7));
		}
	});
	return worker;
}

async function until(check: () => boolean, what: string, timeoutMs = 20_000) {
	const deadline = Date.now() + timeoutMs;
	while (!check()) {
		if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
		await new Promise((resolve) => setTimeout(resolve, 5));
	}
}

async function runState(fx: WorkflowFixture, runId: string) {
	const session = await openStoreSession({ env: fx.env });
	try {
		return await session.getRunState(runId);
	} finally {
		await session.close();
	}
}

/** Six steps that each take a moment; repeatable, so a takeover can finish the run. */
async function sixSteps(fx: WorkflowFixture) {
	return fx.write("six.lobster", {
		steps: Array.from({ length: 6 }, (_, i) => ({
			id: `s${i}`,
			run: `sleep 0.15 && ${fx.fake.shell(`s${i}`)}`,
			recovery: "repeat",
		})),
	});
}

test("chaos: a process frozen past its lease cannot write when it wakes, and starts nothing new", async (t) => {
	const fx = await workflowFixture(t, "lobster-chaos-stop-");
	const flow = await sixSteps(fx);
	const first = await startWorker(t, fx, { FLOW: flow }, 400);
	await until(() => first.saves >= 4, "the first worker to make progress");
	first.child.kill("SIGSTOP");
	const stoppedAt = Object.fromEntries(
		Array.from({ length: 6 }, (_, i) => [`s${i}`, fx.fake.calls(`s${i}`)]),
	);
	// Its lease lapses; another process takes the run over and finishes it.
	await new Promise((resolve) => setTimeout(resolve, 600));
	const second = await startWorker(t, fx, { RUN_ID: first.runId! }, 400);
	await second.exited;
	assert.equal(second.result?.status, "ok", second.stderr);
	const finished = await runState(fx, first.runId!);
	assert.equal(finished?.execution?.status, "completed");
	const callsAfterTakeover = Array.from({ length: 6 }, (_, i) => fx.fake.calls(`s${i}`));

	// The frozen process wakes up: whatever it was doing, the run is no longer its own.
	first.child.kill("SIGCONT");
	await first.exited;
	assert.ok(first.result?.error, `the woken worker must fail, got ${JSON.stringify(first.result)}`);
	const after = await runState(fx, first.runId!);
	assert.equal(after?.revision, finished?.revision, "the woken worker changed the run");
	assert.deepEqual(
		Array.from({ length: 6 }, (_, i) => fx.fake.calls(`s${i}`)),
		callsAfterTakeover,
		"the woken worker started a command",
	);
	// At most the step it was running when frozen ran twice.
	const twice = Object.keys(stoppedAt).filter((name) => fx.fake.calls(name) > 1);
	assert.ok(twice.length <= 1, `ran twice: ${twice.join(", ")}`);
});

test("chaos: a process whose clock runs ahead takes over a run; the real owner is fenced out", async (t) => {
	const fx = await workflowFixture(t, "lobster-chaos-skew-");
	const flow = await sixSteps(fx);
	const owner = await startWorker(t, fx, { FLOW: flow }, 3000);
	await until(() => owner.saves >= 3, "the owner to make progress");
	// One minute ahead, the owner's lease looks long expired.
	const ahead = await startWorker(t, fx, { RUN_ID: owner.runId!, CLOCK_OFFSET_MS: "60000" }, 3000);
	await Promise.all([owner.exited, ahead.exited]);
	const outcomes = [owner.result, ahead.result];
	assert.equal(outcomes.filter((r) => r?.status === "ok").length, 1, JSON.stringify(outcomes));
	assert.equal(outcomes.filter((r) => r?.error).length, 1, JSON.stringify(outcomes));
	const run = await runState(fx, owner.runId!);
	assert.equal(run?.execution?.status, "completed");
	const twice = Array.from({ length: 6 }, (_, i) => `s${i}`).filter((n) => fx.fake.calls(n) > 1);
	assert.ok(twice.length <= 1, `ran twice: ${twice.join(", ")}`);
});

test("chaos: a sleeping run wakes on a clock that is ahead, never on one that is behind", async (t) => {
	const fx = await workflowFixture(t, "lobster-chaos-clock-");
	const flow = await fx.write("nap.lobster", {
		steps: [
			{ id: "nap", sleep: "1h" },
			{ id: "after", pipeline: "fake --name after" },
		],
	});
	const started = await startWorker(t, fx, { FLOW: flow }, 1000);
	await started.exited;
	assert.equal(started.result?.status, "waiting");
	for (const [offset, wakes] of [
		["-7200000", false],
		["0", false],
		["7200000", true],
	] as const) {
		const due = await startWorker(t, fx, { DUE: "1", CLOCK_OFFSET_MS: offset }, 1000);
		await due.exited;
		assert.equal(fx.fake.calls("after"), wakes ? 1 : 0, `clock offset ${offset}`);
	}
	const run = await runState(fx, started.runId!);
	assert.equal(run?.execution?.status, "completed");
});

// ---------------------------------------------------------------------------------------
// A database locked by another process

test("chaos: a database locked by someone else delays a run, or fails it cleanly", async (t) => {
	const fx = await workflowFixture(t, "lobster-chaos-lock-");
	const flow = await fx.write("one.lobster", { steps: [{ id: "w", pipeline: "fake --name w" }] });
	// Create the database, then lock it the way another writer would.
	assert.equal((await fx.run(flow)).status, "ok");
	const db = new DatabaseSync(path.join(fx.env.LOBSTER_STATE_DIR!, "lobster.sqlite"));
	t.after(() => db.close());

	// Held for less than the 5 s busy timeout: the run waits and goes through.
	db.exec("BEGIN IMMEDIATE");
	setTimeout(() => db.exec("COMMIT"), 1500);
	const started = Date.now();
	assert.equal((await fx.run(flow)).status, "ok");
	assert.ok(Date.now() - started >= 1000, "the run did not wait for the lock");

	// Held for longer: the run fails with a storage error and leaves nothing half-done.
	db.exec("BEGIN IMMEDIATE");
	const before = fx.fake.calls("w");
	try {
		await assert.rejects(fx.run(flow), /locked|busy|SQLITE_BUSY/i);
	} finally {
		db.exec("COMMIT");
	}
	assert.equal(fx.fake.calls("w"), before, "the step ran without being able to save");
	const session = await openStoreSession({ env: fx.env });
	try {
		const { runs } = await session.listRuns({ limit: 10 });
		for (const run of runs) {
			const state = await session.getRunState(run.id);
			assert.notEqual(state?.execution?.status, "running", "a run was left running");
		}
	} finally {
		await session.close();
	}
	// Once the lock is gone, everything works again.
	assert.equal((await fx.run(flow)).status, "ok");
});
