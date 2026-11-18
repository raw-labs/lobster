// Large values are kept beside a run's checkpoint as records: a loop's items and each item's
// output, and every step output over 1 KB. A save then costs the same however much came
// before it. These tests check the records stay consistent through crashes, retries, export
// and import, and are removed once the run cannot need them.
import test from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { openStoreSession } from "../src/persistence/session.js";
import { advanceWorkflowRun, runWorkflowFile } from "../src/engine/runner.js";
import { crashAndRecover } from "./helpers/crash_recovery.js";
import { workflowFixture, type WorkflowFixture } from "./helpers/workflow_runner.js";

/** Every checkpoint saved during `task`, as JSON text. */
async function checkpoints(fx: WorkflowFixture, task: (session: any) => Promise<unknown>) {
	const session = await openStoreSession({ env: fx.env });
	const saved: string[] = [];
	const transition = session.backend.transition.bind(session.backend);
	session.backend.transition = (async (...args: any[]) => {
		if (args[0]?.action?.kind === "execution-save")
			saved.push(JSON.stringify(args[0].action.execution));
		return (transition as any)(...args);
	}) as typeof transition;
	try {
		await task(session);
	} finally {
		await session.close();
	}
	return saved;
}

function recordCount(fx: WorkflowFixture, runId?: string) {
	const db = new DatabaseSync(path.join(fx.env.LOBSTER_STATE_DIR!, "lobster.sqlite"));
	try {
		const row = runId
			? db.prepare("SELECT COUNT(*) AS n FROM execution_records WHERE run_id=?").get(runId)
			: db.prepare("SELECT COUNT(*) AS n FROM execution_records").get();
		return Number(row!.n);
	} finally {
		db.close();
	}
}

const bigList = (n: number) =>
	`node -e 'console.log(JSON.stringify(Array.from({length:${n}},(_, i) => "item-" + i + "-" + "x".repeat(40))))'`;

test("a checkpoint stays the same size however long the loop is, and records go when the run ends", async (t) => {
	const fx = await workflowFixture(t);
	const sizes: Record<number, number> = {};
	for (const n of [50, 2000]) {
		const flow = await fx.write(`loop-${n}.lobster`, {
			steps: [
				{ id: "items", run: bigList(n) },
				{ id: "each", for_each: "$items.json", steps: [{ id: "w", pipeline: "fake --name w" }] },
				{
					id: "count",
					run: 'node -e \'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s).length))\'',
					stdin: "$each.json",
				},
			],
		});
		let result: any;
		const saved = await checkpoints(fx, async (session) => {
			result = await runWorkflowFile({
				filePath: flow,
				ctx: { ...(fx.ctx() as any), storeSession: session },
			});
		});
		assert.equal(result.status, "ok");
		assert.deepEqual(result.output, [n]);
		// The largest checkpoint before the loop finished (the last ones carry its result).
		sizes[n] = Math.max(...saved.slice(0, -4).map((text) => text.length));
		assert.equal(recordCount(fx, result.runId), 0, "a completed run keeps no records");
	}
	assert.ok(
		sizes[2000] < sizes[50] * 1.5,
		`checkpoint ${sizes[50]} bytes at 50 items, ${sizes[2000]} at 2000`,
	);
});

test("a large output is saved once, and a run that crashes after it still reads it", async (t) => {
	const fx = await workflowFixture(t);
	const flow = await fx.write("big.lobster", {
		steps: [
			{ id: "big", run: bigList(500) },
			{ id: "slow", run: `sleep 0.3 && ${fx.fake.shell("slow")}`, recovery: "repeat" },
			{ id: "each", for_each: "$big.json", steps: [{ id: "w", pipeline: "fake --name w" }] },
			{
				id: "last",
				run: 'node -e \'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s).at(-1).item))\'',
				stdin: "$each.json",
			},
		],
	});
	const { recovered, killed } = await crashAndRecover(t, {
		dir: fx.dir,
		env: fx.env,
		flowPath: flow,
		// Killed while "slow" runs: "big" is saved, and only by reference.
		killWhen: ({ saved }) => saved.includes('"$lobster.record"') && saved.includes("/slow/"),
		leaseMs: 300,
	});
	assert.ok(killed);
	assert.equal(recovered.status, "ok", JSON.stringify(recovered));
	assert.deepEqual(recovered.output, [`item-499-${"x".repeat(40)}\n`]);
	assert.equal(fx.fake.calls("w"), 500, "each item ran once");
});

test("a loop that fails at an item is retried from that item, its earlier outputs kept", async (t) => {
	const fx = await workflowFixture(t);
	fx.fake.script({ w: [...Array(6).fill("ok"), "fail", "ok"] });
	const flow = await fx.write("retry.lobster", {
		steps: [
			{ id: "items", run: bigList(10) },
			{ id: "each", for_each: "$items.json", steps: [{ id: "w", pipeline: "fake --name w" }] },
		],
	});
	await assert.rejects(runWorkflowFile({ filePath: flow, ctx: fx.ctx() as any }), /fake w failed/);
	const runId = await fx.runId();
	assert.ok(recordCount(fx, runId) > 0, "a failed run keeps its records for a retry");
	const retried = await advanceWorkflowRun({ runId, ctx: fx.ctx() as any });
	assert.equal(retried.status, "ok");
	assert.equal(fx.fake.calls("w"), 11, "items 0-5 once, item 6 twice, the rest once");
	assert.deepEqual(
		(retried.output as any[]).map((entry) => entry.index),
		[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
	);
	assert.equal(recordCount(fx, runId), 0);
});

test("export carries a paused loop's records, and the imported run finishes", async (t) => {
	const source = await workflowFixture(t);
	const flow = {
		steps: [
			{ id: "items", run: bigList(30) },
			{
				id: "each",
				for_each: "$items.json",
				steps: [
					{ id: "w", pipeline: "fake --name w" },
					{ id: "gate", approval: "Next?", when: "$index.json == 20" },
				],
			},
		],
	};
	const paused = await source.run(flow);
	assert.equal(paused.status, "needs_approval");
	const from = await openStoreSession({ env: source.env });
	const archive = await from.backend.export();
	await from.close();
	assert.ok(archive.runs.some((run) => run.records && Object.keys(run.records).length > 0));

	const target = await workflowFixture(t);
	await target.write("flow.lobster", flow);
	const to = await openStoreSession({ env: target.env });
	await to.backend.import(archive);
	await to.close();
	const done = await target.resume(paused, { approve: true });
	assert.equal(done.status, "ok", JSON.stringify(done).slice(0, 300));
	assert.equal((done.output as any[]).length, 30);
	assert.deepEqual(
		(done.output as any[]).map((entry) => entry.index),
		Array.from({ length: 30 }, (_, i) => i),
	);
});

test("deleting a run deletes its records", async (t) => {
	const fx = await workflowFixture(t);
	fx.fake.script({ w: ["fail"] });
	const flow = await fx.write("fails.lobster", {
		steps: [
			{ id: "items", run: bigList(5) },
			{ id: "each", for_each: "$items.json", steps: [{ id: "w", pipeline: "fake --name w" }] },
		],
	});
	await assert.rejects(runWorkflowFile({ filePath: flow, ctx: fx.ctx() as any }));
	assert.ok(recordCount(fx) > 0);
	const session = await openStoreSession({ env: fx.env });
	try {
		assert.equal(
			await session.backend.removeRun(await fx.runId(), new Date(Date.now() + 1000).toISOString()),
			true,
		);
	} finally {
		await session.close();
	}
	assert.equal(recordCount(fx), 0);
});
