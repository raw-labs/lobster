import test from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { abandonWorkflowRun, advanceWorkflowRun, runWorkflowFile } from "../src/engine/runner.js";
import { createToolContext, resumeToolRequest, runToolRequest } from "../src/core/tool_runtime.js";
import { listRuns } from "../src/core/runs.js";
import { openStoreSession } from "../src/persistence/session.js";

async function fixture(t: test.TestContext, steps: unknown[]) {
	const dir = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-interrupted-"));
	t.after(() => fs.rm(dir, { recursive: true, force: true }));
	const filePath = path.join(dir, "flow.lobster");
	await fs.writeFile(filePath, JSON.stringify({ steps }));
	const env = { ...process.env, LOBSTER_STATE_DIR: path.join(dir, "state") };
	return { dir, filePath, env };
}

test("a run stopped mid-command is listed as blocked and can be abandoned and pruned", async (t) => {
	const f = await fixture(t, [
		{ id: "slow", run: `${JSON.stringify(process.execPath)} -e "setTimeout(() => {}, 30000)"` },
		{ id: "after", run: "echo after" },
	]);
	const controller = new AbortController();
	const started = runWorkflowFile({
		filePath: f.filePath,
		ctx: createToolContext({ env: f.env, signal: controller.signal }),
	});
	setTimeout(() => controller.abort(new Error("Ctrl-C")), 500);
	await assert.rejects(started);

	const blocked = await listRuns({ status: "blocked", ctx: { env: f.env } });
	assert.equal(blocked.runs.length, 1, "an interrupted run is not reported as running");
	const runId = blocked.runs[0].id;
	const session = await openStoreSession({ env: f.env, readOnly: true });
	const state = (await session.getRunState(runId))!;
	await session.close();
	const reason = state.execution!.error!;
	assert.match(reason, /Interrupted while slow\/attempt-1\/activity was running/);
	assert.match(reason, /outcome is unknown.*runs abandon.*recovery: repeat/);

	// Advancing a manual-recovery step keeps it blocked rather than running it again, and says so
	// as the stop did.
	await assert.rejects(
		advanceWorkflowRun({ runId, ctx: createToolContext({ env: f.env }) }),
		(error: Error) => error.message === reason,
	);
	const abandoned = await abandonWorkflowRun({ runId, ctx: createToolContext({ env: f.env }) });
	assert.equal(abandoned.status, "cancelled");
	assert.deepEqual(abandoned.abandoned, ["root/slow/attempt-1/activity"]);
	assert.equal((await listRuns({ status: "cancelled", ctx: { env: f.env } })).runs[0].id, runId);

	const store = await openStoreSession({ env: f.env });
	try {
		assert.deepEqual(await store.retain({ before: "2999-01-01", dryRun: true }), [runId]);
	} finally {
		await store.close();
	}
});

test("abandoning a waiting run withdraws its question", async (t) => {
	const f = await fixture(t, [
		{ id: "ship", run: "echo '{}'", approval: "Ship?" },
		{ id: "after", run: "echo after" },
	]);
	const first = await runToolRequest({ filePath: f.filePath, ctx: { env: f.env } });
	assert.equal(first.status, "needs_approval");
	const [run] = (await listRuns({ status: "waiting", ctx: { env: f.env } })).runs;
	const abandoned = await abandonWorkflowRun({
		runId: run.id,
		ctx: createToolContext({ env: f.env }),
	});
	assert.deepEqual(abandoned, { runId: run.id, status: "cancelled", abandoned: [] });
	const late = await resumeToolRequest({
		approvalId: first.requiresApproval.approvalId,
		approved: true,
		ctx: { env: f.env },
	});
	assert.equal(late.ok, false, "the approval no longer resumes anything");
	assert.deepEqual(await listRuns({ status: "waiting", ctx: { env: f.env } }), {
		runs: [],
		nextCursor: null,
	});
});

test("advancing a run that waits for approval keeps it waiting under the same short ID", async (t) => {
	const f = await fixture(t, [
		{ id: "ship", run: "echo '{}'", approval: "Ship?" },
		{ id: "after", run: "echo after" },
	]);
	const first = await runToolRequest({ filePath: f.filePath, ctx: { env: f.env } });
	assert.equal(first.status, "needs_approval");
	const [run] = (await listRuns({ status: "waiting", ctx: { env: f.env } })).runs;
	for (let i = 0; i < 2; i++) {
		const again = await advanceWorkflowRun({
			runId: run.id,
			ctx: createToolContext({ env: f.env }),
		});
		assert.equal(again.status, "needs_approval");
		assert.equal(again.requiresApproval!.approvalId, first.requiresApproval.approvalId);
		assert.equal(
			(await listRuns({ status: "waiting", ctx: { env: f.env } })).runs[0]?.id,
			run.id,
			"still listed as waiting",
		);
	}
	const store = await openStoreSession({ env: f.env, readOnly: true });
	const indexes = Object.keys(await store.backend.listState("approval_"));
	await store.close();
	assert.equal(indexes.length, 1);
	const approved = await resumeToolRequest({
		approvalId: first.requiresApproval.approvalId,
		approved: true,
		ctx: { env: f.env },
	});
	assert.equal(approved.status, "ok", JSON.stringify(approved));
});

test("a finished run keeps its result but not every step's output", async (t) => {
	const f = await fixture(t, [
		{
			id: "big",
			run: `${JSON.stringify(process.execPath)} -e "process.stdout.write('x'.repeat(200000))"`,
		},
		{ id: "small", run: "echo done" },
	]);
	const result = await runToolRequest({ filePath: f.filePath, ctx: { env: f.env } });
	assert.equal(result.status, "ok");
	const [run] = (await listRuns({ status: "completed", ctx: { env: f.env } })).runs;
	const session = await openStoreSession({ env: f.env, readOnly: true });
	try {
		const state = (await session.getRunState(run.id))!;
		assert.deepEqual(state.execution!.activations, {});
		assert.deepEqual(state.execution!.output, ["done\n"]);
		assert.ok(JSON.stringify(state).length < 20_000, "the 200 KB intermediate output is gone");
	} finally {
		await session.close();
	}
});

test("a workflow that cannot start leaves no run record", async (t) => {
	const f = await fixture(t, [{ id: "a", run: "echo a" }]);
	await fs.writeFile(f.filePath, "{ not json");
	const malformed = await runToolRequest({ filePath: f.filePath, ctx: { env: f.env } });
	assert.equal(malformed.ok, false);
	await fs.writeFile(f.filePath, JSON.stringify({ steps: [{ id: "a" }, { id: "a" }] }));
	const invalid = await runToolRequest({ filePath: f.filePath, ctx: { env: f.env } });
	assert.equal(invalid.ok, false);
	assert.deepEqual((await listRuns({ ctx: { env: f.env } })).runs, []);
});

test("finished runs older than the retention period are deleted on a later open", async (t) => {
	const f = await fixture(t, [{ id: "ship", run: "echo '{}'", approval: "Ship?" }]);
	await runToolRequest({ filePath: f.filePath, ctx: { env: f.env } });
	await fs.writeFile(f.filePath, JSON.stringify({ steps: [{ id: "a", run: "echo a" }] }));
	await runToolRequest({ filePath: f.filePath, ctx: { env: f.env } });
	const [waiting] = (await listRuns({ status: "waiting", ctx: { env: f.env } })).runs;
	const [done] = (await listRuns({ status: "completed", ctx: { env: f.env } })).runs;

	// Age both runs by 40 days and forget when the store was last pruned.
	const { DatabaseSync } = await import("node:sqlite");
	const db = new DatabaseSync(path.join(f.env.LOBSTER_STATE_DIR, "lobster.sqlite"));
	const old = new Date(Date.now() - 40 * 24 * 60 * 60 * 1000).toISOString();
	db.prepare("UPDATE runs SET record=json_set(record, '$.updatedAt', ?)").run(old);
	db.exec("DELETE FROM meta");
	db.close();

	const keep = await openStoreSession({ env: { ...f.env, LOBSTER_RUN_RETENTION_DAYS: "0" } });
	await keep.close();
	assert.equal((await listRuns({ ctx: { env: f.env } })).runs.length, 2, "0 keeps everything");

	const store = await openStoreSession({ env: f.env });
	await store.close();
	const ids = (await listRuns({ ctx: { env: f.env } })).runs.map((run) => run.id);
	assert.deepEqual(ids, [waiting.id], "the waiting run stays, the finished one is gone");
	assert.ok(!ids.includes(done.id));

	await assert.rejects(
		openStoreSession({ env: { ...f.env, LOBSTER_RUN_RETENTION_DAYS: "soon" } }),
		/LOBSTER_RUN_RETENTION_DAYS/,
	);
});

test("retention moves past finished runs it cannot delete yet", async (t) => {
	const f = await fixture(t, [{ id: "a", run: "echo a" }]);
	await runToolRequest({ filePath: f.filePath, ctx: { env: f.env } });
	const [done] = (await listRuns({ status: "completed", ctx: { env: f.env } })).runs;

	// 500 old finished runs that sort first and keep an unacknowledged receipt.
	const { DatabaseSync } = await import("node:sqlite");
	const db = new DatabaseSync(path.join(f.env.LOBSTER_STATE_DIR, "lobster.sqlite"));
	const old = new Date(Date.now() - 40 * 24 * 60 * 60 * 1000).toISOString();
	db.prepare("UPDATE runs SET record=json_set(record, '$.updatedAt', ?)").run(old);
	const { record } = db.prepare("SELECT record FROM runs WHERE id=?").get(done.id) as {
		record: string;
	};
	db.exec("BEGIN");
	for (let i = 0; i < 500; i++) {
		const id = `0-stuck-${String(i).padStart(3, "0")}`;
		db.prepare("INSERT INTO runs(id,record) VALUES(?,json_set(?, '$.id', ?))").run(id, record, id);
		db.prepare("INSERT INTO receipts(run_id,operation_id,record) VALUES(?,?,?)").run(
			id,
			"op",
			JSON.stringify({ acknowledged: false }),
		);
	}
	// Put the deletable run last in table order too.
	db.prepare("DELETE FROM runs WHERE id=?").run(done.id);
	db.prepare("INSERT INTO runs(id,record) VALUES(?,?)").run(done.id, record);
	db.exec("COMMIT; DELETE FROM meta");
	db.close();

	for (let open = 0; open < 2; open++) await (await openStoreSession({ env: f.env })).close();
	const check = new DatabaseSync(path.join(f.env.LOBSTER_STATE_DIR, "lobster.sqlite"));
	const ids = (check.prepare("SELECT id FROM runs").all() as { id: string }[]).map((r) => r.id);
	check.close();
	assert.equal(ids.length, 500, "the stuck runs stay");
	assert.ok(!ids.includes(done.id), "the deletable run after them is reached on the next open");
});

test("a lease too short is refused by every entry point before the store is opened", async (t) => {
	const f = await fixture(t, [{ id: "a", run: "echo a" }]);
	const ctx = createToolContext({ env: f.env });
	for (const call of [
		() => runWorkflowFile({ filePath: f.filePath, ctx, leaseMs: 5 }),
		() => advanceWorkflowRun({ runId: "abc", ctx, leaseMs: 5 }),
		() => abandonWorkflowRun({ runId: "abc", ctx, leaseMs: 5 }),
	])
		await assert.rejects(call, /Execution lease must be at least 100ms/);
	await assert.rejects(fs.access(f.env.LOBSTER_STATE_DIR), { code: "ENOENT" });
});
