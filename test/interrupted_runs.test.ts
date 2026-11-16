import test from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { abandonWorkflowRun, advanceWorkflowRun, runWorkflowFile } from "../src/workflows/file.js";
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
	assert.match(state.execution!.error!, /outcome is unknown.*runs abandon/);

	// Advancing a manual-recovery step keeps it blocked rather than running it again.
	await assert.rejects(
		advanceWorkflowRun({ runId, ctx: createToolContext({ env: f.env }) }),
		/interrupted without a saved result/,
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
