import test from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { createDefaultRegistry, resumeToolRequest } from "../src/core/index.js";
import { openStoreSession } from "../src/persistence/session.js";
import { encodeToken } from "../src/token.js";
import { pendingUpgrade, upgradeStore } from "../src/workflows/upgrade.js";

const exec = promisify(execFile);
const repoRoot = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
// Written by `lobster run --mode tool --file` on main (85307ff): one pause at an approval
// step, one at an input step, one inside a pipeline command. `__DIR__` is the test directory.
const fixtures = path.join(repoRoot, "test", "fixtures", "upgrade");

type Pauses = { approval: string; input: string; command: string; approvalId: string };

async function setup(t: test.TestContext) {
	const dir = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-upgrade-"));
	t.after(() => fs.rm(dir, { recursive: true, force: true }));
	const state = path.join(dir, "state");
	await fs.mkdir(state);
	const pauses = {} as Pauses;
	for (const name of await fs.readdir(fixtures)) {
		const text = (await fs.readFile(path.join(fixtures, name), "utf8")).replaceAll("__DIR__", dir);
		const target = name.endsWith(".lobster") ? path.join(dir, name) : path.join(state, name);
		await fs.writeFile(target, text);
		if (name.startsWith("approval_")) pauses.approvalId = name.slice("approval_".length, -5);
		if (!name.startsWith("workflow_resume_")) continue;
		const value = JSON.parse(text);
		const key = name.slice(0, -5);
		if (value.approvalStepId) pauses.approval = key;
		else if (value.inputKind === "pipeline_command") pauses.command = key;
		else pauses.input = key;
	}
	const env = { LOBSTER_STATE_DIR: state };
	const ctx = { env, registry: createDefaultRegistry() };
	const runs = async (file: string) =>
		(await fs.readFile(path.join(dir, file), "utf8").catch(() => "")).split("\n").filter(Boolean)
			.length;
	const token = (key: string) =>
		encodeToken({ protocolVersion: 1, v: 1, kind: "workflow-file", stateKey: key });
	return { dir, state, env, ctx, pauses, runs, token };
}

test("resuming an unconverted workflow pause asks for `lobster upgrade`", async (t) => {
	const { state, ctx, pauses, token } = await setup(t);
	assert.equal((await pendingUpgrade(state)).length, 3);
	const result = await resumeToolRequest({
		token: token(pauses.input),
		response: { ok: true },
		ctx,
	});
	assert.equal(result.ok, false);
	assert.equal(result.error.type, "upgrade_required");
	assert.match(result.error.message, /lobster upgrade/);
});

test("dry run reports without changing anything", async (t) => {
	const { state } = await setup(t);
	const before = (await fs.readdir(state)).sort();
	const report = await upgradeStore({ directory: state, dryRun: true });
	assert.equal(report.converted.length, 3);
	assert.equal(report.skipped.length, 0);
	assert.equal(report.backup, undefined);
	assert.deepEqual((await fs.readdir(state)).sort(), before);
});

test("upgrade converts every pause once, keeps tokens, and never reruns finished steps", async (t) => {
	const { state, ctx, pauses, runs, token } = await setup(t);
	const report = await upgradeStore({ directory: state });
	assert.equal(report.converted.length, 3);
	assert.ok(report.backup);
	const backup = JSON.parse(await fs.readFile(report.backup!, "utf8"));
	assert.ok(backup.states[pauses.input], "backup keeps the original pause");
	assert.deepEqual(await pendingUpgrade(state), []);
	assert.deepEqual(await upgradeStore({ directory: state }), {
		dryRun: false,
		converted: [],
		skipped: [],
	});

	const input = await resumeToolRequest({
		token: token(pauses.input),
		response: { ok: true },
		ctx,
	});
	assert.equal(input.status, "ok", JSON.stringify(input));
	assert.deepEqual(input.output, ["finished\n"]);
	assert.equal(await runs("count_prep"), 0, "the step before the input does not run again");
	assert.equal(await runs("count_finish"), 1);
	const again = await resumeToolRequest({
		token: token(pauses.input),
		response: { ok: true },
		ctx,
	});
	assert.equal(again.ok, false, "a token resumes once");

	const invalid = await resumeToolRequest({ token: token(pauses.command), response: "x", ctx });
	assert.equal(invalid.error.type, "parse_error");
	const command = await resumeToolRequest({ token: token(pauses.command), response: 7, ctx });
	assert.equal(command.status, "ok", JSON.stringify(command));
	assert.equal(await runs("count_pprep"), 0);
	assert.equal(await runs("count_pdone"), 1);

	const approved = await resumeToolRequest({ approvalId: pauses.approvalId, approved: true, ctx });
	assert.equal(approved.status, "ok", JSON.stringify(approved));
	assert.deepEqual(approved.output, ["done\n"]);
	assert.equal(await runs("count_before"), 0);
	assert.equal(await runs("count_ship"), 0, "the approved command does not run again");
	assert.equal(await runs("count_after"), 1);
});

test("a rejected converted approval cancels without running anything", async (t) => {
	const { state, ctx, pauses, runs, token } = await setup(t);
	await upgradeStore({ directory: state });
	const rejected = await resumeToolRequest({ token: token(pauses.approval), approved: false, ctx });
	assert.equal(rejected.status, "cancelled", JSON.stringify(rejected));
	assert.equal((await runs("count_ship")) + (await runs("count_after")), 0);
});

test("approver rules in the workflow file still apply after conversion", async (t) => {
	const { dir, state, ctx, pauses, runs } = await setup(t);
	const file = path.join(dir, "approve.lobster");
	const workflow = JSON.parse(await fs.readFile(file, "utf8"));
	workflow.steps[1].approval = { prompt: "Ship it?", required_approver: "alice" };
	await fs.writeFile(file, JSON.stringify(workflow));
	await upgradeStore({ directory: state });
	const anonymous = await resumeToolRequest({ approvalId: pauses.approvalId, approved: true, ctx });
	assert.equal(anonymous.ok, false);
	assert.match(anonymous.error.message, /requires approver identity/);
	const bob = await resumeToolRequest({
		approvalId: pauses.approvalId,
		approved: true,
		ctx: { ...ctx, env: { ...ctx.env, LOBSTER_APPROVAL_APPROVED_BY: "bob" } },
	});
	assert.equal(bob.ok, false);
	const alice = await resumeToolRequest({
		approvalId: pauses.approvalId,
		approved: true,
		ctx: { ...ctx, env: { ...ctx.env, LOBSTER_APPROVAL_APPROVED_BY: "alice" } },
	});
	assert.equal(alice.status, "ok", JSON.stringify(alice));
	assert.equal(await runs("count_after"), 1);
});

test("pauses whose workflow file is gone or changed are set aside, not lost", async (t) => {
	const { dir, state, pauses } = await setup(t);
	await fs.rm(path.join(dir, "input.lobster"));
	const pcmd = path.join(dir, "pcmd.lobster");
	const changed = JSON.parse(await fs.readFile(pcmd, "utf8"));
	changed.steps.shift(); // the paused step is no longer where the pause left it
	await fs.writeFile(pcmd, JSON.stringify(changed));
	const report = await upgradeStore({ directory: state });
	assert.deepEqual(
		report.converted.map((item) => item.checkpointId),
		[pauses.approval],
	);
	const skipped = Object.fromEntries(report.skipped.map((item) => [item.checkpointId, item]));
	assert.match(skipped[pauses.input].reason, /Workflow file not found/);
	assert.match(skipped[pauses.command].reason, /changed since the pause/);
	for (const item of report.skipped) {
		assert.ok(item.movedTo);
		await fs.access(item.movedTo!);
	}
	assert.deepEqual(await pendingUpgrade(state), []);
	// The backup was taken before anything moved.
	const backup = JSON.parse(await fs.readFile(report.backup!, "utf8"));
	assert.ok(backup.states[pauses.input] && backup.states[pauses.command]);
});

test("upgrade refuses while another Lobster session is open", async (t) => {
	const { state, env } = await setup(t);
	const session = await openStoreSession({ env });
	try {
		await assert.rejects(upgradeStore({ directory: state }), /Stop other running Lobster/);
	} finally {
		await session.close();
	}
	assert.equal((await upgradeStore({ directory: state })).converted.length, 3);
});

test("CLI: run hints at upgrade off a terminal; `lobster upgrade` converts", async (t) => {
	const { dir, state } = await setup(t);
	const bin = path.join(repoRoot, "bin", "lobster.js");
	const env = { ...process.env, LOBSTER_STATE_DIR: state };
	await fs.writeFile(
		path.join(dir, "hello.lobster"),
		'{"steps":[{"id":"hi","command":"echo hi"}]}',
	);
	const run = await exec(
		process.execPath,
		[bin, "run", "--file", path.join(dir, "hello.lobster")],
		{
			env,
		},
	);
	assert.match(run.stderr, /3 workflow runs were paused by an older version.*lobster upgrade/);
	const upgrade = await exec(process.execPath, [bin, "upgrade", "--json"], { env });
	assert.equal(JSON.parse(upgrade.stdout).converted.length, 3);
	const after = await exec(
		process.execPath,
		[bin, "run", "--file", path.join(dir, "hello.lobster")],
		{
			env,
		},
	);
	assert.doesNotMatch(after.stderr, /older version/);
});
