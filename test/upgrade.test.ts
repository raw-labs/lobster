import test from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import {
	createDefaultRegistry,
	resumeToolRequest,
	runToolRequest,
	runWorkflowFile,
} from "../src/core/index.js";
import { UpgradeRequiredError } from "../src/engine/errors.js";
import { openStoreSession } from "../src/persistence/session.js";
import { convertPipelinePause } from "../src/engine/legacy_pauses.js";
import { withFileLock } from "../src/state/file_lock.js";
import { encodeToken, workflowResumePayload } from "../src/resume/token.js";
import { pendingUpgrade, savedByOlderVersion, upgradeStore } from "../src/engine/upgrade.js";

const exec = promisify(execFile);
const repoRoot = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
// State directory contents written by Lobster on main (85307ff): three paused workflow
// files (an approval step, an input step, a command asking for input inside a pipeline
// step), three paused pipelines (at `approve`, at `ask --emit`, and inside `ask`, which
// continues the same stage), short approval IDs and a `state.set` value. The six pauses
// become waiting runs; the rest is imported as it is.
// `__DIR__` stands for the test directory.
const fixtures = path.join(repoRoot, "test", "fixtures", "upgrade");

type Saved = {
	approval: string;
	input: string;
	command: string;
	pipeline: string;
	pipelineInput: string;
	pipelineSameStage: string;
	approvalId: string;
	pipelineApprovalId: string;
};

async function setup(t: test.TestContext) {
	const dir = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-upgrade-"));
	t.after(() => fs.rm(dir, { recursive: true, force: true }));
	const state = path.join(dir, "state");
	await fs.mkdir(state);
	const saved = {} as Saved;
	const approvals: Record<string, string> = {};
	for (const name of await fs.readdir(fixtures)) {
		const text = (await fs.readFile(path.join(fixtures, name), "utf8")).replaceAll("__DIR__", dir);
		const target = name.endsWith(".lobster") ? path.join(dir, name) : path.join(state, name);
		await fs.writeFile(target, text);
		const key = name.slice(0, -5);
		if (name.startsWith("approval_")) approvals[JSON.parse(text).stateKey] = key.slice(9);
		if (name.startsWith("pipeline_resume_")) {
			const value = JSON.parse(text);
			if (value.resumeMode === "same_stage") saved.pipelineSameStage = key;
			else if (value.haltType === "input_request") saved.pipelineInput = key;
			else saved.pipeline = key;
		}
		if (!name.startsWith("workflow_resume_")) continue;
		const value = JSON.parse(text);
		if (value.approvalStepId) saved.approval = key;
		else if (value.inputKind === "pipeline_command") saved.command = key;
		else saved.input = key;
	}
	saved.approvalId = approvals[saved.approval];
	saved.pipelineApprovalId = approvals[saved.pipeline];
	const env = { LOBSTER_STATE_DIR: state };
	const ctx = { env, registry: createDefaultRegistry() };
	const runs = async (file: string) =>
		(await fs.readFile(path.join(dir, file), "utf8").catch(() => "")).split("\n").filter(Boolean)
			.length;
	const token = (key: string) =>
		encodeToken({
			protocolVersion: 1,
			v: 1,
			kind: "workflow-file",
			stateKey: key,
		});
	return { dir, state, env, ctx, saved, runs, token };
}

test("resuming anything saved by an older Lobster asks for `lobster upgrade`", async (t) => {
	const { state, ctx, saved, token } = await setup(t);
	assert.equal((await pendingUpgrade(state)).length, 9);
	const workflow = await resumeToolRequest({
		token: token(saved.input),
		response: { ok: true },
		ctx,
	});
	assert.equal(workflow.ok, false);
	assert.equal(workflow.error.type, "upgrade_required");
	assert.match(workflow.error.message, /lobster upgrade/);
	const pipeline = await resumeToolRequest({
		approvalId: saved.pipelineApprovalId,
		approved: true,
		ctx,
	});
	assert.equal(pipeline.error.type, "upgrade_required");
	// Core's resume says the same as the tool's.
	await assert.rejects(
		runWorkflowFile({
			resume: workflowResumePayload(saved.input),
			response: { ok: true },
			ctx,
		}),
		UpgradeRequiredError,
	);
});

test("a crafted key cannot probe for files outside the state directory", async (t) => {
	const { dir, state } = await setup(t);
	await fs.writeFile(path.join(dir, "outside.json"), "{}");
	assert.equal(await savedByOlderVersion(state, "../outside"), false);
	assert.equal(await savedByOlderVersion(state, "mykey"), true);
});

test("dry run reports without changing anything", async (t) => {
	const { state } = await setup(t);
	const before = (await fs.readdir(state)).sort();
	const report = await upgradeStore({ directory: state, dryRun: true });
	assert.equal(report.converted.length, 6);
	assert.equal(report.imported.length, 3);
	assert.equal(report.skipped.length, 0);
	assert.equal(report.backup, undefined);
	assert.deepEqual((await fs.readdir(state)).sort(), before, "not even a database is created");
});

test("upgrade moves everything into the store once and every old token still works", async (t) => {
	const { state, ctx, saved, runs, token } = await setup(t);
	const report = await upgradeStore({ directory: state });
	assert.equal(report.converted.length, 6);
	assert.equal(report.imported.length, 3);
	assert.ok(report.backup);
	assert.equal((await fs.readdir(report.backup!)).length, 9, "the originals are kept aside");
	assert.deepEqual(await pendingUpgrade(state), []);
	assert.deepEqual(await upgradeStore({ directory: state }), {
		dryRun: false,
		converted: [],
		imported: [],
		skipped: [],
	});

	const input = await resumeToolRequest({
		token: token(saved.input),
		response: { ok: true },
		ctx,
	});
	assert.equal(input.status, "ok", JSON.stringify(input));
	assert.deepEqual(input.output, ["finished\n"]);
	assert.equal(await runs("count_prep"), 0, "the step before the input does not run again");
	assert.equal(await runs("count_finish"), 1);
	const again = await resumeToolRequest({
		token: token(saved.input),
		response: { ok: true },
		ctx,
	});
	assert.equal(again.ok, false, "a token resumes once");

	const invalid = await resumeToolRequest({
		token: token(saved.command),
		response: "x",
		ctx,
	});
	assert.equal(invalid.error.type, "parse_error");
	const command = await resumeToolRequest({
		token: token(saved.command),
		response: 7,
		ctx,
	});
	assert.equal(command.status, "ok", JSON.stringify(command));
	assert.equal(await runs("count_pprep"), 0);
	assert.equal(await runs("count_pdone"), 1);

	const approved = await resumeToolRequest({
		approvalId: saved.approvalId,
		approved: true,
		ctx,
	});
	assert.equal(approved.status, "ok", JSON.stringify(approved));
	assert.deepEqual(approved.output, ["done\n"]);
	assert.equal(await runs("count_ship"), 0, "the approved command does not run again");
	assert.equal(await runs("count_after"), 1);

	const pipeline = await resumeToolRequest({
		approvalId: saved.pipelineApprovalId,
		approved: true,
		ctx,
	});
	assert.equal(pipeline.status, "ok", JSON.stringify(pipeline));
	assert.equal(await runs("count_pipe"), 0);
	assert.equal(await runs("count_pipe_after"), 1);

	const value = await runToolRequest({ pipeline: "state.get mykey", ctx });
	assert.deepEqual(value.output, [{ v: 42 }]);
});

test("paused pipelines waiting for input continue after upgrade with their old tokens", async (t) => {
	const { state, ctx, saved, runs } = await setup(t);
	await upgradeStore({ directory: state });
	const pipelineToken = (key: string) =>
		encodeToken({
			protocolVersion: 1,
			v: 1,
			kind: "pipeline-resume",
			stateKey: key,
		});

	// `ask --emit` halted after its stage: the answer feeds the next stage.
	const wrong = await resumeToolRequest({
		token: pipelineToken(saved.pipelineInput),
		response: 7,
		ctx,
	});
	assert.equal(wrong.error?.type, "parse_error", "the saved schema still applies");
	const next = await resumeToolRequest({
		token: pipelineToken(saved.pipelineInput),
		response: "bob",
		ctx,
	});
	assert.equal(next.status, "ok", JSON.stringify(next));
	assert.deepEqual(next.output, [null]);
	assert.equal(await runs("count_emit"), 0, "the stage before the question does not run again");
	assert.equal(await runs("count_emit_after"), 1);

	// `ask` asked from inside its stage: the same stage continues with the answer.
	const same = await resumeToolRequest({
		token: pipelineToken(saved.pipelineSameStage),
		response: 7,
		ctx,
	});
	assert.equal(same.status, "ok", JSON.stringify(same));
	assert.deepEqual(same.output, [null]);
	assert.equal(await runs("count_ask"), 0);
	assert.equal(await runs("count_ask_after"), 1);

	const again = await resumeToolRequest({
		token: pipelineToken(saved.pipelineSameStage),
		response: 7,
		ctx,
	});
	assert.equal(again.ok, false, "a token resumes once");
	assert.equal(await runs("count_ask_after"), 1);
});

test("a rejected converted approval cancels without running anything", async (t) => {
	const { state, ctx, saved, runs, token } = await setup(t);
	await upgradeStore({ directory: state });
	const rejected = await resumeToolRequest({
		token: token(saved.approval),
		approved: false,
		ctx,
	});
	assert.equal(rejected.status, "cancelled", JSON.stringify(rejected));
	assert.equal((await runs("count_ship")) + (await runs("count_after")), 0);
});

test("approver rules in the workflow file still apply after conversion", async (t) => {
	const { dir, state, ctx, saved, runs } = await setup(t);
	const file = path.join(dir, "approve.lobster");
	const workflow = JSON.parse(await fs.readFile(file, "utf8"));
	workflow.steps[1].approval = {
		prompt: "Ship it?",
		required_approver: "alice",
	};
	await fs.writeFile(file, JSON.stringify(workflow));
	await upgradeStore({ directory: state });
	const anonymous = await resumeToolRequest({
		approvalId: saved.approvalId,
		approved: true,
		ctx,
	});
	assert.equal(anonymous.ok, false);
	assert.match(anonymous.error.message, /requires approver identity/);
	const as = (who: string) => ({
		...ctx,
		env: { ...ctx.env, LOBSTER_APPROVAL_APPROVED_BY: who },
	});
	const bob = await resumeToolRequest({
		approvalId: saved.approvalId,
		approved: true,
		ctx: as("bob"),
	});
	assert.equal(bob.ok, false);
	const alice = await resumeToolRequest({
		approvalId: saved.approvalId,
		approved: true,
		ctx: as("alice"),
	});
	assert.equal(alice.status, "ok", JSON.stringify(alice));
	assert.equal(await runs("count_after"), 1);
});

test("pauses whose workflow file is gone or changed are set aside, not lost", async (t) => {
	const { dir, state, saved } = await setup(t);
	await fs.rm(path.join(dir, "input.lobster"));
	const pcmd = path.join(dir, "pcmd.lobster");
	const changed = JSON.parse(await fs.readFile(pcmd, "utf8"));
	changed.steps.shift(); // the paused step is no longer where the pause left it
	await fs.writeFile(pcmd, JSON.stringify(changed));
	const report = await upgradeStore({ directory: state });
	// Paused pipelines need no workflow file, so they convert either way.
	assert.deepEqual(
		report.converted.map((item) => item.checkpointId),
		[saved.pipelineInput, saved.pipelineSameStage, saved.pipeline, saved.approval].sort(),
	);
	const skipped = Object.fromEntries(report.skipped.map((item) => [item.checkpointId, item]));
	assert.match(skipped[saved.input].reason, /Workflow file not found/);
	assert.match(skipped[saved.command].reason, /changed since the pause/);
	for (const item of report.skipped) await fs.access(item.movedTo!);
	assert.deepEqual(await pendingUpgrade(state), []);
});

test("a value stored under the same key with different contents is not overwritten", async (t) => {
	const { state } = await setup(t);
	const store = await openStoreSession({ directory: state });
	await store.write("mykey", '{"v":1}');
	await store.close();
	const report = await upgradeStore({ directory: state });
	assert.match(
		report.skipped.find((item) => item.checkpointId === "mykey")!.reason,
		/different value/,
	);
	const after = await openStoreSession({ directory: state, readOnly: true });
	assert.equal((await after.read("mykey")).text, '{"v":1}');
	await after.close();
});

test("upgrade refuses while an older Lobster holds a state lock", async (t) => {
	const { state } = await setup(t);
	await withFileLock({
		filePath: path.join(state, "mykey.json"),
		task: () => assert.rejects(upgradeStore({ directory: state }), /Stop other running Lobster/),
	});
	assert.equal((await upgradeStore({ directory: state })).converted.length, 6);
});

test("CLI: run reminds about the upgrade off a terminal; `lobster upgrade` imports", async (t) => {
	const { dir, state } = await setup(t);
	const bin = path.join(repoRoot, "bin", "lobster.js");
	const env = { ...process.env, LOBSTER_STATE_DIR: state };
	await fs.writeFile(
		path.join(dir, "hello.lobster"),
		'{"steps":[{"id":"hi","command":"echo hi"}]}',
	);
	const hello = [bin, "run", "--file", path.join(dir, "hello.lobster")];
	const run = await exec(process.execPath, hello, { env });
	assert.match(run.stderr, /Found 9 saved items .* older version of Lobster.*lobster upgrade/);
	const upgrade = await exec(process.execPath, [bin, "upgrade", "--json"], {
		env,
	});
	const report = JSON.parse(upgrade.stdout).upgrade;
	assert.equal(report.converted.length + report.imported.length, 9);
	const after = await exec(process.execPath, hello, { env });
	assert.doesNotMatch(after.stderr, /older version/);
});

test("CLI: an upgrade that finds only files it already imported says where it moved them", async (t) => {
	const { state, saved } = await setup(t);
	const bin = path.join(repoRoot, "bin", "lobster.js");
	const env = { ...process.env, LOBSTER_STATE_DIR: state };
	const first = await upgradeStore({ directory: state });
	// An upgrade interrupted after converting this pause, before moving its file.
	const name = `${saved.approval}.json`;
	await fs.copyFile(path.join(first.backup!, name), path.join(state, name));
	const upgrade = await exec(process.execPath, [bin, "upgrade"], { env });
	const { summary } = JSON.parse(upgrade.stdout);
	assert.doesNotMatch(summary, /Nothing to upgrade/);
	assert.match(summary, /The original files were moved to /);
	await assert.rejects(fs.access(path.join(state, name)));
});

test("an older pipeline pause inside a stage that is not in its pipeline is not converted", () => {
	const pause = (resumeAtIndex: number) => ({
		pipeline: [{ name: "exec", args: {}, raw: "exec --json --shell 'echo [1]'" }],
		resumeAtIndex,
		items: [],
		haltType: "input_request" as const,
		resumeMode: "same_stage" as const,
		inputSchema: { type: "object" },
		prompt: "Pick",
		commandInput: {
			pending: {
				requestIndex: 0,
				metadata: {
					prompt: "Pick",
					responseSchema: { type: "object" },
					signature: JSON.stringify({ prompt: "Pick", responseSchema: { type: "object" } }),
				},
			},
			history: [],
		},
	});
	// It runs its stage again: stage 0 exists, stage 1 does not.
	convertPipelinePause("pipeline_resume_x", pause(0));
	assert.throws(() => convertPipelinePause("pipeline_resume_x", pause(1)), /Unrecognized saved/);
});

test("a workflow paused inside a pipeline stage that is not in its pipeline is skipped", async (t) => {
	const { state, saved } = await setup(t);
	const file = path.join(state, `${saved.command}.json`);
	const old = JSON.parse(await fs.readFile(file, "utf8"));
	old.pipelineInput.resumeAtIndex = old.pipelineInput.pipeline.length;
	await fs.writeFile(file, JSON.stringify(old));
	const report = await upgradeStore({ directory: state, dryRun: true });
	assert.deepEqual(
		report.skipped.map((entry) => [entry.checkpointId, entry.reason]),
		[[saved.command, "Unrecognized saved workflow state"]],
	);
});

test("an old step output shaped like a saved-record reference comes through an upgrade as it was", async (t) => {
	const { state, ctx, saved, token } = await setup(t);
	const file = path.join(state, `${saved.approval}.json`);
	const old = JSON.parse(await fs.readFile(file, "utf8"));
	const shaped = { "$lobster.record": "value/abc" };
	old.steps.before.json = shaped;
	await fs.writeFile(file, JSON.stringify(old));
	await upgradeStore({ directory: state });
	const resumed = await resumeToolRequest({ token: token(saved.approval), approved: true, ctx });
	assert.equal(resumed.status, "ok", JSON.stringify(resumed));
});

test("an old pipeline pause resumes though today's parser reads its flags differently", async (t) => {
	const { state, ctx, saved } = await setup(t);
	const file = path.join(state, `${saved.pipeline}.json`);
	const old = JSON.parse(await fs.readFile(file, "utf8"));
	// The released parser left `--json=true` as the string "true"; today it is a boolean.
	old.pipeline[0].raw = old.pipeline[0].raw.replace("exec --json ", "exec --json=true ");
	old.pipeline[0].args.json = "true";
	await fs.writeFile(file, JSON.stringify(old));
	await upgradeStore({ directory: state });
	const resumed = await resumeToolRequest({
		approvalId: saved.pipelineApprovalId,
		approved: true,
		ctx,
	});
	assert.equal(resumed.status, "ok", JSON.stringify(resumed));
});
