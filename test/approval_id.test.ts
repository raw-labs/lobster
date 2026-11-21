import test from "node:test";
import assert from "node:assert/strict";
import { promises as fsp } from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";

import { openStoreSession } from "../src/persistence/session.js";
import { findStateKeyByApprovalId, writeApprovalIndex } from "../src/state/store.js";

/** Short approval IDs currently stored in the run store. */
async function approvalIndexes(stateDir: string): Promise<string[]> {
	const store = await openStoreSession({ directory: stateDir, readOnly: true });
	try {
		return Object.keys(await store.backend.listState("approval_"));
	} finally {
		await store.close();
	}
}

function runCli(args: string[], env: Record<string, string | undefined>) {
	const bin = path.join(process.cwd(), "bin", "lobster.js");
	return spawnSync("node", [bin, ...args], {
		encoding: "utf8",
		env: { ...process.env, ...env },
	});
}

test("approval gate returns approvalId alongside resumeToken", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-aid-"));
	const stateDir = path.join(tmpDir, "state");

	const pipeline =
		"exec --json --shell \"node -e 'process.stdout.write(JSON.stringify([{a:1}]))'\" | approve --prompt 'ok?' | pick a";

	const first = runCli(["run", "--mode", "tool", pipeline], { LOBSTER_STATE_DIR: stateDir });
	assert.equal(first.status, 0);
	const json = JSON.parse(first.stdout);
	assert.equal(json.status, "needs_approval");
	assert.ok(json.requiresApproval?.resumeToken, "should have resumeToken");
	assert.ok(json.requiresApproval?.approvalId, "should have approvalId");
	assert.equal(json.requiresApproval.approvalId.length, 8, "approvalId should be 8 hex chars");
	assert.match(json.requiresApproval.approvalId, /^[a-f0-9]{8}$/, "approvalId should be hex");

	// Verify the index was written
	assert.deepEqual(await approvalIndexes(stateDir), [
		`approval_${json.requiresApproval.approvalId}`,
	]);
});

test("resume with --id works as alternative to --token", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-aid-resume-"));
	const stateDir = path.join(tmpDir, "state");

	const pipeline =
		"exec --json --shell \"node -e 'process.stdout.write(JSON.stringify([{b:2}]))'\" | approve --prompt 'ok?' | pick b";

	// Step 1: Run pipeline, get approval ID
	const first = runCli(["run", "--mode", "tool", pipeline], { LOBSTER_STATE_DIR: stateDir });
	assert.equal(first.status, 0);
	const firstJson = JSON.parse(first.stdout);
	assert.equal(firstJson.status, "needs_approval");
	const approvalId = firstJson.requiresApproval.approvalId;
	assert.ok(approvalId);

	// Step 2: Resume using --id instead of --token
	const resumed = runCli(["resume", "--id", approvalId, "--approve", "yes"], {
		LOBSTER_STATE_DIR: stateDir,
	});
	assert.equal(resumed.status, 0, `stderr: ${resumed.stderr}`);
	const resumedJson = JSON.parse(resumed.stdout);
	assert.equal(resumedJson.status, "ok");
	assert.deepEqual(resumedJson.output, [{ b: 2 }]);

	// Step 3: Verify cleanup — approval index should be deleted
	assert.deepEqual(
		await approvalIndexes(stateDir),
		[],
		"approval index should be cleaned up after resume",
	);
});

test("resume with --id cancellation works", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-aid-cancel-"));
	const stateDir = path.join(tmpDir, "state");

	const pipeline =
		"exec --json --shell \"node -e 'process.stdout.write(JSON.stringify([{c:3}]))'\" | approve --prompt 'ok?' | pick c";

	const first = runCli(["run", "--mode", "tool", pipeline], { LOBSTER_STATE_DIR: stateDir });
	const firstJson = JSON.parse(first.stdout);
	const approvalId = firstJson.requiresApproval.approvalId;

	const cancelled = runCli(["resume", "--id", approvalId, "--approve", "no"], {
		LOBSTER_STATE_DIR: stateDir,
	});
	assert.equal(cancelled.status, 0);
	const cancelledJson = JSON.parse(cancelled.stdout);
	assert.equal(cancelledJson.status, "cancelled");
});

test("resume with invalid --id returns clear error", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-aid-invalid-"));
	const stateDir = path.join(tmpDir, "state");

	const result = runCli(["resume", "--id", "deadbeef", "--approve", "yes"], {
		LOBSTER_STATE_DIR: stateDir,
	});
	assert.equal(result.status, 2, `stderr: ${result.stderr}`);
	const json = JSON.parse(result.stdout);
	assert.deepEqual(json, {
		protocolVersion: 1,
		ok: false,
		error: {
			type: "parse_error",
			message:
				'Approval ID "deadbeef" not found or expired: it was answered or withdrawn, or never existed',
		},
	});
});

test("--token resume cleans up orphaned approval index", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-aid-orphan-"));
	const stateDir = path.join(tmpDir, "state");

	const pipeline =
		"exec --json --shell \"node -e 'process.stdout.write(JSON.stringify([{e:5}]))'\" | approve --prompt 'ok?' | pick e";

	// Step 1: Run pipeline, get both approvalId and resumeToken
	const first = runCli(["run", "--mode", "tool", pipeline], { LOBSTER_STATE_DIR: stateDir });
	const firstJson = JSON.parse(first.stdout);
	assert.ok(firstJson.requiresApproval?.approvalId);
	assert.ok(firstJson.requiresApproval?.resumeToken);

	// Verify the index exists
	assert.equal(
		(await approvalIndexes(stateDir)).length,
		1,
		"approval index should exist before resume",
	);

	// Step 2: Resume using --token (NOT --id)
	const resumed = runCli(
		["resume", "--token", firstJson.requiresApproval.resumeToken, "--approve", "yes"],
		{ LOBSTER_STATE_DIR: stateDir },
	);
	assert.equal(resumed.status, 0);
	const resumedJson = JSON.parse(resumed.stdout);
	assert.equal(resumedJson.status, "ok");

	// Step 3: Verify approval index was cleaned up despite using --token
	assert.deepEqual(
		await approvalIndexes(stateDir),
		[],
		"approval index should be cleaned up even when using --token",
	);
});

test("double-resume with same --id returns clear error", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-aid-double-"));
	const stateDir = path.join(tmpDir, "state");

	const pipeline =
		"exec --json --shell \"node -e 'process.stdout.write(JSON.stringify([{f:6}]))'\" | approve --prompt 'ok?' | pick f";

	const first = runCli(["run", "--mode", "tool", pipeline], { LOBSTER_STATE_DIR: stateDir });
	const firstJson = JSON.parse(first.stdout);
	const approvalId = firstJson.requiresApproval.approvalId;

	// First resume — should succeed
	const resumed = runCli(["resume", "--id", approvalId, "--approve", "yes"], {
		LOBSTER_STATE_DIR: stateDir,
	});
	assert.equal(resumed.status, 0);
	const resumedJson = JSON.parse(resumed.stdout);
	assert.equal(resumedJson.status, "ok");

	// Second resume with same ID — should fail cleanly, not crash
	const second = runCli(["resume", "--id", approvalId, "--approve", "yes"], {
		LOBSTER_STATE_DIR: stateDir,
	});
	const secondJson = JSON.parse(second.stdout);
	assert.equal(secondJson.ok, false);
	assert.ok(
		secondJson.error?.message?.includes("not found"),
		`Should report not found: ${secondJson.error?.message}`,
	);
});

test("backward compat: --token still works when approvalId is present", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-aid-compat-"));
	const stateDir = path.join(tmpDir, "state");

	const pipeline =
		"exec --json --shell \"node -e 'process.stdout.write(JSON.stringify([{d:4}]))'\" | approve --prompt 'ok?' | pick d";

	const first = runCli(["run", "--mode", "tool", pipeline], { LOBSTER_STATE_DIR: stateDir });
	const firstJson = JSON.parse(first.stdout);
	assert.ok(firstJson.requiresApproval?.approvalId, "approvalId present");
	assert.ok(firstJson.requiresApproval?.resumeToken, "resumeToken present");

	// Resume using the old --token approach — should still work
	const resumed = runCli(
		["resume", "--token", firstJson.requiresApproval.resumeToken, "--approve", "yes"],
		{ LOBSTER_STATE_DIR: stateDir },
	);
	assert.equal(resumed.status, 0);
	const resumedJson = JSON.parse(resumed.stdout);
	assert.equal(resumedJson.status, "ok");
	assert.deepEqual(resumedJson.output, [{ d: 4 }]);
});

test("approval index writes never overwrite an existing approval ID mapping", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-aid-collision-"));
	const stateDir = path.join(tmpDir, "state");
	const env = { LOBSTER_STATE_DIR: stateDir };

	await writeApprovalIndex({
		env,
		stateKey: "workflow_resume_original",
		approvalId: "deadbeef",
	});

	await assert.rejects(
		() =>
			writeApprovalIndex({
				env,
				stateKey: "workflow_resume_replacement",
				approvalId: "deadbeef",
			}),
		(err: NodeJS.ErrnoException) => err?.code === "LOBSTER_STATE_EXISTS",
	);

	const resolved = await findStateKeyByApprovalId({ env, approvalId: "deadbeef" });
	assert.equal(resolved, "workflow_resume_original");
});

test("corrupt approval index is treated as expired instead of crashing (#113)", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-aid-corrupt-"));
	const stateDir = path.join(tmpDir, "state");
	const env = { LOBSTER_STATE_DIR: stateDir };
	const store = await openStoreSession({ directory: stateDir });
	try {
		await store.write("approval_deadbeef", '{"stateKey"');
	} finally {
		await store.close();
	}

	const resolved = await findStateKeyByApprovalId({ env, approvalId: "deadbeef" });
	assert.equal(resolved, null);

	const resumed = runCli(["resume", "--id", "deadbeef", "--approve", "yes"], env);
	const json = JSON.parse(resumed.stdout);
	assert.equal(json.ok, false);
	assert.match(json.error?.message ?? "", /not found or expired/);
});
