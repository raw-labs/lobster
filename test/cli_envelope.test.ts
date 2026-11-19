// The CLI's contract for agents: every command answers with one JSON envelope on stdout
// ({ protocolVersion, ok, ... } or { ok: false, error }), exit 2 for a mistake in the command
// line, 1 otherwise; stderr is for messages to a person.
import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const bin = path.join(repoRoot, "bin", "lobster.js");

async function cli(t: test.TestContext, extraEnv: Record<string, string> = {}) {
	const dir = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-cli-envelope-"));
	t.after(() => fs.rm(dir, { recursive: true, force: true }));
	const env = { ...process.env, LOBSTER_STATE_DIR: path.join(dir, "state"), ...extraEnv };
	const run = (...args: string[]) => {
		const result = spawnSync(process.execPath, [bin, ...args], { encoding: "utf8", env });
		let envelope: any;
		try {
			envelope = JSON.parse(result.stdout);
		} catch {
			assert.fail(
				`lobster ${args.join(" ")} printed no envelope:\n${result.stdout}${result.stderr}`,
			);
		}
		assert.equal(envelope.protocolVersion, 1, args.join(" "));
		return { ...result, envelope };
	};
	return { dir, run };
}

test("every command takes --mode tool and answers with an envelope", async (t) => {
	const { dir, run } = await cli(t);
	const file = path.join(dir, "ask.lobster");
	await fs.writeFile(file, JSON.stringify({ steps: [{ id: "ask", approval: "Go?" }] }));
	const asked = run("run", "--mode", "tool", file).envelope;
	assert.equal(asked.status, "needs_approval");
	for (const args of [
		["runs", "list"],
		["runs", "show", asked.runId],
		["history", "list"],
		["signal", "nothing.here"],
		["upgrade", "--dry-run"],
		["doctor"],
		["resume", asked.requiresApproval.approvalId, "--approve", "no"],
	]) {
		const { status, envelope, stderr } = run(...args, "--mode", "tool");
		assert.equal(status, 0, `${args.join(" ")}: ${stderr}`);
		assert.equal(envelope.ok, true, args.join(" "));
	}
	const refused = run("runs", "list", "--mode", "tol");
	assert.equal(refused.status, 2);
	assert.match(refused.envelope.error.message, /--mode must be human or tool/);
});

test("a dry run's plan is in the envelope", async (t) => {
	const { dir, run } = await cli(t);
	const file = path.join(dir, "flow.lobster");
	await fs.writeFile(file, JSON.stringify({ steps: [{ id: "hello", run: "echo hi" }] }));
	const workflow = run("run", "--mode", "tool", "--dry-run", file).envelope;
	assert.match(workflow.plan, /1\. hello {2}\[shell\]/);
	const pipeline = run(
		"run",
		"--mode",
		"tool",
		"--dry-run",
		"exec --json --shell 'echo [1]'",
	).envelope;
	assert.match(pipeline.plan, /1\. exec/);
});

test("runs advance --due reports a run that failed: not ok, exit 1, and a line on stderr", async (t) => {
	// Sleeps pause the run (no waiting in place), so the failing step runs on the advance.
	const { dir, run } = await cli(t, { LOBSTER_INLINE_SLEEP_MS: "0" });
	const file = path.join(dir, "flow.lobster");
	await fs.writeFile(
		file,
		JSON.stringify({
			steps: [
				{ id: "nap", sleep: "1ms" },
				{ id: "boom", run: "exit 3" },
			],
		}),
	);
	const started = run("run", "--mode", "tool", file).envelope;
	assert.equal(started.status, "waiting", JSON.stringify(started));
	await new Promise((resolve) => setTimeout(resolve, 20));
	const due = run("runs", "advance", "--due");
	assert.equal(due.status, 1);
	assert.equal(due.envelope.ok, false);
	assert.match(due.envelope.error.message, /1 of 1 due runs did not advance/);
	assert.deepEqual(
		due.envelope.advanced.map((outcome: { runId: string; status: string }) => [
			outcome.runId,
			outcome.status,
		]),
		[[started.runId, "failed"]],
	);
	assert.match(due.stderr, new RegExp(`run ${started.runId} is failed: `));
});

test("signal refuses a delivery ID used before for other data", async (t) => {
	const { run } = await cli(t);
	assert.equal(run("signal", "order.update", "--id", "k1", "--data", '{"n":1}').envelope.ok, true);
	const again = run("signal", "order.update", "--id", "k1", "--data", '{"n":1}');
	assert.equal(again.envelope.ok, true, "the same event again is a retry");
	assert.equal(again.envelope.event.inserted, false);
	const other = run("signal", "order.update", "--id", "k1", "--data", '{"n":2}');
	assert.equal(other.status, 1);
	assert.match(other.envelope.error.message, /Event ID k1 was already delivered with other data/);
	const elsewhere = run("signal", "order.cancel", "--id", "k1", "--data", '{"n":1}');
	assert.match(elsewhere.envelope.error.message, /already delivered for another event/);
});

test("answering a question again names the run that asked it", async (t) => {
	const { dir, run } = await cli(t);
	const file = path.join(dir, "ask.lobster");
	await fs.writeFile(file, JSON.stringify({ steps: [{ id: "ask", approval: "Go?" }] }));
	const asked = run("run", "--mode", "tool", file).envelope;
	const token = asked.requiresApproval.resumeToken;
	assert.equal(run("resume", token, "--approve", "no").envelope.status, "cancelled");
	const again = run("resume", token, "--approve", "no");
	assert.equal(again.status, 1);
	assert.match(
		again.envelope.error.message,
		new RegExp(`already answered or withdrawn \\(run ${asked.runId} is cancelled\\)`),
	);
});

test("a bad flag value is a mistake in the command line that says what is expected", async (t) => {
	const { run } = await cli(t);
	const status = run("runs", "list", "--status", "bogus");
	assert.equal(status.status, 2);
	assert.match(status.envelope.error.message, /--status takes one of running, waiting, blocked/);
	const date = run("history", "prune", "yesterday");
	assert.equal(date.status, 2);
	assert.match(date.envelope.error.message, /takes a date or time .* such as 2026-01-31/);
	const id = run("runs", "show", "bad id!");
	assert.equal(id.status, 2);
	assert.match(id.envelope.error.message, /Invalid run ID/);
	const pruned = run("history", "prune", "2000-01-01").envelope;
	assert.deepEqual(pruned, { protocolVersion: 1, ok: true, dryRun: true, runs: [] });
});
