import test from "node:test";
import assert from "node:assert/strict";
import { promises as fsp } from "node:fs";
import path from "node:path";
import os from "node:os";
import { PassThrough } from "node:stream";

import { runWorkflowFile } from "../src/engine/runner.js";
import { loadWorkflowFile } from "../src/workflows/load.js";
import { computeDelay, resolveRetryConfig } from "../src/workflows/retry.js";

test("resolveRetryConfig fills defaults", () => {
	const config = resolveRetryConfig(undefined);
	assert.equal(config.max, 1);
	assert.equal(config.backoff, "fixed");
	assert.equal(config.delay_ms, 1000);
	assert.equal(config.max_delay_ms, 30000);
	assert.equal(config.jitter, false);
});

test("resolveRetryConfig preserves provided values", () => {
	const config = resolveRetryConfig({ max: 5, backoff: "exponential", jitter: true });
	assert.equal(config.max, 5);
	assert.equal(config.backoff, "exponential");
	assert.equal(config.jitter, true);
	assert.equal(config.delay_ms, 1000); // default
});

test("computeDelay waits a fixed time, or doubles up to max_delay_ms", () => {
	const fixed = resolveRetryConfig({ delay_ms: 50 });
	assert.deepEqual(
		[0, 1, 2].map((attempt) => computeDelay(fixed, attempt)),
		[50, 50, 50],
	);
	const exponential = resolveRetryConfig({
		backoff: "exponential",
		delay_ms: 50,
		max_delay_ms: 150,
	});
	assert.deepEqual(
		[0, 1, 2, 3].map((attempt) => computeDelay(exponential, attempt)),
		[50, 100, 150, 150],
	);
});

test("computeDelay jitters by at most 10% and never past max_delay_ms", () => {
	const config = resolveRetryConfig({ delay_ms: 1000, max_delay_ms: 1050, jitter: true });
	for (let i = 0; i < 200; i++) {
		const delay = computeDelay(config, 0);
		assert.ok(delay >= 900 && delay <= 1050, String(delay));
	}
});

// --- Validation tests ---

async function loadWorkflow(workflow: any) {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-retry-"));
	const filePath = path.join(tmpDir, "workflow.lobster");
	await fsp.writeFile(filePath, JSON.stringify(workflow), "utf8");
	return loadWorkflowFile(filePath);
}

test("retry validation rejects non-object", async () => {
	await assert.rejects(
		loadWorkflow({ name: "bad", steps: [{ id: "x", command: "echo", retry: "yes" }] }),
		/retry must be an object/,
	);
});

test("retry validation rejects non-integer max", async () => {
	await assert.rejects(
		loadWorkflow({ name: "bad", steps: [{ id: "x", command: "echo", retry: { max: 1.5 } }] }),
		/retry.max must be a positive integer/,
	);
});

test("retry validation rejects max < 1", async () => {
	await assert.rejects(
		loadWorkflow({ name: "bad", steps: [{ id: "x", command: "echo", retry: { max: 0 } }] }),
		/retry.max must be a positive integer/,
	);
});

test("retry validation rejects invalid backoff", async () => {
	await assert.rejects(
		loadWorkflow({
			name: "bad",
			steps: [{ id: "x", command: "echo", retry: { backoff: "linear" } }],
		}),
		/retry.backoff must be "fixed" or "exponential"/,
	);
});

test("retry validation rejects negative delay_ms", async () => {
	await assert.rejects(
		loadWorkflow({ name: "bad", steps: [{ id: "x", command: "echo", retry: { delay_ms: -1 } }] }),
		/retry.delay_ms must be a number from 0 to 2147483647/,
	);
});

test("retry validation rejects non-boolean jitter", async () => {
	await assert.rejects(
		loadWorkflow({ name: "bad", steps: [{ id: "x", command: "echo", retry: { jitter: "yes" } }] }),
		/retry.jitter must be a boolean/,
	);
});

test("retry validation accepts valid config", async () => {
	const wf = await loadWorkflow({
		name: "ok",
		steps: [
			{
				id: "x",
				command: "echo",
				retry: { max: 3, backoff: "exponential", delay_ms: 500, jitter: true },
			},
		],
	});
	assert.ok(wf.steps[0].retry);
});

// --- Integration tests ---

async function runWorkflow(workflow: any) {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-retry-"));
	const stateDir = path.join(tmpDir, "state");
	const filePath = path.join(tmpDir, "workflow.lobster");
	await fsp.writeFile(filePath, JSON.stringify(workflow, null, 2), "utf8");

	const stderrChunks: string[] = [];
	const stderr = new PassThrough();
	stderr.on("data", (d: Buffer) => stderrChunks.push(d.toString()));

	const result = await runWorkflowFile({
		filePath,
		ctx: {
			stdin: process.stdin,
			stdout: process.stdout,
			stderr,
			env: { ...process.env, LOBSTER_STATE_DIR: stateDir },
			mode: "tool",
		},
	});
	return { result, stderrOutput: stderrChunks.join("") };
}

test("step retries and succeeds after transient failure", async () => {
	// Script that fails twice then succeeds on third run using a counter file
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-retry-"));
	const counterFile = path.join(tmpDir, "counter");
	await fsp.writeFile(counterFile, "0", "utf8");

	const workflow = {
		name: "retry-succeed",
		steps: [
			{
				id: "flaky",
				command: `node -e "const fs=require('fs');const c=Number(fs.readFileSync('${counterFile}','utf8'))+1;fs.writeFileSync('${counterFile}',String(c));if(c<3){process.exit(1);}process.stdout.write(JSON.stringify({attempt:c}))"`,
				retry: { max: 3, delay_ms: 50 },
			},
		],
	};
	const { result, stderrOutput } = await runWorkflow(workflow);
	assert.equal(result.status, "ok");
	const output = result.output as any[];
	assert.equal(output[0].attempt, 3);
	assert.ok(stderrOutput.includes("[RETRY]"), "should log retry attempts");
});

test("step with timeout_ms + retry retries on per-attempt timeout (issue #105)", async () => {
	// Behavior proof: a step that hangs past timeout_ms on its first two attempts
	// must be retried (per-attempt timeout produces an AbortError that should NOT
	// bypass retry) and succeed on the third. Pre-fix, withRetry short-circuited on
	// any AbortError, so retry.max was inert for timed-out steps and this ran once.
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-retry-"));
	const counterFile = path.join(tmpDir, "counter");
	await fsp.writeFile(counterFile, "0", "utf8");

	// The counter is incremented synchronously before the hang, so each timed-out
	// (SIGKILLed) attempt is still recorded. Attempts 1-2 hang 8s (killed by the
	// 3000ms timeout); attempt 3 returns immediately. The timeout leaves enough
	// room for slow CI machines to start Node and write the counter before kill.
	const workflow = {
		name: "retry-timeout",
		steps: [
			{
				id: "slow",
				command: `node -e "const fs=require('fs');const c=Number(fs.readFileSync('${counterFile}','utf8'))+1;fs.writeFileSync('${counterFile}',String(c));if(c<3){setTimeout(()=>{},6000);}else{process.stdout.write(JSON.stringify({attempt:c}));}"`,
				timeout_ms: 3000,
				retry: { max: 3, delay_ms: 50 },
			},
		],
	};
	const { result, stderrOutput } = await runWorkflow(workflow);
	assert.equal(result.status, "ok");
	const output = result.output as any[];
	assert.equal(output[0].attempt, 3);
	assert.ok(stderrOutput.includes("[RETRY]"), "should log retry attempts on timeout");
});

test("step exhausts retries and throws", async () => {
	const workflow = {
		name: "retry-exhaust",
		steps: [
			{
				id: "fail",
				command: 'node -e "process.exit(1)"',
				retry: { max: 2, delay_ms: 50 },
			},
		],
	};
	await assert.rejects(
		runWorkflow(workflow).then((r) => r.result),
		/workflow command failed/,
	);
});

test("step without retry fails immediately (no retry)", async () => {
	const workflow = {
		name: "no-retry",
		steps: [{ id: "fail", command: 'node -e "process.exit(1)"' }],
	};
	await assert.rejects(
		runWorkflow(workflow).then((r) => r.result),
		/workflow command failed/,
	);
});

test("dry-run renders retry config", async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-retry-"));
	const stateDir = path.join(tmpDir, "state");
	const filePath = path.join(tmpDir, "workflow.lobster");
	const workflow = {
		name: "dry-run-retry",
		steps: [
			{
				id: "fetch",
				command: "curl https://example.com",
				retry: { max: 3, backoff: "exponential", delay_ms: 1000 },
			},
		],
	};
	await fsp.writeFile(filePath, JSON.stringify(workflow, null, 2), "utf8");

	const stderrChunks: string[] = [];
	const stderr = new PassThrough();
	stderr.on("data", (d: Buffer) => stderrChunks.push(d.toString()));

	await runWorkflowFile({
		filePath,
		ctx: {
			stdin: process.stdin,
			stdout: process.stdout,
			stderr,
			env: { ...process.env, LOBSTER_STATE_DIR: stateDir },
			mode: "tool",
			dryRun: true,
		},
	});

	const output = stderrChunks.join("");
	assert.ok(output.includes("retry:"), "should show retry config");
	assert.ok(output.includes("3 attempts"), "should show max attempts");
	assert.ok(output.includes("exponential"), "should show backoff type");
});
