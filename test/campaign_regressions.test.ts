// Regression tests for bugs found by the testing campaign (test/campaign).
import test from "node:test";
import assert from "node:assert/strict";
import { openStoreSession } from "../src/persistence/session.js";
import { advanceWorkflowRun, runWorkflowFile } from "../src/workflows/file.js";
import { promises as fs } from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { checkConditionSyntax } from "../src/workflows/conditions.js";
import { workflowFixture } from "./helpers/workflow_runner.js";

test("a stop right after a step is marked started, before its command runs, does not block the run", async (t) => {
	for (const kind of ["pipeline", "shell"] as const) {
		const fx = await workflowFixture(t);
		const step =
			kind === "pipeline"
				? { id: `w-${kind}`, pipeline: `fake --name w-${kind}` }
				: { id: `w-${kind}`, run: fx.fake.shell(`w-${kind}`) };
		const file = await fx.write(`${kind}.lobster`, { steps: [step] });
		const session = await openStoreSession({ env: fx.env });
		t.after(() => session.close());
		const controller = new AbortController();
		// Stop the run as soon as the step's "started" checkpoint is written.
		const transition = session.backend.transition.bind(session.backend);
		session.backend.transition = (async (...args: any[]) => {
			const result = await (transition as any)(...args);
			const execution = args[0]?.action?.execution;
			if (
				Object.entries(execution?.activations ?? {}).some(
					([path, frame]: [string, any]) => path.endsWith("/activity") && frame.phase === "started",
				)
			)
				controller.abort(new Error("stopped"));
			return result;
		}) as typeof transition;
		const ctx = { ...(fx.ctx() as any), storeSession: session };
		await assert.rejects(
			runWorkflowFile({ filePath: file, ctx: { ...ctx, signal: controller.signal } }),
		);
		assert.equal(fx.fake.calls(step.id), 0, "the command never started");
		const { runs } = await session.listRuns({ limit: 1 });
		const run = await session.getRunState(runs[0].id);
		assert.notEqual(run?.execution?.status, "blocked", `${kind}: nothing ran, nothing to inspect`);
		session.backend.transition = transition;
		const result = await advanceWorkflowRun({ runId: runs[0].id, ctx });
		assert.equal(result.status, "ok", kind);
		assert.equal(fx.fake.calls(step.id), 1, `${kind}: the command ran exactly once`);
	}
});

test("a pathologically nested or long condition is refused, not a stack overflow", () => {
	for (const text of [
		"!".repeat(20_000) + "true",
		"(".repeat(20_000) + "true" + ")".repeat(20_000),
		"[".repeat(20_000) + "1" + "]".repeat(20_000),
		"$a.json.v == 1 && ".repeat(5_000) + "true",
	])
		assert.throws(
			() => checkConditionSyntax(text),
			(error: Error) =>
				!(error instanceof RangeError) &&
				/nested too deeply|too long or nested too deeply/.test(error.message),
		);
	// Real conditions are nowhere near the limits.
	assert.doesNotThrow(() => checkConditionSyntax("((!($a.ok)))" + " && $b.ok".repeat(100)));
});

test("broken workflow files are refused with a message, not a TypeError", async (t) => {
	const fx = await workflowFixture(t);
	await assert.rejects(
		fx.run({
			steps: [{ id: "ask", input: { prompt: "?", responseSchema: { type: "object" } }, steps: {} }],
		}),
		/Workflow step ask has steps, which only for_each and repeat take/,
	);
	await assert.rejects(
		fx.run({ steps: [{ id: "child", workflow: "a\u0000b.lobster" }] }),
		/Workflow step child workflow cannot contain a NUL character/,
	);
	const yaml = path.join(fx.dir, "alias.lobster");
	await fs.writeFile(yaml, "steps:\n  - *missing\n");
	await assert.rejects(fx.run(yaml), /alias\.lobster is not valid YAML: /);
});

test("CLI usage mistakes are one line with exit code 2; a missing run is exit code 1", async (t) => {
	const fx = await workflowFixture(t);
	const bin = new URL("../../bin/lobster.js", import.meta.url).pathname;
	const cli = (...args: string[]) =>
		spawnSync(process.execPath, [bin, ...args], { env: fx.env as any, encoding: "utf8" });
	const usage = cli("runs", "frobnicate");
	assert.equal(usage.status, 2);
	assert.match(usage.stderr, /^Error: runs list/);
	assert.doesNotMatch(usage.stderr, /\n\s+at /);
	const missing = cli("runs", "show", "no-such-run");
	assert.equal(missing.status, 1);
	assert.equal(missing.stderr, "Error: Run no-such-run not found\n");
});

test("every CLI usage mistake exits 2 with one line, including signal, runs advance and abandon", async (t) => {
	const fx = await workflowFixture(t);
	const bin = new URL("../../bin/lobster.js", import.meta.url).pathname;
	for (const args of [
		["signal"],
		["signal", "x", "--bogus", "1"],
		["signal", "x", "--data", "{"],
		["runs", "advance"],
		["runs", "advance", "--due", "--limit", "abc"],
		["runs", "abandon"],
	]) {
		const run = spawnSync(process.execPath, [bin, ...args], {
			env: fx.env as any,
			encoding: "utf8",
		});
		assert.equal(run.status, 2, `lobster ${args.join(" ")}: ${run.stderr}`);
		assert.match(run.stderr, /^Error: [^\n]+\n$/, args.join(" "));
		assert.equal(run.stdout, "", args.join(" "));
	}
});

test("read-only commands work on a database made before records existed", async (t) => {
	const fx = await workflowFixture(t);
	await fx.run({ steps: [{ id: "w", pipeline: "fake --name w" }] });
	const runId = await fx.runId();
	const db = new DatabaseSync(path.join(fx.env.LOBSTER_STATE_DIR!, "lobster.sqlite"));
	db.exec("DROP TABLE execution_records");
	db.close();
	const bin = new URL("../../bin/lobster.js", import.meta.url).pathname;
	for (const args of [
		["runs", "show", runId],
		["storage", "export"],
		["history", "list"],
	]) {
		const run = spawnSync(process.execPath, [bin, ...args], {
			env: fx.env as any,
			encoding: "utf8",
		});
		assert.equal(run.status, 0, `lobster ${args.join(" ")}: ${run.stderr}`);
	}
});
