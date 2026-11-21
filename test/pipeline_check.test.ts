import test from "node:test";
import assert from "node:assert/strict";
import { promises as fsp } from "node:fs";
import os from "node:os";
import path from "node:path";

import { resumeToolRequest, runToolRequest } from "../src/core/index.js";

async function setup(t: test.TestContext, workflow: unknown) {
	const dir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-pipeline-check-"));
	t.after(() => fsp.rm(dir, { recursive: true, force: true }));
	const filePath = path.join(dir, "flow.lobster");
	await fsp.writeFile(filePath, JSON.stringify(workflow), "utf8");
	return { filePath, env: { ...process.env, LOBSTER_STATE_DIR: path.join(dir, "state") } };
}

test("a pipeline flag built from an input answer is left for the run to check", async (t) => {
	const { filePath, env } = await setup(t, {
		steps: [
			{
				id: "ask",
				input: { prompt: "Format?", responseSchema: { type: "object" } },
			},
			{
				id: "show",
				pipeline: "exec --stdin $ask.response.fmt --shell 'cat'",
				stdin: '{"a":1}',
			},
		],
	});
	const paused = await runToolRequest({ filePath, ctx: { env } });
	assert.equal(paused.status, "needs_input", JSON.stringify(paused));
	const done = await resumeToolRequest({
		token: paused.requiresInput!.resumeToken,
		response: { fmt: "json" },
		ctx: { env },
	});
	assert.equal(done.status, "ok", JSON.stringify(done));
});

test("a blank pipeline beside a shell command is no pipeline, as the run treats it", async (t) => {
	const { filePath, env } = await setup(t, {
		steps: [
			{ id: "a", run: "echo a", pipeline: "" },
			{ id: "p", parallel: { branches: [{ id: "b", run: "echo b", pipeline: " " }] } },
		],
	});
	const result = await runToolRequest({ filePath, ctx: { env } });
	assert.equal(result.status, "ok", JSON.stringify(result));
});

test("a step whose only command is blank is refused, not run as a no-op", async (t) => {
	for (const [step, message] of [
		[{ id: "a", run: "  " }, /Workflow step a requires run, command, pipeline/],
		[
			{ id: "p", parallel: { branches: [{ id: "b", pipeline: " " }] } },
			/parallel branch b requires run, command, or pipeline/,
		],
	] as const) {
		const { filePath, env } = await setup(t, { steps: [step] });
		const result = await runToolRequest({ filePath, ctx: { env } });
		assert.equal(result.ok, false);
		assert.match(result.error?.message ?? "", message);
	}
});
