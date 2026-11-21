import test from "node:test";
import assert from "node:assert/strict";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { Lobster } from "../src/sdk/Lobster.js";
import { approve, diffLast, exec, stateGet, stateSet } from "../src/sdk/primitives.js";
import { sdkStageRegistry } from "../src/sdk/runtime.js";
import { approveCommand } from "../src/commands/stdlib/approve.js";
import { execCommand } from "../src/commands/stdlib/exec.js";

async function tempDir(t: test.TestContext, prefix: string) {
	const dir = await mkdtemp(path.join(os.tmpdir(), prefix));
	t.after(() => rm(dir, { recursive: true, force: true }));
	return dir;
}

const node = (script: string) => `${JSON.stringify(process.execPath)} -e ${JSON.stringify(script)}`;

test("SDK approve asks with the items and a preview, and passes the items on after a yes", async (t) => {
	const stateDir = await tempDir(t, "lobster-sdk-approve-");
	const sent: unknown[] = [];
	const pipeline = () =>
		new Lobster({ stateDir })
			.pipe(() => ["a", "b", "c"])
			.pipe(approve({ prompt: "Send?", preview: true, limit: 2 }))
			.pipe((items: unknown[]) => {
				sent.push(...items);
				return items;
			});
	const paused = await pipeline().run();
	assert.equal(paused.status, "needs_approval");
	assert.equal(paused.requiresApproval?.prompt, "Send?");
	assert.deepEqual(paused.requiresApproval?.items, ["a", "b", "c"]);
	assert.equal(paused.requiresApproval?.preview, "a\nb");
	const done = await pipeline().resume(paused.requiresApproval!.resumeToken, { approved: true });
	assert.equal(done.status, "ok", JSON.stringify(done));
	assert.deepEqual(sent, ["a", "b", "c"]);
});

test("SDK stages carry the meta the engine acts on", () => {
	const registry = sdkStageRegistry([
		approve(),
		exec("true"),
		{
			meta: { repeatable: false, sideEffects: ["sends_message"] },
			run: async () => ({ output: [] }),
		},
		(items: unknown[]) => items,
	]);
	const meta = (index: number) => registry.get(`sdk.stage.${index}`)!.meta;
	assert.equal(meta(0).resumeSafeBeforeInput, approveCommand.meta.resumeSafeBeforeInput);
	assert.equal(meta(1).repeatable, execCommand.meta.repeatable);
	assert.equal(meta(2).repeatable, false);
	assert.equal(meta(3).repeatable, undefined);
	// The stage's own name and flags stay out of the registry entry.
	assert.equal(meta(0).description, "SDK stage 0");
});

test("SDK exec yields one item per line without JSON, and writes its input to stdin", async () => {
	const lines = await new Lobster()
		.pipe(exec(node('console.log("one\\ntwo")'), { json: false }))
		.run();
	assert.deepEqual(lines.output, ["one", "two"]);

	const echoed = await new Lobster()
		.pipe(() => [{ n: 1 }, { n: 2 }])
		.pipe(
			exec(node("process.stdin.pipe(process.stdout)"), {
				stdin: "json",
			}),
		)
		.run();
	assert.deepEqual(echoed.output, [{ n: 1 }, { n: 2 }]);
});

test(
	"SDK exec runs a quoted executable path with a space directly, not as a shell line",
	{ skip: process.platform === "win32" },
	async (t) => {
		const dir = await tempDir(t, "lobster-sdk-exec-space-");
		const script = path.join(dir, "my tool");
		await writeFile(script, `#!${process.execPath}\nconsole.log(JSON.stringify(["ran"]));\n`);
		await chmod(script, 0o755);
		const result = await new Lobster({ env: { ...process.env, LOBSTER_SHELL: "/nonexistent" } })
			.pipe(exec(JSON.stringify(script)))
			.run();
		assert.equal(result.ok, true, JSON.stringify(result));
		assert.deepEqual(result.output, ["ran"]);
	},
);

test("SDK diffLast and state stages behave as their commands", async (t) => {
	const stateDir = await tempDir(t, "lobster-sdk-diff-");
	const run = (value: unknown, changesOnly = false) =>
		new Lobster({ stateDir })
			.pipe(() => value)
			.pipe(diffLast("snapshot", { changesOnly }))
			.run();
	const first = await run({ v: 1 });
	assert.deepEqual(first.output, [
		{ kind: "diff.last", key: "snapshot", changed: true, before: null, after: { v: 1 } },
	]);
	const same = await run({ v: 1 }, true);
	assert.deepEqual(same.output, [
		{ kind: "diff.last", key: "snapshot", changed: false, suppressed: true },
	]);

	const stored = await new Lobster({ stateDir })
		.pipe(() => ({ saved: true }))
		.pipe(stateSet("key"))
		.run();
	assert.deepEqual(stored.output, [{ saved: true }]);
	const read = await new Lobster({ stateDir }).pipe(stateGet("key")).run();
	assert.deepEqual(read.output, [{ saved: true }]);
});
