// The files in examples/ are documentation people copy: each must load, and run as its comments
// say it does.
import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { runToolRequest, signalEvent } from "../src/core/index.js";

const repoRoot = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const examples = path.join(repoRoot, "examples");
const cli = path.join(repoRoot, "bin", "lobster.js");
// The commands in the examples are POSIX shell.
const posix = { skip: process.platform === "win32" };

async function workflowFiles(dir = examples): Promise<string[]> {
	const found: string[] = [];
	for (const entry of await fs.readdir(dir, { withFileTypes: true })) {
		const full = path.join(dir, entry.name);
		if (entry.isDirectory()) found.push(...(await workflowFiles(full)));
		else if (entry.name.endsWith(".lobster")) found.push(full);
	}
	return found.sort();
}

async function stateEnv(t: test.TestContext) {
	const dir = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-examples-"));
	t.after(() => fs.rm(dir, { recursive: true, force: true }));
	// Model answers are cached per run here, not in the current directory.
	return {
		...process.env,
		LOBSTER_STATE_DIR: path.join(dir, "state"),
		LOBSTER_CACHE_DIR: path.join(dir, "cache"),
	};
}

/** Run an example in tool mode; `file` is relative to examples/. */
async function run(t: test.TestContext, file: string, extra: Record<string, unknown> = {}) {
	return runToolRequest({
		filePath: path.join(examples, file),
		ctx: { env: await stateEnv(t) },
		...extra,
	});
}

test("every example workflow loads and plans", async () => {
	const files = await workflowFiles();
	assert.ok(files.length >= 9, files.join("\n"));
	for (const file of files) {
		const result = spawnSync(process.execPath, [cli, "run", "--mode", "tool", "--dry-run", file], {
			encoding: "utf8",
		});
		assert.equal(result.status, 0, `${file}\n${result.stdout}${result.stderr}`);
	}
});

for (const [file, output] of [
	["hello.lobster", ["HELLO, WORLD!"]],
	["batch-process.lobster", [{ rows: 410 }]],
	["wait-for-ci.lobster", ["CI finished (success) after 3 checks.\n"]],
	["fan-out.lobster", ["21 degrees, 3 meetings; downloaded from the fast mirror.\n"]],
	[
		"compose/release-notes.lobster",
		["# Release 2.0\n- Fix sign-in on Safari\n- Faster search\n- Export to CSV\n"],
	],
] as const)
	test(`examples/${file} runs as its comments say`, posix, async (t) => {
		const result = await run(t, file);
		assert.equal(result.ok, true, JSON.stringify(result));
		assert.deepEqual(result.ok && result.output, output);
	});

test("examples/hello.lobster takes a name", posix, async (t) => {
	const result = await run(t, "hello.lobster", { args: { name: "Ada" } });
	assert.deepEqual(result.ok && result.output, ["Nice to meet you.\n"]);
});

test(
	"examples/deploy-with-approval.lobster stops for a person before deploying",
	posix,
	async (t) => {
		const result = await run(t, "deploy-with-approval.lobster", { args: { version: "1.4.2" } });
		assert.equal(result.ok && result.status, "needs_approval", JSON.stringify(result));
		assert.equal(result.ok && result.requiresApproval?.prompt, "Deploy this build to production?");
	},
);

test(
	"examples/wait-for-event.lobster waits, and continues when the event arrives",
	posix,
	async (t) => {
		const env = await stateEnv(t);
		const filePath = path.join(examples, "wait-for-event.lobster");
		const waiting = await runToolRequest({ filePath, args: { order: "o-1" }, ctx: { env } });
		assert.equal(waiting.ok && waiting.status, "waiting", JSON.stringify(waiting));
		const signalled = await signalEvent({
			name: "order.update",
			data: { order: "o-1", status: "paid" },
			ctx: { env },
		});
		assert.deepEqual(
			signalled.advanced.map((run) => run.status),
			["ok"],
		);
	},
);

test(
	"examples/triage-with-classifier.lobster routes a sure answer and asks about an unsure one",
	posix,
	async (t) => {
		const answer = (confidence: number) => ({
			model: async () => ({
				ok: true,
				result: { model: "stand-in", output: { data: { label: "refund", confidence } } },
			}),
		});
		// A fresh store and cache for each: the same email would otherwise get the cached answer.
		const triage = async (confidence: number) =>
			runToolRequest({
				filePath: path.join(examples, "triage-with-classifier.lobster"),
				ctx: {
					env: { ...(await stateEnv(t)), LOBSTER_LLM_PROVIDER: "model" },
					llmAdapters: answer(confidence),
				},
			});
		const sure = await triage(0.95);
		assert.deepEqual(sure.ok && sure.output, ["Refund opened.\n"], JSON.stringify(sure));
		const unsure = await triage(0.4);
		assert.equal(unsure.ok && unsure.status, "needs_input", JSON.stringify(unsure));
	},
);

test(
	"examples/sdk.ts runs, pauses for the person and resumes with the answer",
	// Node runs a .ts file directly where it strips types.
	{ skip: process.platform === "win32" || !process.features.typescript },
	async (t) => {
		const result = spawnSync(process.execPath, [path.join(examples, "sdk.ts")], {
			encoding: "utf8",
			input: "n\n",
			env: await stateEnv(t),
			cwd: repoRoot,
		});
		assert.equal(result.status, 0, result.stderr);
		assert.match(result.stdout, /grace@example\.com/);
		assert.match(result.stdout, /Send these emails\? \[y\/N\] cancelled \[\]/);
	},
);
