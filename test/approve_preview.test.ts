import test from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { createDefaultRegistry } from "../src/commands/registry.js";
import { defineCommand } from "../src/commands/define.js";
import { runToolRequest } from "../src/core/index.js";

function streamOf(items) {
	return (async function* () {
		for (const item of items) yield item;
	})();
}

test("approve preview includes stdin sample when requested", async () => {
	const registry = createDefaultRegistry();
	const cmd = registry.get("approve");

	const result = await cmd.run({
		input: streamOf([{ a: 1 }, { a: 2 }]),
		args: {
			_: [],
			emit: true,
			prompt: "ok?",
			"preview-from-stdin": true,
			limit: 1,
		},
		ctx: {
			stdin: process.stdin,
			stdout: process.stdout,
			stderr: process.stderr,
			env: process.env,
			registry,
			mode: "tool",
			render: { json() {}, lines() {} },
		},
	});

	const items = [];
	for await (const item of result.output) items.push(item);
	assert.equal(items[0].type, "approval_request");
	assert.ok(String(items[0].preview).includes('"a": 1'));
});

test("a flag given with its alias, a flag without its value, or a number flag without a number, is refused before any stage runs", async (t) => {
	const dir = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-flag-spelling-"));
	t.after(() => fs.rm(dir, { recursive: true, force: true }));
	const marker = path.join(dir, "acted");
	const env = { ...process.env, LOBSTER_STATE_DIR: path.join(dir, "state"), MARKER: marker };
	const act = `exec --json --shell 'echo x > "$MARKER"; echo 1'`;
	for (const stage of [
		"approve --limit 2 --preview-limit 3",
		"approve --preview-from-stdin --previewFromStdin",
		"ask --subject-from-stdin --subjectFromStdin",
		"approve --limit abc",
		"approve --limit --prompt ok",
		"approve --prompt --limit 2",
		"ask --prompt --emit",
	]) {
		const result = await runToolRequest({ pipeline: `${act} | ${stage}`, ctx: { env } });
		assert.equal(result.ok, false, stage);
		assert.equal(!result.ok && result.error.type, "parse_error", stage);
		assert.match(
			!result.ok ? result.error.message : "",
			/not both|takes a number|takes a value/,
			stage,
		);
		await assert.rejects(fs.access(marker), { code: "ENOENT" }, stage);
	}
});

test("an alias of a flag the command does not declare is refused when the command is defined", () => {
	assert.throws(
		() =>
			defineCommand({
				name: "test.alias",
				meta: {
					description: "Alias to nowhere",
					argsSchema: {
						type: "object",
						properties: { previewLimit: { type: "number", aliasOf: "preview_limit" } },
					},
					sideEffects: [],
					repeatable: true,
				},
				help: () => "",
				run: async () => ({ output: [] }),
			}),
		/flag "previewLimit" is an alias of undeclared flag "preview_limit"/,
	);
});
