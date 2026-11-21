import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { access, mkdtemp, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";

const targetForms = {
	"--file": (target: string) => ["--file", target],
	positional: (target: string) => [target],
	"positional --dry-run": (target: string) => [target, "--dry-run"],
};

for (const [form, targetArgs] of Object.entries(targetForms)) {
	for (const kind of ["missing", "directory", "extension"] as const) {
		test(`tool run reports ${kind} ${form} workflow target errors as JSON`, async () => {
			const dir = await mkdtemp(join(tmpdir(), "lobster-target-error-"));
			try {
				const target =
					kind === "directory"
						? dir
						: join(dir, kind === "extension" ? "workflow.txt" : "missing.lobster");
				if (kind === "extension")
					await writeFile(target, JSON.stringify({ steps: [{ id: "ok", run: "echo ok" }] }));
				const result = spawnSync(
					process.execPath,
					["bin/lobster.js", "run", "--mode", "tool", ...targetArgs(target)],
					{ encoding: "utf8" },
				);
				assert.equal(result.status, 2);
				const envelope = JSON.parse(result.stdout);
				assert.equal(envelope.protocolVersion, 1);
				assert.equal(envelope.ok, false);
				assert.equal(envelope.error.type, "parse_error");
				assert.match(
					envelope.error.message,
					kind === "missing"
						? /Workflow file not found/
						: kind === "directory"
							? /not a file/
							: /must end in/,
				);
				assert.equal(result.stderr, "");
			} finally {
				await rm(dir, { recursive: true, force: true });
			}
		});
	}
}

test("human run reports a missing relative positional workflow as a parse error", async () => {
	const dir = await mkdtemp(join(tmpdir(), "lobster-positional-error-"));
	try {
		const result = spawnSync(
			process.execPath,
			[resolve("bin/lobster.js"), "run", "missing.lobster"],
			{ cwd: dir, encoding: "utf8" },
		);
		assert.equal(result.status, 2);
		assert.equal(result.stdout, "");
		assert.match(result.stderr, /^Parse error: Workflow file not found/);
		assert.doesNotMatch(result.stderr, /Unknown command/);
	} finally {
		await rm(dir, { recursive: true, force: true });
	}
});

for (const token of ["llm.invok", "--unknown=flows/data.json"]) {
	test(`run still reports ${token} as an unknown command`, () => {
		const result = spawnSync(process.execPath, ["bin/lobster.js", "run", "--mode", "tool", token], {
			encoding: "utf8",
		});
		// Refused before a run is created: an input error, with no run left behind.
		assert.equal(result.status, 2);
		const envelope = JSON.parse(result.stdout);
		assert.equal(envelope.error.type, "parse_error");
		assert.equal(envelope.error.message, `Unknown command: ${token}`);
		assert.equal(envelope.runId, undefined);
	});
}

test("run keeps a quoted pipeline with path-like arguments as a pipeline", () => {
	const result = spawnSync(
		process.execPath,
		[
			"bin/lobster.js",
			"run",
			"--mode",
			"tool",
			'exec --json node -e "console.log(JSON.stringify(process.argv.slice(1)))" flows/data.json',
		],
		{ encoding: "utf8" },
	);
	assert.equal(result.status, 0, result.stderr);
	const envelope = JSON.parse(result.stdout);
	assert.equal(envelope.ok, true);
	assert.deepEqual(envelope.output, ["flows/data.json"]);
});

test("run keeps a quoted pipeline with --dry-run as a pipeline", () => {
	const result = spawnSync(
		process.execPath,
		["bin/lobster.js", "run", "--mode", "tool", "a/b|json", "--dry-run"],
		{ encoding: "utf8" },
	);
	assert.equal(result.status, 2);
	const envelope = JSON.parse(result.stdout);
	assert.equal(envelope.error.type, "parse_error");
	assert.equal(envelope.error.message, "Unknown command: a/b");
});

test("human run reports missing workflow without an uncaught stack", async () => {
	const dir = await mkdtemp(join(tmpdir(), "lobster-human-error-"));
	try {
		const result = spawnSync(
			process.execPath,
			["bin/lobster.js", "run", "--file", join(dir, "missing.json")],
			{ encoding: "utf8" },
		);
		assert.equal(result.status, 2);
		assert.equal(result.stdout, "");
		assert.match(result.stderr, /^Parse error: Workflow file not found/);
		assert.doesNotMatch(result.stderr, /at async|node:internal/);
	} finally {
		await rm(dir, { recursive: true, force: true });
	}
});

for (const flag of ["--help", "-h"]) {
	test(`graph ${flag} shows graph-specific options`, () => {
		const result = spawnSync(process.execPath, ["bin/lobster.js", "graph", flag], {
			encoding: "utf8",
		});
		assert.equal(result.status, 0);
		assert.match(result.stdout, /--format\s+Output format: mermaid \(default\), dot, ascii/);
		assert.equal(result.stderr, "");
	});
}

test("help graph shows the graph help listed under Commands", () => {
	const result = spawnSync(process.execPath, ["bin/lobster.js", "help", "graph"], {
		encoding: "utf8",
	});
	assert.equal(result.status, 0);
	assert.match(result.stdout, /--format\s+Output format: mermaid \(default\), dot, ascii/);
	assert.equal(result.stderr, "");
});

for (const topic of ["run", "resume", "doctor", "version", "help"]) {
	test(`help ${topic} shows the usage that documents it instead of Unknown command`, () => {
		const result = spawnSync(process.execPath, ["bin/lobster.js", "help", topic], {
			encoding: "utf8",
		});
		const flagHelp = spawnSync(process.execPath, ["bin/lobster.js", topic, "--help"], {
			encoding: "utf8",
		});
		assert.equal(result.status, 0);
		assert.equal(result.stderr, "");
		assert.match(result.stdout, new RegExp(`^lobster ${topic}\\b`, "m"));
		assert.equal(result.stdout, flagHelp.stdout);
	});
}

test("help with an unknown topic still fails", () => {
	const result = spawnSync(process.execPath, ["bin/lobster.js", "help", "nope"], {
		encoding: "utf8",
	});
	assert.equal(result.status, 2);
	assert.match(result.stderr, /^Unknown command: nope\b.*lobster help/);
});

test("human run names a run that ended in an error, so it can be looked at", async () => {
	const dir = await mkdtemp(join(tmpdir(), "lobster-failed-run-"));
	try {
		const file = join(dir, "flow.lobster");
		await writeFile(file, JSON.stringify({ steps: [{ id: "boom", run: "exit 3" }] }));
		const env = { ...process.env, LOBSTER_STATE_DIR: join(dir, "state") };
		const result = spawnSync(process.execPath, ["bin/lobster.js", "run", file], {
			encoding: "utf8",
			env,
		});
		assert.equal(result.status, 1);
		const named = /^Run ([0-9a-f-]{36}): /.exec(result.stderr);
		assert.ok(named, result.stderr);
		const runs = spawnSync(process.execPath, ["bin/lobster.js", "runs", "list"], {
			encoding: "utf8",
			env,
		});
		assert.equal(JSON.parse(runs.stdout).runs[0]?.id, named[1]);
		// A file that never starts a run has no run to name.
		const missing = spawnSync(process.execPath, ["bin/lobster.js", "run", join(dir, "x.lobster")], {
			encoding: "utf8",
			env,
		});
		assert.doesNotMatch(missing.stderr, /^Run /);
	} finally {
		await rm(dir, { recursive: true, force: true });
	}
});

test("run refuses a misspelled --mode instead of running in human mode", () => {
	const result = spawnSync(
		process.execPath,
		["bin/lobster.js", "run", "--mode", "tol", "exec --json --shell 'echo [1]'"],
		{ encoding: "utf8" },
	);
	assert.equal(result.status, 2);
	assert.match(result.stderr, /--mode must be human or tool \(got "tol"\)/);
	assert.equal(result.stdout, "");
});

test("run refuses arguments its target would not use", async () => {
	const dir = await mkdtemp(join(tmpdir(), "lobster-dropped-args-"));
	try {
		const file = join(dir, "flow.lobster");
		await writeFile(file, JSON.stringify({ steps: [{ id: "s", run: "echo ok" }] }));
		const env = { ...process.env, LOBSTER_STATE_DIR: join(dir, "state") };
		const run = (...args: string[]) =>
			spawnSync(process.execPath, ["bin/lobster.js", ...args], { encoding: "utf8", env });

		const argsForPipeline = run("run", "--args-json", '{"n":5}', "json");
		assert.equal(argsForPipeline.status, 2);
		assert.match(argsForPipeline.stderr, /--args-json applies to a workflow file/);

		const wordsAfterFile = run("run", "--file", file, '{"n":5}');
		assert.equal(wordsAfterFile.status, 2);
		assert.match(wordsAfterFile.stderr, /run --file takes no pipeline/);

		const dryRunAfterFile = run("run", "--file", file, "--dry-run");
		assert.equal(dryRunAfterFile.status, 0, dryRunAfterFile.stderr);
	} finally {
		await rm(dir, { recursive: true, force: true });
	}
});

test("runs advance reads --due as a flag, and refuses --due options without it", async () => {
	const dir = await mkdtemp(join(tmpdir(), "lobster-advance-flags-"));
	try {
		const env = { ...process.env, LOBSTER_STATE_DIR: join(dir, "state") };
		const run = (...args: string[]) =>
			spawnSync(process.execPath, ["bin/lobster.js", "runs", "advance", ...args], {
				encoding: "utf8",
				env,
			});
		const explicit = run("--due=true");
		assert.equal(explicit.status, 0, explicit.stderr);
		assert.deepEqual(JSON.parse(explicit.stdout).due, []);

		const off = run("--due", "false");
		assert.equal(off.status, 2, off.stdout);
		assert.match(JSON.parse(off.stdout).error.message, /Usage: lobster runs advance <runId>/);

		const limitWithoutDue = run("some-run", "--limit", "3");
		assert.equal(limitWithoutDue.status, 2);
		assert.match(
			JSON.parse(limitWithoutDue.stdout).error.message,
			/--limit applies only with --due/,
		);
	} finally {
		await rm(dir, { recursive: true, force: true });
	}
});

test("a pipeline dry run returns the same tool envelope as any successful run", () => {
	const result = spawnSync(
		process.execPath,
		["bin/lobster.js", "run", "--mode", "tool", "--dry-run", "exec --json --shell 'echo [1]'"],
		{ encoding: "utf8" },
	);
	assert.equal(result.status, 0, result.stderr);
	const envelope = JSON.parse(result.stdout);
	assert.equal(envelope.status, "ok");
	assert.equal(envelope.requiresApproval, null);
	assert.equal(envelope.requiresInput, null);
});

test("history and storage name the command typed in a usage error", () => {
	const run = (...args: string[]) =>
		spawnSync(process.execPath, ["bin/lobster.js", ...args], { encoding: "utf8" });
	const exported = run("storage", "export", "--json");
	assert.equal(exported.status, 2);
	assert.match(JSON.parse(exported.stdout).error.message, /storage export has no flag --json/);
	const shown = run("history", "show", "some-run", "--apply");
	assert.equal(shown.status, 2);
	assert.match(JSON.parse(shown.stdout).error.message, /history show has no flag --apply/);
});

test("runs advance names the run in its error envelope, as run --mode tool does", async () => {
	const dir = await mkdtemp(join(tmpdir(), "lobster-advance-error-"));
	try {
		const file = join(dir, "flow.lobster");
		await writeFile(file, JSON.stringify({ steps: [{ id: "boom", run: "exit 3" }] }));
		const env = { ...process.env, LOBSTER_STATE_DIR: join(dir, "state") };
		const run = (...args: string[]) =>
			spawnSync(process.execPath, ["bin/lobster.js", ...args], { encoding: "utf8", env });
		const first = JSON.parse(run("run", "--mode", "tool", file).stdout);
		assert.equal(first.ok, false);
		assert.ok(first.runId);
		const again = run("runs", "advance", first.runId);
		assert.equal(again.status, 1);
		const envelope = JSON.parse(again.stdout);
		assert.equal(envelope.ok, false);
		assert.equal(envelope.runId, first.runId);
	} finally {
		await rm(dir, { recursive: true, force: true });
	}
});

test("resume by wait ID reports an answered wait as wait_conflict, with or without a revision", async () => {
	const dir = await mkdtemp(join(tmpdir(), "lobster-answered-wait-"));
	try {
		const file = join(dir, "flow.lobster");
		await writeFile(file, JSON.stringify({ steps: [{ id: "ask", approval: "Go?" }] }));
		const env = { ...process.env, LOBSTER_STATE_DIR: join(dir, "state") };
		const run = (...args: string[]) =>
			spawnSync(process.execPath, ["bin/lobster.js", ...args], { encoding: "utf8", env });
		const asked = JSON.parse(run("run", "--mode", "tool", file).stdout);
		const shown = JSON.parse(run("runs", "show", asked.runId).stdout);
		const waitId = shown.run.pendingWaits[0].id;
		assert.equal(JSON.parse(run("resume", waitId, "--approve", "yes").stdout).ok, true);
		for (const args of [[], ["--expected-revision", "0"]]) {
			const again = JSON.parse(run("resume", waitId, "--approve", "yes", ...args).stdout);
			assert.equal(again.error?.type, "wait_conflict", JSON.stringify(again));
		}
	} finally {
		await rm(dir, { recursive: true, force: true });
	}
});

test("exec --shell refuses words after its command line instead of dropping them", () => {
	const result = spawnSync(
		process.execPath,
		["bin/lobster.js", "run", "--mode", "tool", `exec --json --shell "echo [1]" rm-me-too`],
		{ encoding: "utf8" },
	);
	const envelope = JSON.parse(result.stdout);
	assert.equal(envelope.ok, false);
	assert.match(envelope.error.message, /got extra words: rm-me-too/);
});

test("exec refuses a bare --shell instead of running only the first word", () => {
	const result = spawnSync(
		process.execPath,
		["bin/lobster.js", "run", "--mode", "tool", "exec echo one two --shell"],
		{ encoding: "utf8" },
	);
	const envelope = JSON.parse(result.stdout);
	assert.equal(envelope.ok, false);
	assert.match(envelope.error.message, /exec --shell takes a value/);
});

test("exec refuses a bare --stdin instead of dropping the input", () => {
	const result = spawnSync(
		process.execPath,
		[
			"bin/lobster.js",
			"run",
			"--mode",
			"tool",
			`exec --json "echo [1,2]" | exec --stdin --json cat`,
		],
		{ encoding: "utf8" },
	);
	const envelope = JSON.parse(result.stdout);
	assert.equal(envelope.ok, false);
	assert.match(envelope.error.message, /exec --stdin takes one of raw, json/);
});

test("graph refuses words it would not use", async () => {
	const dir = await mkdtemp(join(tmpdir(), "lobster-graph-words-"));
	try {
		const file = join(dir, "a.lobster");
		await writeFile(file, JSON.stringify({ steps: [{ id: "s", run: "echo ok" }] }));
		for (const args of [
			["graph", file, "ascii"],
			["graph", "--file", file, file],
		]) {
			const result = spawnSync(process.execPath, ["bin/lobster.js", ...args], { encoding: "utf8" });
			assert.equal(result.status, 2, args.join(" "));
			assert.match(result.stderr, /Usage: lobster graph/);
		}
		const ok = spawnSync(process.execPath, ["bin/lobster.js", "graph", file], { encoding: "utf8" });
		assert.equal(ok.status, 0, ok.stderr);
	} finally {
		await rm(dir, { recursive: true, force: true });
	}
});

test("exec refuses an empty --stdin value instead of dropping the input", () => {
	const result = spawnSync(
		process.execPath,
		[
			"bin/lobster.js",
			"run",
			"--mode",
			"tool",
			`exec --json --shell 'echo [1,2]' | exec --stdin= --shell 'cat'`,
		],
		{ encoding: "utf8" },
	);
	const envelope = JSON.parse(result.stdout);
	assert.equal(envelope.ok, false);
	assert.match(envelope.error.message, /--stdin must be one of raw, json, jsonl/);
});

test("run reports a pipeline that does not parse before touching the store", async () => {
	const dir = await mkdtemp(join(tmpdir(), "lobster-parse-first-"));
	try {
		const state = join(dir, "state");
		const result = spawnSync(
			process.execPath,
			["bin/lobster.js", "run", "--mode", "tool", `exec "foo`],
			{
				encoding: "utf8",
				env: { ...process.env, LOBSTER_STATE_DIR: state },
			},
		);
		assert.equal(result.status, 2);
		assert.equal(JSON.parse(result.stdout).error.type, "parse_error");
		await assert.rejects(access(state), "nothing was written");
		// A state directory that cannot be opened does not hide the parse error.
		const blocked = join(dir, "file");
		await writeFile(blocked, "");
		const again = spawnSync(
			process.execPath,
			["bin/lobster.js", "run", "--mode", "tool", `exec "foo`],
			{
				encoding: "utf8",
				env: { ...process.env, LOBSTER_STATE_DIR: blocked },
			},
		);
		assert.equal(JSON.parse(again.stdout).error.type, "parse_error");
	} finally {
		await rm(dir, { recursive: true, force: true });
	}
});

test("graph reports a missing workflow file as an input error (exit 2)", () => {
	const result = spawnSync(process.execPath, ["bin/lobster.js", "graph", "missing.lobster"], {
		encoding: "utf8",
	});
	assert.equal(result.status, 2, result.stderr);
});

test("runs advance and abandon refuse a malformed run ID before touching the store", async () => {
	const dir = await mkdtemp(join(tmpdir(), "lobster-bad-run-id-"));
	try {
		const state = join(dir, "state");
		for (const command of ["advance", "abandon"]) {
			const result = spawnSync(process.execPath, ["bin/lobster.js", "runs", command, "bad id!"], {
				encoding: "utf8",
				env: { ...process.env, LOBSTER_STATE_DIR: state },
			});
			assert.match(result.stdout + result.stderr, /Invalid run ID/, command);
		}
		await assert.rejects(access(state), "nothing was written");
	} finally {
		await rm(dir, { recursive: true, force: true });
	}
});

test("history show and list follow the run-ID rules of runs show and runs list", async () => {
	const dir = await mkdtemp(join(tmpdir(), "lobster-history-ids-"));
	try {
		const env = { ...process.env, LOBSTER_STATE_DIR: join(dir, "state") };
		const run = (...args: string[]) =>
			spawnSync(process.execPath, ["bin/lobster.js", ...args], { encoding: "utf8", env });
		const error = (result: { stdout: string }) => JSON.parse(result.stdout).error.message;
		const missing = run("history", "show", "nosuchrun");
		assert.equal(missing.status, 1);
		assert.match(error(missing), /Run nosuchrun not found/);
		// A malformed ID or cursor is a mistake in the command line.
		const malformed = run("history", "show", "bad id!");
		assert.equal(malformed.status, 2);
		assert.match(error(malformed), /Invalid run ID/);
		const cursor = run("history", "list", "zz!");
		assert.equal(cursor.status, 2);
		assert.match(error(cursor), /Invalid run cursor/);
	} finally {
		await rm(dir, { recursive: true, force: true });
	}
});

test("resume reads any well-formed wait ID as a wait ID, not as a token", () => {
	// Not UUID-shaped, but a valid run ID: refused as a wait that does not exist.
	const waitId = `run_1.${"a".repeat(64)}`;
	const result = spawnSync(
		process.execPath,
		["bin/lobster.js", "resume", waitId, "--approve", "yes"],
		{
			encoding: "utf8",
		},
	);
	const envelope = JSON.parse(result.stdout);
	assert.equal(envelope.ok, false);
	assert.doesNotMatch(envelope.error.message, /Invalid token/);
});

test("exec refuses an unknown --stdin format before the stages before it run", async () => {
	const dir = await mkdtemp(join(tmpdir(), "lobster-stdin-format-"));
	try {
		const marker = join(dir, "side");
		const result = spawnSync(
			process.execPath,
			[
				"bin/lobster.js",
				"run",
				"--mode",
				"tool",
				`exec --shell 'echo X >> ${marker}; echo a' | exec --stdin bogus cat`,
			],
			{ encoding: "utf8", env: { ...process.env, LOBSTER_STATE_DIR: join(dir, "state") } },
		);
		assert.match(
			JSON.parse(result.stdout).error.message,
			/--stdin must be one of raw, json, jsonl/,
		);
		await assert.rejects(access(marker), "the stage before it did not run");
	} finally {
		await rm(dir, { recursive: true, force: true });
	}
});
