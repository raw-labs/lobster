// Fuzzing: random and broken conditions, workflow files, pipelines and command lines. Lobster
// may refuse any of them, but only with an ordinary error that says what is wrong: never a
// TypeError or a stack overflow from deep inside, never a hang, never a crash of the CLI.
import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { stringify as toYaml } from "yaml";
import {
	checkCondition,
	evaluateCondition,
	parseCondition,
} from "../../src/workflows/conditions.js";
import { loadWorkflowFile } from "../../src/workflows/load.js";
import { snapshotDefinition } from "../../src/workflows/definition.js";
import { runWorkflowFile } from "../../src/engine/runner.js";
import { parsePipeline } from "../../src/pipeline/parser.js";
import { createDefaultRegistry } from "../../src/commands/registry.js";
import { workflowFixture } from "../helpers/workflow_runner.js";
import { BASE_SEED, cases, random, type Random } from "./settings.js";
import { generate } from "./model.js";

/** An error Lobster meant to raise, as opposed to one that escaped from a bug. */
function isDeliberate(error: unknown) {
	return (
		error instanceof Error &&
		!(error instanceof TypeError) &&
		!(error instanceof RangeError) &&
		!(error instanceof ReferenceError) &&
		Boolean(error.message)
	);
}

function expectClean(what: string, run: () => unknown) {
	try {
		run();
	} catch (error) {
		assert.ok(isDeliberate(error), `${what}\nescaped: ${(error as Error)?.stack ?? error}`);
	}
}

async function expectCleanAsync(what: string, run: () => Promise<unknown>) {
	try {
		await run();
	} catch (error) {
		assert.ok(isDeliberate(error), `${what}\nescaped: ${(error as Error)?.stack ?? error}`);
	}
}

// ---------------------------------------------------------------------------------------
// Conditions

const PIECES = [
	"$a.json.v",
	"$b.approved",
	"$c.json.list",
	"$a.json.s",
	"$",
	"$.",
	"$a.",
	"null",
	"true",
	"false",
	"0",
	"-1",
	"1e309",
	"0x10",
	"'x'",
	'"y"',
	'"',
	"'",
	"[",
	"]",
	"[1, 2]",
	"[",
	",",
	"(",
	")",
	"!",
	"!!",
	"&&",
	"||",
	"==",
	"!=",
	"<",
	">=",
	"=",
	"in",
	"contains",
	"matches",
	'matches "^a"',
	'matches "("',
	"word",
	" ",
	"\t",
	"\n",
	"\u0000",
	"é",
	"🦞",
	"__proto__",
	"$__proto__.json",
	"$a.json.constructor",
];

/** A condition built from the grammar, so most of them parse and reach evaluation. */
function grammatical(r: Random, depth = 0): string {
	const value = () =>
		r.pick([
			"$a.json.v",
			"$a.json.s",
			"$a.json.list",
			"$b.approved",
			"$c.json.list",
			"$a.json.missing",
			"1",
			"-1",
			'"abc"',
			"'b'",
			"null",
			"true",
			"[1, 2]",
			"[]",
			"word",
		]);
	const roll = r.next();
	if (depth > 3 || roll < 0.3) return value();
	if (roll < 0.5)
		return `${value()} ${r.pick(["==", "!=", "<", ">=", "in", "contains"])} ${value()}`;
	if (roll < 0.55) return `${value()} matches ${r.pick(['"^a"', '"b$"', '"[0-9]+"'])}`;
	if (roll < 0.65) return `!${grammatical(r, depth + 1)}`;
	if (roll < 0.75) return `(${grammatical(r, depth + 1)})`;
	return `${grammatical(r, depth + 1)} ${r.pick(["&&", "||"])} ${grammatical(r, depth + 1)}`;
}

function conditionText(r: Random) {
	if (r.chance(0.5)) {
		const text = grammatical(r);
		if (r.chance(0.6)) return text;
		const at = r.int(0, text.length);
		return text.slice(0, at) + r.pick(PIECES) + text.slice(at + r.int(0, 2));
	}
	if (r.chance(0.05)) {
		// Very deep nesting: must be refused, not overflow the stack.
		const depth = r.int(1000, 20000);
		const open = r.pick(["(", "!", "[", "(!"]);
		return open.repeat(depth) + "true" + (open === "!" ? "" : ")".repeat(depth));
	}
	if (r.chance(0.05)) return "$a.json.v == 1 && ".repeat(r.int(500, 5000)) + "true";
	return Array.from({ length: r.int(1, 12) }, () => r.pick(PIECES)).join(r.pick(["", " "]));
}

const RESULTS = {
	a: { id: "a", json: { v: 3, s: "abc", list: [1, 2], constructor: 1 } },
	b: { id: "b", approved: true },
	c: { id: "c", json: { list: ["x", { y: 1 }] } },
} as any;

test(
	"fuzz: conditions are parsed, checked and evaluated, or refused cleanly",
	{ timeout: 300_000 },
	() => {
		const r = random(BASE_SEED * 7 + 1);
		const total = cases(5_000, 200_000);
		let parsed = 0;
		for (let i = 0; i < total; i++) {
			const text = conditionText(r);
			const what = `condition #${i}: ${JSON.stringify(text.slice(0, 200))}`;
			const started = Date.now();
			expectClean(what, () => {
				parseCondition(text);
				parsed++;
				checkCondition(text, new Set(["a", "b", "c"]));
				const value = evaluateCondition(text, RESULTS);
				assert.equal(typeof value, "boolean", what);
			});
			assert.ok(Date.now() - started < 2000, `${what} took ${Date.now() - started} ms`);
		}
		assert.ok(parsed > total / 20, `only ${parsed} of ${total} parsed: the fuzzer is too blunt`);
	},
);

// ---------------------------------------------------------------------------------------
// Workflow files

const ODD_VALUES = [
	null,
	true,
	0,
	-1,
	1.5,
	1e300,
	"",
	"  ",
	"x",
	"$nope.json",
	"${missing}",
	[],
	[null],
	{},
	{ id: "x" },
	{ __proto__: { polluted: true } },
	"\u0000",
	"a".repeat(10_000),
];
const ODD_KEYS = ["wehn", "__proto__", "constructor", "steps", "when", "approval", "repeat", "x"];

/** Change one random place in a JSON value. */
function mutate(value: any, r: Random, depth = 0): any {
	if (value === null || typeof value !== "object" || (depth > 0 && r.chance(0.25))) {
		return r.chance(0.5) ? r.pick(ODD_VALUES) : r.chance(0.5) ? [value] : { value };
	}
	if (Array.isArray(value)) {
		if (!value.length || r.chance(0.15)) return r.chance(0.5) ? [] : [...value, r.pick(ODD_VALUES)];
		const copy = [...value];
		const at = r.int(0, copy.length - 1);
		const roll = r.next();
		if (roll < 0.15) copy.splice(at, 1);
		else if (roll < 0.3) copy.push(copy[at]); // duplicate ids
		else copy[at] = mutate(copy[at], r, depth + 1);
		return copy;
	}
	const copy: Record<string, unknown> = { ...value };
	const keys = Object.keys(copy);
	const roll = r.next();
	if (!keys.length || roll < 0.15) copy[r.pick(ODD_KEYS)] = r.pick(ODD_VALUES);
	else if (roll < 0.3) delete copy[r.pick(keys)];
	else {
		const key = r.pick(keys);
		copy[key] = mutate(copy[key], r, depth + 1);
	}
	return copy;
}

test(
	"fuzz: broken workflow files load, or are refused cleanly",
	{ timeout: 600_000 },
	async (t) => {
		const fx = await workflowFixture(t, "lobster-fuzz-");
		const r = random(BASE_SEED * 7 + 2);
		const total = cases(300, 10_000);
		let loaded = 0;
		for (let i = 0; i < total; i++) {
			const g = generate(BASE_SEED * 7_000 + i, (name) => fx.fake.shell(name));
			let workflow: any = g.workflow;
			for (let n = r.int(0, 2); n > 0; n--) workflow = mutate(workflow, r);
			for (const [file, child] of Object.entries(g.children))
				await fx.write(file, r.chance(0.3) ? mutate(child, r) : child);
			const yaml = r.chance(0.3);
			const file = path.join(fx.dir, yaml ? "fuzz.lobster" : "fuzz.json");
			let text = yaml ? toYaml(workflow) : JSON.stringify(workflow);
			if (r.chance(0.1)) {
				// Damage the text itself.
				const at = r.int(0, text.length);
				text =
					text.slice(0, at) +
					r.pick(["{", "]", ":", "\t", "- ", "'", "&a", "*a", "!!js"]) +
					text.slice(at + 1);
			}
			await fs.writeFile(file, text);
			const what = `workflow #${i}: ${text.slice(0, 1500)}`;
			await expectCleanAsync(what, async () => {
				await loadWorkflowFile(file);
				await snapshotDefinition(file, {});
				loaded++;
				// A file that loads can be planned.
				await runWorkflowFile({ filePath: file, ctx: { ...(fx.ctx() as any), dryRun: true } });
			});
		}
		t.diagnostic(`loaded ${loaded} of ${total}`);
		assert.ok(loaded > 0 && loaded < total, `loaded ${loaded} of ${total}: the mutations miss`);
	},
);

// ---------------------------------------------------------------------------------------
// Pipelines

const PIPELINE_PIECES = [
	"exec",
	"--json",
	"--shell",
	"'echo [1]'",
	'"',
	"'",
	"|",
	"||",
	"where",
	"'0>=0'",
	"head",
	"--n",
	"2",
	"--n=",
	"-",
	"--",
	"approve",
	"--prompt",
	"ask",
	"map",
	"--unknown",
	"nope.cmd",
	"\\",
	"$(id)",
	"`id`",
	";",
	"&&",
	"\n",
	"🦞",
	"=",
];

test("fuzz: pipeline text is parsed, or refused cleanly", { timeout: 300_000 }, () => {
	const r = random(BASE_SEED * 7 + 3);
	const registry = createDefaultRegistry();
	for (let i = 0, total = cases(5_000, 100_000); i < total; i++) {
		const text = Array.from({ length: r.int(1, 10) }, () => r.pick(PIPELINE_PIECES)).join(
			r.pick([" ", "", " | "]),
		);
		expectClean(`pipeline #${i}: ${JSON.stringify(text)}`, () => parsePipeline(text, registry));
	}
});

// ---------------------------------------------------------------------------------------
// The command line

const ARGS = [
	"run",
	"resume",
	"runs",
	"list",
	"show",
	"advance",
	"abandon",
	"waits",
	"signal",
	"graph",
	"upgrade",
	"doctor",
	"history",
	"storage",
	"export",
	"prune",
	"--mode",
	"tool",
	"--file",
	"missing.lobster",
	"--args-json",
	"{",
	"[]",
	'{"a":1}',
	"--approve",
	"yes",
	"maybe",
	"--response-json",
	"--due",
	"--limit",
	"-1",
	"0",
	"--dry-run",
	"--format",
	"svg",
	"--data",
	"--id",
	"--cancel",
	"--expected-revision",
	"x",
	"--status",
	"weird",
	"--help",
	"-h",
	"exec --json --shell 'echo [1]'",
	"",
	"--",
	"🦞",
	"not-a-run-id",
];

test(
	"fuzz: the CLI answers any arguments with an exit code, not a crash",
	{ timeout: 600_000 },
	async (t) => {
		const dir = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-fuzz-cli-"));
		t.after(() => fs.rm(dir, { recursive: true, force: true }));
		const bin = new URL("../../../bin/lobster.js", import.meta.url).pathname;
		const r = random(BASE_SEED * 7 + 4);
		for (let i = 0, total = cases(40, 1_000); i < total; i++) {
			const args = Array.from({ length: r.int(0, 6) }, () => r.pick(ARGS));
			const run = spawnSync(process.execPath, [bin, ...args], {
				cwd: dir,
				env: { ...process.env, LOBSTER_STATE_DIR: path.join(dir, "state") },
				encoding: "utf8",
				timeout: 30_000,
				input: "",
			});
			const what = `lobster ${args.map((a) => JSON.stringify(a)).join(" ")}\nstatus ${run.status} signal ${run.signal}\nstderr: ${run.stderr.slice(0, 2000)}`;
			assert.equal(run.signal, null, `killed or timed out: ${what}`);
			assert.ok([0, 1, 2].includes(run.status!), what);
			assert.doesNotMatch(
				run.stderr,
				/TypeError|RangeError|ReferenceError|Maximum call stack|\n\s+at .*\(.*:\d+:\d+\)/,
				what,
			);
		}
	},
);
