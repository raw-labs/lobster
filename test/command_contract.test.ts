// Every command Lobster ships is held to what it declares: a flag schema that matches the flags
// its code reads, help under each name it answers to, rejection of unknown flags, and a
// `repeatable` setting the engine enforces. Adding a command makes these run for it too.
import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { promises as fs } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { builtinPacks, createDefaultRegistry, createRegistry } from "../src/commands/registry.js";
import { defineCommand } from "../src/commands/define.js";
import type { CommandPack, DefinedCommand } from "../src/commands/types.js";
import { parsePipeline } from "../src/pipeline/parser.js";
import { runPipeline } from "../src/pipeline/runtime.js";
import { crashAndRecover } from "./helpers/crash_recovery.js";
import { workflowFixture } from "./helpers/workflow_runner.js";

const registry = createDefaultRegistry();
const commands = [...new Set(builtinPacks.flatMap((pack) => pack.commands))];
const repoRoot = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "..");

/** The source file that defines a command, found by its `name:` literal. */
function sourceOf(command: DefinedCommand): string {
	const files = (dir: string): string[] =>
		readdirSync(dir, { withFileTypes: true }).flatMap((entry) =>
			entry.isDirectory()
				? files(path.join(dir, entry.name))
				: entry.name.endsWith(".ts")
					? [path.join(dir, entry.name)]
					: [],
		);
	const all = [
		...files(path.join(repoRoot, "src", "commands")),
		...files(path.join(repoRoot, "src", "packs")),
	];
	const found = all.find((file) => readFileSync(file, "utf8").includes(`name: "${command.name}"`));
	assert.ok(found, `no source file defines ${command.name}`);
	return readFileSync(found, "utf8");
}

const empty = async function* () {};

async function run(pipeline: string) {
	return runPipeline({
		pipeline: parsePipeline(pipeline, registry),
		registry,
		input: empty(),
		stdin: process.stdin,
		stdout: process.stdout,
		stderr: process.stderr,
		env: { ...process.env },
		mode: "tool",
	} as any);
}

for (const command of commands) {
	const names = [command.name, ...(command.meta.aliases ?? [])];

	test(`${command.name}: declares its contract`, () => {
		assert.ok(command.meta.description.trim());
		assert.equal(command.meta.argsSchema.type, "object");
		assert.ok(Array.isArray(command.meta.sideEffects));
		assert.equal(typeof command.meta.repeatable, "boolean");
		for (const name of names) assert.equal(registry.get(name), command, `${name} resolves`);
	});

	test(`${command.name}: every flag its code reads is declared`, () => {
		const declared = Object.keys(command.meta.argsSchema.properties);
		const read = new Set(
			[...sourceOf(command).matchAll(/args\??\.([A-Za-z_]\w*)|args\["([^"]+)"\]/g)].map(
				(m) => m[1] ?? m[2],
			),
		);
		const undeclared = [...read].filter((flag) => !declared.includes(flag));
		assert.deepEqual(undeclared, [], `${command.name} reads undeclared flags`);
	});

	test(`${command.name}: help names the command under each of its names`, () => {
		for (const name of names)
			assert.match(command.help(name), new RegExp(name.replace(".", "\\.")));
	});

	test(`${command.name}: an unknown flag fails before the command runs`, async () => {
		await assert.rejects(
			run(`${command.name} --definitely-not-a-flag x`),
			new RegExp(`${command.name.replace(".", "\\.")} has no flag --definitely-not-a-flag`),
		);
	});

	const booleans = Object.entries(command.meta.argsSchema.properties)
		.filter(([, spec]) => spec.type === "boolean")
		.map(([flag]) => flag);
	if (booleans.length)
		test(`${command.name}: boolean flags never swallow the next word`, () => {
			for (const flag of booleans) {
				const [stage] = parsePipeline(`${command.name} --${flag} word`, registry);
				assert.equal(stage.args[flag], true);
				assert.deepEqual(stage.args._, ["word"]);
				const [off] = parsePipeline(`${command.name} --${flag} false`, registry);
				assert.equal(off.args[flag], false);
			}
		});
}

test("a misspelled flag gets a suggestion", async () => {
	await assert.rejects(run("head --m 2"), /head has no flag --m \(did you mean --n\?\)/);
	await assert.rejects(run("sort --dsec"), /did you mean --desc\?/);
});

// Changing this list changes what Lobster may repeat on its own; it should be deliberate.
test("only commands that act externally are not repeatable, and each reports dispatch", () => {
	const unrepeatable = commands.filter((c) => !c.meta.repeatable).map((c) => c.name);
	assert.deepEqual(unrepeatable.sort(), ["gog.gmail.send", "openclaw.agent", "openclaw.invoke"]);
	for (const command of commands.filter((c) => !c.meta.repeatable))
		assert.match(
			sourceOf(command),
			/ctx\.onNonRetryableSideEffect\?\.\(\)/,
			`${command.name} must report dispatch so retries stop`,
		);
});

test("backward-compatible names are aliases or variants, not copies", () => {
	assert.equal(registry.get("clawd.invoke"), registry.get("openclaw.invoke"));
	const variant = registry.get("llm_task.invoke")!;
	assert.notEqual(variant, registry.get("llm.invoke"), "llm_task.invoke keeps its own namespace");
	assert.match(variant.help("llm_task.invoke"), /alias for llm\.invoke/);
});

test("commands.list marks aliases and repeatability", async () => {
	const result = await run("commands.list");
	const byName = Object.fromEntries(result.items.map((item: any) => [item.name, item]));
	assert.equal(byName["clawd.invoke"].aliasOf, "openclaw.invoke");
	assert.equal(byName["openclaw.invoke"].aliasOf, undefined);
	assert.equal(byName["gog.gmail.send"].repeatable, false);
	assert.equal(byName["where"].repeatable, true);
	assert.deepEqual(
		result.items.map((item: any) => item.name),
		registry.list(),
	);
});

test("a registry refuses a name claimed twice and a named workflow that is not repeatable", () => {
	const twin = (name: string, aliases?: string[]): DefinedCommand =>
		defineCommand({
			name,
			meta: {
				aliases,
				description: "twin",
				argsSchema: { type: "object", properties: {} },
				sideEffects: [],
				repeatable: true,
			},
			help: () => name,
			run: async () => ({}),
		});
	const pack = (name: string, extra: Partial<CommandPack> = {}): CommandPack => ({
		name,
		description: name,
		commands: [],
		...extra,
	});
	assert.throws(
		() =>
			createRegistry({
				packs: [pack("a", { commands: [twin("x")] }), pack("b", { commands: [twin("y", ["x"])] })],
			}),
		/"x" is defined by both pack a and pack b/,
	);
	assert.throws(
		() =>
			createRegistry({
				packs: [
					pack("w", {
						workflows: [
							{
								name: "w.run",
								description: "w",
								argsSchema: { type: "object", properties: {} },
								sideEffects: [],
								repeatable: false as true,
								run: async () => null,
							},
						],
					}),
				],
			}),
		/must be repeatable/,
	);
	const only = createRegistry({ packs: [pack("empty")] });
	assert.deepEqual(only.list(), []);
	assert.deepEqual(only.workflows!(), []);
});

test("defineCommand rejects an incomplete declaration", () => {
	const base = {
		name: "x",
		help: () => "x",
		run: async () => ({}),
	};
	const meta = {
		description: "x",
		argsSchema: { type: "object" as const, properties: {} },
		sideEffects: [],
		repeatable: true,
	};
	assert.throws(() => defineCommand({ ...base, name: "bad name", meta }), /invalid name/);
	assert.throws(
		() => defineCommand({ ...base, meta: { ...meta, description: " " } }),
		/description/,
	);
	assert.throws(
		() =>
			defineCommand({
				...base,
				meta: { ...meta, argsSchema: { type: "object", properties: {}, required: ["missing"] } },
			}),
		/required flag "missing" is not declared/,
	);
});

// A crash leaves no record of whether the send happened, so `recovery: repeat` must not re-run
// a step containing a command that may not be repeated. Before the contract, only openclaw.agent
// was covered this way.
test("crash recovery does not re-run a step whose pipeline sends email", async (t) => {
	const f = await workflowFixture(t);
	const sent = path.join(f.dir, "sent.log");
	const gog = path.join(f.dir, "mock-gog.mjs");
	await fs.writeFile(
		gog,
		// The first send hangs so the worker can be killed mid-send; a repeated send would finish.
		`import fs from "node:fs";\n` +
			`const first = !fs.existsSync(${JSON.stringify(sent)});\n` +
			`fs.appendFileSync(${JSON.stringify(sent)}, "send\\n");\n` +
			`if (first) setInterval(() => {}, 1000);\nelse process.stdout.write("{}");\n`,
	);
	const flowPath = path.join(f.dir, "flow.lobster");
	const draft = JSON.stringify([{ to: "a@example.com", subject: "s", body: "b" }]);
	await fs.writeFile(
		flowPath,
		JSON.stringify({
			steps: [
				{
					id: "send",
					recovery: "repeat",
					pipeline: `exec --json --shell ${JSON.stringify(`echo '${draft}'`)} | gog.gmail.send`,
				},
			],
		}),
	);
	const sends = () =>
		existsSync(sent) ? readFileSync(sent, "utf8").split("\n").filter(Boolean).length : 0;
	const outcome = await crashAndRecover(t, {
		dir: f.dir,
		env: { ...f.env, GOG_BIN: gog },
		flowPath,
		killWhen: () => sends() === 1,
	});
	assert.equal(outcome.killed, true);
	assert.match(outcome.recovered.error?.message ?? "", /interrupted without a saved result/);
	assert.equal(sends(), 1, "the email is not sent a second time");
});

test("lobster --help lists every command the registry knows", () => {
	const help = spawnSync(process.execPath, [path.join(repoRoot, "bin", "lobster.js"), "--help"], {
		encoding: "utf8",
	}).stdout;
	const listed = help.split("Commands:\n")[1]?.trim().split(", ");
	assert.deepEqual(listed, registry.list());
});
