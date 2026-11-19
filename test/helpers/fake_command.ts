// A scripted stand-in for the work a workflow step does, usable both as a pipeline
// command (`fake --name <name>`) and as a shell command (`fake.shell(<name>)`).
//
// Every invocation is appended to a log in the fixture directory, so counts are shared
// by the test process, shell children and the separate processes of a crash test.
// The behaviour of the Nth invocation of a name is the Nth entry of its script (the
// last entry repeats; an unscripted name succeeds):
//   "ok"      return { name, call, marker } (plus the input it was given, if any)
//   "fail"    fail (pipeline: throw; shell: exit 1)
//   "hang"    never finish; a pipeline invocation ends when its abort signal fires
//   { output } return this value instead
import { appendFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { createDefaultRegistry } from "../../src/commands/registry.js";

export type FakeBehavior = "ok" | "fail" | "hang" | { output: unknown };

const SCRIPT = "fake-script.json";
const CALLS = "fake-calls.jsonl";

/** The value an "ok" invocation returns. `marker` lets tests find it in saved state. */
export function fakeResult(name: string, call: number, input?: unknown) {
	return { name, call, marker: `result-of-${name}`, ...(input !== undefined ? { input } : {}) };
}

function invocations(dir: string): Array<{ name: string; runId?: string }> {
	const file = path.join(dir, CALLS);
	if (!existsSync(file)) return [];
	return readFileSync(file, "utf8")
		.split("\n")
		.filter(Boolean)
		.map((line) => JSON.parse(line));
}

/** Record one invocation of `name` and choose its scripted behaviour. */
export function invokeFake(dir: string, name: string, runId?: string) {
	appendFileSync(path.join(dir, CALLS), JSON.stringify({ name, runId }) + "\n");
	const call = invocations(dir).filter((entry) => entry.name === name).length;
	const file = path.join(dir, SCRIPT);
	const script: FakeBehavior[] =
		(existsSync(file) ? JSON.parse(readFileSync(file, "utf8")) : {})[name] ?? [];
	const behavior: FakeBehavior = script[Math.min(call - 1, script.length - 1)] ?? "ok";
	return { call, behavior };
}

export type FakeCommand = ReturnType<typeof createFakeCommand>;

export function createFakeCommand(dir: string) {
	const bin = path.join(dir, "fake-bin.mjs");
	const helper = new URL(import.meta.url).href;
	writeFileSync(
		bin,
		`import { invokeFake, fakeResult } from ${JSON.stringify(helper)};
import path from "node:path";
const [name, flag] = process.argv.slice(2);
let input;
if (flag === "--stdin") {
  let text = "";
  for await (const chunk of process.stdin) text += chunk;
  try { input = JSON.parse(text); } catch { input = text; }
}
const { call, behavior } = invokeFake(${JSON.stringify(dir)}, name, process.env.LOBSTER_RUN_ID);
if (behavior === "fail") { process.stderr.write("fake " + name + " failed (call " + call + ")\\n"); process.exit(1); }
if (behavior === "hang") setInterval(() => {}, 1000);
else process.stdout.write(JSON.stringify(typeof behavior === "object" ? behavior.output : fakeResult(name, call, input)));
`,
	);
	const command = {
		name: "fake",
		meta: {
			description: "Scripted test command",
			argsSchema: { type: "object", properties: { name: { type: "string" } } },
			sideEffects: [],
		},
		help() {
			return "fake --name <name>\n";
		},
		async run({ input, args, ctx }: { input: AsyncIterable<unknown>; args: any; ctx: any }) {
			const items: unknown[] = [];
			for await (const item of input) items.push(item);
			const name = String(args.name);
			const { call, behavior } = invokeFake(dir, name, ctx.env?.LOBSTER_RUN_ID);
			if (behavior === "fail") throw new Error(`fake ${name} failed (call ${call})`);
			if (behavior === "hang") {
				// Keeps a worker process alive until it is killed or the step is aborted.
				const keepAlive = setInterval(() => {}, 1000);
				try {
					await new Promise<never>((_, reject) => {
						const signal: AbortSignal | undefined = ctx.signal;
						if (signal?.aborted) reject(signal.reason);
						signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
					});
				} finally {
					clearInterval(keepAlive);
				}
			}
			const value =
				typeof behavior === "object"
					? behavior.output
					: fakeResult(
							name,
							call,
							items.length === 0 ? undefined : items.length === 1 ? items[0] : items,
						);
			return {
				output: (async function* () {
					yield value;
				})(),
			};
		},
	};
	const base = createDefaultRegistry();
	return {
		dir,
		command,
		/** A command registry with the default commands plus `fake`. */
		registry: { ...base, get: (name: string) => (name === "fake" ? command : base.get(name)) },
		/** Set the behaviour of each named fake; replaces earlier scripts for those names. */
		script(behaviors: Record<string, FakeBehavior[]>) {
			const file = path.join(dir, SCRIPT);
			const current = existsSync(file) ? JSON.parse(readFileSync(file, "utf8")) : {};
			writeFileSync(file, JSON.stringify({ ...current, ...behaviors }));
		},
		/** How many times `name` has been invoked (in any process). */
		calls(name: string) {
			return invocations(dir).filter((entry) => entry.name === name).length;
		},
		/** A shell command that invokes the fake; with `stdin`, it echoes its input back. */
		shell(name: string, options: { stdin?: boolean } = {}) {
			return `${JSON.stringify(process.execPath)} ${JSON.stringify(bin)} ${name}${options.stdin ? " --stdin" : ""}`;
		},
		async waitForCalls(name: string, count = 1, timeoutMs = 10000) {
			const deadline = Date.now() + timeoutMs;
			while (this.calls(name) < count) {
				if (Date.now() > deadline)
					throw new Error(`fake ${name} was not invoked ${count} time(s) within ${timeoutMs}ms`);
				await new Promise((resolve) => setTimeout(resolve, 10));
			}
		},
	};
}
