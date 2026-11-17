// Contract matrix: every workflow node type against every cross-cutting concern.
//
// NODES holds one minimal example per node type. For each node and each concern in
// CONCERNS a test is generated: the shared ("generic") test for that concern, a test
// written for that node, or an explicit skip with its reason. A node that declares
// nothing for a concern gets a failing test, and a node file in src/engine/nodes that
// no example exercises fails the coverage check, so a new node type cannot go
// untested silently.
//
// Conventions shared by the examples:
// - the node under test has id "n"; its work is the scripted fake "w" (see
//   helpers/fake_command.ts), invoked `workCalls` times when the node succeeds;
// - steps it needs first (the list a loop iterates, the value a condition reads) are in
//   `setup`; later steps are fakes named "next" or the echoing "probe".
import test, { type TestContext } from "node:test";
import assert from "node:assert/strict";
import { readdirSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";
import { loadWorkflowFile } from "../src/workflows/file.js";
import { snapshotDefinition } from "../src/workflows/definition.js";
import { renderWorkflowGraph } from "../src/workflows/graph.js";
import {
	abandonWorkflowRun,
	advanceDueRuns,
	advanceWorkflowRun,
	signalEvent,
} from "../src/workflows/file.js";
import type { WorkflowStep } from "../src/workflows/types.js";
import { fakeResult, type FakeBehavior } from "./helpers/fake_command.js";
import {
	workflowFixture,
	interactiveStdin,
	type Answers,
	type RunResult,
	type WorkflowFixture,
} from "./helpers/workflow_runner.js";
import { crashAndRecover, type KillPoint } from "./helpers/crash_recovery.js";

const CONCERNS = [
	"validation",
	"dry-run",
	"graph",
	"reference",
	"when",
	"on_error: stop",
	"on_error: continue",
	"on_error: skip_rest",
	"retry",
	"timeout_ms",
	"cancellation",
	"recovery: repeat",
	"recovery: manual",
	"nested in for_each",
	"nested in child workflow",
	"pruned when finished",
] as const;
type Concern = (typeof CONCERNS)[number];

type Recovery = "repeat" | "manual";
type Custom = (t: TestContext, fx: WorkflowFixture, node: NodeExample) => Promise<void>;
type Cell = "generic" | Custom | { skip: string };

type NodeExample = {
	name: string;
	/** The files in src/engine/nodes this example exercises. */
	engine: string[];
	/** Steps that must come first, e.g. the list a loop iterates. */
	setup?: WorkflowStep[];
	/** The node, with id "n". `recovery` goes on the step(s) that do the work. */
	build(
		fx: WorkflowFixture,
		options?: { recovery?: Recovery; file?: string },
	): Promise<WorkflowStep>;
	/** Invocations of fake "w" in one successful execution of the node. */
	workCalls: number;
	/** Behaviour of the fakes other than "w" (defaults: every call succeeds). */
	script?: Record<string, FakeBehavior[]>;
	/** How the gates of this node are answered. */
	answers?: Answers;
	/** A later step reads `expr`; it must see `expected`. */
	reference: { expr: string; expected: unknown };
	/** A `when` that makes the node skip (default: false). */
	skipWhen?: unknown;
	dryRun: RegExp[];
	graph: { type: string; diamond?: boolean };
	/** Node-specific edits that validation must reject (shared ones are in SHARED_INVALID). */
	invalid: Array<[string, (step: any) => any]>;
	/** Extra condition (on the last saved checkpoint) before a crash test may kill. */
	crashReady?: (saved: string) => boolean;
	/** Where the node may not be nested: the error that rejects it. */
	rejectedIn?: { forEach?: RegExp; child?: RegExp };
	cells: Partial<Record<Concern, Cell>>;
};

/** Edits that make any step invalid, whatever its type. */
const SHARED_INVALID: Array<[string, (step: any) => any]> = [
	["unknown step key", (step) => ({ ...step, wehn: true })],
	["timeout_ms as a string", (step) => ({ ...step, timeout_ms: "100" })],
	["retry.max of 0", (step) => ({ ...step, retry: { max: 0 } })],
	["unknown retry key", (step) => ({ ...step, retry: { max: 2, delayms: 5 } })],
	["unknown on_error", (step) => ({ ...step, on_error: "ignore" })],
	["when as a number", (step) => ({ ...step, when: 5 })],
	["unknown recovery", (step) => ({ ...step, recovery: "sometimes" })],
];

const APPROVE: Answers = { approve: true, response: { ok: true } };
const all = (cell: Cell) =>
	Object.fromEntries(CONCERNS.map((c) => [c, cell])) as Record<Concern, Cell>;
const rec = (recovery?: Recovery) => (recovery ? { recovery } : {});

/** Cells for gates that do no work of their own: nothing can fail, so nothing retries. */
const NO_WORK = (why: string): Partial<Record<Concern, Cell>> => ({
	"on_error: stop": { skip: `${why}; on_error has nothing to handle` },
	"on_error: continue": { skip: `${why}; on_error has nothing to handle` },
	"on_error: skip_rest": { skip: `${why}; on_error has nothing to handle` },
	retry: { skip: `${why}, so there is no failure to retry` },
});

// ---------------------------------------------------------------------------------------
// Node examples

const NODES: NodeExample[] = [
	{
		name: "run",
		engine: ["activity", "condition", "sequence"],
		build: async (fx, o = {}) => ({ id: "n", run: fx.fake.shell("w"), ...rec(o.recovery) }),
		workCalls: 1,
		reference: { expr: "$n.json", expected: fakeResult("w", 1) },
		dryRun: [/2?\d\. n {2}\[shell\]/, /run: .*fake-bin\.mjs" w/],
		graph: { type: "run" },
		invalid: [["run as a number", (step) => ({ ...step, run: 5 })]],
		cells: all("generic"),
	},
	{
		name: "pipeline",
		engine: ["activity", "condition", "sequence"],
		build: async (_fx, o = {}) => ({ id: "n", pipeline: "fake --name w", ...rec(o.recovery) }),
		workCalls: 1,
		reference: { expr: "$n.json", expected: fakeResult("w", 1) },
		dryRun: [/\d\. n {2}\[pipeline\]/, /pipeline: fake --name w/],
		graph: { type: "pipeline" },
		invalid: [["pipeline as a list", (step) => ({ ...step, pipeline: ["fake"] })]],
		cells: all("generic"),
	},
	{
		name: "approval",
		engine: ["activity", "approval", "condition", "sequence"],
		build: async (_fx, o = {}) => ({
			id: "n",
			pipeline: "fake --name w",
			approval: { prompt: "Ship it?" },
			...rec(o.recovery),
		}),
		workCalls: 1,
		reference: { expr: "$n.approved", expected: true },
		dryRun: [/\d\. n {2}\[pipeline\]/, /\[approval required\]/],
		graph: { type: "pipeline", diamond: true },
		invalid: [
			["approval as a number", (step) => ({ ...step, approval: 5 })],
			["approval as a list", (step) => ({ ...step, approval: ["yes"] })],
			["approval.prompt as a number", (step) => ({ ...step, approval: { prompt: 5 } })],
			["approval.items as a string", (step) => ({ ...step, approval: { items: "all" } })],
			["unknown approval key", (step) => ({ ...step, approval: { promt: "Ship?" } })],
		],
		rejectedIn: {
			forEach: /cannot (define|contain) approval/,
			child: /approval\/input gates are not supported in composition/,
		},
		cells: {
			...all("generic"),
			"on_error: continue": async (t, fx, node) => {
				await GENERIC["on_error: continue"](t, fx, node);
				await rejectionIsNotAnError(t, node, "continue");
			},
			"on_error: skip_rest": async (t, fx, node) => {
				await GENERIC["on_error: skip_rest"](t, fx, node);
				await rejectionIsNotAnError(t, node, "skip_rest");
			},
			cancellation: async (t, fx, node) => {
				await GENERIC.cancellation(t, fx, node);
				await cancelWhileAsking(t, node);
			},
			timeout_ms: async (t, fx, node) => {
				await GENERIC.timeout_ms(t, fx, node);
				await answeringTakesLongerThanTimeout(t, node);
			},
		},
	},
	{
		name: "input",
		engine: ["input", "condition", "sequence"],
		build: async (_fx, o = {}) => ({
			id: "n",
			input: {
				prompt: "Ready?",
				responseSchema: {
					type: "object",
					properties: { ok: { type: "boolean" } },
					required: ["ok"],
				},
			},
			...rec(o.recovery),
		}),
		workCalls: 0,
		reference: { expr: "$n.response", expected: { ok: true } },
		dryRun: [/\d\. n {2}\[input\]/, /prompt: Ready\?/, /\[input required\]/],
		graph: { type: "input" },
		invalid: [
			["input.prompt as a number", (step) => ({ ...step, input: { ...step.input, prompt: 5 } })],
			[
				"input.responseSchema as a string",
				(step) => ({ ...step, input: { ...step.input, responseSchema: "object" } }),
			],
			["unknown input key", (step) => ({ ...step, input: { ...step.input, promt: "?" } })],
			["input with a command", (step) => ({ ...step, run: "true" })],
		],
		rejectedIn: {
			forEach: /cannot (define|contain) (approval or )?input/,
			child: /approval\/input gates are not supported in composition/,
		},
		cells: {
			...all("generic"),
			"on_error: stop": (t) => badAnswerIsRefused(t, "stop"),
			"on_error: continue": (t) => badAnswerIsRefused(t, "continue"),
			"on_error: skip_rest": (t) => badAnswerIsRefused(t, "skip_rest"),
			retry: {
				skip: "an input gate does no work that can fail: a response that does not match the schema is refused at resume and the question stays open (on_error cells), so there is no failure to retry",
			},
			timeout_ms: (t, _fx, node) => answeringTakesLongerThanTimeout(t, node),
			cancellation: (t, _fx, node) => cancelWhileAsking(t, node),
			"recovery: repeat": (t, fx, node) => questionSurvivesRestart(t, fx, node, "repeat"),
			"recovery: manual": (t, fx, node) => questionSurvivesRestart(t, fx, node, "manual"),
		},
	},
	{
		name: "when/condition",
		engine: ["activity", "condition", "sequence"],
		setup: [{ id: "seed", pipeline: "fake --name seed" }],
		build: async (_fx, o = {}) => ({
			id: "n",
			pipeline: "fake --name w",
			when: "$seed.json.call == 1",
			...rec(o.recovery),
		}),
		skipWhen: "$seed.json.call == 2",
		workCalls: 1,
		reference: { expr: "$n.json", expected: fakeResult("w", 1) },
		dryRun: [/\d\. n {2}\[pipeline\]/, /pipeline: fake --name w/],
		graph: { type: "pipeline" },
		invalid: [
			["when as an object", (step) => ({ ...step, when: { eq: 1 } })],
			["condition as a number", (step) => ({ ...step, when: undefined, condition: 1 })],
		],
		cells: all("generic"),
	},
	{
		name: "for_each",
		engine: ["for-each", "activity", "condition", "sequence"],
		setup: [{ id: "items", pipeline: "fake --name items" }],
		script: { items: [{ output: ["a", "b"] }] },
		build: async (_fx, o = {}) => ({
			id: "n",
			for_each: "$items.json",
			steps: [{ id: "w", pipeline: "fake --name w", ...rec(o.recovery) }],
		}),
		workCalls: 2,
		reference: {
			expr: "$n.json",
			expected: [
				{ item: "a", index: 0, w: fakeResult("w", 1) },
				{ item: "b", index: 1, w: fakeResult("w", 2) },
			],
		},
		dryRun: [
			/\d\. n {2}\[for_each\]/,
			/for_each: \$items\.json/,
			/1\. w {2}\[pipeline\] pipeline: fake --name w/,
		],
		graph: { type: "for_each" },
		invalid: [
			["for_each as a number", (step) => ({ ...step, for_each: 5 })],
			["batch_size of 0", (step) => ({ ...step, batch_size: 0 })],
			["empty steps", (step) => ({ ...step, steps: [] })],
			["unknown sub-step key", (step) => ({ ...step, steps: [{ ...step.steps[0], wehn: true }] })],
		],
		rejectedIn: { forEach: /cannot define workflow, parallel, or for_each/ },
		cells: all("generic"),
	},
	...(["all", "any"] as const).map((wait): NodeExample => ({
		name: `parallel (wait ${wait})`,
		engine: ["parallel", "activity", "condition", "sequence"],
		// With "any", the other branch never finishes, so "w" always wins the race.
		script: { other: [wait === "any" ? "hang" : "ok"] },
		build: async (_fx, o = {}) => ({
			id: "n",
			parallel: {
				...(wait === "any" ? { wait } : {}),
				branches: [
					{ id: "w", pipeline: "fake --name w", ...rec(o.recovery) },
					{ id: "other", pipeline: "fake --name other", ...rec(o.recovery) },
				],
			},
		}),
		workCalls: 1,
		reference: {
			expr: "$n.json",
			expected:
				wait === "any"
					? { w: fakeResult("w", 1) }
					: { w: fakeResult("w", 1), other: fakeResult("other", 1) },
		},
		dryRun: [
			/\d\. n {2}\[parallel\]/,
			new RegExp(`wait: ${wait}`),
			/branch w: \[pipeline\] fake --name w/,
			/branch other: \[pipeline\] fake --name other/,
		],
		graph: { type: "parallel" },
		invalid: [
			["unknown wait", (step) => ({ ...step, parallel: { ...step.parallel, wait: "some" } })],
			["no branches", (step) => ({ ...step, parallel: { ...step.parallel, branches: [] } })],
			[
				"a branch with when",
				(step) => ({
					...step,
					parallel: {
						...step.parallel,
						branches: [{ ...step.parallel.branches[0], when: "true" }, step.parallel.branches[1]],
					},
				}),
			],
			[
				"branch run as a number",
				(step) => ({
					...step,
					parallel: {
						...step.parallel,
						branches: [{ id: "w", run: 5 }, step.parallel.branches[1]],
					},
				}),
			],
		],
		// The completed branch must be saved before the crash, or recovery would rightly repeat it.
		crashReady: wait === "all" ? (saved) => saved.includes("result-of-other") : undefined,
		rejectedIn: { forEach: /cannot define workflow, parallel, or for_each/ },
		cells: all("generic"),
	})),
	{
		name: "child workflow",
		engine: ["child-workflow", "activity", "condition", "sequence"],
		build: async (fx, o = {}) => {
			const file = o.file ?? "child.lobster";
			await fx.write(file, { steps: [{ id: "w", pipeline: "fake --name w", ...rec(o.recovery) }] });
			return { id: "n", workflow: file };
		},
		workCalls: 1,
		reference: { expr: "$n.json", expected: fakeResult("w", 1) },
		dryRun: [/\d\. n {2}\[workflow\]/, /workflow: child\.lobster/],
		graph: { type: "workflow" },
		invalid: [
			["workflow as a number", (step) => ({ ...step, workflow: 5 })],
			["workflow_args as a list", (step) => ({ ...step, workflow_args: ["x"] })],
			["missing child file", (step) => ({ ...step, workflow: "missing.lobster" })],
		],
		rejectedIn: { forEach: /cannot define workflow, parallel, or for_each/ },
		cells: {
			...all("generic"),
			validation: async (t, fx, node) => {
				await GENERIC.validation(t, fx, node);
				// The child file is validated before the run starts, like the parent.
				const accepted: string[] = [];
				for (const [label, edit] of SHARED_INVALID) {
					await fx.write("child.lobster", {
						steps: [edit({ id: "w", pipeline: "fake --name w" })],
					});
					if (await loads(fx, { steps: [{ id: "n", workflow: "child.lobster" }] }))
						accepted.push(label);
				}
				assert.deepEqual(accepted, [], "a child with these invalid edits must be rejected");
			},
		},
	},
	{
		name: "sleep",
		engine: ["sleep"],
		// Short enough to wait in place; sleepPausesAndResumes covers a sleep that pauses the run.
		build: async () => ({ id: "n", sleep: "150ms" }),
		workCalls: 0,
		reference: { expr: "$n.id", expected: "n" },
		dryRun: [/\d\. n {2}\[sleep\]/, /sleep: 150ms/],
		graph: { type: "sleep" },
		invalid: [
			["sleep as a word", (step) => ({ ...step, sleep: "soon" })],
			["negative sleep", (step) => ({ ...step, sleep: -1 })],
			["sleep with a command", (step) => ({ ...step, run: "true" })],
			["sleep with an approval", (step) => ({ ...step, approval: "Ok?" })],
		],
		rejectedIn: {
			forEach: /cannot contain approval or input steps \(nor sleep or wait steps\)/,
			child: /not supported in composition \(nor sleep or wait steps\)/,
		},
		cells: {
			...all("generic"),
			...NO_WORK("a sleep does no work that can fail"),
			timeout_ms: (t, _fx, node) => gateTakesLongerThanTimeout(t, node, "400ms"),
			cancellation: (t) => sleepSurvivesStop(t),
			"recovery: repeat": (t, fx) => sleepSurvivesCrash(t, fx, "repeat"),
			"recovery: manual": (t, fx) => sleepSurvivesCrash(t, fx, "manual"),
		},
	},
	{
		name: "wait (event)",
		engine: ["event"],
		build: async () => ({ id: "n", wait: { event: "contract.done", match: { ok: true } } }),
		answers: { event: { ok: true, n: 7 } },
		workCalls: 0,
		reference: { expr: "$n.json.n", expected: 7 },
		dryRun: [/\d\. n {2}\[wait\]/, /event: contract\.done/, /\[event required\]/],
		graph: { type: "wait" },
		invalid: [
			["wait as a string", (step) => ({ ...step, wait: "contract.done" })],
			["wait without an event", (step) => ({ ...step, wait: { match: {} } })],
			["unknown wait key", (step) => ({ ...step, wait: { ...step.wait, evnet: "x" } })],
			["wait.timeout as a word", (step) => ({ ...step, wait: { ...step.wait, timeout: "soon" } })],
			[
				"on_timeout without a timeout",
				(step) => ({ ...step, wait: { ...step.wait, on_timeout: "continue" } }),
			],
			["wait with a command", (step) => ({ ...step, run: "true" })],
		],
		rejectedIn: {
			forEach: /cannot contain approval or input steps \(nor sleep or wait steps\)/,
			child: /not supported in composition \(nor sleep or wait steps\)/,
		},
		cells: {
			...all("generic"),
			...NO_WORK("an event wait does no work that can fail"),
			timeout_ms: (t, _fx, node) => gateTakesLongerThanTimeout(t, node),
			cancellation: (t, _fx, node) => abandoningWithdrawsTheWait(t, node),
			"recovery: repeat": (t, fx, node) => eventWaitSurvivesRestart(t, fx, node, "repeat"),
			"recovery: manual": (t, fx, node) => eventWaitSurvivesRestart(t, fx, node, "manual"),
		},
	},
];

// ---------------------------------------------------------------------------------------
// Shared tests

async function workflowWith(
	fx: WorkflowFixture,
	node: NodeExample,
	edit: (step: any) => any = (s) => s,
	after: WorkflowStep[] = [],
	recovery?: Recovery,
) {
	fx.fake.script(node.script ?? {});
	return { steps: [...(node.setup ?? []), edit(await node.build(fx, { recovery })), ...after] };
}

/** Whether a run of `workflow` would be admitted (its definition, children included, loads). */
async function loads(fx: WorkflowFixture, workflow: unknown) {
	try {
		await snapshotDefinition(await fx.write("flow.lobster", workflow), {});
		return true;
	} catch {
		return false;
	}
}

const probe = (fx: WorkflowFixture, stdin: unknown): WorkflowStep => ({
	id: "probe",
	run: fx.fake.shell("probe", { stdin: true }),
	stdin,
});
const next: WorkflowStep = { id: "next", pipeline: "fake --name next" };

/** `w` succeeds until its last call in one execution of the node, which behaves as `last`. */
function scriptWork(
	fx: WorkflowFixture,
	node: NodeExample,
	last: FakeBehavior,
	then: FakeBehavior[] = [],
) {
	fx.fake.script({ w: [...Array(Math.max(node.workCalls - 1, 0)).fill("ok"), last, ...then] });
}

async function expectSettled(fx: WorkflowFixture, status: string) {
	const execution = await fx.execution();
	assert.equal(execution.status, status);
	if (status === "failed")
		// Kept so the run can be retried from the step that failed.
		assert.ok(
			Object.values(execution.activations).some((frame) => frame.phase === "failed"),
			"a failed run keeps the records a retry needs",
		);
	else assert.deepEqual(execution.activations, {}, "a finished run keeps no step records");
}

const GENERIC: Record<Concern, Custom> = {
	async validation(_t, fx, node) {
		const valid = await workflowWith(fx, node);
		await snapshotDefinition(await fx.write("flow.lobster", valid), {});
		const accepted: string[] = [];
		for (const [label, edit] of [...SHARED_INVALID, ...node.invalid])
			if (await loads(fx, await workflowWith(fx, node, edit))) accepted.push(label);
		assert.deepEqual(accepted, [], `${node.name}: these invalid edits must be rejected`);
	},

	async "dry-run"(_t, fx, node) {
		const result = await fx.run(await workflowWith(fx, node), { dryRun: true });
		assert.equal(result.status, "ok");
		const plan = fx.stderr();
		for (const pattern of node.dryRun) assert.match(plan, pattern);
		assert.doesNotMatch(plan, /\bn {2}\[skipped/);
		assert.equal(fx.fake.calls("w"), 0);
	},

	async graph(_t, fx, node) {
		const workflow = await loadWorkflowFile(
			await fx.write("flow.lobster", await workflowWith(fx, node, (s) => s, [next])),
		);
		const ascii = renderWorkflowGraph({ workflow, format: "ascii" });
		assert.match(ascii, new RegExp(`^- n \\[${node.graph.type}\\]`, "m"));
		assert.match(ascii, /^- n -> next \(next\)$/m);
		const mermaid = renderWorkflowGraph({ workflow, format: "mermaid" });
		assert.match(mermaid, node.graph.diamond ? /^ {2}n\{"n/m : /^ {2}n\["n/m);
		const dot = renderWorkflowGraph({ workflow, format: "dot" });
		assert.match(
			dot,
			new RegExp(`^ {2}"n" \\[shape=${node.graph.diamond ? "diamond" : "box"}`, "m"),
		);
	},

	async reference(_t, fx, node) {
		const result = await fx.drive(
			await workflowWith(fx, node, (s) => s, [probe(fx, node.reference.expr)]),
			node.answers ?? APPROVE,
		);
		assert.equal(result.status, "ok");
		assert.deepEqual((result.output[0] as any).input, node.reference.expected);
	},

	async when(_t, fx, node) {
		const workflow = await workflowWith(fx, node, (s) => ({ ...s, when: node.skipWhen ?? false }), [
			probe(fx, "$n.skipped"),
		]);
		// Not driven: a skipped gate must not ask anything.
		const result = await fx.run(workflow);
		assert.equal(result.status, "ok", JSON.stringify(result));
		assert.equal((result.output[0] as any).input, true);
		assert.equal(fx.fake.calls("w"), 0);
	},

	async "on_error: stop"(_t, fx, node) {
		const workflow = await workflowWith(fx, node, (s) => ({ ...s, on_error: "stop" }), [next]);
		scriptWork(fx, node, "fail");
		await assert.rejects(
			fx.drive(workflow, node.answers ?? APPROVE),
			/fake w failed|command failed/,
		);
		assert.equal(fx.fake.calls("next"), 0);
		await expectSettled(fx, "failed");
	},

	async "on_error: continue"(_t, fx, node) {
		const workflow = await workflowWith(fx, node, (s) => ({ ...s, on_error: "continue" }), [
			probe(fx, "$n.error"),
		]);
		scriptWork(fx, node, "fail");
		const result = await fx.drive(workflow, node.answers ?? APPROVE);
		assert.equal(result.status, "ok");
		assert.equal((result.output[0] as any).input, true, "a later step sees the error");
		await expectSettled(fx, "completed");
	},

	async "on_error: skip_rest"(_t, fx, node) {
		const workflow = await workflowWith(fx, node, (s) => ({ ...s, on_error: "skip_rest" }), [next]);
		scriptWork(fx, node, "fail");
		const result = await fx.drive(workflow, node.answers ?? APPROVE);
		assert.equal(result.status, "ok");
		assert.equal(fx.fake.calls("next"), 0, "the rest of the sequence is skipped");
		await expectSettled(fx, "completed");
	},

	async retry(_t, fx, node) {
		const workflow = await workflowWith(fx, node, (s) => ({
			...s,
			retry: { max: 2, delay_ms: 0 },
		}));
		scriptWork(fx, node, "fail", ["ok"]);
		const result = await fx.drive(workflow, node.answers ?? APPROVE);
		assert.equal(result.status, "ok");
		// The failed attempt's successful calls are repeated: a retry reruns the whole node.
		assert.equal(fx.fake.calls("w"), node.workCalls * 2);
		assert.match(fx.stderr(), /\[RETRY\] Step 'n' failed \(attempt 1\/2\)/);
	},

	async timeout_ms(_t, fx, node) {
		const workflow = await workflowWith(fx, node, (s) => ({ ...s, timeout_ms: 300 }));
		scriptWork(fx, node, "hang");
		const started = Date.now();
		await assert.rejects(
			fx.drive(workflow, node.answers ?? APPROVE),
			/Step 'n' timed out after 300ms/,
		);
		assert.ok(Date.now() - started < 5000);
		await expectSettled(fx, "failed");
	},

	async cancellation(_t, fx, node) {
		const workflow = await workflowWith(fx, node);
		scriptWork(fx, node, "hang");
		const controller = new AbortController();
		const running = fx.run(workflow, { signal: controller.signal });
		await fx.fake.waitForCalls("w", node.workCalls);
		controller.abort(new Error("stopped by host"));
		await assert.rejects(running, /stopped by host/);
		const execution = await fx.execution();
		assert.equal(execution.status, "blocked");
		assert.match(execution.error ?? "", /^Interrupted while root\/n\/.*\/activity was running/);
	},

	"recovery: repeat": (t, fx, node) => recoveryCell(t, fx, node, "repeat"),
	"recovery: manual": (t, fx, node) => recoveryCell(t, fx, node, "manual"),

	async "nested in for_each"(_t, fx, node) {
		fx.fake.script({ ...node.script, rows: [{ output: [1, 2] }] });
		const steps = async (edit: (s: any) => any = (s) => s) => ({
			steps: [
				...(node.setup ?? []),
				{ id: "rows", pipeline: "fake --name rows" },
				{ id: "loop", for_each: "$rows.json", steps: [edit(await node.build(fx))] },
			],
		});
		if (node.rejectedIn?.forEach) {
			await assert.rejects(
				snapshotDefinition(await fx.write("flow.lobster", await steps()), {}),
				node.rejectedIn.forEach,
			);
			return;
		}
		const result = await fx.drive(await steps(), node.answers ?? APPROVE);
		assert.equal(result.status, "ok");
		assert.equal(fx.fake.calls("w"), node.workCalls * 2);
		// A sub-step is validated like any other step.
		const accepted: string[] = [];
		for (const [label, edit] of [...SHARED_INVALID, ...node.invalid])
			if (await loads(fx, await steps(edit))) accepted.push(label);
		assert.deepEqual(
			accepted,
			[],
			`${node.name} inside for_each: these invalid edits must be rejected`,
		);
	},

	async "nested in child workflow"(_t, fx, node) {
		fx.fake.script(node.script ?? {});
		await fx.write("nested.lobster", {
			steps: [...(node.setup ?? []), await node.build(fx, { file: "grandchild.lobster" })],
		});
		const workflow = { steps: [{ id: "c", workflow: "nested.lobster" }, next] };
		if (node.rejectedIn?.child) {
			await assert.rejects(fx.drive(workflow, node.answers ?? APPROVE), node.rejectedIn.child);
			assert.equal(fx.fake.calls("w"), 0, "nothing runs before the gate is refused");
			return;
		}
		const result = await fx.drive(workflow, node.answers ?? APPROVE);
		assert.equal(result.status, "ok");
		assert.equal(fx.fake.calls("w"), node.workCalls);
		assert.equal(fx.fake.calls("next"), 1);
	},

	async "pruned when finished"(_t, fx, node) {
		const hold: WorkflowStep = { id: "hold", approval: { prompt: "Hold?" } };
		let result = await fx.run(await workflowWith(fx, node, (s) => s, [hold]));
		while (result.status !== "ok" && result.requiresApproval?.prompt !== "Hold?")
			result =
				result.status === "waiting"
					? await fx.proceed(result, node.answers ?? APPROVE)
					: await fx.resume(result, node.answers ?? APPROVE);
		// Paused after the node: only the sequence keeps its result, for later references.
		const paused = await fx.execution();
		const below = Object.keys(paused.activations).filter(
			(key) => key === "root/n" || key.startsWith("root/n/"),
		);
		assert.deepEqual(below, [], "a completed node keeps no activations");
		result = await fx.resume(result, { approve: true });
		assert.equal(result.status, "ok");
		const { definition: _definition, ...finished } = await fx.execution();
		assert.equal(finished.status, "completed");
		assert.deepEqual(finished.activations, {});
		assert.doesNotMatch(JSON.stringify(finished), /result-of-|"response"/);
	},
};

async function recoveryCell(
	t: TestContext,
	fx: WorkflowFixture,
	node: NodeExample,
	recovery: Recovery,
) {
	const workflow = await workflowWith(
		fx,
		node,
		(s) => s,
		[{ id: "after", pipeline: "fake --name after" }],
		recovery,
	);
	scriptWork(fx, node, "hang", ["ok"]);
	const killWhen: KillPoint = ({ calls, saved }) =>
		calls("w") >= node.workCalls && (node.crashReady?.(saved) ?? true);
	const flowPath = await fx.write("flow.lobster", workflow);
	const { runId, killed, recovered } = await crashAndRecover(t, {
		dir: fx.dir,
		env: fx.env,
		flowPath,
		killWhen,
	});
	assert.ok(killed);
	for (const step of node.setup ?? [])
		assert.equal(fx.fake.calls(step.id), 1, `${step.id} is not repeated`);
	if (recovery === "manual") {
		assert.equal(
			recovered.error?.code,
			"LOBSTER_ACTIVITY_OUTCOME_UNKNOWN",
			JSON.stringify(recovered),
		);
		assert.equal(fx.fake.calls("w"), node.workCalls, "an uncertain activity is not repeated");
		assert.equal(fx.fake.calls("after"), 0);
		assert.equal((await fx.execution(runId)).status, "blocked");
		return;
	}
	let result = recovered as RunResult;
	while (result.status === "needs_approval" || result.status === "needs_input")
		result = await fx.resume(result, node.answers ?? APPROVE);
	assert.equal(result.status, "ok", JSON.stringify(result));
	// Only the interrupted call runs again; work completed before the crash is kept.
	assert.equal(fx.fake.calls("w"), node.workCalls + 1);
	assert.equal(fx.fake.calls("after"), 1);
	await expectSettled(fx, "completed");
}

// ---------------------------------------------------------------------------------------
// Tests written for one node type

/** Approval: a "no" cancels the run whatever on_error says; it is a decision, not an error. */
async function rejectionIsNotAnError(t: TestContext, node: NodeExample, onError: string) {
	const fx = await workflowFixture(t);
	const asked = await fx.run(
		await workflowWith(fx, node, (s) => ({ ...s, on_error: onError }), [next]),
	);
	assert.equal(asked.status, "needs_approval");
	const result = await fx.resume(asked, { approve: false });
	assert.equal(result.status, "cancelled");
	assert.equal(fx.fake.calls("next"), 0);
	await expectSettled(fx, "cancelled");
}

/** A gate stopped while a person is being asked: the question is still open afterwards. */
async function cancelWhileAsking(t: TestContext, node: NodeExample) {
	const fx = await workflowFixture(t);
	const workflow = await workflowWith(fx, node, (s) => s, [next]);
	const controller = new AbortController();
	const stdin = interactiveStdin();
	const running = fx.run(workflow, { signal: controller.signal, mode: "human", stdin });
	// The prompt is shown once the work before it has finished and the run is saved.
	if (node.workCalls) await fx.fake.waitForCalls("w", node.workCalls);
	for (const deadline = Date.now() + 10000; ; await sleep(10)) {
		if (
			await fx.runId().then(
				() => true,
				() => false,
			)
		)
			break;
		if (Date.now() > deadline) throw new Error("the run was never saved");
	}
	await sleep(200);
	controller.abort(new Error("stopped by host"));
	await assert.rejects(running, /stopped by host/);
	assert.notEqual((await fx.execution()).status, "failed");
	const runId = await fx.runId();
	const asked = await advanceWorkflowRun({ runId, ctx: fx.ctx() as any });
	assert.match(asked.status, /^needs_(approval|input)$/);
	const result = await fx.resume(asked, node.answers ?? APPROVE);
	assert.equal(result.status, "ok");
	assert.equal(fx.fake.calls("w"), node.workCalls, "the work before the gate is not repeated");
	assert.equal(fx.fake.calls("next"), 1);
}

/** A step's timeout covers its work, not the time a person takes to answer. */
async function answeringTakesLongerThanTimeout(t: TestContext, node: NodeExample) {
	const fx = await workflowFixture(t);
	const asked = await fx.run(
		await workflowWith(fx, node, (s) => ({ ...s, timeout_ms: 100 }), [next]),
	);
	assert.match(asked.status, /^needs_(approval|input)$/);
	await sleep(300);
	const result = await fx.resume(asked, node.answers ?? APPROVE);
	assert.equal(result.status, "ok");
	assert.equal(fx.fake.calls("next"), 1);
}

/** Input: a response that fails the schema is refused and the question stays open. */
async function badAnswerIsRefused(t: TestContext, onError: string) {
	const fx = await workflowFixture(t);
	const node = NODES.find((n) => n.name === "input")!;
	const asked = await fx.run(
		await workflowWith(fx, node, (s) => ({ ...s, on_error: onError }), [probe(fx, "$n.response")]),
	);
	assert.equal(asked.status, "needs_input");
	await assert.rejects(fx.resume(asked, { response: { ok: "yes" } }), /ok|schema|must be boolean/i);
	const result = await fx.resume(asked, { response: { ok: true } });
	assert.equal(result.status, "ok");
	assert.deepEqual((result.output[0] as any).input, { ok: true });
}

/** Input: the open question survives a process restart unchanged, whatever `recovery` says. */
async function questionSurvivesRestart(
	t: TestContext,
	fx: WorkflowFixture,
	node: NodeExample,
	recovery: Recovery,
) {
	const workflow = await workflowWith(fx, node, (s) => s, [next], recovery);
	const flowPath = await fx.write("flow.lobster", workflow);
	const { first, recovered } = await crashAndRecover(t, { dir: fx.dir, env: fx.env, flowPath });
	assert.equal(first?.status, "needs_input");
	assert.equal(recovered.status, "needs_input", JSON.stringify(recovered));
	assert.equal(recovered.requiresInput.resumeToken, first!.requiresInput.resumeToken);
	const result = await fx.resume(recovered as RunResult, node.answers ?? APPROVE);
	assert.equal(result.status, "ok");
	assert.equal(fx.fake.calls("next"), 1);
}

/** A gate's waiting is not work: timeout_ms does not expire while it waits. */
async function gateTakesLongerThanTimeout(t: TestContext, node: NodeExample, sleepFor?: string) {
	const fx = await workflowFixture(t);
	const workflow = await workflowWith(
		fx,
		node,
		(s) => ({ ...s, timeout_ms: 100, ...(sleepFor ? { sleep: sleepFor } : {}) }),
		[next],
	);
	let result = await fx.run(workflow);
	if (result.status === "waiting" && result.waitingFor?.kind === "event") await sleep(300);
	while (result.status === "waiting") result = await fx.proceed(result, node.answers ?? APPROVE);
	assert.equal(result.status, "ok", JSON.stringify(result));
	assert.equal(fx.fake.calls("next"), 1);
}

/** A sleep stopped by the host leaves a sleeping run that continues once it is due. */
async function sleepSurvivesStop(t: TestContext) {
	const fx = await workflowFixture(t);
	const controller = new AbortController();
	const running = fx.run(
		{ steps: [{ id: "n", sleep: "1500ms" }, next] },
		{ signal: controller.signal },
	);
	await sleep(200);
	controller.abort(new Error("stopped by host"));
	await assert.rejects(running, /stopped by host/);
	assert.equal((await fx.execution()).status, "waiting");
	const early = await advanceDueRuns({ ctx: fx.ctx() as any });
	assert.deepEqual(early.due, [], "not due before its time");
	await sleep(1400);
	const { advanced } = await advanceDueRuns({ ctx: fx.ctx() as any });
	assert.deepEqual(
		advanced.map((a) => a.status),
		["ok"],
	);
	assert.equal(fx.fake.calls("next"), 1);
	await expectSettled(fx, "completed");
}

/** A process killed mid-sleep: the sleep keeps its wake time and nothing runs twice. */
async function sleepSurvivesCrash(t: TestContext, fx: WorkflowFixture, recovery: Recovery) {
	const flowPath = await fx.write("flow.lobster", {
		steps: [
			{ id: "before", pipeline: "fake --name before", recovery },
			{ id: "n", sleep: "800ms" },
			{ id: "after", pipeline: "fake --name after", recovery },
		],
	});
	const { killed, recovered } = await crashAndRecover(t, {
		dir: fx.dir,
		env: fx.env,
		flowPath,
		killWhen: ({ saved }) => saved.includes('"kind":"timer"'),
	});
	assert.ok(killed);
	assert.equal(recovered.status, "ok", JSON.stringify(recovered));
	assert.equal(fx.fake.calls("before"), 1, "work before the sleep is not repeated");
	assert.equal(fx.fake.calls("after"), 1);
}

/** Abandoning a run that waits for an event withdraws the wait; a later event finds nothing. */
async function abandoningWithdrawsTheWait(t: TestContext, node: NodeExample) {
	const fx = await workflowFixture(t);
	const waiting = await fx.run(await workflowWith(fx, node, (s) => s, [next]));
	assert.equal(waiting.status, "waiting");
	const abandoned = await abandonWorkflowRun({ runId: waiting.runId!, ctx: fx.ctx() as any });
	assert.equal(abandoned.status, "cancelled");
	const delivered = await signalEvent({
		name: "contract.done",
		data: { ok: true },
		ctx: fx.ctx() as any,
	});
	assert.deepEqual(delivered.advanced, []);
	assert.equal(fx.fake.calls("next"), 0);
}

/** An event wait outlives the process that published it, and the event still arrives. */
async function eventWaitSurvivesRestart(
	t: TestContext,
	fx: WorkflowFixture,
	node: NodeExample,
	recovery: Recovery,
) {
	const workflow = await workflowWith(fx, node, (s) => s, [next], recovery);
	const flowPath = await fx.write("flow.lobster", workflow);
	const { first, recovered } = await crashAndRecover(t, { dir: fx.dir, env: fx.env, flowPath });
	assert.equal(first?.status, "waiting");
	assert.equal(recovered.status, "waiting", JSON.stringify(recovered));
	const result = await fx.proceed(recovered as RunResult, node.answers ?? APPROVE);
	assert.equal(result.status, "ok");
	assert.equal(fx.fake.calls("next"), 1);
}

// ---------------------------------------------------------------------------------------
// The matrix

test("node contract: every engine node file is exercised by an example", () => {
	const files = readdirSync(new URL("../src/engine/nodes/", import.meta.url))
		.filter((file) => file.endsWith(".js"))
		.map((file) => file.slice(0, -3));
	const covered = new Set(NODES.flatMap((node) => node.engine));
	assert.deepEqual(
		files.filter((file) => !covered.has(file)),
		[],
		"add an example to NODES for these node types",
	);
});

for (const node of NODES) {
	for (const concern of CONCERNS) {
		const cell = node.cells[concern];
		const name = `node contract: ${node.name} / ${concern}`;
		if (cell === undefined)
			test(name, () =>
				assert.fail(
					`${node.name} declares nothing for "${concern}": add a test, "generic", or { skip: reason }`,
				),
			);
		else if (typeof cell === "object") test(name, { skip: cell.skip }, () => {});
		else
			test(name, { timeout: 60000 }, async (t) => {
				const fx = await workflowFixture(t);
				await (cell === "generic" ? GENERIC[concern] : cell)(t, fx, node);
			});
	}
}
