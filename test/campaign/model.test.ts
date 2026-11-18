// Model-based randomized testing: a random workflow is run twice, once undisturbed (the
// reference) and once with interference — runs interrupted at random checkpoints and
// continued, answers replayed with their old tokens, timers advanced twice at once. The
// second run must end exactly like the first, and Lobster's guarantees must hold:
//   - a step that may not be repeated runs exactly as often as in the reference, or the
//     run stops as blocked for a person; a repeatable one runs again at most once per
//     interruption;
//   - an answered question never accepts a second answer;
//   - every run ends completed, cancelled, failed or blocked, never stuck;
//   - a completed or cancelled run keeps no step records.
import test, { type TestContext } from "node:test";
import assert from "node:assert/strict";
import { workflowFixture } from "../helpers/workflow_runner.js";
import { abandonWorkflowRun } from "../../src/workflows/file.js";
import { BASE_SEED, cases, random } from "./settings.js";
import { drive, generate, normalize, type Generated } from "./model.js";

async function prepare(t: TestContext, generated: (shell: (name: string) => string) => Generated) {
	const fx = await workflowFixture(t, "lobster-campaign-");
	fx.env.LOBSTER_INLINE_SLEEP_MS = "0";
	const g = generated((name) => fx.fake.shell(name));
	await fx.write("flow.lobster", g.workflow);
	for (const [file, child] of Object.entries(g.children)) await fx.write(file, child);
	fx.fake.script(g.scripts);
	return { fx, g };
}

const names = (g: Generated) => Object.keys(g.scripts);
const kindsSeen = new Map<string, number>();
const outcomes = new Map<string, number>();
const CASES = cases(25, 400);

for (let i = 0; i < CASES; i++) {
	const seed = BASE_SEED * 10_000 + i;
	test(`model: seed ${seed}`, { timeout: 120_000 }, async (t) => {
		const reference = await prepare(t, (shell) => generate(seed, shell));
		const chaos = await prepare(t, (shell) => generate(seed, shell));
		const describe = () =>
			`\nseed ${seed} (replay: LOBSTER_CAMPAIGN_SEED=${BASE_SEED} with this case)\nworkflow: ${JSON.stringify(
				{ ...reference.g.workflow, children: reference.g.children, scripts: reference.g.scripts },
			)}`;
		for (const kind of reference.g.kinds) kindsSeen.set(kind, (kindsSeen.get(kind) ?? 0) + 1);

		const expected = await drive(reference.fx, seed);
		assert.ok(
			["completed", "cancelled", "failed"].includes(expected.status),
			`an undisturbed run ended ${expected.status}: ${expected.error}${describe()}`,
		);

		const r = random(seed ^ 0x5bd1e995);
		const points = () => [...new Set(Array.from({ length: r.int(0, 2) }, () => r.int(1, 25)))];
		const interference = {
			abortAtSaves: points(),
			crashAtSaves: points(),
			failAtSaves: points(),
			replayAnswers: r.chance(0.5),
			raceAdvances: r.chance(0.5),
		};
		const actual = await drive(chaos.fx, seed, interference);
		const label = `${expected.status} -> ${actual.status}${actual.interruptions ? " (interrupted)" : ""}`;
		outcomes.set(label, (outcomes.get(label) ?? 0) + 1);
		const where = `\ninterference: ${JSON.stringify(interference)}; reference ${expected.status} (${expected.error ?? ""}), actual ${actual.status} (${actual.error ?? ""})${describe()}`;

		const calls = (fx: typeof chaos.fx, name: string) => fx.fake.calls(name);
		if (actual.status === "blocked") {
			// Stopped at a step that may have acted: legitimate only after an interruption.
			assert.ok(actual.interruptions > 0, `blocked without interruption${where}`);
			assert.match(
				actual.error ?? "",
				/Interrupted while|outcome is unknown|may have acted|interrupted without a saved result/,
				where,
			);
			for (const name of names(chaos.g))
				assert.ok(
					calls(chaos.fx, name) <=
						calls(reference.fx, name) + actual.interruptions * chaos.g.inFlight[name],
					`${name} ran ${calls(chaos.fx, name)} times, reference ${calls(reference.fx, name)}${where}`,
				);
			// The operator's way out: abandoning closes the run and withdraws its questions.
			await abandonWorkflowRun({ runId: actual.runId, ctx: chaos.fx.ctx() as any });
			const closed = await chaos.fx.execution(actual.runId);
			assert.equal(closed.status, "cancelled", where);
			assert.deepEqual(closed.activations, {}, `an abandoned run kept step records${where}`);
			return;
		}
		assert.equal(actual.status, expected.status, `ended differently${where}`);
		if (expected.status === "completed")
			assert.deepEqual(
				normalize(actual.output),
				normalize(expected.output),
				`output differs${where}`,
			);
		if (expected.status === "failed")
			assert.equal(
				(actual.error ?? "").replace(/\(call \d+\)/g, ""),
				(expected.error ?? "").replace(/\(call \d+\)/g, ""),
				`failed differently${where}`,
			);
		for (const name of names(chaos.g)) {
			const ran = calls(chaos.fx, name);
			const ref = calls(reference.fx, name);
			if (chaos.g.repeatable.has(name))
				assert.ok(
					ran >= ref && ran <= ref + actual.interruptions * chaos.g.inFlight[name],
					`repeatable ${name} ran ${ran} times, reference ${ref}${where}`,
				);
			else
				assert.equal(
					ran,
					ref,
					`${name} (may not repeat) ran ${ran} times, reference ${ref}${where}`,
				);
		}
		const execution = await chaos.fx.execution(actual.runId);
		assert.equal(execution.status, actual.status === "completed" ? "completed" : execution.status);
		if (actual.status !== "failed")
			assert.deepEqual(execution.activations, {}, `a finished run kept step records${where}`);
	});
}

test("model: every node kind was generated", { skip: CASES < 20 && "too few cases" }, (t) => {
	t.diagnostic(`kinds: ${JSON.stringify(Object.fromEntries(kindsSeen))}`);
	t.diagnostic(`outcomes: ${JSON.stringify(Object.fromEntries(outcomes))}`);
	for (const kind of [
		"activity",
		"gated activity",
		"approval",
		"input",
		"sleep",
		"wait",
		"switch",
		"for_each",
		"for_each (concurrency)",
		"repeat",
		"parallel",
		"child",
	])
		assert.ok(kindsSeen.has(kind), `no workflow had a ${kind}`);
});
