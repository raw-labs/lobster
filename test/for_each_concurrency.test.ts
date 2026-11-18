import test from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import path from "node:path";
import { advanceWorkflowRun, runWorkflowFile } from "../src/workflows/file.js";
import { crashAndRecover } from "./helpers/crash_recovery.js";
import { workflowFixture } from "./helpers/workflow_runner.js";

/** A step that logs when it starts and ends, so a test can see what overlapped. */
const logged = (log: string, seconds: string) =>
	`echo "start $N" >> '${log}'; sleep ${seconds}; echo "end $N" >> '${log}'; echo "{\\"n\\":$N}"`;

function maxOverlap(text: string) {
	let running = 0;
	let most = 0;
	for (const line of text.trim().split("\n")) {
		running += line.startsWith("start") ? 1 : -1;
		most = Math.max(most, running);
	}
	return most;
}

test("concurrency runs up to N items at once and keeps the output in item order", async (t) => {
	const fx = await workflowFixture(t);
	const log = path.join(fx.dir, "log");
	const result = await fx.run({
		steps: [
			{ id: "items", run: "echo '[5,1,4,2,3,1,2,1]'" },
			{
				id: "each",
				for_each: "$items.json",
				concurrency: 3,
				// Item n takes n tenths of a second, so later items often finish first.
				steps: [{ id: "w", run: logged(log, "0.$N"), env: { N: "$item.json" } }],
			},
		],
	});
	assert.equal(result.status, "ok");
	assert.deepEqual(
		(result.output as any[]).map((entry) => entry.w.n),
		[5, 1, 4, 2, 3, 1, 2, 1],
	);
	assert.equal(maxOverlap(await fs.readFile(log, "utf8")), 3);
});

test("a failed item lets running items finish, starts no more, and a retry runs only what is left", async (t) => {
	const fx = await workflowFixture(t);
	const log = path.join(fx.dir, "log");
	const flag = path.join(fx.dir, "fixed");
	const flow = await fx.write("flow.lobster", {
		steps: [
			{ id: "items", run: "echo '[1,2,3,4,5,6,7,8]'" },
			{
				id: "each",
				for_each: "$items.json",
				concurrency: 3,
				steps: [
					{
						id: "w",
						// Item 3 fails until the file "fixed" exists; the others take a moment.
						run: `if [ "$N" = 3 ] && [ ! -e '${flag}' ]; then exit 1; fi; ${logged(log, "0.2")}`,
						env: { N: "$item.json" },
					},
				],
			},
		],
	});
	await assert.rejects(runWorkflowFile({ filePath: flow, ctx: fx.ctx() as any }), /command failed/);
	const firstRun = (await fs.readFile(log, "utf8")).trim().split("\n");
	const started = firstRun.filter((line) => line.startsWith("start")).map((line) => line.slice(6));
	const ended = firstRun.filter((line) => line.startsWith("end")).map((line) => line.slice(4));
	// Items 1 and 2 were running when 3 failed: they finished; nothing new started after.
	assert.deepEqual(started.sort(), ended.sort(), "every started item finished");
	assert.ok(started.length < 7, `no new items after the failure, started ${started}`);

	await fs.writeFile(flag, "");
	await fs.writeFile(log, "");
	const retried = await advanceWorkflowRun({ runId: await fx.runId(), ctx: fx.ctx() as any });
	assert.equal(retried.status, "ok");
	const secondRun = (await fs.readFile(log, "utf8")).trim().split("\n");
	const rerun = secondRun.filter((line) => line.startsWith("start")).map((line) => line.slice(6));
	for (const item of started) assert.ok(!rerun.includes(item), `item ${item} ran twice`);
	assert.deepEqual([...started, ...rerun].sort(), ["1", "2", "4", "5", "6", "7", "8", "3"].sort());
	assert.deepEqual(
		(retried.output as any[]).map((entry) => entry.w.n),
		[1, 2, 3, 4, 5, 6, 7, 8],
	);
});

test("a run killed while items run at once recovers, running each finished item once", async (t) => {
	const fx = await workflowFixture(t);
	const flow = await fx.write("flow.lobster", {
		steps: [
			{ id: "items", run: "node -e 'console.log(JSON.stringify([...Array(12).keys()]))'" },
			{
				id: "each",
				for_each: "$items.json",
				concurrency: 4,
				steps: [{ id: "w", run: `sleep 0.1 && ${fx.fake.shell("w")}`, recovery: "repeat" }],
			},
		],
	});
	const { recovered, killed } = await crashAndRecover(t, {
		dir: fx.dir,
		env: fx.env,
		flowPath: flow,
		killWhen: ({ calls }) => calls("w") >= 6,
		leaseMs: 300,
	});
	assert.ok(killed);
	assert.equal(recovered.status, "ok", JSON.stringify(recovered));
	assert.deepEqual(
		(recovered.output as any[]).map((entry) => entry.item),
		[...Array(12).keys()],
	);
	// At most the four items running at the kill ran again.
	assert.ok(
		fx.fake.calls("w") >= 12 && fx.fake.calls("w") <= 16,
		`w ran ${fx.fake.calls("w")} times`,
	);
});

test("a loop with concurrency refuses what would wait or pace items one at a time", async (t) => {
	const fx = await workflowFixture(t);
	const loop = (extra: object, steps: object[] = [{ id: "w", run: "echo hi" }]) => ({
		steps: [
			{ id: "items", run: "echo '[1,2]'" },
			{ id: "each", for_each: "$items.json", concurrency: 2, ...extra, steps },
		],
	});
	await assert.rejects(
		fx.run(loop({}, [{ id: "ok", approval: "Send?" }])),
		/for_each with concurrency cannot contain approval, input, sleep or wait steps/,
	);
	await assert.rejects(
		fx.run(
			loop({}, [
				{ id: "pick", switch: { cases: [{ when: true, steps: [{ id: "nap", sleep: "1s" }] }] } },
			]),
		),
		/for_each with concurrency cannot contain/,
	);
	await assert.rejects(fx.run(loop({ pause_ms: 10 })), /cannot combine concurrency with pause_ms/);
	await assert.rejects(
		fx.run({ steps: [{ id: "w", run: "echo hi", concurrency: 2 }] }),
		/concurrency applies only to for_each/,
	);
	// A pipeline that asks is refused when it asks.
	await assert.rejects(
		fx.run(loop({}, [{ id: "review", pipeline: "ask --prompt 'Review?'" }])),
		/not supported inside a for_each with concurrency|requestInput is not supported/,
	);
});
