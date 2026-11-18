// Load and soak: many runs at once, long loops, and (nightly) a long steady stream of runs.
// They check that everything finishes, that `runs advance --due --concurrency` helps, that
// the store and saved state stay small, and that time grows linearly with work. Numbers are
// reported as diagnostics, so the nightly job also records how fast Lobster is.
import test from "node:test";
import assert from "node:assert/strict";
import { statSync } from "node:fs";
import path from "node:path";
import { runWorkflowFile } from "../../src/engine/runner.js";
import { advanceDueRuns } from "../../src/engine/due.js";
import { openStoreSession } from "../../src/persistence/session.js";
import { workflowFixture, type WorkflowFixture } from "../helpers/workflow_runner.js";
import { NIGHTLY, cases } from "./settings.js";

function storeBytes(fx: WorkflowFixture) {
	const base = path.join(fx.env.LOBSTER_STATE_DIR!, "lobster.sqlite");
	let total = 0;
	for (const file of [base, base + "-wal"]) {
		try {
			total += statSync(file).size;
		} catch {
			/* not there */
		}
	}
	return total;
}

async function statuses(fx: WorkflowFixture) {
	const session = await openStoreSession({ env: fx.env });
	try {
		const counts: Record<string, number> = {};
		let after: string | undefined;
		do {
			const page = await session.listRuns({ limit: 100, ...(after ? { after } : {}) });
			for (const run of page.runs) counts[run.status] = (counts[run.status] ?? 0) + 1;
			after = page.nextCursor ?? undefined;
		} while (after);
		return counts;
	} finally {
		await session.close();
	}
}

/** Start `count` runs that each work, pause on a sleep, then run a short command. */
async function startNapping(fx: WorkflowFixture, count: number) {
	const flow = await fx.write("nap.lobster", {
		steps: [
			{ id: "before", pipeline: "fake --name before" },
			{ id: "nap", sleep: "20ms" },
			{ id: "after", run: "sleep 0.05; echo '{\"done\":true}'" },
		],
	});
	const started = Date.now();
	for (let i = 0; i < count; i++) {
		const result = await runWorkflowFile({ filePath: flow, ctx: fx.ctx() as any });
		assert.equal(result.status, "waiting");
	}
	return Date.now() - started;
}

test(
	"load: many sleeping runs are woken by --due; concurrency shortens the wait",
	{ timeout: 1_800_000 },
	async (t) => {
		const runs = cases(40, 1_000);
		const timings: Record<number, number> = {};
		for (const concurrency of [1, 4, 16]) {
			const fx = await workflowFixture(t, "lobster-load-");
			fx.env.LOBSTER_INLINE_SLEEP_MS = "0";
			const startMs = await startNapping(fx, runs);
			await new Promise((resolve) => setTimeout(resolve, 30));
			const started = Date.now();
			// At most 100 runs per call, as a cron job would see them: call until none is due.
			const advanced: Awaited<ReturnType<typeof advanceDueRuns>>["advanced"] = [];
			for (;;) {
				const batch = await advanceDueRuns({ ctx: fx.ctx() as any, limit: 100, concurrency });
				if (!batch.due.length) break;
				advanced.push(...batch.advanced);
			}
			timings[concurrency] = Date.now() - started;
			assert.equal(advanced.length, runs, "every run was due once");
			assert.deepEqual(
				[...new Set(advanced.map((outcome) => outcome.status))],
				["ok"],
				JSON.stringify(advanced.filter((outcome) => outcome.status !== "ok").slice(0, 3)),
			);
			assert.deepEqual(await statuses(fx), { completed: runs });
			assert.equal(fx.fake.calls("before"), runs, "no step ran twice");
			const bytes = storeBytes(fx);
			t.diagnostic(
				`concurrency ${concurrency}: started ${runs} runs in ${startMs} ms (${Math.round((runs * 1000) / startMs)}/s), woke them in ${timings[concurrency]} ms (${Math.round((runs * 1000) / timings[concurrency])}/s); store ${Math.round(bytes / 1024)} KiB, ${Math.round(bytes / runs)} bytes per finished run`,
			);
			// A finished run keeps its definition, output and history, not its step records.
			assert.ok(bytes / runs < 64 * 1024, `${Math.round(bytes / runs)} bytes per run`);
		}
		// The 50 ms command dominates each wake-up, so running several at once must help.
		assert.ok(
			timings[4] < timings[1] * 0.7,
			`concurrency 4 (${timings[4]} ms) vs 1 (${timings[1]} ms)`,
		);
	},
);

test(
	"load: long loops take time in proportion to their length",
	{ timeout: 1_800_000 },
	async (t) => {
		const fx = await workflowFixture(t, "lobster-load-loops-");
		const size = cases(200, 2_000);
		const timeLoop = async (kind: "for_each" | "repeat", n: number) => {
			const flow = await fx.write(`${kind}-${n}.lobster`, {
				steps:
					kind === "for_each"
						? [
								{
									id: "items",
									run: `node -e 'console.log(JSON.stringify([...Array(${n}).keys()]))'`,
								},
								{
									id: "each",
									for_each: "$items.json",
									steps: [{ id: "w", pipeline: "head --n 1" }],
								},
							]
						: [
								{
									id: "loop",
									repeat: { until: "false", max: n, on_max: "continue" },
									steps: [{ id: "w", pipeline: "head --n 1" }],
								},
							],
			});
			const started = Date.now();
			const result = await runWorkflowFile({ filePath: flow, ctx: fx.ctx() as any });
			assert.equal(result.status, "ok", JSON.stringify(result).slice(0, 300));
			return Date.now() - started;
		};
		for (const kind of ["repeat", "for_each"] as const) {
			// A for_each keeps its items and outputs beside the checkpoint, and so are large step
			// outputs (the list it loops over), so a save does not grow with the loop.
			await t.test(kind, async (t) => {
				const small = await timeLoop(kind, size);
				const large = await timeLoop(kind, size * 4);
				t.diagnostic(
					`${kind}: ${size} in ${small} ms, ${size * 4} in ${large} ms (${Math.round((size * 4 * 1000) / large)}/s)`,
				);
				// Linear would be 4x.
				assert.ok(
					large < small * 6,
					`${kind}: ${size} took ${small} ms, ${size * 4} took ${large} ms`,
				);
			});
		}
	},
);

const SOAK_MINUTES = Number(process.env.LOBSTER_CAMPAIGN_SOAK_MINUTES ?? (NIGHTLY ? 10 : 0));

test(
	"soak: a steady stream of runs keeps its pace and memory",
	{
		skip: !SOAK_MINUTES && "nightly only (LOBSTER_CAMPAIGN_SOAK_MINUTES)",
		timeout: (SOAK_MINUTES + 5) * 60_000,
	},
	async (t) => {
		const fx = await workflowFixture(t, "lobster-soak-");
		fx.env.LOBSTER_INLINE_SLEEP_MS = "0";
		const end = Date.now() + SOAK_MINUTES * 60_000;
		const minutes: Array<{ runs: number; heapMb: number; storeKb: number }> = [];
		let minuteEnd = Date.now() + 60_000;
		let done = 0;
		let finishedThisMinute = 0;
		while (Date.now() < end) {
			await startNapping(fx, 20);
			await new Promise((resolve) => setTimeout(resolve, 25));
			const { advanced } = await advanceDueRuns({ ctx: fx.ctx() as any, concurrency: 4 });
			assert.ok(
				advanced.every((outcome) => outcome.status === "ok"),
				JSON.stringify(advanced[0]),
			);
			done += advanced.length;
			finishedThisMinute += advanced.length;
			if (Date.now() >= minuteEnd) {
				global.gc?.();
				minutes.push({
					runs: finishedThisMinute,
					heapMb: Math.round(process.memoryUsage().heapUsed / 1e6),
					storeKb: Math.round(storeBytes(fx) / 1024),
				});
				finishedThisMinute = 0;
				minuteEnd += 60_000;
			}
		}
		t.diagnostic(`soak: ${done} runs; per minute ${JSON.stringify(minutes)}`);
		const first = minutes[0];
		const last = minutes.at(-1)!;
		assert.ok(last.runs >= first.runs * 0.5, "throughput fell by more than half");
		assert.ok(last.heapMb <= Math.max(first.heapMb * 2, first.heapMb + 64), "memory keeps growing");
	},
);

test(
	"load: a for_each with concurrency finishes its items that much sooner",
	{ timeout: 600_000 },
	async (t) => {
		const fx = await workflowFixture(t, "lobster-load-concurrency-");
		const items = cases(40, 400);
		const timings: Record<number, number> = {};
		for (const concurrency of [1, 8]) {
			const flow = await fx.write(`c-${concurrency}.lobster`, {
				steps: [
					{
						id: "items",
						run: `node -e 'console.log(JSON.stringify([...Array(${items}).keys()]))'`,
					},
					{
						id: "each",
						for_each: "$items.json",
						concurrency,
						steps: [{ id: "w", run: "sleep 0.05; echo '{}'" }],
					},
				],
			});
			const started = Date.now();
			const result = await runWorkflowFile({ filePath: flow, ctx: fx.ctx() as any });
			timings[concurrency] = Date.now() - started;
			assert.equal(result.status, "ok");
			assert.equal((result.output as unknown[]).length, items);
		}
		t.diagnostic(
			`${items} items of 50 ms: ${timings[1]} ms one at a time, ${timings[8]} ms eight at a time`,
		);
		assert.ok(timings[8] < timings[1] / 3, JSON.stringify(timings));
	},
);
