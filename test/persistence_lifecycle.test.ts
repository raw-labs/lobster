import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
	createDefaultRegistry,
	getRun,
	getWait,
	openStoreSession,
	resumeToolRequest,
	runToolRequest,
} from "../src/core/index.js";
import { Lobster } from "../src/sdk/Lobster.js";

for (const backend of ["files", "sqlite"] as const) {
	for (const gate of ["approval", "input"] as const) {
		for (const boundary of ["definition", "start"] as const) {
			test(`${backend}: SDK ${gate} survives a ${boundary} failure before claiming`, async (t) => {
				const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-sdk-preclaim-"));
				t.after(() => fs.rm(directory, { recursive: true, force: true }));
				const store = await openStoreSession({ directory, backend, history: true });
				t.after(() => store.close());
				let effects = 0;
				const lobster = new Lobster({ storeSession: store })
					.pipe(() => [1])
					.pipe({
						async run() {
							return {
								halt: true,
								output: [
									gate === "approval"
										? { type: "approval_request", prompt: "Review", items: [1] }
										: {
												type: "input_request",
												prompt: "Choose",
												responseSchema: { type: "integer" },
											},
								],
							};
						},
					})
					.pipe((items) => {
						effects++;
						return items;
					});
				const first = await lobster.run();
				const request = first.requiresApproval ?? first.requiresInput;
				const ctx = { storeSession: store };
				const run = await getRun({ runId: first.runId, ctx });
				const wait = await getWait({ waitId: run.pendingWaits[0].id, ctx });
				const transition = store.backend.transition.bind(store.backend);
				let failed = false;
				store.backend.transition = async (...args) => {
					if (!failed && args[0].action.kind === boundary) {
						failed = true;
						throw new Error("Temporary history failure");
					}
					return transition(...args);
				};
				const decision = gate === "approval" ? { approved: true } : { response: 5 };
				const resumed = await lobster.resume(request.resumeToken, decision);
				store.backend.transition = transition;
				assert.equal(resumed.ok, false);
				assert.equal(resumed.error.type, "persistence_error");
				assert.equal(effects, 0);
				assert.deepEqual(await getWait({ waitId: wait.id, ctx }), wait);
				assert.equal((await getRun({ runId: first.runId, ctx })).status, "waiting");
				const retried = await lobster.resume(request.resumeToken, decision);
				assert.equal(retried.ok, true, JSON.stringify(retried));
				assert.equal(retried.runId, first.runId);
				assert.equal(effects, 1);
				assert.equal((await getRun({ runId: first.runId, ctx })).status, "completed");
			});
		}
	}

	for (const fail of [false, true]) {
		test(`${backend}: unused lazy output closes its history, downstream failure=${fail}`, async (t) => {
			const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-unused-stage-"));
			t.after(() => fs.rm(directory, { recursive: true, force: true }));
			const store = await openStoreSession({ directory, backend, history: true });
			t.after(() => store.close());
			let produced = 0;
			const registry = {
				get(name: string) {
					return name === "source"
						? {
								async run() {
									return {
										output: (async function* () {
											produced++;
											yield 1;
										})(),
									};
								},
							}
						: {
								async run() {
									if (fail) throw new Error("Downstream failed");
									return { output: [42] };
								},
							};
				},
			};
			const result = await runToolRequest({
				pipeline: "source | ignore",
				ctx: { storeSession: store, registry },
			});
			assert.equal(result.ok, !fail);
			assert.equal(produced, 0);
			const run = await store.getRun(result.runId);
			assert.equal(run.status, fail ? "failed" : "completed");
			assert.deepEqual(
				run.attempts.map((attempt) => attempt.status),
				["closed", fail ? "failed" : "completed"],
			);
			assert.deepEqual(await store.retain({ before: "2099-01-01", dryRun: false }), [run.id]);
		});
	}

	for (const history of [false, true]) {
		test(`${backend}: pruning a failed resumed pipeline cannot revive its token, history=${history}`, async (t) => {
			const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-failed-retention-"));
			t.after(() => fs.rm(directory, { recursive: true, force: true }));
			const store = await openStoreSession({ directory, backend, trackRuns: true, history });
			t.after(() => store.close());
			let effects = 0;
			const defaults = createDefaultRegistry();
			const registry = {
				get(name: string) {
					return name === "fail"
						? {
								async run() {
									effects++;
									throw new Error("Effect failed");
								},
							}
						: defaults.get(name);
				},
			};
			const ctx = { storeSession: store, registry };
			const first = await runToolRequest({
				pipeline: 'ask --emit --prompt Choose --schema \'{"type":"integer"}\' | fail',
				ctx,
			});
			const token = first.requiresInput.resumeToken;
			assert.equal((await resumeToolRequest({ token, response: 3, ctx })).ok, false);
			assert.equal((await getRun({ runId: first.runId, ctx })).status, "failed");
			assert.equal((await resumeToolRequest({ token, response: 3, ctx })).ok, false);
			assert.deepEqual(await store.retain({ before: "2099-01-01", dryRun: false }), [first.runId]);
			assert.equal((await resumeToolRequest({ token, response: 3, ctx })).ok, false);
			assert.equal(effects, 1);
		});
	}
}

for (const backend of ["files", "sqlite"] as const) {
	test(`${backend}: cancelling tracked SDK command input competes with answering it`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-sdk-decision-race-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const store = await openStoreSession({ directory, backend, trackRuns: true });
		t.after(() => store.close());
		for (let iteration = 0; iteration < 3; iteration++) {
			let effects = 0;
			const sdk = new Lobster({ storeSession: store }).pipe({
				async run({ ctx }) {
					const answer = await ctx.requestInput({
						prompt: "Choose",
						responseSchema: { type: "integer" },
					});
					effects++;
					return { output: [answer] };
				},
			});
			const first = await sdk.run();
			const run = await getRun({ runId: first.runId, ctx: { storeSession: store } });
			const wait = run.pendingWaits[0];
			const decisions = await Promise.allSettled([
				sdk.resumeWait({ waitId: wait.id, expectedRevision: wait.revision, response: 7 }),
				sdk.resumeWait({ waitId: wait.id, expectedRevision: wait.revision, cancel: true }),
			]);
			const successes = decisions.filter((entry) => entry.status === "fulfilled" && entry.value.ok);
			assert.equal(successes.length, 1, JSON.stringify(decisions));
			const result = successes[0].status === "fulfilled" ? successes[0].value : null;
			assert.equal(effects, result.status === "ok" ? 1 : 0);
			const final = await getRun({ runId: first.runId, ctx: { storeSession: store } });
			assert.equal(final.status, result.status === "ok" ? "completed" : "cancelled");
			assert.equal(final.pendingWaits.length, 0);
		}
	});
}

for (const backend of ["legacy", "files", "sqlite"] as const) {
	test(`${backend}: a failed for_each pipeline cannot restore an approval after an effect`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-loop-replay-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const stateDir = path.join(directory, "state");
		const store =
			backend === "legacy"
				? undefined
				: await openStoreSession({ directory: stateDir, backend, history: true });
		t.after(() => store?.close());
		let effects = 0;
		const defaults = createDefaultRegistry();
		const registry = {
			get(name: string) {
				if (name === "data")
					return {
						async run() {
							return { output: [[1]] };
						},
					};
				if (name === "effect")
					return {
						async run() {
							effects++;
							throw new Error("Effect failed after dispatch");
						},
					};
				return defaults.get(name);
			},
		};
		const filePath = path.join(directory, "flow.lobster");
		await fs.writeFile(
			filePath,
			JSON.stringify({
				steps: [
					{ id: "data", pipeline: "data" },
					{ id: "review", approval: "Proceed?" },
					{ id: "loop", for_each: "$data.json", steps: [{ id: "write", pipeline: "effect" }] },
				],
			}),
		);
		const ctx = {
			storeSession: store,
			registry,
			env: { LOBSTER_STATE_DIR: stateDir },
		};
		const first = await runToolRequest({ filePath, ctx });
		assert.equal(first.status, "needs_approval", JSON.stringify(first));
		const token = first.requiresApproval.resumeToken;
		const failed = await resumeToolRequest({ token, approved: true, ctx });
		assert.equal(failed.ok, false);
		assert.match(failed.error.message, /Effect failed after dispatch/);
		const repeated = await resumeToolRequest({ token, approved: true, ctx });
		assert.equal(repeated.ok, false);
		assert.match(repeated.error.message, /resume state not found/);
		assert.equal(effects, 1);
		if (store) {
			const run = await getRun({ runId: first.runId, ctx });
			assert.equal(run.status, "failed");
			assert.equal(run.pendingWaits.length, 0);
		}
	});
}
