import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { promisify } from "node:util";
import {
	getRun,
	getWait,
	listRuns,
	openStoreSession,
	resumeToolRequest,
	runToolRequest,
	type ToolRunContext,
} from "../src/core/index.js";
import { migrateStore } from "../src/persistence/migrate.js";
import { Lobster } from "../src/sdk/index.js";
import { parseResumeArgs } from "../src/resume.js";

const exec = promisify(execFile);
const schema = {
	type: "object",
	properties: { choice: { enum: ["accept", "revise"] } },
	required: ["choice"],
};
const draft = "Review this saved draft. ".repeat(6000);

function shellQuote(value: string) {
	return `'${value.replaceAll("'", "'\\''")}'`;
}

function script(source: string) {
	return `${shellQuote(process.execPath)} -e ${shellQuote(source)}`;
}

async function reviewFixture(directory: string, chained = false) {
	const effects = path.join(directory, "effects.txt");
	const filePath = path.join(directory, "review.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			args: { effects: {} },
			steps: [
				{
					id: "prepare",
					run: script(
						`require('fs').appendFileSync(process.env.LOBSTER_ARG_EFFECTS,'prepare'+String.fromCharCode(10)); console.log(JSON.stringify({draft:${JSON.stringify(draft)}}))`,
					),
				},
				{
					id: "review",
					input: {
						prompt: "Review the prepared draft",
						responseSchema: schema,
						defaults: { choice: "accept" },
					},
					stdin: "$prepare.json",
				},
				...(chained
					? [{ id: "followup", input: { prompt: "Confirm your review", responseSchema: schema } }]
					: []),
				{
					id: "finish",
					run: script(
						"require('fs').appendFileSync(process.env.LOBSTER_ARG_EFFECTS,'finish'+String.fromCharCode(10)); console.log(JSON.stringify({done:true}))",
					),
				},
			],
		}),
	);
	return { filePath, effects };
}

function scoped(
	storeSession: ToolRunContext["storeSession"],
	id = "logical-session-a",
): ToolRunContext {
	return {
		storeSession,
		env: { LOBSTER_MAX_TOOL_ENVELOPE_BYTES: "16384" },
		runMetadata: { callerId: id },
	};
}

for (const backend of ["files", "sqlite"] as const) {
	for (const history of [false, true]) {
		test(`${backend}: discover and answer saved reviews after reopen, history=${history}`, async (t) => {
			const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-discovery-"));
			t.after(() => fs.rm(directory, { recursive: true, force: true }));
			const fixture = await reviewFixture(directory, true);
			const state = path.join(directory, "state");
			let store = await openStoreSession({ directory: state, backend, trackRuns: true, history });
			try {
				const first = await runToolRequest({
					filePath: fixture.filePath,
					args: { effects: fixture.effects },
					ctx: scoped(store),
				});
				assert.equal(first.status, "needs_input", JSON.stringify(first));
				assert.ok(first.runId);
				// The tool returns a bounded preview; discovery must recover the original.
				assert.ok(JSON.stringify(first.requiresInput.subject).length < draft.length);
				await store.close();
				store = await openStoreSession({ directory: state });
				const ctx = scoped(store);
				const page = await listRuns({ status: "waiting", ctx });
				assert.deepEqual(
					page.runs.map((run) => run.id),
					[first.runId],
				);
				assert.equal(page.nextCursor, null);
				const run = await getRun({ runId: first.runId, ctx });
				assert.equal(run.historyEnabled, history);
				assert.equal(run.pendingWaits.length, 1);
				const wait = await getWait({ waitId: run.pendingWaits[0].id, ctx });
				assert.equal(wait.kind, "input");
				assert.deepEqual(wait.request, {
					kind: "input",
					prompt: "Review the prepared draft",
					responseSchema: schema,
					defaults: { choice: "accept" },
					subject: { draft },
				});
				assert.equal(JSON.stringify(wait).includes("resumeToken"), false);
				assert.equal(JSON.stringify(run).includes("stateKey"), false);
				assert.deepEqual(await store.retain({ before: "2099-01-01", dryRun: false }), []);

				const invalid = await resumeToolRequest({
					waitId: wait.id,
					expectedRevision: wait.revision,
					response: { choice: "invalid" },
					ctx,
				});
				assert.equal(invalid.error.type, "parse_error");
				assert.deepEqual(await getWait({ waitId: wait.id, ctx }), wait);
				const stale = await resumeToolRequest({
					waitId: wait.id,
					expectedRevision: wait.revision + 1,
					response: { choice: "accept" },
					ctx,
				});
				assert.equal(stale.error.type, "wait_conflict");

				const second = await resumeToolRequest({
					waitId: wait.id,
					expectedRevision: wait.revision,
					response: { choice: "accept" },
					ctx,
				});
				assert.equal(second.status, "needs_input", JSON.stringify(second));
				assert.equal(second.runId, first.runId);
				assert.equal(await getWait({ waitId: wait.id, ctx }), null);
				const updated = await getRun({ runId: first.runId, ctx });
				const next = await getWait({ waitId: updated.pendingWaits[0].id, ctx });
				assert.notEqual(next.id, wait.id);
				assert.equal(next.request.prompt, "Confirm your review");
				await store.close();
				store = await openStoreSession({ directory: state });
				const done = await resumeToolRequest({
					waitId: next.id,
					expectedRevision: next.revision,
					response: { choice: "accept" },
					ctx: scoped(store),
				});
				assert.equal(done.status, "ok", JSON.stringify(done));
				assert.equal(done.runId, first.runId);
				assert.deepEqual(done.output, [{ done: true }]);
				assert.equal(await fs.readFile(fixture.effects, "utf8"), "prepare\nfinish\n");
				assert.deepEqual(
					(await getRun({ runId: first.runId, ctx: scoped(store) })).pendingWaits,
					[],
				);
				const record = await store.getRun(first.runId);
				assert.equal(record.status, "completed");
				if (history) assert.ok(record.attempts.length > 0);
				else {
					assert.deepEqual(record.attempts, []);
					assert.deepEqual(record.events, []);
				}
			} finally {
				await store.close();
			}
		});
	}

	test(`${backend}: an oversized question remains discoverable when tool rendering fails`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-discovery-oversize-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const filePath = path.join(directory, "large-question.lobster");
		const prompt = "Full original question. ".repeat(2000);
		await fs.writeFile(
			filePath,
			JSON.stringify({ steps: [{ id: "review", input: { prompt, responseSchema: schema } }] }),
		);
		let store = await openStoreSession({
			directory: path.join(directory, "state"),
			backend,
			trackRuns: true,
		});
		try {
			const first = await runToolRequest({ filePath, ctx: scoped(store) });
			assert.equal(first.ok, false);
			assert.match(first.error.message, /envelope exceeds/);
			assert.ok(first.runId);
			await store.close();
			store = await openStoreSession({ directory: path.join(directory, "state") });
			const run = await getRun({ runId: first.runId, ctx: scoped(store) });
			assert.equal(run.status, "waiting");
			const wait = await getWait({ waitId: run.pendingWaits[0].id, ctx: scoped(store) });
			assert.equal(wait.request.prompt, prompt);
			assert.equal(
				(
					await resumeToolRequest({
						waitId: wait.id,
						expectedRevision: wait.revision,
						response: { choice: "accept" },
						ctx: scoped(store),
					})
				).status,
				"ok",
			);
		} finally {
			await store.close();
		}
	});

	test(`${backend}: scope all discovery and decisions to trusted caller context`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-discovery-scope-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const store = await openStoreSession({ directory, backend, trackRuns: true });
		try {
			const a = await runToolRequest({
				pipeline: 'ask --prompt "First" --schema \'{"type":"integer"}\'',
				ctx: scoped(store),
			});
			const b = await runToolRequest({
				pipeline: 'ask --prompt "Second" --schema \'{"type":"integer"}\'',
				ctx: scoped(store),
			});
			const foreign = await runToolRequest({
				pipeline: 'ask --prompt "Foreign" --schema \'{"type":"integer"}\'',
				ctx: scoped(store, "other-session"),
			});
			assert.equal(a.status, "needs_input", JSON.stringify(a));
			const page = await listRuns({ status: "waiting", limit: 1, ctx: scoped(store) });
			assert.equal(page.runs.length, 1);
			assert.ok(page.nextCursor);
			const next = await listRuns({
				status: "waiting",
				limit: 1,
				after: page.nextCursor,
				ctx: scoped(store),
			});
			assert.deepEqual([page.runs[0].id, next.runs[0].id].sort(), [a.runId, b.runId].sort());
			assert.equal(next.nextCursor, null);
			assert.equal(await getRun({ runId: foreign.runId, ctx: scoped(store) }), null);
			const wait = (await getRun({ runId: a.runId, ctx: scoped(store) })).pendingWaits[0];
			assert.equal(await getWait({ waitId: wait.id, ctx: scoped(store, "other-session") }), null);
			const denied = await resumeToolRequest({
				waitId: wait.id,
				expectedRevision: wait.revision,
				response: 1,
				ctx: scoped(store, "other-session"),
			});
			assert.equal(denied.error.type, "wait_conflict");
			assert.ok(await getWait({ waitId: wait.id, ctx: scoped(store) }));
			await assert.rejects(listRuns({ limit: 0, ctx: scoped(store) }), /limit/);
			await assert.rejects(listRuns({ after: "../invalid", ctx: scoped(store) }), /cursor/);
			const ambiguous = await resumeToolRequest({
				waitId: wait.id,
				expectedRevision: wait.revision,
				approved: true,
				response: 1,
				ctx: scoped(store),
			});
			assert.equal(ambiguous.error.type, "parse_error");
			assert.ok(await getWait({ waitId: wait.id, ctx: scoped(store) }));
			await assert.rejects(getWait({ waitId: "../escape", ctx: scoped(store) }), /Invalid wait ID/);
		} finally {
			await store.close();
		}
	});

	test(`${backend}: wait and token resumes compete for the same checkpoint`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-discovery-race-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const effects = path.join(directory, "effects");
		const filePath = path.join(directory, "approval.lobster");
		await fs.writeFile(
			filePath,
			JSON.stringify({
				steps: [
					{ id: "review", approval: "Approve this fixture" },
					{
						id: "effect",
						run: script(
							`require('fs').appendFileSync(${JSON.stringify(effects)},'once'); console.log(1)`,
						),
					},
				],
			}),
		);
		const store = await openStoreSession({
			directory: path.join(directory, "state"),
			backend,
			trackRuns: true,
			history: true,
		});
		try {
			const first = await runToolRequest({ filePath, ctx: scoped(store) });
			const wait = (await getRun({ runId: first.runId, ctx: scoped(store) })).pendingWaits[0];
			const question = await getWait({ waitId: wait.id, ctx: scoped(store) });
			assert.equal(question.request.prompt, "Approve this fixture");
			const results = await Promise.all([
				resumeToolRequest({
					waitId: wait.id,
					expectedRevision: wait.revision,
					approved: true,
					ctx: scoped(store),
				}),
				resumeToolRequest({
					token: first.requiresApproval.resumeToken,
					approved: true,
					ctx: scoped(store),
				}),
			]);
			assert.equal(results.filter((result) => result.ok).length, 1, JSON.stringify(results));
			assert.equal(await fs.readFile(effects, "utf8"), "once");
			const finished = await store.getRun(first.runId);
			assert.equal(
				finished.attempts.some((attempt) => attempt.status === "started"),
				false,
			);
			assert.equal(await getWait({ waitId: wait.id, ctx: scoped(store) }), null);
		} finally {
			await store.close();
		}
	});

	test(`${backend}: answer revision is checked atomically at checkpoint consumption`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-discovery-cas-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const store = await openStoreSession({ directory, backend, trackRuns: true });
		try {
			const first = await runToolRequest({
				pipeline: 'ask --prompt "Question" --schema \'{"type":"integer"}\'',
				ctx: scoped(store),
			});
			const wait = (await getRun({ runId: first.runId, ctx: scoped(store) })).pendingWaits[0];
			const original = store.backend.write.bind(store.backend);
			let changed = false;
			store.backend.write = async (key, text, options) => {
				if (options?.expectedWaitRevision !== undefined && !changed) {
					changed = true;
					await original(key, (await store.read(key)).text);
				}
				return original(key, text, options);
			};
			const result = await resumeToolRequest({
				waitId: wait.id,
				expectedRevision: wait.revision,
				response: 42,
				ctx: scoped(store),
			});
			assert.equal(changed, true);
			assert.equal(result.error.type, "wait_conflict");
			const current = await getWait({ waitId: wait.id, ctx: scoped(store) });
			assert.ok(current.revision > wait.revision);
			assert.equal(
				(
					await resumeToolRequest({
						waitId: current.id,
						expectedRevision: current.revision,
						response: 42,
						ctx: scoped(store),
					})
				).status,
				"ok",
			);
		} finally {
			await store.close();
		}
	});

	test(`${backend}: cancellation retires a discovered wait without executing later steps`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-discovery-cancel-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const fixture = await reviewFixture(directory);
		const store = await openStoreSession({
			directory: path.join(directory, "state"),
			backend,
			trackRuns: true,
		});
		try {
			const first = await runToolRequest({
				filePath: fixture.filePath,
				args: { effects: fixture.effects },
				ctx: scoped(store),
			});
			const wait = (await getRun({ runId: first.runId, ctx: scoped(store) })).pendingWaits[0];
			const cancelled = await resumeToolRequest({
				waitId: wait.id,
				expectedRevision: wait.revision,
				cancel: true,
				ctx: scoped(store),
			});
			assert.equal(cancelled.status, "cancelled", JSON.stringify(cancelled));
			assert.equal((await getRun({ runId: first.runId, ctx: scoped(store) })).status, "cancelled");
			assert.equal(await fs.readFile(fixture.effects, "utf8"), "prepare\n");
			assert.equal(await getWait({ waitId: wait.id, ctx: scoped(store) }), null);
			assert.equal(
				(
					await resumeToolRequest({
						token: first.requiresInput.resumeToken,
						response: { choice: "accept" },
						ctx: scoped(store),
					})
				).ok,
				false,
			);
		} finally {
			await store.close();
		}
	});

	test(`${backend}: migration preserves discoverable wait identities and review context`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-discovery-migrate-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		let store = await openStoreSession({ directory, backend, trackRuns: true });
		try {
			const first = await runToolRequest({
				pipeline: 'ask --prompt "Persisted question" --schema \'{"type":"integer"}\'',
				ctx: scoped(store),
			});
			const run = await getRun({ runId: first.runId, ctx: scoped(store) });
			const wait = await getWait({ waitId: run.pendingWaits[0].id, ctx: scoped(store) });
			await store.close();
			await migrateStore({
				directory,
				backend: backend === "files" ? "sqlite" : "files",
				offline: true,
			});
			store = await openStoreSession({ directory });
			assert.deepEqual(await getWait({ waitId: wait.id, ctx: scoped(store) }), wait);
			assert.equal(
				(
					await resumeToolRequest({
						waitId: wait.id,
						expectedRevision: wait.revision,
						response: 7,
						ctx: scoped(store),
					})
				).status,
				"ok",
			);
		} finally {
			await store.close();
		}
	});

	test(`${backend}: SDK waits are discoverable and resume with the supplied stages`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-discovery-sdk-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		let preparations = 0;
		let store = await openStoreSession({ directory, backend, trackRuns: true });
		const workflow = () =>
			new Lobster({ storeSession: store, runMetadata: { callerId: "logical-session-a" } })
				.pipe(() => {
					preparations++;
					return [1];
				})
				.pipe({
					async run({ ctx }) {
						return {
							output: [
								await ctx.requestInput({
									prompt: "SDK question",
									responseSchema: { type: "integer" },
								}),
							],
						};
					},
				})
				.pipe((items) => items.map((value) => value + 1));
		try {
			const first = await workflow().run();
			assert.equal(first.status, "needs_input", JSON.stringify(first));
			const run = await getRun({ runId: first.runId, ctx: scoped(store) });
			const wait = await getWait({ waitId: run.pendingWaits[0].id, ctx: scoped(store) });
			assert.equal(wait.kind, "input");
			assert.equal(wait.request.prompt, "SDK question");
			await store.close();
			store = await openStoreSession({ directory });
			await assert.rejects(
				workflow().resumeWait({
					waitId: wait.id,
					expectedRevision: wait.revision,
					response: "invalid",
				}),
				/schema/,
			);
			const resumed = await workflow().resumeWait({
				waitId: wait.id,
				expectedRevision: wait.revision,
				response: 9,
			});
			assert.equal(resumed.status, "ok", JSON.stringify(resumed));
			assert.equal(resumed.runId, first.runId);
			assert.deepEqual(resumed.output, [10]);
			assert.equal(preparations, 1);
		} finally {
			await store.close();
		}
	});

	test(`${backend}: CLI discovers and answers a review through fresh processes`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-discovery-cli-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const env = {
			...process.env,
			LOBSTER_STATE_DIR: directory,
			LOBSTER_BACKEND: backend,
			LOBSTER_RUNS: "1",
			LOBSTER_HISTORY: "0",
		};
		const cli = async (...args: string[]) =>
			JSON.parse(
				(
					await exec(process.execPath, [path.join(process.cwd(), "bin/lobster.js"), ...args], {
						env,
					})
				).stdout,
			);
		const first = await cli(
			"run",
			"--mode",
			"tool",
			'ask --prompt "CLI question" --schema \'{"type":"integer"}\'',
		);
		assert.equal(first.status, "needs_input", JSON.stringify(first));
		const page = await cli("runs", "list", "--status", "waiting");
		assert.equal(page.runs[0].id, first.runId);
		const run = await cli("runs", "show", first.runId);
		const wait = await cli("waits", "show", run.pendingWaits[0].id);
		assert.equal(wait.request.prompt, "CLI question");
		const done = await cli(
			"resume",
			"--wait",
			wait.id,
			"--expected-revision",
			String(wait.revision),
			"--response-json",
			"42",
		);
		assert.equal(done.status, "ok", JSON.stringify(done));
		assert.deepEqual(done.output, [42]);
		assert.equal(done.runId, first.runId);
	});
}

test("wait resume flags require a revision and preserve legacy argument parsing", () => {
	assert.deepEqual(parseResumeArgs(["--token", "fixture", "--approve", "yes"]), {
		token: "fixture",
		approvalId: null,
		approved: true,
	});
	assert.deepEqual(
		parseResumeArgs(["--wait=fixture", "--expected-revision=3", "--response-json", "42"]),
		{ token: null, approvalId: null, waitId: "fixture", expectedRevision: 3, response: 42 },
	);
	assert.throws(() => parseResumeArgs(["--wait", "fixture", "--cancel"]), /expected-revision/);
	assert.throws(
		() => parseResumeArgs(["--wait", "fixture", "--expected-revision", "-1", "--cancel"]),
		/non-negative/,
	);
	assert.throws(
		() =>
			parseResumeArgs([
				"--wait",
				"fixture",
				"--expected-revision",
				"3",
				"--token",
				"old",
				"--cancel",
			]),
		/cannot be combined/,
	);
});
