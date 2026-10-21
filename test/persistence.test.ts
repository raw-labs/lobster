import assert from "node:assert/strict";
import { execFile, spawn, type ExecFileException } from "node:child_process";
import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import type { Worker } from "node:worker_threads";
import { runToolRequest, resumeToolRequest } from "../src/core/tool_runtime.js";
import { PersistenceOutcomeUnknown } from "../src/persistence/backend.js";
import { migrateStore } from "../src/persistence/migrate.js";
import type { Transition } from "../src/persistence/model.js";
import type { SqliteBackend } from "../src/persistence/sqlite.js";
import { bindStoreSession, openStoreSession } from "../src/persistence/session.js";
import { decodeResumeToken } from "../src/resume.js";
import { Lobster } from "../src/sdk/Lobster.js";
import { diffAndStore, readStateJsonWithLock, writeStateJson } from "../src/state/store.js";

const exec = promisify(execFile);

async function processCall(mode: string, value: string, env: NodeJS.ProcessEnv) {
	const { stdout } = await exec(
		process.execPath,
		[fileURLToPath(new URL("./fixtures/persistence-process.js", import.meta.url)), mode, value],
		{ env, timeout: 10000 },
	);
	return JSON.parse(stdout);
}

for (const backend of ["files", "sqlite"] as const) {
	test(`${backend}: writes expose complete state and run commit identities`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-write-outcomes-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const store = await openStoreSession({ directory, backend, history: true });
		try {
			const stateCommit = {
				status: "committed",
				kind: "state",
				confirmation: "acknowledged",
				signalAbortedAfterCommit: false,
			};
			assert.deepEqual(await store.write("fixture", "42"), stateCommit);
			await assert.rejects(store.write("fixture", "99", { exclusive: true }), { code: "EEXIST" });
			assert.equal((await store.read("fixture")).text, "42");
			assert.deepEqual(await store.write("fixture", null), stateCommit);
			assert.equal((await store.read("fixture")).text, null);

			const recorder = await store.createRun("fixture");
			const key = `pipeline_resume_${randomUUID()}`;
			const pending = JSON.stringify({ pipeline: [], items: [], resumeAtIndex: 0 });
			const tracked = await store.write(key, pending, { runId: recorder.runId });
			assert.ok(tracked.kind === "run");
			assert.match(tracked.operationId, /^[a-f0-9-]{36}$/);
			assert.deepEqual(tracked, {
				status: "committed",
				kind: "run",
				runId: recorder.runId,
				operationId: tracked.operationId,
				revision: 2,
				confirmation: "acknowledged",
				signalAbortedAfterCommit: false,
			});
			const receipt = await store.reconcile(tracked.runId, tracked.operationId);
			assert.equal(receipt.revision, tracked.revision);
			assert.equal(receipt.acknowledged, true);

			const retired = await store.write(key, null);
			assert.ok(retired.kind === "run");
			assert.equal(retired.runId, recorder.runId);
			assert.equal(retired.revision, 3);
			assert.equal((await store.getRun(recorder.runId)).waits[key].status, "retired");
		} finally {
			await store.close();
		}
	});

	test(`${backend}: a lost write reply returns the reconciled original run receipt`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-write-reconcile-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const store = await openStoreSession({ directory, backend, history: true });
		try {
			const recorder = await store.createRun("fixture");
			const write = store.backend.write.bind(store.backend);
			let operationId: string;
			const fault = t.mock.method(store.backend, "write", async (...args) => {
				const committed = await write(...args);
				assert.ok(committed.kind === "run");
				operationId = committed.operationId;
				throw new PersistenceOutcomeUnknown(
					operationId,
					new Error("fixture lost reply"),
					committed.runId,
				);
			});
			const key = `pipeline_resume_${randomUUID()}`;
			const pending = JSON.stringify({ pipeline: [], items: [], resumeAtIndex: 0 });
			assert.deepEqual(await store.write(key, pending, { runId: recorder.runId }), {
				status: "committed",
				kind: "run",
				runId: recorder.runId,
				operationId,
				revision: 2,
				confirmation: "reconciled",
				signalAbortedAfterCommit: false,
			});
			assert.equal(fault.mock.callCount(), 1);
			assert.equal((await store.getRun(recorder.runId)).revision, 2);
			assert.equal((await store.reconcile(recorder.runId, operationId)).acknowledged, true);
		} finally {
			await store.close();
		}
	});
}

test("selected file sessions preserve legacy publication hooks and cancellation results", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-file-publication-hooks-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const store = await openStoreSession({ directory, backend: "files", history: true });
	try {
		const controller = new AbortController();
		let publications = 0;
		const result = await writeStateJson({
			env: bindStoreSession(process.env, store),
			key: "fixture",
			value: { value: 42 },
			signal: controller.signal,
			atomicWriteOptions: {
				async renameFile(from, to) {
					await fs.rename(from, to);
					publications++;
					controller.abort(new Error("fixture cancelled after commit"));
				},
			},
		});
		assert.equal(publications, 1);
		assert.deepEqual(result, { signalAbortedAfterCommit: true });
		assert.deepEqual(JSON.parse((await store.read("fixture")).text), { value: 42 });
	} finally {
		await store.close();
	}
});

test("SQLite sessions keep filesystem publication functions out of worker messages", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-sqlite-hook-boundary-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const store = await openStoreSession({ directory, backend: "sqlite" });
	try {
		const result = await writeStateJson({
			env: bindStoreSession(process.env, store),
			key: "fixture",
			value: 42,
			atomicWriteOptions: {
				async renameFile() {
					assert.fail("SQLite must not call filesystem publication hooks");
				},
			},
		});
		assert.deepEqual(result, { signalAbortedAfterCommit: false });
		assert.equal(JSON.parse((await store.read("fixture")).text), 42);
	} finally {
		await store.close();
	}
});

for (const backend of ["files", "sqlite"] as const) {
	test(`${backend}: actual workflow pause/reopen/resume keeps run and attempts`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-persistence-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const filePath = path.join(directory, "workflow.lobster");
		await fs.writeFile(
			filePath,
			JSON.stringify({
				steps: [
					{ id: "fetch", run: 'node -e "console.log(JSON.stringify({ok:1}))"' },
					{ id: "review", approval: "Test fixture approval", stdin: "$fetch.json" },
					{ id: "finish", run: 'node -e "console.log(JSON.stringify({done:true}))"' },
				],
			}),
		);
		const env = {
			...process.env,
			LOBSTER_STATE_DIR: path.join(directory, "state"),
			LOBSTER_BACKEND: backend,
			LOBSTER_HISTORY: "1",
		};
		const first = await runToolRequest({ filePath, ctx: { env } });
		assert.equal(first.status, "needs_approval", JSON.stringify(first));
		const session = await openStoreSession({ env });
		const listing = await session.listRuns();
		assert.equal(listing.runs.length, 1);
		const id = listing.runs[0].id;
		const key = decodeResumeToken(first.requiresApproval!.resumeToken!).stateKey;
		assert.equal((await session.read(key)).runId, id);
		assert.equal((await session.getRun(id)).status, "waiting");
		await session.close();
		const resumed = await resumeToolRequest({
			token: first.requiresApproval!.resumeToken!,
			approved: true,
			ctx: { env },
		});
		assert.equal(resumed.status, "ok", JSON.stringify(resumed));
		const reopened = await openStoreSession({ env });
		try {
			const run = await reopened.getRun(id);
			assert.equal(run.status, "completed");
			assert.equal((await reopened.read(key)).text, null);
			assert.deepEqual(
				run.attempts.map((a) => a.name),
				["fetch", "review", "finish"],
			);
			assert.ok(
				run.events.some((event) => event.kind === "state" && event.payload?.status === "consumed"),
			);
			assert.equal((await reopened.listRuns()).runs.length, 1);
		} finally {
			await reopened.close();
		}
		const stale = await resumeToolRequest({
			token: first.requiresApproval!.resumeToken!,
			approved: true,
			ctx: { env },
		});
		assert.equal(stale.ok, false);
	});
}

for (const backend of ["files", "sqlite"] as const) {
	test(`${backend}: different processes compete for one continuation`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-persistence-process-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const filePath = path.join(directory, "flow.lobster");
		const effect = path.join(directory, "effects.txt");
		await fs.writeFile(
			filePath,
			JSON.stringify({
				steps: [
					{ id: "review", approval: "Fixture approval" },
					{
						id: "effect",
						run: "node -e \"require('fs').appendFileSync(process.env.EFFECT_FILE,'one\\n');console.log(1)\"",
					},
				],
			}),
		);
		const env = {
			...process.env,
			EFFECT_FILE: effect,
			LOBSTER_STATE_DIR: path.join(directory, "state"),
			LOBSTER_BACKEND: backend,
			LOBSTER_HISTORY: "1",
		};
		const first = await processCall("run", filePath, env);
		assert.equal(first.status, "needs_approval", JSON.stringify(first));
		const results = await Promise.all([
			processCall("resume", first.requiresApproval.resumeToken, env),
			processCall("resume", first.requiresApproval.resumeToken, env),
		]);
		assert.equal(results.filter((result) => result.ok).length, 1, JSON.stringify(results));
		assert.equal(await fs.readFile(effect, "utf8"), "one\n");
		const session = await openStoreSession({ env });
		try {
			const { runs } = await session.listRuns();
			assert.equal(runs.length, 1);
			assert.equal((await session.getRun(runs[0].id)).status, "completed");
		} finally {
			await session.close();
		}
	});
}

test("legacy pending and consumed state survives interrupted import and activation", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-persistence-migrate-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "flow.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{ id: "review", approval: "Fixture" },
				{ id: "finish", run: 'node -e "console.log(42)"' },
			],
		}),
	);
	const env = { ...process.env, LOBSTER_STATE_DIR: path.join(directory, "state") };
	const first = await runToolRequest({ filePath, ctx: { env } });
	assert.equal(first.status, "needs_approval");
	await fs.writeFile(
		path.join(env.LOBSTER_STATE_DIR, "pipeline_resume_consumed.json"),
		JSON.stringify({
			type: "lobster.consumed-resume-state.v1",
			consumedAt: new Date().toISOString(),
			claimId: "old",
		}),
	);
	await assert.rejects(
		migrateStore({
			directory: env.LOBSTER_STATE_DIR,
			backend: "sqlite",
			offline: true,
			onPhase: async (phase) => {
				if (phase === "prepared") {
					throw new Error("injected interruption");
				}
			},
		}),
		/injected/,
	);
	const stillFiles = await openStoreSession({ env });
	assert.equal(stillFiles.backend.kind, "files");
	await stillFiles.close();
	await migrateStore({ directory: env.LOBSTER_STATE_DIR, backend: "sqlite", offline: true });
	const migratedEnv = { ...env, LOBSTER_HISTORY: "1" };
	const resumed = await resumeToolRequest({
		approvalId: first.requiresApproval.approvalId,
		approved: true,
		ctx: { env: migratedEnv },
	});
	assert.equal(resumed.status, "ok", JSON.stringify(resumed));
	const session = await openStoreSession({ env: migratedEnv });
	try {
		assert.equal(JSON.parse((await session.read("pipeline_resume_consumed")).text).claimId, "old");
		const { runs } = await session.listRuns();
		assert.equal(runs[0].partialHistory, true);
		await assert.rejects(
			migrateStore({ directory: env.LOBSTER_STATE_DIR, backend: "files", offline: true }),
			/Close all/,
		);
	} finally {
		await session.close();
	}
	await migrateStore({ directory: env.LOBSTER_STATE_DIR, backend: "files", offline: true });
	const roundtrip = await openStoreSession({ env: migratedEnv });
	try {
		assert.equal((await roundtrip.listRuns()).runs[0].status, "completed");
	} finally {
		await roundtrip.close();
	}
});

for (const backend of ["files", "sqlite"] as const) {
	test(`${backend}: lazy failure and early closure are truthful without draining`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-history-stream-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const env = {
			...process.env,
			LOBSTER_STATE_DIR: directory,
			LOBSTER_BACKEND: backend,
			LOBSTER_HISTORY: "1",
		};
		let produced = 0;
		const registry = {
			get(name: string) {
				return name === "source"
					? {
							async run() {
								return {
									output: (async function* () {
										for (let n = 0; n < 10; n++) {
											produced++;
											yield n;
										}
										throw new Error("lazy failure");
									})(),
								};
							},
						}
					: {
							async run({ input }) {
								for await (const item of input) {
									return { output: [item] };
								}
								return { output: [] };
							},
						};
			},
		};
		const closed = await runToolRequest({ pipeline: "source | first", ctx: { env, registry } });
		assert.equal(closed.ok, true);
		assert.equal(produced, 1);
		const failed = await runToolRequest({ pipeline: "source", ctx: { env, registry } });
		assert.equal(failed.ok, false);
		const session = await openStoreSession({ env });
		try {
			const runs = await session.backend.listRuns();
			assert.ok(
				runs.some(
					(run) =>
						run.status === "completed" &&
						run.attempts.some((a) => a.name === "source" && a.status === "closed"),
				),
			);
			assert.ok(
				runs.some(
					(run) => run.status === "failed" && run.attempts.some((a) => a.status === "failed"),
				),
			);
		} finally {
			await session.close();
		}
	});
	test(`${backend}: SDK tracked gates retain identity and supplied closures`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-history-sdk-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const env = {
			...process.env,
			LOBSTER_STATE_DIR: directory,
			LOBSTER_BACKEND: backend,
			LOBSTER_HISTORY: "1",
		};
		const lobster = new Lobster({ env })
			.pipe(() => [1])
			.pipe({
				async run({ input }) {
					const items = [];
					for await (const item of input) {
						items.push(item);
					}
					return { halt: true, output: [{ type: "approval_request", prompt: "Fixture", items }] };
				},
			})
			.pipe((items) => items.map((n) => n + 1));
		const first = await lobster.run();
		assert.equal(first.status, "needs_approval");
		const resumed = await lobster.resume(first.requiresApproval.resumeToken, { approved: true });
		assert.equal(resumed.status, "ok", JSON.stringify(resumed));
		assert.deepEqual(resumed.output, [2]);
		await assert.rejects(
			lobster.resume(first.requiresApproval.resumeToken, { approved: true }),
			/not found/,
		);
		const session = await openStoreSession({ env });
		try {
			const { runs } = await session.listRuns();
			assert.equal(runs.length, 1);
			assert.equal(runs[0].status, "completed");
			const run = await session.getRun(runs[0].id);
			assert.deepEqual(
				run.attempts.map((a) => a.ordinal),
				[0, 1, 2],
			);
		} finally {
			await session.close();
		}
	});
	test(`${backend}: asynchronous paired cache publication rolls state back under the shared barrier`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-history-cache-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const session = await openStoreSession({ directory, backend });
		const env = bindStoreSession(process.env, session);
		try {
			await diffAndStore({ env, key: "reuse", value: { old: true } });
			let release: () => void;
			let started: () => void;
			const entered = new Promise<void>((resolve) => {
				started = resolve;
			});
			const barrier = new Promise<void>((resolve) => {
				release = resolve;
			});
			const publication = diffAndStore({
				env,
				key: "reuse",
				value: { new: true },
				afterStore: async () => {
					started();
					await barrier;
					throw new Error("cache publication failed");
				},
			});
			await entered;
			const reader = readStateJsonWithLock({ env, key: "reuse" });
			release();
			await assert.rejects(publication, /cache publication failed/);
			assert.deepEqual(await reader, { old: true });
		} finally {
			await session.close();
		}
	});
}

for (const backend of ["files", "sqlite"] as const) {
	test(`${backend}: revisions, receipts, invalid transitions and retention`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-history-contract-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const session = await openStoreSession({ directory, backend, history: true });
		try {
			const recorder = await session.createRun("fixture");
			const operation = {
				operationId: randomUUID(),
				runId: recorder.runId,
				expectedRevision: 1,
				action: { kind: "definition" as const, fingerprint: "fixture" },
			};
			const committed = await session.backend.transition(operation);
			assert.deepEqual(committed, { status: "committed", revision: 2 });
			await session.acknowledge(recorder.runId, operation.operationId);
			assert.deepEqual(await session.backend.transition(operation), committed);
			assert.equal((await session.getRun(recorder.runId)).events.length, 2);
			assert.equal(
				(await session.backend.transition({ ...operation, operationId: randomUUID() })).status,
				"conflict",
			);
			await assert.rejects(
				session.backend.transition({
					operationId: randomUUID(),
					runId: recorder.runId,
					action: { kind: "finish", id: "missing", status: "completed" },
				}),
				/not active/,
			);
			assert.equal((await session.getRun(recorder.runId)).revision, 2);
			const before = "2099-01-01T00:00:00.000Z";
			assert.deepEqual(await session.retain({ before, dryRun: false }), []);
			await recorder.settle("completed");
			assert.deepEqual(await session.retain({ before }), [recorder.runId]);
			assert.ok(await session.getRun(recorder.runId));
			assert.deepEqual(await session.retain({ before, dryRun: false }), [recorder.runId]);
			assert.equal(await session.getRun(recorder.runId), null);
		} finally {
			await session.close();
		}
	});
	test(`${backend}: actual LLM cache and state publication retain reuse and provenance`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-history-llm-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const env = {
			...process.env,
			LOBSTER_STATE_DIR: path.join(directory, "state"),
			LOBSTER_CACHE_DIR: path.join(directory, "cache"),
			LOBSTER_BACKEND: backend,
			LOBSTER_HISTORY: "1",
			LOBSTER_LLM_MODEL: "test/model",
		};
		let calls = 0;
		const adapter = {
			source: "test",
			async invoke() {
				calls++;
				return {
					ok: true,
					result: {
						runId: "fixture",
						model: "test/model",
						status: "completed",
						output: {
							format: "json",
							text: '{"category":"support"}',
							data: { category: "support" },
						},
					},
				};
			},
		};
		const pipeline =
			'llm.invoke --provider pi --prompt "Fixture" | state.set result | state.get result';
		const first = await runToolRequest({ pipeline, ctx: { env, llmAdapters: { pi: adapter } } });
		assert.equal(first.ok, true, JSON.stringify(first));
		const second = await runToolRequest({ pipeline, ctx: { env, llmAdapters: { pi: adapter } } });
		assert.equal(second.ok, true, JSON.stringify(second));
		assert.equal(calls, 1);
		assert.equal(
			(second.output[0] as { output: { data: { category: string } } }).output.data.category,
			"support",
		);
	});
}

test("SQLite lost acknowledgement reconciles a receipt after real worker termination", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-history-worker-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const session = await openStoreSession({ directory, backend: "sqlite", history: true });
	try {
		const recorder = await session.createRun("fixture");
		const operation = {
			operationId: randomUUID(),
			runId: recorder.runId,
			expectedRevision: 1,
			action: { kind: "definition" as const, fingerprint: "committed" },
		};
		const worker = (session.backend as unknown as { worker: Worker }).worker;
		// Intercept an actual post-COMMIT reply before the caller's acknowledgement handler.
		worker.removeAllListeners("message");
		worker.once("message", () => {
			void worker.terminate();
		});
		await assert.rejects(
			session.backend.transition(operation),
			(error) =>
				error instanceof PersistenceOutcomeUnknown &&
				error.operationId === operation.operationId &&
				error.runId === recorder.runId,
		);
		const receipt = await session.reconcile(recorder.runId, operation.operationId);
		assert.equal(receipt.revision, 2);
		assert.deepEqual(await session.backend.transition(operation), {
			status: "committed",
			revision: 2,
		});
		assert.equal((await session.getRun(recorder.runId)).events.length, 2);
	} finally {
		await session.close();
	}
});

for (const backend of ["files", "sqlite"] as const) {
	test(`${backend}: retries, input validation, cancellation and nested occurrences`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-history-behaviors-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const env = {
			...process.env,
			LOBSTER_STATE_DIR: path.join(directory, "state"),
			LOBSTER_BACKEND: backend,
			LOBSTER_HISTORY: "1",
		};
		let tries = 0;
		const registry = {
			get() {
				return {
					async run() {
						if (++tries === 1) {
							throw new Error("retry fixture");
						}
						return { output: [1] };
					},
				};
			},
		};
		const filePath = path.join(directory, "flow.lobster");
		await fs.writeFile(
			filePath,
			JSON.stringify({
				steps: [
					{ id: "retry", pipeline: "flaky", retry: { max: 2, delay_ms: 1 } },
					{ id: "source", run: 'node -e "console.log(JSON.stringify([1,2]))"' },
					{
						id: "loop",
						for_each: "$source.json",
						steps: [{ id: "member", run: 'node -e "console.log(1)"' }],
					},
					{
						id: "branches",
						parallel: {
							branches: [
								{ id: "a", run: 'node -e "console.log(1)"' },
								{ id: "b", run: 'node -e "console.log(2)"' },
							],
						},
					},
					{ id: "answer", input: { prompt: "Fixture", responseSchema: { type: "integer" } } },
					{ id: "finish", run: 'node -e "console.log(3)"' },
				],
			}),
		);
		const first = await runToolRequest({ filePath, ctx: { env, registry } });
		assert.equal(first.status, "needs_input", JSON.stringify(first));
		const invalid = await resumeToolRequest({
			token: first.requiresInput.resumeToken,
			response: "wrong",
			ctx: { env, registry },
		});
		assert.equal(invalid.ok, false);
		const session = await openStoreSession({ env });
		const { runs } = await session.listRuns();
		const run = await session.getRun(runs[0].id);
		assert.equal(run.status, "waiting");
		assert.deepEqual(
			run.attempts.filter((a) => a.name === "retry").map((a) => a.status),
			["failed", "completed"],
		);
		assert.deepEqual(
			run.attempts.filter((a) => a.name === "member").map((a) => a.ordinal),
			[0, 1],
		);
		assert.equal(run.attempts.filter((a) => a.path.includes("/branch/")).length, 2);
		const page = await session.inspect(run.id, { limit: 2 });
		assert.equal(page.events.length, 2);
		assert.ok(!("states" in page));
		const aborted = new AbortController();
		aborted.abort();
		const cancelled = await resumeToolRequest({
			token: first.requiresInput.resumeToken,
			response: 4,
			ctx: { env, registry, signal: aborted.signal },
		}).catch(() => ({ ok: false }));
		assert.equal(cancelled.ok, false);
		assert.equal((await session.getRun(run.id)).status, "waiting");
		await session.close();
		const resumed = await resumeToolRequest({
			token: first.requiresInput.resumeToken,
			response: 4,
			ctx: { env, registry },
		});
		assert.equal(resumed.status, "ok", JSON.stringify(resumed));
	});
}

for (const backend of ["files", "sqlite"] as const) {
	test(`${backend}: retention protects an unacknowledged terminal receipt`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-retention-receipt-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const session = await openStoreSession({ directory, backend, history: true });
		try {
			const recorder = await session.createRun("fixture");
			const operationId = randomUUID();
			await session.backend.transition({
				operationId,
				runId: recorder.runId,
				action: { kind: "settle", status: "completed" },
			});
			assert.deepEqual(await session.retain({ before: "2099-01-01", dryRun: false }), []);
			await session.acknowledge(recorder.runId, operationId);
			assert.deepEqual(await session.retain({ before: "2099-01-01", dryRun: false }), [
				recorder.runId,
			]);
		} finally {
			await session.close();
		}
	});
}

test("SQLite admission is bounded while a database lock leaves the caller responsive", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-admission-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const session = await openStoreSession({
		directory,
		backend: "sqlite",
		history: true,
		maxPending: 1,
	});
	try {
		const recorder = await session.createRun("fixture");
		const child = spawn(
			process.execPath,
			[
				fileURLToPath(new URL("./fixtures/sqlite-lock.js", import.meta.url)),
				path.join(directory, "lobster.sqlite"),
			],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		const exited = new Promise<void>((resolve, reject) => {
			child.on("error", reject);
			child.on("exit", (code) =>
				code === 0 ? resolve() : reject(new Error("lock fixture failed")),
			);
		});
		await new Promise<void>((resolve, reject) => {
			child.stdout.once("data", () => resolve());
			child.once("error", reject);
		});
		let ticks = 0;
		const timer = setInterval(() => ticks++, 5);
		try {
			const operationId = randomUUID();
			const pending = session.backend.transition({
				operationId,
				runId: recorder.runId,
				action: { kind: "definition", fingerprint: "held" },
			});
			await new Promise((resolve) => setImmediate(resolve));
			await assert.rejects(
				session.backend.transition({
					operationId: randomUUID(),
					runId: recorder.runId,
					action: { kind: "definition", fingerprint: "overflow" },
				}),
				/admission limit/,
			);
			await pending;
			assert.ok(ticks >= 5, `only ${ticks} caller ticks`);
			await session.acknowledge(recorder.runId, operationId);
		} finally {
			clearInterval(timer);
			await exited;
		}
	} finally {
		await session.close();
	}
});

test("SQLite worker death before dispatch leaves no committed operation receipt", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-worker-before-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const session = await openStoreSession({ directory, backend: "sqlite", history: true });
	try {
		const recorder = await session.createRun("fixture");
		const operationId = randomUUID();
		const worker = (session.backend as unknown as { worker: Worker }).worker;
		worker.postMessage = () => {
			void worker.terminate();
		};
		await assert.rejects(
			session.backend.transition({
				operationId,
				runId: recorder.runId,
				action: { kind: "definition", fingerprint: "never" },
			}),
			PersistenceOutcomeUnknown,
		);
		assert.equal(await session.reconcile(recorder.runId, operationId), null);
		assert.equal((await session.getRun(recorder.runId)).revision, 1);
	} finally {
		await session.close();
	}
});

for (const backend of ["files", "sqlite"] as const) {
	test(`${backend}: cancellation during receipt acknowledgement restores the pre-dispatch wait`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-ack-cancel-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const session = await openStoreSession({ directory, backend, history: true });
		try {
			let effects = 0;
			const registry = {
				get(name: string) {
					return name === "gate"
						? {
								async run() {
									return {
										halt: true,
										output: [{ type: "approval_request", prompt: "Fixture", items: [] }],
									};
								},
							}
						: {
								async run() {
									effects++;
									return { output: [1] };
								},
							};
				},
			};
			const first = await runToolRequest({
				pipeline: "gate | effect",
				ctx: { storeSession: session, registry },
			});
			assert.equal(first.status, "needs_approval", JSON.stringify(first));
			const controller = new AbortController();
			const acknowledge = session.backend.acknowledge.bind(session.backend);
			session.backend.acknowledge = async (runId, operationId) => {
				await acknowledge(runId, operationId);
				const run = await session.getRun(runId);
				if (
					!controller.signal.aborted &&
					Object.values(run.waits).some((wait) => wait.status === "consumed")
				) {
					controller.abort(new Error("fixture cancel after receipt"));
				}
			};
			const stopped = await resumeToolRequest({
				token: first.requiresApproval.resumeToken,
				approved: true,
				ctx: { storeSession: session, registry, signal: controller.signal },
			}).catch(() => ({ ok: false }));
			assert.equal(stopped.ok, false);
			assert.equal(effects, 0);
			assert.equal((await session.listRuns()).runs[0].status, "waiting");
			const resumed = await resumeToolRequest({
				token: first.requiresApproval.resumeToken,
				approved: true,
				ctx: { storeSession: session, registry },
			});
			assert.equal(resumed.status, "ok", JSON.stringify(resumed));
			assert.equal(effects, 1);
		} finally {
			await session.close();
		}
	});
}

for (const phase of ["backed_up", "imported", "verified", "prepared", "activated"] as const) {
	test(`migration interrupted at ${phase} keeps exactly one authority`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-migration-phase-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		await fs.writeFile(path.join(directory, "legacy.json"), '{"version":7}\n');
		await assert.rejects(
			migrateStore({
				directory,
				backend: "sqlite",
				offline: true,
				onPhase: async (current) => {
					if (current === phase) {
						throw new Error("fixture interruption");
					}
				},
			}),
			/fixture interruption/,
		);
		const selected = await openStoreSession({ directory });
		try {
			assert.equal(selected.backend.kind, phase === "activated" ? "sqlite" : "files");
			assert.deepEqual(JSON.parse((await selected.read("legacy")).text), { version: 7 });
		} finally {
			await selected.close();
		}
		await migrateStore({ directory, backend: "sqlite", offline: true });
		const retried = await openStoreSession({ directory });
		try {
			assert.equal(retried.backend.kind, "sqlite");
			assert.deepEqual(JSON.parse((await retried.read("legacy")).text), { version: 7 });
		} finally {
			await retried.close();
		}
	});
}

test("corrupt source cannot activate a SQLite migration", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-migration-corrupt-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	await fs.writeFile(path.join(directory, "broken.json"), "{invalid");
	await assert.rejects(migrateStore({ directory, backend: "sqlite", offline: true }), SyntaxError);
	await assert.rejects(fs.access(path.join(directory, ".backend")), { code: "ENOENT" });
});

for (const backend of ["files", "sqlite"] as const) {
	test(`${backend}: tracked waits survive history being disabled and inspection is read-only`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-history-disabled-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const env = {
			...process.env,
			LOBSTER_STATE_DIR: directory,
			LOBSTER_BACKEND: backend,
			LOBSTER_HISTORY: "1",
		};
		const first = await runToolRequest({ pipeline: 'approve --prompt "Fixture"', ctx: { env } });
		assert.equal(first.status, "needs_approval");
		assert.ok(first.runId);
		const readOnly = await openStoreSession({ directory, readOnly: true });
		try {
			assert.equal((await readOnly.inspect(first.runId)).status, "waiting");
			await assert.rejects(readOnly.write("forbidden", "1"), /read-only/);
		} finally {
			await readOnly.close();
		}
		const resumed = await resumeToolRequest({
			token: first.requiresApproval.resumeToken,
			approved: true,
			ctx: { env: { ...env, LOBSTER_HISTORY: "0" } },
		});
		assert.equal(resumed.status, "ok", JSON.stringify(resumed));
		assert.equal(resumed.runId, first.runId);
	});
	test(`${backend}: SDK same-stage input remains pending after invalid response`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-sdk-input-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const lobster = new Lobster({
			env: { ...process.env, LOBSTER_STATE_DIR: directory, LOBSTER_BACKEND: backend },
			history: true,
		}).pipe({
			async run({ ctx }) {
				const response = await ctx.requestInput({
					prompt: "Fixture",
					responseSchema: { type: "integer" },
				});
				return { output: [response] };
			},
		});
		const first = await lobster.run();
		assert.equal(first.status, "needs_input", JSON.stringify(first));
		await assert.rejects(
			lobster.resume(first.requiresInput.resumeToken, { response: "invalid" }),
			/schema/,
		);
		const resumed = await lobster.resume(first.requiresInput.resumeToken, { response: 9 });
		assert.equal(resumed.status, "ok", JSON.stringify(resumed));
		assert.deepEqual(resumed.output, [9]);
	});
}

for (const backend of ["files", "sqlite"] as const) {
	test(`${backend}: CLI records history and dry-run does not initialize storage`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-cli-history-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const file = path.join(directory, "flow.lobster");
		await fs.writeFile(
			file,
			JSON.stringify({ steps: [{ id: "print", run: 'node -e "console.log(42)"' }] }),
		);
		const env = {
			...process.env,
			LOBSTER_STATE_DIR: path.join(directory, "state"),
			LOBSTER_BACKEND: backend,
			LOBSTER_HISTORY: "1",
		};
		const bin = fileURLToPath(new URL("../../bin/lobster.js", import.meta.url));
		const dry = await exec(
			process.execPath,
			[bin, "run", "--mode", "tool", "--dry-run", "--file", file],
			{ env },
		);
		assert.equal(JSON.parse(dry.stdout).status, "ok");
		await assert.rejects(fs.access(env.LOBSTER_STATE_DIR), { code: "ENOENT" });
		const actual = await exec(process.execPath, [bin, "run", "--mode", "tool", "--file", file], {
			env,
		});
		const envelope = JSON.parse(actual.stdout);
		assert.equal(envelope.status, "ok");
		assert.ok(envelope.runId);
		const shown = await exec(process.execPath, [bin, "history", "show", envelope.runId], { env });
		assert.equal(JSON.parse(shown.stdout).status, "completed");
	});
	test(`${backend}: CLI emits one error envelope when the terminal history reply is lost`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-cli-history-fault-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const file = path.join(directory, "flow.lobster");
		await fs.writeFile(
			file,
			JSON.stringify({ steps: [{ id: "print", run: 'node -e "console.log(42)"' }] }),
		);
		const env = {
			...process.env,
			LOBSTER_STATE_DIR: path.join(directory, "state"),
			LOBSTER_BACKEND: backend,
			LOBSTER_HISTORY: "1",
		};
		const fixture = fileURLToPath(new URL("./fixtures/persistence-process.js", import.meta.url));
		const error: ExecFileException & { stdout: string } = await exec(
			process.execPath,
			[fixture, "cli-fault", file],
			{ env },
		).then(
			() => assert.fail("fixture should fail"),
			(error) => error,
		);
		assert.equal(error.code, 1);
		// Parsing the whole output rejects concatenated success and error envelopes.
		const envelope = JSON.parse(error.stdout);
		assert.equal(envelope.ok, false);
		assert.equal(envelope.error.type, "persistence_outcome_unknown");
		const store = await openStoreSession({ env });
		try {
			const receipt = await store.reconcile(envelope.error.runId, envelope.error.operationId);
			assert.equal(receipt.acknowledged, false);
			assert.equal((await store.getRun(envelope.error.runId)).status, "completed");
			assert.deepEqual(await store.retain({ before: "2099-01-01T00:00:00Z", dryRun: false }), []);
		} finally {
			await store.close();
		}
	});
}

test("injected sessions keep nested roots in the explicit storage scope", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-scope-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const first = await openStoreSession({ directory: path.join(directory, "one"), history: true });
	const second = await openStoreSession({
		directory: path.join(directory, "two"),
		backend: "sqlite",
		history: true,
	});
	try {
		const childRegistry = {
			get() {
				return {
					async run() {
						return { output: [2] };
					},
				};
			},
		};
		const registry = {
			get() {
				return {
					async run({ ctx }) {
						const child = await runToolRequest({
							pipeline: "child",
							ctx: { env: ctx.env, storeSession: second, registry: childRegistry },
						});
						assert.equal(child.ok, true);
						return { output: child.output };
					},
				};
			},
		};
		const result = await runToolRequest({
			pipeline: "parent",
			ctx: { storeSession: first, registry },
		});
		assert.equal(result.ok, true, JSON.stringify(result));
		const one = await first.listRuns();
		const two = await second.listRuns();
		assert.equal(one.runs.length, 1);
		assert.equal(two.runs.length, 1);
		assert.notEqual(one.runs[0].id, two.runs[0].id);
	} finally {
		await first.close();
		await second.close();
	}
});

for (const backend of ["files", "sqlite"] as const) {
	test(`${backend}: close waits for admitted writes and keeps migration fenced`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-close-drain-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const store = await openStoreSession({ directory, backend });
		let release: () => void;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		const originalWrite = store.backend.write.bind(store.backend);
		t.mock.method(store.backend, "write", async (...args) => {
			await gate;
			return originalWrite(...args);
		});
		const write = store.write("fixture", "42");
		let closed = false;
		const closing = store.close().then(() => {
			closed = true;
		});
		try {
			await new Promise((resolve) => setImmediate(resolve));
			assert.equal(closed, false);
			await assert.rejects(
				migrateStore({
					directory,
					backend: backend === "files" ? "sqlite" : "files",
					offline: true,
				}),
				/Close all persistence sessions/,
			);
		} finally {
			release();
			await write;
			await closing;
		}
		const reopened = await openStoreSession({ directory });
		try {
			assert.equal((await reopened.read("fixture")).text, "42");
		} finally {
			await reopened.close();
		}
	});
	test(`${backend}: closing a session rejects new history and state operations`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-session-close-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const session = await openStoreSession({ directory, backend, history: true });
		const recorder = await session.createRun("fixture");
		await session.close();
		await assert.rejects(session.read("fixture"), /closed/);
		await assert.rejects(session.listRuns(), /closed/);
		await assert.rejects(session.createRun("fixture"), /closed/);
		await assert.rejects(recorder.settle("completed"), /closed/);
		const reopened = await openStoreSession({ directory });
		try {
			assert.equal((await reopened.getRun(recorder.runId)).status, "running");
		} finally {
			await reopened.close();
		}
	});
}

test("SQLite opening failure closes its connection and permits a clean retry", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-session-open-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const { SqliteBackend } = await import("../src/persistence/sqlite.js");
	const originalWrite = fs.writeFile;
	const close = t.mock.method(SqliteBackend.prototype, "close");
	const write = t.mock.method(fs, "writeFile", async (...args: Parameters<typeof fs.writeFile>) => {
		if (String(args[0]).includes(".session-")) {
			throw new Error("fixture lease failure");
		}
		return originalWrite(...args);
	});
	await assert.rejects(openStoreSession({ directory, backend: "sqlite" }), /fixture lease failure/);
	assert.equal(close.mock.callCount(), 1);
	write.mock.restore();
	close.mock.restore();
	const retried = await openStoreSession({ directory });
	try {
		assert.equal(retried.backend.kind, "sqlite");
		await retried.write("fixture", "42");
		assert.equal((await retried.read("fixture")).text, "42");
	} finally {
		await retried.close();
	}
});

test("SQLite rejects an undispatchable request without occupying admission", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-worker-dispatch-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const store = await openStoreSession({ directory, backend: "sqlite", maxPending: 1 });
	try {
		// Extra uncloneable data deliberately exercises failure before worker admission.
		const request = { key: "fixture", invalid: () => {} };
		await assert.rejects((store.backend as SqliteBackend).request("read", request), {
			name: "DataCloneError",
		});
		await store.write("fixture", "42");
		assert.equal((await store.read("fixture")).text, "42");
	} finally {
		await store.close();
	}
});

test("SQLite cannot reconcile another writer's identical exclusive value as its own insert", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-exclusive-reconcile-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const store = await openStoreSession({ directory, backend: "sqlite" });
	try {
		await store.write("fixture", "42");
		t.mock.method(store.backend, "write", async () => {
			throw new PersistenceOutcomeUnknown("fixture-lost");
		});
		await assert.rejects(
			store.write("fixture", "42", { exclusive: true }),
			PersistenceOutcomeUnknown,
		);
		assert.equal((await store.read("fixture")).text, "42");
	} finally {
		await store.close();
	}
});

for (const backend of ["files", "sqlite"] as const) {
	test(`${backend}: a shared session isolates 50 concurrent roots and paginates their history`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-concurrent-history-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const store = await openStoreSession({ directory, backend, history: true });
		const registry = {
			get() {
				return {
					async run({ ctx }) {
						return { output: [ctx.env.FIXTURE_ROOT] };
					},
				};
			},
		};
		try {
			const results = await Promise.all(
				Array.from({ length: 50 }, (_, index) =>
					runToolRequest({
						pipeline: "first | second",
						ctx: {
							storeSession: store,
							registry,
							env: { ...process.env, FIXTURE_ROOT: String(index) },
							runMetadata: { hostRunId: String(index) },
						},
					}),
				),
			);
			assert.equal(new Set(results.map((result) => result.runId)).size, 50);
			for (let index = 0; index < results.length; index++) {
				assert.equal(results[index].ok, true, JSON.stringify(results[index]));
				assert.deepEqual(results[index].output, [String(index)]);
				const run = await store.getRun(results[index].runId);
				assert.equal(run.status, "completed");
				assert.deepEqual(run.metadata, { hostRunId: String(index) });
				assert.deepEqual(
					run.attempts.map((a) => a.name),
					["first", "second"],
				);
			}
			const ids: string[] = [];
			let after = "";
			while (true) {
				const page = await store.listRuns({ after, limit: 7 });
				if (!page.runs.length) {
					break;
				}
				assert.ok(page.runs.length <= 7);
				ids.push(...page.runs.map((run) => run.id));
				after = page.nextCursor;
			}
			assert.deepEqual(ids, results.map((result) => result.runId).sort());
		} finally {
			await store.close();
		}
	});
	for (const unknown of [false, true]) {
		test(`${backend}: ${unknown ? "unknown" : "failed"} history write cannot retry an executed step`, async (t) => {
			const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-history-retry-"));
			t.after(() => fs.rm(directory, { recursive: true, force: true }));
			const store = await openStoreSession({
				directory: path.join(directory, "state"),
				backend,
				history: true,
			});
			const filePath = path.join(directory, "flow.lobster");
			const effects = path.join(directory, "effects");
			await fs.writeFile(
				filePath,
				JSON.stringify({
					steps: [
						{
							id: "effect",
							run: "node -e \"require('fs').appendFileSync(process.env.FIXTURE_EFFECTS,'x');console.log(1)\"",
							retry: { max: 3, delay_ms: 1 },
							on_error: "continue",
						},
						{
							id: "successor",
							run: "node -e \"require('fs').appendFileSync(process.env.FIXTURE_EFFECTS,'y');console.log(2)\"",
						},
					],
				}),
			);
			const transition = store.backend.transition.bind(store.backend);
			let failedOperation: Transition | undefined;
			t.mock.method(store.backend, "transition", async (request, signal) => {
				if (
					!failedOperation &&
					request.action.kind === "finish" &&
					request.action.status === "completed"
				) {
					failedOperation = request;
					if (unknown) {
						await transition(request, signal);
						throw new PersistenceOutcomeUnknown(
							request.operationId,
							new Error("fixture lost reply"),
							request.runId,
						);
					}
					throw new Error("fixture failed write");
				}
				return transition(request, signal);
			});
			try {
				const result = await runToolRequest({
					filePath,
					ctx: { storeSession: store, env: { ...process.env, FIXTURE_EFFECTS: effects } },
				});
				assert.equal(result.ok, false);
				assert.equal(
					result.error.type,
					unknown ? "persistence_outcome_unknown" : "persistence_error",
				);
				assert.equal(result.error.operationId, failedOperation.operationId);
				assert.equal(result.error.runId, failedOperation.runId);
				assert.equal(await fs.readFile(effects, "utf8"), "x");
				const run = await store.getRun(failedOperation.runId);
				assert.equal(run.status, unknown ? "running" : "failed");
				assert.equal(run.attempts.length, 1);
				assert.equal(run.attempts[0].status, unknown ? "completed" : "started");
				const receipt = await store.reconcile(run.id, failedOperation.operationId);
				assert.equal(Boolean(receipt), unknown);
				if (unknown) {
					assert.equal(receipt.acknowledged, false);
				}
			} finally {
				await store.close();
			}
		});
	}
	test(`${backend}: pipeline stages retain loop and branch structural identities`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-history-paths-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const store = await openStoreSession({
			directory: path.join(directory, "state"),
			backend,
			history: true,
		});
		const filePath = path.join(directory, "flow.lobster");
		await fs.writeFile(
			filePath,
			JSON.stringify({
				steps: [
					{ id: "source", pipeline: "items" },
					{ id: "loop", for_each: "$source.json", steps: [{ id: "member", pipeline: "child" }] },
					{
						id: "branches",
						parallel: {
							branches: [
								{ id: "a", pipeline: "child" },
								{ id: "b", pipeline: "child" },
							],
						},
					},
				],
			}),
		);
		const registry = {
			get(name) {
				return {
					async run() {
						return { output: name === "items" ? [1, 2] : [3] };
					},
				};
			},
		};
		try {
			const result = await runToolRequest({ filePath, ctx: { storeSession: store, registry } });
			assert.equal(result.ok, true, JSON.stringify(result));
			const run = await store.getRun(result.runId);
			const children = run.attempts.filter((a) => a.name === "child");
			assert.equal(children.length, 4);
			assert.equal(new Set(children.map((a) => a.path)).size, 4);
			assert.ok(children.some((a) => a.path.includes("step/1/item/0/")));
			assert.ok(children.some((a) => a.path.includes("step/1/item/1/")));
			assert.ok(children.some((a) => a.path.includes("step/2/branch/a/")));
			assert.ok(children.some((a) => a.path.includes("step/2/branch/b/")));
		} finally {
			await store.close();
		}
	});
	test(`${backend}: a resumed workflow pipeline keeps the original stage ordinal`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-history-ordinal-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const store = await openStoreSession({
			directory: path.join(directory, "state"),
			backend,
			history: true,
		});
		const filePath = path.join(directory, "flow.lobster");
		await fs.writeFile(
			filePath,
			JSON.stringify({ steps: [{ id: "flow", pipeline: "before | choose | after" }] }),
		);
		const registry = {
			get(name) {
				return {
					async run({ ctx, input }) {
						if (name === "choose") {
							return {
								output: [
									await ctx.requestInput({
										prompt: "Fixture",
										responseSchema: { type: "integer" },
									}),
								],
							};
						}
						return { output: name === "before" ? [1] : input };
					},
				};
			},
		};
		try {
			const first = await runToolRequest({ filePath, ctx: { storeSession: store, registry } });
			assert.equal(first.status, "needs_input", JSON.stringify(first));
			const resumed = await resumeToolRequest({
				token: first.requiresInput.resumeToken,
				response: 4,
				ctx: { storeSession: store, registry },
			});
			assert.equal(resumed.ok, true, JSON.stringify(resumed));
			assert.equal(resumed.runId, first.runId);
			const run = await store.getRun(first.runId);
			assert.deepEqual(
				run.attempts.filter((a) => a.name === "choose").map((a) => a.ordinal),
				[1, 1],
			);
			assert.equal(run.attempts.find((a) => a.name === "after").ordinal, 2);
		} finally {
			await store.close();
		}
	});
}
