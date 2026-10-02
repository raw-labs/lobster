import test from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runToolRequest, resumeToolRequest } from "../src/core/tool_runtime.js";
import { openStoreSession } from "../src/persistence/session.js";
import { decodeResumeToken } from "../src/resume.js";
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
				run.events.some(
					(event) => event.kind === "state" && (event.payload as any).status === "consumed",
				),
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

import { promisify } from "node:util";
import { execFile } from "node:child_process";
import { migrateStore } from "../src/persistence/migrate.js";
const exec = promisify(execFile);
async function processCall(mode: string, value: string, env: any) {
	const { stdout } = await exec(
		process.execPath,
		[new URL("./fixtures/persistence-process.js", import.meta.url).pathname, mode, value],
		{ env, timeout: 10000 },
	);
	return JSON.parse(stdout);
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
				{ id: "finish", run: "node -e 'console.log(42)'" },
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
				if (phase === "prepared") throw new Error("injected interruption");
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

import { Lobster } from "../src/sdk/Lobster.js";
import { bindStoreSession } from "../src/persistence/session.js";
import { diffAndStore, readStateJsonWithLock } from "../src/state/store.js";
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
								for await (const item of input) return { output: [item] };
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
					for await (const item of input) items.push(item);
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

import { randomUUID } from "node:crypto";
import { PersistenceOutcomeUnknown } from "../src/persistence/backend.js";
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
			assert.deepEqual(await session.backend.transition(operation), committed);
			assert.equal((await session.getRun(recorder.runId)).events.length, 2);
			assert.equal(
				(await session.backend.transition({ ...operation, operationId: randomUUID() })).conflict,
				true,
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
		assert.equal((second.output[0] as any).output.data.category, "support");
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
		const worker = (session.backend as any).worker;
		// Intercept an actual post-COMMIT reply before the caller's acknowledgement handler.
		worker.removeAllListeners("message");
		worker.once("message", () => {
			void worker.terminate();
		});
		await assert.rejects(
			session.backend.transition(operation),
			(error) =>
				error instanceof PersistenceOutcomeUnknown && error.operationId === operation.operationId,
		);
		const receipt = await session.reconcile(recorder.runId, operation.operationId);
		assert.equal(receipt.revision, 2);
		assert.deepEqual(await session.backend.transition(operation), { revision: 2 });
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
						if (++tries === 1) throw new Error("retry fixture");
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
						steps: [{ id: "member", run: "node -e 'console.log(1)'" }],
					},
					{
						id: "branches",
						parallel: {
							branches: [
								{ id: "a", run: "node -e 'console.log(1)'" },
								{ id: "b", run: "node -e 'console.log(2)'" },
							],
						},
					},
					{ id: "answer", input: { prompt: "Fixture", responseSchema: { type: "integer" } } },
					{ id: "finish", run: "node -e 'console.log(3)'" },
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
