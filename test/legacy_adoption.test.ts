import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import {
	adoptLegacyCheckpoint,
	createDefaultRegistry,
	decodeResumeToken,
	getRun,
	getWait,
	listLegacyCheckpoints,
	listRuns,
	migrateStore,
	openStoreSession,
	resumeToolRequest,
	runToolRequest,
} from "../src/core/index.js";
import { PersistenceOutcomeUnknown } from "../src/persistence/backend.js";
import { Lobster } from "../src/sdk/Lobster.js";
import { decodeToken } from "../src/token.js";

const exec = promisify(execFile);
const schema = { type: "integer" };

async function seed(directory: string, kind: "pipeline" | "workflow" = "pipeline") {
	const env = {
		...process.env,
		LOBSTER_STATE_DIR: directory,
		LOBSTER_RUNS: "0",
		LOBSTER_HISTORY: "0",
		LOBSTER_BACKEND: "files",
	};
	let prepared = 0;
	let finished = 0;
	const defaults = createDefaultRegistry();
	const registry = {
		get(name: string) {
			if (name === "prepare")
				return {
					async run() {
						prepared++;
						return { output: ["saved draft"] };
					},
				};
			if (name === "finish")
				return {
					async run() {
						finished++;
						return { output: ["done"] };
					},
				};
			return defaults.get(name);
		},
	};
	let filePath: string;
	if (kind === "workflow") {
		filePath = path.join(path.dirname(directory), "review.lobster");
		await fs.writeFile(
			filePath,
			JSON.stringify({
				steps: [
					{ id: "prepare", pipeline: "prepare" },
					{
						id: "review",
						input: { prompt: "Review", responseSchema: schema },
						stdin: "$prepare.json",
					},
					{ id: "finish", pipeline: "finish" },
				],
			}),
		);
	}
	const first = await runToolRequest({
		...(filePath
			? { filePath }
			: {
					pipeline: 'prepare | ask --emit --prompt Review --schema \'{"type":"integer"}\' | finish',
				}),
		ctx: { env, registry },
	});
	assert.equal(first.status, "needs_input", JSON.stringify(first));
	const checkpointId = decodeResumeToken(first.requiresInput.resumeToken).stateKey;
	const checkpointPath = path.join(directory, checkpointId + ".json");
	const legacy = JSON.parse(await fs.readFile(checkpointPath, "utf8"));
	delete legacy.wait;
	delete legacy.historyOffset;
	await fs.writeFile(checkpointPath, JSON.stringify(legacy) + "\n");
	return {
		env,
		registry,
		first,
		checkpointId,
		get prepared() {
			return prepared;
		},
		get finished() {
			return finished;
		},
	};
}

for (const backend of ["files", "sqlite"] as const) {
	for (const kind of ["pipeline", "workflow"] as const) {
		test(`${backend}: adopt a legacy ${kind} before its first resume and preserve its token`, async (t) => {
			const root = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-adoption-"));
			t.after(() => fs.rm(root, { recursive: true, force: true }));
			const directory = path.join(root, "state");
			const fixture = await seed(directory, kind);
			if (backend === "sqlite") await migrateStore({ directory, backend, offline: true });
			let store = await openStoreSession({ directory, backend, trackRuns: true });
			t.after(() => store.close());
			const inventory = await listLegacyCheckpoints({ ctx: { storeSession: store }, limit: 1 });
			assert.deepEqual(inventory.checkpoints, [{ checkpointId: fixture.checkpointId, kind }]);
			assert.deepEqual((await listRuns({ ctx: { storeSession: store } })).runs, []);
			const scoped = () => ({
				storeSession: store,
				registry: fixture.registry,
				runMetadata: { callerId: "owner-a" },
			});
			const adopted = await adoptLegacyCheckpoint({
				checkpointId: fixture.checkpointId,
				ctx: scoped(),
			});
			assert.equal(adopted.status, "waiting");
			assert.equal(adopted.partialHistory, true);
			assert.equal(adopted.historyEnabled, false);
			assert.equal(adopted.metadata.callerId, "owner-a");
			assert.equal(fixture.prepared, 1);
			assert.equal(fixture.finished, 0);
			assert.equal(
				(await adoptLegacyCheckpoint({ checkpointId: fixture.checkpointId, ctx: scoped() }))
					.revision,
				adopted.revision,
			);
			assert.deepEqual(
				(await listLegacyCheckpoints({ ctx: { storeSession: store } })).checkpoints,
				[],
			);
			await assert.rejects(
				adoptLegacyCheckpoint({
					checkpointId: fixture.checkpointId,
					ctx: { storeSession: store, runMetadata: { callerId: "owner-b" } },
				}),
				/Wait changed/,
			);
			await store.close();
			store = await openStoreSession({ directory });
			assert.equal((await listRuns({ status: "waiting", ctx: scoped() })).runs[0].id, adopted.id);
			assert.equal(
				(await listRuns({ ctx: { storeSession: store, runMetadata: { callerId: "owner-b" } } }))
					.runs.length,
				0,
			);
			const wait = await getWait({ waitId: adopted.pendingWaits[0].id, ctx: scoped() });
			assert.equal(wait.request === null, kind === "workflow");
			const resumed = await resumeToolRequest({
				token: fixture.first.requiresInput.resumeToken,
				response: 4,
				ctx: scoped(),
			});
			assert.equal(resumed.status, "ok", JSON.stringify(resumed));
			assert.equal(resumed.runId, adopted.id);
			assert.equal(fixture.prepared, 1);
			assert.equal(fixture.finished, 1);
			assert.equal((await getRun({ runId: adopted.id, ctx: scoped() })).status, "completed");
			await store.close();
			await migrateStore({
				directory,
				backend: backend === "files" ? "sqlite" : "files",
				offline: true,
			});
			store = await openStoreSession({ directory });
			assert.equal((await getRun({ runId: adopted.id, ctx: scoped() })).status, "completed");
			assert.equal(
				(
					await resumeToolRequest({
						token: fixture.first.requiresInput.resumeToken,
						response: 4,
						ctx: scoped(),
					})
				).ok,
				false,
			);
			assert.equal(fixture.finished, 1);
		});
	}

	test(`${backend}: competing processes adopt a legacy checkpoint exactly once`, async (t) => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-adoption-race-"));
		t.after(() => fs.rm(root, { recursive: true, force: true }));
		const directory = path.join(root, "state");
		const fixture = await seed(directory);
		if (backend === "sqlite") await migrateStore({ directory, backend, offline: true });
		const core = new URL("../src/core/index.js", import.meta.url).href;
		const results = await Promise.all(
			["owner-a", "owner-b"].map(async (callerId) => {
				const script = `const {adoptLegacyCheckpoint,openStoreSession}=await import(${JSON.stringify(core)}); const store=await openStoreSession({directory:${JSON.stringify(directory)}}); try {const run=await adoptLegacyCheckpoint({checkpointId:${JSON.stringify(fixture.checkpointId)},ctx:{storeSession:store,runMetadata:{callerId:${JSON.stringify(callerId)}}}}); console.log(JSON.stringify({ok:true,id:run.id}));} catch(error){console.log(JSON.stringify({ok:false,code:error.code}));} finally{await store.close();}`;
				const { stdout } = await exec(process.execPath, ["--input-type=module", "-e", script], {
					timeout: 15000,
				});
				return JSON.parse(stdout);
			}),
		);
		assert.equal(results.filter((result) => result.ok).length, 1);
		assert.equal(results.find((result) => !result.ok).code, "LOBSTER_WAIT_CONFLICT");
		const store = await openStoreSession({ directory });
		try {
			assert.equal((await store.listRuns()).runs.length, 1);
		} finally {
			await store.close();
		}
	});

	test(`${backend}: failed and uncertain adoption never lose or duplicate the checkpoint`, async (t) => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-adoption-outcome-"));
		t.after(() => fs.rm(root, { recursive: true, force: true }));
		const directory = path.join(root, "state");
		const fixture = await seed(directory);
		if (backend === "sqlite") await migrateStore({ directory, backend, offline: true });
		const store = await openStoreSession({ directory });
		t.after(() => store.close());
		const ctx = { storeSession: store, runMetadata: { callerId: "owner-a" } };
		const transition = store.backend.transition.bind(store.backend);
		store.backend.transition = async () => {
			throw new Error("Before commit");
		};
		await assert.rejects(
			adoptLegacyCheckpoint({ checkpointId: fixture.checkpointId, ctx }),
			/Before commit/,
		);
		assert.equal((await store.listRuns()).runs.length, 0);
		assert.equal(
			(await listLegacyCheckpoints({ ctx: { storeSession: store } })).checkpoints.length,
			1,
		);
		let operationId: string;
		store.backend.transition = async (...args) => {
			await transition(...args);
			operationId = args[0].operationId;
			throw new PersistenceOutcomeUnknown(operationId, new Error("Lost reply"), args[0].runId);
		};
		await assert.rejects(
			adoptLegacyCheckpoint({ checkpointId: fixture.checkpointId, ctx }),
			PersistenceOutcomeUnknown,
		);
		store.backend.transition = transition;
		const adopted = await adoptLegacyCheckpoint({ checkpointId: fixture.checkpointId, ctx });
		assert.equal((await store.listRuns()).runs.length, 1);
		assert.equal(adopted.pendingWaits.length, 1);
		assert.ok(await store.reconcile(adopted.id, operationId));
		assert.equal(await store.acknowledge(adopted.id, operationId), true);
	});

	test(`${backend}: adopted SDK command-input frames retain the legacy token and supplied stages`, async (t) => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-sdk-adoption-"));
		t.after(() => fs.rm(root, { recursive: true, force: true }));
		const directory = path.join(root, "state");
		const lobster = new Lobster({
			stateDir: directory,
			env: { ...process.env, LOBSTER_BACKEND: "files", LOBSTER_HISTORY: "0", LOBSTER_RUNS: "0" },
		}).pipe({
			async run({ ctx }) {
				const answer = await ctx.requestInput({
					prompt: "Saved SDK question",
					responseSchema: schema,
				});
				return { output: [answer] };
			},
		});
		const first = await lobster.run();
		assert.equal(first.status, "needs_input");
		const token = first.requiresInput.resumeToken;
		const checkpointId = decodeToken(token).stateKey;
		if (backend === "sqlite") await migrateStore({ directory, backend, offline: true });
		const store = await openStoreSession({ directory });
		t.after(() => store.close());
		const ctx = { storeSession: store, runMetadata: { callerId: "sdk-owner" } };
		const adopted = await adoptLegacyCheckpoint({ checkpointId, ctx });
		const wait = await getWait({ waitId: adopted.pendingWaits[0].id, ctx });
		assert.equal(wait.request.prompt, "Saved SDK question");
		const resumedSdk = new Lobster({ storeSession: store, runMetadata: ctx.runMetadata }).pipe({
			async run({ ctx }) {
				const answer = await ctx.requestInput({
					prompt: "Saved SDK question",
					responseSchema: schema,
				});
				return { output: [answer] };
			},
		});
		const resumed = await resumedSdk.resume(token, { response: 7 });
		assert.equal(resumed.ok, true, JSON.stringify(resumed));
		assert.deepEqual(resumed.output, [7]);
		assert.equal(resumed.runId, adopted.id);
		assert.equal((await getRun({ runId: adopted.id, ctx })).status, "completed");
		await assert.rejects(resumedSdk.resume(token, { response: 7 }), /not found/);
	});
}

for (const backend of ["files", "sqlite"] as const) {
	test(`${backend}: legacy inventory is bounded and adoption requires explicit trusted ownership`, async (t) => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-legacy-maintenance-"));
		t.after(() => fs.rm(root, { recursive: true, force: true }));
		const directory = path.join(root, "state");
		const fixture = await seed(directory);
		const another = await runToolRequest({
			pipeline: 'ask --emit --prompt Another --schema \'{"type":"integer"}\'',
			ctx: { env: fixture.env },
		});
		assert.equal(another.status, "needs_input");
		if (backend === "sqlite") await migrateStore({ directory, backend, offline: true });
		let store = await openStoreSession({ directory });
		t.after(() => store.close());
		const ctx = { storeSession: store };
		const first = await listLegacyCheckpoints({ ctx, limit: 1 });
		assert.equal(first.checkpoints.length, 1);
		assert.ok(first.nextCursor);
		const second = await listLegacyCheckpoints({ ctx, limit: 1, after: first.nextCursor });
		assert.equal(second.checkpoints.length, 1);
		assert.equal(second.nextCursor, null);
		assert.notEqual(second.checkpoints[0].checkpointId, first.checkpoints[0].checkpointId);
		await assert.rejects(listLegacyCheckpoints({ ctx, limit: 101 }), /limit/);
		await assert.rejects(listLegacyCheckpoints({ ctx, after: "../invalid" }), /cursor/);
		await assert.rejects(
			listLegacyCheckpoints({ ctx: { ...ctx, runMetadata: { callerId: "owner" } } }),
			/unscoped/,
		);
		await assert.rejects(
			adoptLegacyCheckpoint({ checkpointId: fixture.checkpointId, ctx }),
			/trusted caller/,
		);
		await assert.rejects(
			adoptLegacyCheckpoint({ checkpointId: "../invalid", ctx }),
			/checkpoint ID/,
		);
		await assert.rejects(
			adoptLegacyCheckpoint({
				checkpointId: fixture.checkpointId,
				ctx: { ...ctx, signal: AbortSignal.abort(), runMetadata: { callerId: "owner" } },
			}),
		);
		const retired = {
			...ctx,
			runMetadata: { callerId: "owner" },
			assertInvocationCurrent() {
				throw new Error("Invocation retired");
			},
		};
		await assert.rejects(
			adoptLegacyCheckpoint({ checkpointId: fixture.checkpointId, ctx: retired }),
			/Invocation retired/,
		);
		assert.equal((await listLegacyCheckpoints({ ctx })).checkpoints.length, 2);
		assert.deepEqual((await store.listRuns()).runs, []);
		await store.close();
		store = await openStoreSession({ directory, readOnly: true });
		await assert.rejects(
			adoptLegacyCheckpoint({
				checkpointId: fixture.checkpointId,
				ctx: { storeSession: store, runMetadata: { callerId: "owner" } },
			}),
			/read-only/,
		);
	});

	test(`${backend}: explicit adoption can bind an already-associated unowned legacy run`, async (t) => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-legacy-partial-"));
		t.after(() => fs.rm(root, { recursive: true, force: true }));
		const directory = path.join(root, "state");
		const env = {
			...process.env,
			LOBSTER_STATE_DIR: directory,
			LOBSTER_RUNS: "0",
			LOBSTER_HISTORY: "0",
			LOBSTER_BACKEND: "files",
		};
		const first = await runToolRequest({
			pipeline:
				'ask --emit --prompt First --schema \'{"type":"integer"}\' | ask --emit --prompt Second --schema \'{"type":"integer"}\'',
			ctx: { env },
		});
		if (backend === "sqlite") await migrateStore({ directory, backend, offline: true });
		const store = await openStoreSession({ directory, trackRuns: true });
		t.after(() => store.close());
		const second = await resumeToolRequest({
			token: first.requiresInput.resumeToken,
			response: 1,
			ctx: { storeSession: store },
		});
		assert.equal(second.status, "needs_input");
		const checkpointId = decodeResumeToken(second.requiresInput.resumeToken).stateKey;
		const ctx = {
			storeSession: store,
			runMetadata: { callerId: "owner", hostRunId: "trigger", secret: "must not persist" },
		};
		assert.deepEqual((await listRuns({ ctx })).runs, []);
		const adopted = await adoptLegacyCheckpoint({ checkpointId, ctx });
		assert.equal(adopted.id, second.runId);
		assert.deepEqual(adopted.metadata, { callerId: "owner", hostRunId: "trigger" });
		assert.equal((await listRuns({ ctx })).runs[0].id, adopted.id);
		const wait = await getWait({ waitId: adopted.pendingWaits[0].id, ctx });
		assert.equal(
			(
				await resumeToolRequest({
					waitId: wait.id,
					expectedRevision: wait.revision,
					response: 2,
					ctx,
				})
			).status,
			"ok",
		);
	});
}

test("CLI legacy inventory and adoption use the same persisted run identity", async (t) => {
	const root = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-cli-adoption-"));
	t.after(() => fs.rm(root, { recursive: true, force: true }));
	const fixture = await seed(path.join(root, "state"));
	// Compiled tests live under dist/test; bin lives at the repository root.
	const bin = new URL("../../bin/lobster.js", import.meta.url);
	const list = await exec(process.execPath, [fileURLToPath(bin), "storage", "legacy", "list"], {
		env: fixture.env,
	});
	assert.equal(JSON.parse(list.stdout).checkpoints[0].checkpointId, fixture.checkpointId);
	const adopt = await exec(
		process.execPath,
		[
			fileURLToPath(bin),
			"storage",
			"legacy",
			"adopt",
			fixture.checkpointId,
			"--caller",
			"cli-owner",
		],
		{ env: fixture.env },
	);
	const adopted = JSON.parse(adopt.stdout);
	assert.equal(adopted.status, "waiting");
	assert.equal(adopted.metadata.callerId, "cli-owner");
	const listed = await exec(process.execPath, [fileURLToPath(bin), "runs", "list"], {
		env: fixture.env,
	});
	assert.equal(JSON.parse(listed.stdout).runs[0].id, adopted.id);
	assert.equal(fixture.finished, 0);
});

for (const backend of ["files", "sqlite"] as const) {
	test(`${backend}: explicit adoption preserves a legacy short approval ID`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-short-id-adoption-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const env = {
			...process.env,
			LOBSTER_STATE_DIR: directory,
			LOBSTER_BACKEND: "files",
			LOBSTER_RUNS: "0",
			LOBSTER_HISTORY: "0",
		};
		const first = await runToolRequest({
			pipeline: "approve --prompt Review | state.set approved",
			ctx: { env },
		});
		assert.equal(first.status, "needs_approval");
		assert.ok(first.requiresApproval.approvalId);
		if (backend === "sqlite") await migrateStore({ directory, backend, offline: true });
		const store = await openStoreSession({ directory });
		t.after(() => store.close());
		const ctx = { storeSession: store, runMetadata: { callerId: "owner" } };
		const checkpointId = decodeResumeToken(first.requiresApproval.resumeToken).stateKey;
		const adopted = await adoptLegacyCheckpoint({ checkpointId, ctx });
		const resumed = await resumeToolRequest({
			approvalId: first.requiresApproval.approvalId,
			approved: true,
			ctx,
		});
		assert.equal(resumed.status, "ok", JSON.stringify(resumed));
		assert.equal(resumed.runId, adopted.id);
		assert.equal((await getRun({ runId: adopted.id, ctx })).status, "completed");
		assert.equal(
			(await resumeToolRequest({ token: first.requiresApproval.resumeToken, approved: true, ctx }))
				.ok,
			false,
		);
	});

	test(`${backend}: adoption resolves a historical workflow-key alias and preserves its original token`, async (t) => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-alias-adoption-"));
		t.after(() => fs.rm(root, { recursive: true, force: true }));
		const directory = path.join(root, "state");
		const fixture = await seed(directory, "workflow");
		const alternate = fixture.checkpointId.replace("workflow_resume_", "workflow-resume_");
		await fs.rename(
			path.join(directory, fixture.checkpointId + ".json"),
			path.join(directory, alternate + ".json"),
		);
		if (backend === "sqlite") await migrateStore({ directory, backend, offline: true });
		const store = await openStoreSession({ directory });
		t.after(() => store.close());
		assert.equal(
			(await listLegacyCheckpoints({ ctx: { storeSession: store } })).checkpoints[0].checkpointId,
			alternate,
		);
		const ctx = {
			storeSession: store,
			registry: fixture.registry,
			runMetadata: { callerId: "owner" },
		};
		const adopted = await adoptLegacyCheckpoint({ checkpointId: fixture.checkpointId, ctx });
		const resumed = await resumeToolRequest({
			token: fixture.first.requiresInput.resumeToken,
			response: 2,
			ctx,
		});
		assert.equal(resumed.status, "ok", JSON.stringify(resumed));
		assert.equal(resumed.runId, adopted.id);
		assert.equal(fixture.prepared, 1);
		assert.equal(fixture.finished, 1);
		assert.equal((await getRun({ runId: adopted.id, ctx })).status, "completed");
	});
}

test("adoption through an initially untracked file session stays authoritative for ordinary invocations", async (t) => {
	const root = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-adoption-selection-"));
	t.after(() => fs.rm(root, { recursive: true, force: true }));
	const directory = path.join(root, "state");
	const fixture = await seed(directory);
	const store = await openStoreSession({ directory });
	let adopted: Awaited<ReturnType<typeof adoptLegacyCheckpoint>>;
	try {
		adopted = await adoptLegacyCheckpoint({
			checkpointId: fixture.checkpointId,
			ctx: { storeSession: store, runMetadata: { callerId: "owner" } },
		});
	} finally {
		await store.close();
	}
	assert.deepEqual(JSON.parse(await fs.readFile(path.join(directory, ".backend"), "utf8")), {
		version: 1,
		backend: "files",
	});
	const plain = {
		env: {
			...process.env,
			LOBSTER_STATE_DIR: directory,
			LOBSTER_BACKEND: undefined,
			LOBSTER_HISTORY: "0",
			LOBSTER_RUNS: "0",
		},
		registry: fixture.registry,
	};
	const resumed = await resumeToolRequest({
		token: fixture.first.requiresInput.resumeToken,
		response: 5,
		ctx: plain,
	});
	assert.equal(resumed.status, "ok", JSON.stringify(resumed));
	assert.equal(resumed.runId, adopted.id);
	assert.equal(fixture.finished, 1);
	assert.equal(
		(
			await resumeToolRequest({
				token: fixture.first.requiresInput.resumeToken,
				response: 5,
				ctx: plain,
			})
		).ok,
		false,
	);
	assert.equal(fixture.finished, 1);
});
