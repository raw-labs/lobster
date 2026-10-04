import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test, { type TestContext } from "node:test";
import { fileURLToPath } from "node:url";
import {
	createDefaultRegistry,
	decodeResumeToken,
	getRun,
	getWait,
	openStoreSession,
	resumeToolRequest,
	runToolRequest,
	type ToolRunContext,
} from "../src/core/index.js";
import { PersistenceOutcomeUnknown } from "../src/persistence/backend.js";
import { fingerprint, type Transition } from "../src/persistence/model.js";
import { publishTrackedCheckpoint } from "../src/persistence/continuations.js";
import { bindRecorder, bindStoreSession } from "../src/persistence/context.js";
import { consumeResumeState } from "../src/state/store.js";
import { Lobster } from "../src/sdk/Lobster.js";

const pipeline =
	'ask --emit --prompt first --schema \'{"type":"integer"}\' | ask --emit --prompt second --schema \'{"type":"integer"}\' | effect';
const schema = { type: "integer" };

type BoundaryMessage = { operationId: string; successor: string };

function deferred() {
	let resolve: () => void;
	const promise = new Promise<void>((done) => {
		resolve = done;
	});
	return { promise, resolve };
}

async function fixture(t: TestContext, backend: "files" | "sqlite", history = true) {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-handoff-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "flow.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{ id: "first", input: { prompt: "first", responseSchema: schema } },
				{ id: "second", input: { prompt: "second", responseSchema: schema } },
				{ id: "finish", pipeline: "effect" },
			],
		}),
	);
	const state = path.join(directory, "state");
	let store = await openStoreSession({ directory: state, backend, trackRuns: true, history });
	let effects = 0;
	const defaults = createDefaultRegistry();
	const registry = {
		get(name: string) {
			return name === "effect"
				? {
						name,
						async run() {
							effects++;
							return { output: [effects] };
						},
					}
				: defaults.get(name);
		},
	};
	t.after(() => store.close());
	return {
		state,
		get store() {
			return store;
		},
		get effects() {
			return effects;
		},
		ctx: (): ToolRunContext => ({ storeSession: store, registry }),
		start: (kind: "pipeline" | "workflow") =>
			runToolRequest({
				...(kind === "pipeline" ? { pipeline } : { filePath }),
				ctx: { storeSession: store, registry },
			}),
		async reopen() {
			await store.close();
			store = await openStoreSession({ directory: state, trackRuns: true, history });
		},
	};
}

async function killAtHandoff(
	directory: string,
	wait: { id: string; revision: number },
	phase: "before" | "after",
) {
	const child = fork(
		fileURLToPath(new URL("./fixtures/handoff-process.js", import.meta.url)),
		[directory, wait.id, String(wait.revision), phase],
		{ stdio: ["ignore", "ignore", "pipe", "ipc"] },
	);
	let stderr = "";
	child.stderr.on("data", (data) => {
		stderr += data;
	});
	try {
		return await new Promise<BoundaryMessage>((resolve, reject) => {
			const timer = setTimeout(
				() => reject(new Error(`Handoff fixture timed out: ${stderr}`)),
				10000,
			);
			child.once("message", (message: BoundaryMessage) => {
				clearTimeout(timer);
				resolve(message);
			});
			child.once("exit", (code) => {
				clearTimeout(timer);
				reject(new Error(`Fixture exited ${code}: ${stderr}`));
			});
		});
	} finally {
		if (child.exitCode === null && child.signalCode === null) {
			const exited = once(child, "exit");
			child.kill("SIGKILL");
			await exited;
		}
	}
}

for (const backend of ["files", "sqlite"] as const) {
	for (const kind of ["pipeline", "workflow"] as const) {
		for (const history of [false, true]) {
			for (const phase of ["before", "after"] as const) {
				test(`${backend}: ${kind} survives a crash ${phase} handoff, history=${history}`, async (t) => {
					const f = await fixture(t, backend, history);
					const first = await f.start(kind);
					assert.equal(first.status, "needs_input", JSON.stringify(first));
					const original = (await getRun({ runId: first.runId, ctx: f.ctx() })).pendingWaits[0];
					const originalKey = decodeResumeToken(first.requiresInput.resumeToken).stateKey;
					await f.store.close();
					const boundary = await killAtHandoff(f.state, original, phase);
					await f.reopen();

					const run = await f.store.getRun(first.runId);
					const pending = (await getRun({ runId: first.runId, ctx: f.ctx() })).pendingWaits;
					assert.equal(pending.length, 1);
					assert.equal(
						(await getWait({ waitId: pending[0].id, ctx: f.ctx() })).request.prompt,
						phase === "before" ? "first" : "second",
					);
					assert.equal(run.states[boundary.successor] !== undefined, phase === "after");
					assert.equal(Boolean(run.receipts[boundary.operationId]), phase === "after");
					assert.equal(
						run.events.some((event) => event.kind === "handoff"),
						history && phase === "after",
					);
					if (phase === "after") {
						assert.equal(run.states[originalKey], null);
						assert.equal(run.waits[originalKey].status, "retired");
						assert.equal(run.receipts[boundary.operationId].acknowledged, false);
						const stale = await resumeToolRequest({
							token: first.requiresInput.resumeToken,
							response: 1,
							ctx: f.ctx(),
						});
						assert.equal(stale.ok, false);
						await f.store.acknowledge(first.runId, boundary.operationId);
					}

					let next = await resumeToolRequest({
						waitId: pending[0].id,
						expectedRevision: pending[0].revision,
						response: 2,
						ctx: f.ctx(),
					});
					if (phase === "before") {
						assert.equal(next.status, "needs_input", JSON.stringify(next));
						const wait = (await getRun({ runId: first.runId, ctx: f.ctx() })).pendingWaits[0];
						next = await resumeToolRequest({
							waitId: wait.id,
							expectedRevision: wait.revision,
							response: 3,
							ctx: f.ctx(),
						});
					}
					assert.equal(next.status, "ok", JSON.stringify(next));
					assert.equal(f.effects, 1);
				});
			}
		}

		for (const fault of [
			"cancel-before",
			"cancel-after",
			"lost-reply",
			"unreadable-receipt",
		] as const) {
			test(`${backend}: ${kind} preserves the right checkpoint on ${fault}`, async (t) => {
				const f = await fixture(t, backend);
				const first = await f.start(kind);
				const previousKey = decodeResumeToken(first.requiresInput.resumeToken).stateKey;
				const controller = new AbortController();
				const transition = f.store.backend.transition.bind(f.store.backend);
				let request: Transition;
				const injected = t.mock.method(f.store.backend, "transition", async (value, signal) => {
					if (value.action.kind !== "handoff") return transition(value, signal);
					request = value;
					if (fault === "cancel-before") controller.abort();
					const result = await transition(value, signal);
					if (fault === "cancel-after") controller.abort();
					if (fault === "lost-reply" || fault === "unreadable-receipt") {
						if (fault === "unreadable-receipt")
							t.mock.method(f.store.backend, "getRun", async () => {
								throw new Error("Read unavailable");
							});
						throw new PersistenceOutcomeUnknown(
							value.operationId,
							new Error("Lost reply"),
							value.runId,
						);
					}
					return result;
				});
				const next = await resumeToolRequest({
					token: first.requiresInput.resumeToken,
					response: 1,
					ctx: { ...f.ctx(), signal: controller.signal },
				});
				injected.mock.restore();
				t.mock.restoreAll();
				assert.ok(request);
				assert.equal(
					next.ok,
					fault === "cancel-after" || fault === "lost-reply",
					JSON.stringify(next),
				);
				if (fault === "unreadable-receipt")
					assert.equal(next.error.type, "persistence_outcome_unknown");
				await f.reopen();
				const run = await f.store.getRun(first.runId);
				const pending = (await getRun({ runId: first.runId, ctx: f.ctx() })).pendingWaits;
				assert.equal(pending.length, 1);
				assert.equal(
					(await getWait({ waitId: pending[0].id, ctx: f.ctx() })).request.prompt,
					fault === "cancel-before" ? "first" : "second",
				);
				assert.equal(
					run.waits[previousKey].status,
					fault === "cancel-before" ? "pending" : "retired",
				);
				if (fault === "lost-reply")
					assert.equal(run.receipts[request.operationId].acknowledged, true);
				assert.equal(f.effects, 0);
			});
		}

		test(`${backend}: ${kind} accepts one of two competing answers`, async (t) => {
			const f = await fixture(t, backend);
			const first = await f.start(kind);
			const previousKey = decodeResumeToken(first.requiresInput.resumeToken).stateKey;
			const wait = (await getRun({ runId: first.runId, ctx: f.ctx() })).pendingWaits[0];
			const answers = await Promise.all([
				resumeToolRequest({ token: first.requiresInput.resumeToken, response: 1, ctx: f.ctx() }),
				resumeToolRequest({
					waitId: wait.id,
					expectedRevision: wait.revision,
					response: 2,
					ctx: f.ctx(),
				}),
			]);
			assert.equal(answers.filter((result) => result.ok).length, 1, JSON.stringify(answers));
			const winner = answers.find((result) => result.ok);
			const nextKey = decodeResumeToken(winner.requiresInput.resumeToken).stateKey;
			const run = await f.store.getRun(first.runId);
			assert.deepEqual(
				Object.entries(run.waits)
					.filter(([, value]) => value.status === "pending")
					.map(([key]) => key),
				[nextKey],
			);
			const handoff = run.events.find((event) => event.kind === "handoff");
			assert.ok(handoff);
			const page = await f.store.inspect(run.id, { afterSequence: handoff.sequence - 1, limit: 1 });
			assert.deepEqual(Object.keys(page.waits).sort(), [previousKey, nextKey].sort());
			assert.equal(
				Object.values(run.receipts).filter((receipt) => receipt.revision === handoff.sequence)
					.length,
				1,
			);
			const complete = await resumeToolRequest({
				token: winner.requiresInput.resumeToken,
				response: 3,
				ctx: f.ctx(),
			});
			assert.equal(complete.status, "ok", JSON.stringify(complete));
			assert.equal(f.effects, 1);
		});
	}

	for (const gates of [
		["approval", "approval"],
		["input", "approval"],
		["approval", "input"],
	] as const) {
		test(`${backend}: approval IDs follow the ${gates.join(" to ")} handoff`, async (t) => {
			const f = await fixture(t, backend);
			const commands = gates.map((kind, index) =>
				kind === "approval"
					? `approve --prompt gate${index}`
					: `ask --emit --prompt gate${index} --schema '{"type":"integer"}'`,
			);
			const first = await runToolRequest({
				pipeline: commands.join(" | ") + " | effect",
				ctx: f.ctx(),
			});
			const original = first.requiresApproval ?? first.requiresInput;
			const previousKey = decodeResumeToken(original.resumeToken).stateKey;
			const second = await resumeToolRequest({
				token: original.resumeToken,
				...(gates[0] === "approval" ? { approved: true } : { response: 1 }),
				ctx: f.ctx(),
			});
			assert.equal(
				second.status,
				gates[1] === "approval" ? "needs_approval" : "needs_input",
				JSON.stringify(second),
			);
			assert.equal((await f.store.getRun(first.runId)).waits[previousKey].status, "retired");
			assert.deepEqual(
				Object.values((await f.store.getRun(first.runId)).waits[previousKey].decision),
				gates[0] === "approval" ? [true] : [fingerprint(1)],
			);
			if (first.requiresApproval?.approvalId) {
				const stale = await resumeToolRequest({
					approvalId: first.requiresApproval.approvalId,
					approved: true,
					ctx: f.ctx(),
				});
				assert.equal(stale.ok, false);
			}
			const request = second.requiresApproval ?? second.requiresInput;
			const complete = await resumeToolRequest({
				...(second.requiresApproval?.approvalId
					? { approvalId: second.requiresApproval.approvalId }
					: { token: request.resumeToken }),
				...(gates[1] === "approval" ? { approved: true } : { response: 2 }),
				ctx: f.ctx(),
			});
			assert.equal(complete.status, "ok", JSON.stringify(complete));
			assert.equal(f.effects, 1);
		});
	}

	test(`${backend}: handoff requires the actual pending snapshot or owned claim and a fresh successor`, async (t) => {
		const f = await fixture(t, backend);
		const first = await f.start("pipeline");
		const key = decodeResumeToken(first.requiresInput.resumeToken).stateKey;
		const state = JSON.parse((await f.store.read(key)).text);
		const run = await f.store.getRun(first.runId);
		const successor = { key: `pipeline_resume_${randomUUID()}`, text: JSON.stringify(state) };
		const request: Transition = {
			operationId: randomUUID(),
			runId: run.id,
			action: {
				kind: "handoff",
				predecessor: {
					key,
					status: "pending",
					fingerprint: fingerprint(state),
					expectedWaitRevision: run.waits[key].revision,
				},
				successor,
			},
		};
		const wrong = structuredClone(request);
		assert.ok(wrong.action.kind === "handoff" && wrong.action.predecessor.status === "pending");
		wrong.action.predecessor.fingerprint = fingerprint({ wrong: true });
		assert.equal((await f.store.transition(wrong)).status, "conflict");
		wrong.action.predecessor.fingerprint = fingerprint(state);
		wrong.action.predecessor.expectedWaitRevision++;
		assert.equal((await f.store.transition(wrong)).status, "conflict");
		const foreign = await f.store.createRun("fixture");
		await f.store.write(successor.key, successor.text, { runId: foreign.runId });
		assert.equal((await f.store.transition(request)).status, "conflict");
		assert.equal((await f.store.read(successor.key)).runId, foreign.runId);

		const claimId = randomUUID();
		await f.store.write(
			key,
			JSON.stringify({
				type: "lobster.consumed-resume-state.v1",
				claimId,
				consumedAt: new Date().toISOString(),
			}),
		);
		assert.ok(request.action.kind === "handoff");
		request.action.successor.key = `pipeline_resume_${randomUUID()}`;
		request.action.predecessor = { key, status: "consumed", claimId: "not-the-owner" };
		assert.equal((await f.store.transition(request)).status, "conflict");
		request.action.predecessor.claimId = claimId;
		const committed = await f.store.transition(request);
		assert.equal(committed.status, "committed");
		assert.deepEqual(await f.store.transition(request), committed);
		assert.equal((await f.store.getRun(run.id)).revision, committed.revision);
	});

	test(`${backend}: a legacy token claim and a handoff share the predecessor barrier`, async (t) => {
		const f = await fixture(t, backend);
		const first = await f.start("pipeline");
		const key = decodeResumeToken(first.requiresInput.resumeToken).stateKey;
		const state = JSON.parse((await f.store.read(key)).text);
		const recorder = await f.store.recorderForState(key);
		const env = bindRecorder(bindStoreSession({}, f.store), recorder);
		const successorKey = `pipeline_resume_${randomUUID()}`;
		const atWrite = deferred();
		const release = deferred();
		const atLock = deferred();
		const write = f.store.backend.write.bind(f.store.backend);
		t.mock.method(f.store.backend, "write", async (...args) => {
			if (args[0] === key) {
				atWrite.resolve();
				await release.promise;
			}
			return write(...args);
		});
		const claim = consumeResumeState({ env, key, expectedState: state });
		await atWrite.promise;
		const mkdir = fs.mkdir.bind(fs);
		t.mock.method(fs, "mkdir", async (...args) => {
			if (String(args[0]) === path.join(f.state, `${key}.json.lock`)) atLock.resolve();
			return mkdir(...args);
		});
		const handoff = publishTrackedCheckpoint({
			env,
			key: successorKey,
			state,
			predecessor: { key, state },
		}).then(
			() => null,
			(error: Error) => error,
		);
		try {
			await Promise.race([
				atLock.promise,
				handoff.then(() => {
					throw new Error("Handoff bypassed the in-flight claim");
				}),
			]);
		} finally {
			release.resolve();
		}
		assert.equal((await claim).consumed, true);
		assert.equal((await handoff).name, "WaitConflictError");
		assert.equal((await f.store.read(successorKey)).text, null);
		assert.equal((await f.store.getRun(first.runId)).waits[key].status, "consumed");
	});

	test(`${backend}: SDK carries successive same-stage questions through one handoff`, async (t) => {
		const f = await fixture(t, backend);
		let effects = 0;
		const workflow = new Lobster({ storeSession: f.store })
			.pipe({
				async run({ ctx }) {
					await ctx.requestInput({ prompt: "first", responseSchema: schema });
					const answer = await ctx.requestInput({ prompt: "second", responseSchema: schema });
					return { output: [answer] };
				},
			})
			.pipe((items) => {
				effects++;
				return items;
			});
		const first = await workflow.run();
		assert.equal(first.status, "needs_input", JSON.stringify(first));
		const second = await workflow.resume(first.requiresInput.resumeToken, { response: 1 });
		assert.equal(second.status, "needs_input", JSON.stringify(second));
		const run = await f.store.getRun(first.runId);
		assert.equal(Object.values(run.waits).filter((wait) => wait.status === "pending").length, 1);
		assert.equal(run.events.filter((event) => event.kind === "handoff").length, 1);
		await assert.rejects(
			workflow.resume(first.requiresInput.resumeToken, { response: 2 }),
			/not found/,
		);
		const completed = await workflow.resume(second.requiresInput.resumeToken, { response: 3 });
		assert.equal(completed.status, "ok", JSON.stringify(completed));
		assert.deepEqual(completed.output, [3]);
		assert.equal(effects, 1);
	});
}

for (const backend of ["files", "sqlite"] as const) {
	test(`${backend}: a failure inside publication rolls back checkpoints, history, and receipt`, async (t) => {
		const f = await fixture(t, backend);
		const first = await f.start("pipeline");
		const key = decodeResumeToken(first.requiresInput.resumeToken).stateKey;
		const state = JSON.parse((await f.store.read(key)).text);
		const before = await f.store.getRun(first.runId);
		const successor = { key: `pipeline_resume_${randomUUID()}`, text: JSON.stringify(state) };
		const request: Transition = {
			operationId: randomUUID(),
			runId: before.id,
			action: {
				kind: "handoff",
				predecessor: { key, status: "pending", fingerprint: fingerprint(state) },
				successor,
			},
		};
		if (backend === "files") {
			const rename = fs.rename.bind(fs);
			t.mock.method(fs, "rename", async (...args) => {
				if (String(args[1]) === path.join(f.state, ".history", `${before.id}.json`)) {
					throw Object.assign(new Error("Injected publication failure"), { code: "EIO" });
				}
				return rename(...args);
			});
		} else {
			const { DatabaseSync } = await import("node:sqlite");
			const database = new DatabaseSync(path.join(f.state, "lobster.sqlite"));
			try {
				// saveRun has already written metadata, history, receipts, and the old
				// checkpoint by the time this fresh successor insertion fails.
				database.exec(`CREATE TRIGGER fail_successor BEFORE INSERT ON state
					WHEN NEW.key = '${successor.key}'
					BEGIN SELECT RAISE(ABORT, 'Injected publication failure'); END`);
			} finally {
				database.close();
			}
		}
		await assert.rejects(f.store.transition(request), /Injected publication failure/);
		assert.deepEqual(await f.store.getRun(before.id), before);
		assert.equal((await f.store.read(successor.key)).text, null);
	});
}

test("files: a directory sync failure after handoff publication reconciles its receipt", async (t) => {
	const f = await fixture(t, "files", false);
	const first = await f.start("pipeline");
	const previousKey = decodeResumeToken(first.requiresInput.resumeToken).stateKey;
	const open = fs.open.bind(fs);
	let injected = false;
	t.mock.method(fs, "open", async (...args) => {
		const handle = await open(...args);
		if (!injected && String(args[0]) === path.join(f.state, ".history") && args[1] === "r") {
			injected = true;
			t.mock.method(handle, "sync", async () => {
				throw Object.assign(new Error("Injected directory sync failure"), { code: "EIO" });
			});
		}
		return handle;
	});
	const next = await resumeToolRequest({
		token: first.requiresInput.resumeToken,
		response: 1,
		ctx: f.ctx(),
	});
	assert.equal(injected, true);
	assert.equal(next.status, "needs_input", JSON.stringify(next));
	const run = await f.store.getRun(first.runId);
	assert.equal(run.waits[previousKey].status, "retired");
	assert.equal(Object.values(run.waits).filter((wait) => wait.status === "pending").length, 1);
	assert.equal(
		Object.values(run.receipts).every((receipt) => receipt.acknowledged),
		true,
	);
});
