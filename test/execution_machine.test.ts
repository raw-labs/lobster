import test, { type TestContext } from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { pathToFileURL } from "node:url";
import { runToolRequest, resumeToolRequest, createToolContext } from "../src/core/tool_runtime.js";
import { openStoreSession } from "../src/persistence/session.js";
import { advanceWorkflowRun } from "../src/workflows/file.js";
import { ExecutionStore } from "../src/engine/store.js";

const coreUrl = pathToFileURL(path.resolve("dist/src/core/tool_runtime.js")).href;
const fileUrl = pathToFileURL(path.resolve("dist/src/workflows/file.js")).href;
const sessionUrl = pathToFileURL(path.resolve("dist/src/persistence/session.js")).href;
const contextUrl = pathToFileURL(path.resolve("dist/src/persistence/context.js")).href;

async function fixture(t: TestContext, backend: string) {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-machine-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "flow.lobster");
	const env = {
		...process.env,
		LOBSTER_STATE_DIR: path.join(directory, "state"),
		LOBSTER_BACKEND: backend,
	};
	return { directory, filePath, env };
}

for (const backend of ["files", "sqlite"] as const) {
	for (const mode of [
		"after-result",
		"repeat",
		"manual",
		"loop",
		"parallel",
		"child",
		"retry",
	] as const) {
		test(`machine ${backend}: fresh process recovery ${mode}`, { timeout: 45000 }, async (t) => {
			const f = await fixture(t, backend);
			const counts = path.join(f.directory, "calls.jsonl");
			const second = {
				id: "second",
				pipeline: "work --name second",
				...(mode !== "manual" ? { recovery: "repeat" } : {}),
			};
			const main = [
				{ id: "first", pipeline: "work --name first" },
				second,
				{ id: "last", pipeline: "work --name original" },
			];
			const childPath = path.join(f.directory, "child.lobster");
			let steps: unknown[] = main;
			if (mode === "loop")
				steps = [
					{ id: "items", pipeline: "work --name items" },
					{
						id: "loop",
						for_each: "$items.json",
						steps: [{ id: "workItem", pipeline: "work --name $item.json", recovery: "repeat" }],
					},
					main[2],
				];
			if (mode === "parallel")
				steps = [{ id: "branches", parallel: { branches: [main[0], second] } }, main[2]];
			if (mode === "child") {
				await fs.writeFile(childPath, JSON.stringify({ steps: main.slice(0, 2) }));
				steps = [{ id: "child", workflow: childPath }, main[2]];
			}
			if (mode === "retry")
				steps = [main[0], { ...second, retry: { max: 2, delay_ms: 7000 } }, main[2]];
			await fs.writeFile(f.filePath, JSON.stringify({ steps }));
			const worker = path.join(f.directory, "worker.mjs");
			await fs.writeFile(
				worker,
				`
import fs from "node:fs/promises";
import { createToolContext } from ${JSON.stringify(coreUrl)};
import { runWorkflowFile, advanceWorkflowRun } from ${JSON.stringify(fileUrl)};
import { openStoreSession } from ${JSON.stringify(sessionUrl)};
import { recorderFromEnv } from ${JSON.stringify(contextUrl)};
const store = await openStoreSession({ env: process.env });
const ctx = createToolContext({ storeSession: store, env: process.env });
let firstSaved;
const firstCheckpoint = new Promise(resolve => firstSaved = resolve);
const command = { name: "work", meta: { argsSchema: { type: "object" } }, help() { return ""; },
async run({ args, ctx }) {
  await fs.appendFile(process.env.COUNTS, args.name + "\\n");
  if (args.name === "second" && !process.env.RECOVER && process.env.MODE === "retry") throw new Error("reported failure");
  if (args.name === "second" && !process.env.RECOVER && process.env.MODE !== "after-result") {
    if (process.env.MODE === "parallel") await firstCheckpoint;
    process.stdout.write("READY:" + recorderFromEnv(ctx.env).runId + "\\n");
    await new Promise(() => { setInterval(() => {}, 1000); });
  }
  return { output: (async function* () { yield args.name === "items" ? ["first", "second"] : { name: args.name, value: 42 }; })() };
}};
const registry = ctx.registry;
ctx.registry = { get: name => name === "work" ? command : registry.get(name) };
{
  const transition = store.backend.transition.bind(store.backend);
  store.backend.transition = async (...args) => {
    const result = await transition(...args);
    const request = args[0];
    if (request.action.kind === "execution-save") {
      const frames = Object.values(request.action.execution.activations);
      const first = frames.some(a => a.phase === "completed" && a.result?.json?.name === "first");
      if (first) firstSaved();
      if (!process.env.RECOVER && ((process.env.MODE === "after-result" && first) ||
          (process.env.MODE === "retry" && frames.some(a => a.retryAt)))) {
        process.stdout.write("READY:" + request.runId + "\\n");
        process.kill(process.pid, "SIGKILL");
      }
    }
    return result;
  };
}

try {
  const result = process.env.RECOVER ? await advanceWorkflowRun({ runId: process.env.RUN_ID, ctx, leaseMs: 5000 })
    : await runWorkflowFile({ filePath: process.env.FLOW, ctx, leaseMs: 5000 });
  process.stdout.write(JSON.stringify(result) + "\\n");
} catch (error) { process.stdout.write(JSON.stringify({ code: error.code, message: error.message }) + "\\n"); }
finally { await store.close(); }
`,
			);
			const child = spawn(process.execPath, [worker], {
				env: { ...f.env, COUNTS: counts, MODE: mode, FLOW: f.filePath },
				stdio: ["ignore", "pipe", "pipe"],
			});
			t.after(() => {
				if (child.exitCode === null && !child.killed) child.kill("SIGKILL");
			});
			const exit = once(child, "exit");
			let text = "",
				workerErrors = "";
			child.stderr.on("data", (chunk) => (workerErrors += chunk));
			const ready = new Promise<string>((resolve, reject) => {
				child.stdout.on("data", (chunk) => {
					text += chunk;
					const match = /READY:([^\n]+)/.exec(text);
					if (match) resolve(match[1]);
				});
				child.on("error", reject);
				child.on("exit", () => {
					if (!text.includes("READY:"))
						reject(new Error("Worker exited before checkpoint: " + text + workerErrors));
				});
			});
			const runId = await ready;
			if (mode !== "after-result" && mode !== "retry") child.kill("SIGKILL");
			await exit;
			// Source edits must not alter an already admitted execution.
			await fs.writeFile(
				f.filePath,
				JSON.stringify({ steps: [{ id: "changed", pipeline: "work --name changed" }] }),
			);
			if (mode === "child")
				await fs.writeFile(
					childPath,
					JSON.stringify({ steps: [{ id: "changed", pipeline: "work --name changed" }] }),
				);
			let retryAt: number | undefined;
			const store = await openStoreSession({ env: f.env });
			try {
				// Recover after the recorded lease. A kill during a file-lock owner write
				// may also require the file backend's 30-second abandoned-owner grace.
				const run = await store.getRunState(runId);
				retryAt = Object.values(run!.execution!.activations).find(
					(frame) => frame.retryAt,
				)?.retryAt;
				if (mode === "retry") assert.ok(retryAt);
				const due = run!.executionLease!.expiresAt - Date.now() + 5;
				if (due > 0) await new Promise((resolve) => setTimeout(resolve, due));
			} finally {
				await store.close();
			}
			const recovery = spawn(process.execPath, [worker], {
				env: {
					...f.env,
					COUNTS: counts,
					MODE: mode,
					FLOW: f.filePath,
					RECOVER: "1",
					RUN_ID: runId,
				},
				stdio: ["ignore", "pipe", "pipe"],
			});
			t.after(() => {
				if (recovery.exitCode === null && !recovery.killed) recovery.kill("SIGKILL");
			});
			let output = "",
				errors = "";
			recovery.stdout.on("data", (chunk) => (output += chunk));
			recovery.stderr.on("data", (chunk) => (errors += chunk));
			const [code] = await once(recovery, "exit");
			assert.equal(code, 0, errors);
			const result = JSON.parse(output.trim());
			if (retryAt) assert.ok(Date.now() >= retryAt);
			const calls = (await fs.readFile(counts, "utf8")).trim().split("\n");
			assert.equal(calls.filter((name) => name === "first").length, 1);
			if (mode === "manual") {
				assert.equal(result.code, "LOBSTER_ACTIVITY_OUTCOME_UNKNOWN", output);
				assert.deepEqual(calls, ["first", "second"]);
				const saved = await openStoreSession({ env: f.env });
				try {
					assert.equal((await saved.getRunState(runId))!.execution!.status, "blocked");
				} finally {
					await saved.close();
				}
			} else {
				assert.equal(result.status, "ok", output);
				assert.deepEqual(result.output, [{ name: "original", value: 42 }]);
				assert.equal(
					calls.filter((name) => name === "second").length,
					mode === "after-result" ? 1 : 2,
				);
				assert.equal(calls.includes("changed"), false);
			}
		});
	}

	test(`machine ${backend}: saved approval, definition and decision are authoritative`, async (t) => {
		const f = await fixture(t, backend);
		await fs.writeFile(
			f.filePath,
			JSON.stringify({
				steps: [
					{ id: "gate", approval: { prompt: "Proceed?", required_approver: "alice" } },
					{ id: "finish", run: "node -e \"process.stdout.write('original')\"" },
				],
			}),
		);
		const ctx = { env: f.env };
		const start = await runToolRequest({ filePath: f.filePath, ctx });
		assert.equal(start.status, "needs_approval");
		const token = start.requiresApproval!.resumeToken;
		await fs.writeFile(f.filePath, JSON.stringify({ steps: [{ id: "new", run: "exit 99" }] }));
		const denied = await resumeToolRequest({ token, approved: true, ctx });
		assert.equal(denied.ok, false);
		const resumed = await resumeToolRequest({
			token,
			approved: true,
			ctx: { env: { ...f.env, LOBSTER_APPROVAL_APPROVED_BY: "alice" } },
		});
		assert.equal(resumed.status, "ok", JSON.stringify(resumed));
		assert.deepEqual(resumed.output, ["original"]);
		const duplicate = await resumeToolRequest({ token, approved: true, ctx });
		assert.equal(duplicate.ok, false);
	});

	test(`machine ${backend}: ownership conflict and stale checkpoint fencing`, async (t) => {
		const f = await fixture(t, backend);
		const session = await openStoreSession({ env: f.env, history: true });
		try {
			const recorder = await session.createRun("workflow");
			const first = new ExecutionStore(session, recorder.runId);
			await first.claim();
			await first.save({
				version: 1,
				revision: 0,
				definition: {
					filePath: f.filePath,
					hash: "fixture",
					workflow: { steps: [{ id: "safe", run: "true" }] },
					children: {},
				},
				args: {},
				activations: {},
				status: "running",
			});
			const second = new ExecutionStore(session, recorder.runId);
			await assert.rejects(second.claim(), /owned by another executor/);
			await first.close();
			const orphan = await recorder.start("activity", "interrupted");
			const third = new ExecutionStore(session, recorder.runId);
			const checkpoint = (await third.claim())!;
			assert.equal(
				(await session.getRun(recorder.runId))!.attempts.find((a) => a.id === orphan)!.status,
				"closed",
			);
			const stale = await session.transition({
				operationId: "stale",
				runId: recorder.runId,
				action: { kind: "execution-release", owner: first.owner, epoch: 1 },
			});
			assert.equal(stale.status, "conflict");
			const staleCheckpoint = await session.transition({
				operationId: "stale-checkpoint",
				runId: recorder.runId,
				action: {
					kind: "execution-save",
					owner: first.owner,
					epoch: 1,
					expectedVersion: checkpoint.revision,
					execution: { ...checkpoint, revision: checkpoint.revision + 1 },
				},
			});
			assert.equal(staleCheckpoint.status, "conflict");
			await third.close();
		} finally {
			await session.close();
		}
	});
	for (const boundary of ["question", "answer", "result"] as const) {
		test(`machine ${backend}: cancellation after committed ${boundary}`, async (t) => {
			const f = await fixture(t, backend);
			const store = await openStoreSession({ env: f.env });
			t.after(() => store.close());
			let calls = 0;
			const registry = {
				get: (name: string) =>
					name === "effect"
						? {
								name: "effect",
								async run() {
									calls++;
									return { output: [{ done: true }] };
								},
							}
						: undefined,
			};
			await fs.writeFile(
				f.filePath,
				JSON.stringify({
					steps: [
						{ id: "approve", approval: "Proceed?" },
						{ id: "effect", pipeline: "effect" },
					],
				}),
			);
			const controller = new AbortController();
			const ctx = { env: f.env, storeSession: store, registry, signal: controller.signal };
			const transition = store.backend.transition.bind(store.backend);
			let interrupted = false;
			store.backend.transition = async (...args) => {
				const result = await transition(...args);
				const action = args[0].action;
				if (
					action.kind === "execution-save" &&
					!interrupted &&
					((boundary === "question" && action.publish) ||
						(boundary === "answer" && action.consume) ||
						(boundary === "result" &&
							Object.values(action.execution.activations).some(
								(a) => a.phase === "completed" && (a.result?.json as { done?: boolean })?.done,
							)))
				) {
					interrupted = true;
					controller.abort(new Error("cancel at committed " + boundary));
				}
				return result;
			};
			const started = await runToolRequest({ filePath: f.filePath, ctx });
			const failed =
				boundary === "question"
					? started
					: await resumeToolRequest({
							token: started.requiresApproval!.resumeToken!,
							approved: true,
							ctx,
						});
			assert.equal(failed.ok, false, JSON.stringify(failed));
			assert.equal(interrupted, true);
			const {
				runs: [run],
			} = await store.listRuns();
			const next = await advanceWorkflowRun({
				runId: run.id,
				ctx: createToolContext({ ...ctx, signal: undefined }),
			});
			if (boundary === "question") {
				assert.equal(calls, 0);
				assert.equal(next.status, "needs_approval");
				const approved = await resumeToolRequest({
					token: next.requiresApproval!.resumeToken!,
					approved: true,
					ctx: { ...ctx, signal: undefined },
				});
				assert.equal(approved.status, "ok");
			} else {
				assert.equal(next.status, "ok");
				const duplicate = await resumeToolRequest({
					token: started.requiresApproval!.resumeToken!,
					approved: true,
					ctx: { ...ctx, signal: undefined },
				});
				assert.equal(duplicate.ok, false);
			}
			assert.equal(calls, 1);
		});
	}

	test(`machine ${backend}: token and short ID cannot launch competing effects`, async (t) => {
		const f = await fixture(t, backend);
		await fs.writeFile(
			f.filePath,
			JSON.stringify({
				steps: [
					{ id: "approve", approval: "Proceed?" },
					{ id: "effect", pipeline: "effect" },
				],
			}),
		);
		let entered!: () => void, release!: () => void;
		const startedEffect = new Promise<void>((resolve) => (entered = resolve));
		const held = new Promise<void>((resolve) => (release = resolve));
		t.after(() => release());
		let calls = 0;
		const registry = {
			get: (name: string) =>
				name === "effect"
					? {
							name: "effect",
							async run() {
								calls++;
								entered();
								await held;
								return { output: [{ done: true }] };
							},
						}
					: undefined,
		};
		const ctx = { env: f.env, registry };
		const first = await runToolRequest({ filePath: f.filePath, ctx });
		const pending = resumeToolRequest({
			token: first.requiresApproval!.resumeToken!,
			approved: true,
			ctx,
		});
		await startedEffect;
		const competing = await resumeToolRequest({
			approvalId: first.requiresApproval!.approvalId!,
			approved: true,
			ctx,
		});
		assert.equal(competing.ok, false);
		release();
		assert.equal((await pending).status, "ok");
		assert.equal(calls, 1);
	});
	for (const boundary of ["answer", "dispatch", "successor"] as const) {
		test("machine " + backend + ": host retirement before " + boundary, async (t) => {
			const f = await fixture(t, backend);
			const store = await openStoreSession({ env: f.env });
			t.after(() => store.close());
			let calls = 0,
				current = true;
			const registry = {
				get: (name: string) =>
					name === "effect"
						? {
								name,
								async run() {
									calls++;
									return { output: [calls] };
								},
							}
						: undefined,
			};
			await fs.writeFile(
				f.filePath,
				JSON.stringify({
					steps: [
						{ id: "first", approval: "First?" },
						boundary === "successor"
							? { id: "second", approval: "Second?" }
							: {
									id: "effect",
									pipeline: "effect",
									recovery: "repeat",
									retry: { max: 3, delay_ms: 0 },
									on_error: "continue",
								},
					],
				}),
			);
			const ctx = {
				env: f.env,
				storeSession: store,
				registry,
				assertInvocationCurrent() {
					if (!current) throw new Error("host invocation retired");
				},
			};
			const started = await runToolRequest({ filePath: f.filePath, ctx });
			const transition = store.backend.transition.bind(store.backend);
			let retired = false;
			store.backend.transition = async (...args) => {
				const action = args[0].action;
				if (
					!retired &&
					action.kind === "execution-save" &&
					((boundary === "answer" && action.consume) ||
						(boundary === "successor" && action.publish))
				) {
					retired = true;
					current = false;
				}
				const result = await transition(...args);
				if (
					!retired &&
					action.kind === "execution-save" &&
					boundary === "dispatch" &&
					Object.values(action.execution.activations).some(
						(frame) => frame.phase === "started" && frame.input?.text === "effect",
					)
				) {
					retired = true;
					current = false;
				}
				return result;
			};
			const failed = await resumeToolRequest({
				token: started.requiresApproval!.resumeToken!,
				approved: true,
				ctx,
			});
			assert.equal(retired, true);
			assert.equal(failed.ok, false);
			assert.match(failed.error!.message, /host invocation retired/);
			assert.equal(calls, 0);
			current = true;
			const resumed =
				boundary === "answer"
					? await resumeToolRequest({
							token: started.requiresApproval!.resumeToken!,
							approved: true,
							ctx,
						})
					: await advanceWorkflowRun({ runId: started.runId!, ctx: createToolContext(ctx) });
			if (boundary === "successor") {
				assert.equal(resumed.status, "needs_approval");
				assert.equal(resumed.requiresApproval!.prompt, "Second?");
			} else {
				assert.equal(resumed.status, "ok", JSON.stringify(resumed));
				assert.equal(calls, 1);
			}
		});
	}
}
