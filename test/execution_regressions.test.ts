import test from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { createToolContext, resumeToolRequest, runToolRequest } from "../src/core/tool_runtime.js";
import * as core from "../src/core/index.js";
import { advanceWorkflowRun, runIdOf, runWorkflowFile } from "../src/engine/runner.js";
import { getRun, getWait } from "../src/core/runs.js";
import { openStoreSession } from "../src/persistence/session.js";
import { bindResumeGuard, bindStoreSession } from "../src/persistence/context.js";
import { decodeResumeToken } from "../src/resume/resume.js";
import { WaitConflictError } from "../src/persistence/backend.js";

/** An `llm.invoke` call to `paid`, an adapter whose every answer costs $0.0125 (gpt-4o). */
const paidCall = {
	id: "paid",
	pipeline: "llm.invoke --provider paid --model gpt-4o --prompt hi --disable-cache",
};
const llmAdapters = {
	paid: async () => ({
		ok: true,
		result: {
			model: "gpt-4o",
			output: { text: "paid" },
			usage: { inputTokens: 1000, outputTokens: 1000 },
		},
	}),
};

for (const backend of ["sqlite"]) {
	test(`machine ${backend}: parent budget stops further child and parent effects`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-budget-regression-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const marker = path.join(directory, "effect");
		const effect = {
			id: "effect",
			run: `node -e 'require("fs").writeFileSync(process.env.MARKER,"executed")'`,
		};
		await fs.writeFile(
			path.join(directory, "child.lobster"),
			JSON.stringify({ steps: [paidCall, effect] }),
		);
		const filePath = path.join(directory, "parent.lobster");
		await fs.writeFile(
			filePath,
			JSON.stringify({
				cost_limit: { max_usd: 0.00001, action: "stop" },
				steps: [{ id: "child", workflow: "child.lobster" }, effect],
			}),
		);
		await assert.rejects(
			runWorkflowFile({
				filePath,
				ctx: createToolContext({
					env: {
						...process.env,
						MARKER: marker,
						LOBSTER_STATE_DIR: path.join(directory, "state"),
					},
					llmAdapters,
				}),
			}),
			/Cost limit exceeded/,
		);
		await assert.rejects(fs.access(marker), { code: "ENOENT" });
	});

	for (const parallel of [false, true]) {
		test(`machine ${backend}: input subject follows ${parallel ? "parallel aggregate" : "last successful step"}`, async (t) => {
			const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-subject-regression-"));
			t.after(() => fs.rm(directory, { recursive: true, force: true }));
			const filePath = path.join(directory, "flow.lobster");
			const data = { id: "data", run: `echo '{"value":42}'` };
			await fs.writeFile(
				filePath,
				JSON.stringify({
					steps: [
						parallel
							? {
									id: "group",
									parallel: { branches: [data, { id: "other", run: `echo '{"value":7}'` }] },
								}
							: data,
						{ id: "skipped", when: false, run: "exit 99" },
						{ id: "question", input: { prompt: "Review?", responseSchema: { type: "object" } } },
					],
				}),
			);
			const result = await runWorkflowFile({
				filePath,
				ctx: createToolContext({
					env: {
						...process.env,
						LOBSTER_STATE_DIR: path.join(directory, "state"),
					},
				}),
			});
			assert.equal(result.status, "needs_input");
			assert.deepEqual(
				result.requiresInput!.subject,
				parallel ? { data: { value: 42 }, other: { value: 7 } } : { value: 42 },
			);
		});
	}

	for (const where of ["switch case", "loop body"]) {
		test(`machine ${backend}: an input step first in a ${where} shows the step before it`, async (t) => {
			const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-subject-regression-"));
			t.after(() => fs.rm(directory, { recursive: true, force: true }));
			const filePath = path.join(directory, "flow.lobster");
			const question = {
				id: "question",
				input: { prompt: "Review?", responseSchema: { type: "object" } },
			};
			const around =
				where === "switch case"
					? {
							id: "route",
							switch: { cases: [{ when: "$data.json.value > 1", steps: [question] }] },
						}
					: { id: "loop", for_each: "$data.json.items", steps: [question] };
			await fs.writeFile(
				filePath,
				JSON.stringify({
					steps: [{ id: "data", run: `echo '{"value":42,"items":[1]}'` }, around],
				}),
			);
			const result = await runWorkflowFile({
				filePath,
				ctx: createToolContext({
					env: { ...process.env, LOBSTER_STATE_DIR: path.join(directory, "state") },
				}),
			});
			assert.equal(result.status, "needs_input");
			assert.deepEqual(result.requiresInput!.subject, { value: 42, items: [1] });
		});
	}

	test(`machine ${backend}: child inherits resolved parent environment`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-env-regression-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		await fs.writeFile(
			path.join(directory, "child.lobster"),
			JSON.stringify({
				args: { target: { default: "child" } },
				env: { CHILD: "${target}" },
				steps: [
					{
						id: "show",
						run: `node -e 'console.log(JSON.stringify({parent:process.env.PARENT,child:process.env.CHILD,step:process.env.STEP,source:process.env.SOURCE}))'`,
					},
				],
			}),
		);
		const filePath = path.join(directory, "parent.lobster");
		await fs.writeFile(
			filePath,
			JSON.stringify({
				args: { target: { default: "parent" } },
				env: { PARENT: "${target}" },
				steps: [
					{ id: "data", run: `echo '{"value":42}'` },
					{
						id: "child",
						workflow: "child.lobster",
						env: { STEP: "${target}", SOURCE: "$data.json.value" },
					},
				],
			}),
		);
		const result = await runWorkflowFile({
			filePath,
			ctx: createToolContext({
				env: {
					...process.env,
					LOBSTER_STATE_DIR: path.join(directory, "state"),
				},
			}),
		});
		assert.deepEqual(result.output, [
			{ parent: "parent", child: "child", step: "parent", source: "42" },
		]);
	});

	test(`machine ${backend}: retries preserve activity ID and increment dispatch attempt`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-attempt-regression-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const filePath = path.join(directory, "flow.lobster");
		await fs.writeFile(
			filePath,
			JSON.stringify({
				steps: [{ id: "work", pipeline: "probe", retry: { max: 2, delay_ms: 0 } }],
			}),
		);
		const seen: Array<{ id: string; attempt: string }> = [];
		const ctx = createToolContext({
			env: {
				...process.env,
				LOBSTER_STATE_DIR: path.join(directory, "state"),
			},
		});
		const original = ctx.registry;
		ctx.registry = {
			get: (name) =>
				name === "probe"
					? {
							name: "probe",
							meta: { argsSchema: { type: "object" } },
							async run({ ctx }: { ctx: { env: Record<string, string> } }) {
								seen.push({
									id: ctx.env.LOBSTER_ACTIVITY_ID,
									attempt: ctx.env.LOBSTER_ACTIVITY_ATTEMPT,
								});
								if (seen.length === 1) throw new Error("transient");
								return {
									output: (async function* () {
										yield "ok";
									})(),
								};
							},
						}
					: original.get(name),
		};
		const result = await runWorkflowFile({ filePath, ctx });
		assert.equal(result.status, "ok");
		assert.deepEqual(
			seen.map((entry) => entry.attempt),
			["1", "2"],
		);
		assert.equal(seen[0].id, seen[1].id);
	});
}

test(
	"parallel timeout bounds a command that ignores cancellation",
	{ timeout: 5000 },
	async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-parallel-timeout-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const filePath = path.join(directory, "flow.lobster");
		await fs.writeFile(
			filePath,
			JSON.stringify({
				steps: [
					{
						id: "group",
						parallel: { timeout_ms: 1000, branches: [{ id: "stalled", pipeline: "stall" }] },
					},
				],
			}),
		);
		let release!: () => void;
		let entered = false;
		const stalled = new Promise<void>((resolve) => {
			release = resolve;
		});
		t.after(() => release());
		const ctx = createToolContext({
			env: { ...process.env, LOBSTER_STATE_DIR: path.join(directory, "state") },
		});
		const original = ctx.registry;
		ctx.registry = {
			get: (name) =>
				name === "stall"
					? {
							name: "stall",
							meta: { argsSchema: { type: "object" } },
							async run() {
								entered = true;
								await stalled;
								return {
									output: (async function* () {
										yield "late";
									})(),
								};
							},
						}
					: original.get(name),
		};
		await assert.rejects(
			runWorkflowFile({ filePath, ctx }),
			/did not settle after cancellation|timed out/,
		);
		assert.equal(entered, true, "the uncooperative command must actually start");
		release();
	},
);

test("static child defers a descendant selected by parent output", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-descendant-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "root.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{ id: "data", run: `echo '{"route":"selected"}'` },
				{ id: "child", workflow: "child.lobster", workflow_args: { route: "$data.json.route" } },
			],
		}),
	);
	await fs.writeFile(
		path.join(directory, "child.lobster"),
		JSON.stringify({
			args: { route: { default: "wrong" } },
			steps: [{ id: "grandchild", workflow: "${route}.lobster" }],
		}),
	);
	await fs.writeFile(
		path.join(directory, "selected.lobster"),
		JSON.stringify({ steps: [{ id: "output", run: "echo correct" }] }),
	);
	const result = await runWorkflowFile({
		filePath,
		ctx: createToolContext({
			env: { ...process.env, LOBSTER_STATE_DIR: path.join(directory, "state") },
		}),
	});
	assert.deepEqual(result.output, ["correct\n"]);
});

test("child budget excludes prior parent spending", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-child-budget-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "root.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [paidCall, { id: "child", workflow: "child.lobster" }],
		}),
	);
	await fs.writeFile(
		path.join(directory, "child.lobster"),
		JSON.stringify({
			cost_limit: { max_usd: 0.001, action: "stop" },
			steps: [{ id: "free", run: "echo free" }],
		}),
	);
	const result = await runWorkflowFile({
		filePath,
		ctx: createToolContext({
			env: { ...process.env, LOBSTER_STATE_DIR: path.join(directory, "state") },
			llmAdapters,
		}),
	});
	assert.deepEqual(result.output, ["free\n"]);
	assert.equal(result._meta!.cost!.estimatedCostUsd, 0.0125);
});

for (const decision of ["rejection", "timeout"]) {
	test(
		`interactive ${decision} cannot retry an approval step's command`,
		{ timeout: 5000 },
		async (t) => {
			const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-rejection-"));
			t.after(() => fs.rm(directory, { recursive: true, force: true }));
			const filePath = path.join(directory, "root.lobster");
			await fs.writeFile(
				filePath,
				JSON.stringify({
					steps: [
						{
							id: "work",
							pipeline: "probe",
							approval: { prompt: "Proceed?" },
							retry: { max: 3, delay_ms: 0 },
							on_error: "continue",
						},
						{ id: "later", run: "exit 99" },
					],
				}),
			);
			const stdin = Object.assign(new PassThrough(), { isTTY: true });
			const stdout = new PassThrough();
			t.after(() => {
				stdin.destroy();
				stdout.destroy();
			});
			stdout.on("data", () => {
				if (decision === "rejection") stdin.write("no\n");
			});
			const ctx = createToolContext({
				env: {
					...process.env,
					LOBSTER_STATE_DIR: path.join(directory, "state"),
					LOBSTER_APPROVAL_INPUT_TIMEOUT_MS: "10",
				},
				stdin,
				stdout,
			});
			let calls = 0;
			const original = ctx.registry;
			ctx.registry = {
				get: (name) =>
					name === "probe"
						? {
								name: "probe",
								meta: { argsSchema: { type: "object" } },
								async run() {
									calls++;
									return {
										output: (async function* () {
											yield "subject";
										})(),
									};
								},
							}
						: original.get(name),
			};
			const run = runWorkflowFile({ filePath, ctx: { ...ctx, mode: "human" } });
			if (decision === "rejection") {
				// A "no" cancels the run, as an answered rejection does.
				const result = await run;
				assert.equal(result.status, "cancelled");
				assert.equal(result.reason, "Not approved");
			} else await assert.rejects(run, /Timed out waiting for input/);
			assert.equal(calls, 1);
		},
	);
}

test("a question published earlier and rejected at a terminal leaves nothing to answer", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-terminal-no-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "root.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({ steps: [{ id: "ship", approval: { prompt: "Ship?" } }] }),
	);
	const env = { ...process.env, LOBSTER_STATE_DIR: path.join(directory, "state") };
	const paused = await runWorkflowFile({ filePath, ctx: createToolContext({ env }) });
	assert.equal(paused.status, "needs_approval");
	const token = paused.requiresApproval!.resumeToken;

	const stdin = Object.assign(new PassThrough(), { isTTY: true });
	const stdout = new PassThrough();
	t.after(() => {
		stdin.destroy();
		stdout.destroy();
	});
	stdout.on("data", () => stdin.write("no\n"));
	const ctx = { ...createToolContext({ env, stdin, stdout }), mode: "human" as const };
	const advanced = await advanceWorkflowRun({ runId: paused.runId!, ctx });
	assert.equal(advanced.status, "cancelled");
	assert.equal(advanced.reason, "Not approved");

	const store = await openStoreSession({ env, readOnly: true });
	try {
		const run = await store.getRunState(paused.runId!);
		const statuses = Object.values(run!.waits).map((wait) => wait.status);
		assert.equal(statuses.length, 1);
		assert.notEqual(statuses[0], "pending");
	} finally {
		await store.close();
	}
	const late = await resumeToolRequest({ token, approved: true, ctx: { env } });
	assert.equal(late.ok, false);
});

test("a rejection sent with resume carries its reason", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-resume-no-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "root.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({ steps: [{ id: "ship", approval: { prompt: "Ship?" } }] }),
	);
	const env = { ...process.env, LOBSTER_STATE_DIR: path.join(directory, "state") };
	const paused = await runWorkflowFile({ filePath, ctx: createToolContext({ env }) });
	const token = paused.requiresApproval!.resumeToken;
	const answered = await resumeToolRequest({ token, approved: false, ctx: { env } });
	assert.equal(answered.status, "cancelled");
	const store = await openStoreSession({ env, readOnly: true });
	try {
		assert.equal((await store.getRunState(paused.runId!))!.execution!.error, "Not approved");
	} finally {
		await store.close();
	}
});

test("getWait returns who may approve and what a no does", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-wait-view-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "root.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{
					id: "ship",
					approval: { prompt: "Ship?", required_approver: "alice", on_reject: "continue" },
				},
			],
		}),
	);
	const env = { ...process.env, LOBSTER_STATE_DIR: path.join(directory, "state") };
	const paused = await runWorkflowFile({ filePath, ctx: createToolContext({ env }) });
	const run = await getRun({ runId: paused.runId!, ctx: { env } });
	const wait = await getWait({ waitId: run!.pendingWaits[0].id, ctx: { env } });
	assert.equal(wait!.request?.kind, "approval");
	assert.deepEqual(
		{ ...wait!.request, prompt: undefined, items: undefined },
		{
			kind: "approval",
			prompt: undefined,
			items: undefined,
			requiredApprover: "alice",
			onReject: "continue",
		},
	);
});

test("an answer that makes more than one decision is refused, and the run keeps waiting", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-two-decisions-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "root.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({ steps: [{ id: "ship", approval: { prompt: "Ship?" } }] }),
	);
	const env = { ...process.env, LOBSTER_STATE_DIR: path.join(directory, "state") };
	const paused = await runWorkflowFile({ filePath, ctx: createToolContext({ env }) });
	const token = paused.requiresApproval!.resumeToken;
	const refused = await resumeToolRequest({
		token,
		approved: true,
		cancel: true,
		ctx: { env },
	} as never);
	assert.equal(refused.ok, false);
	const run = await getRun({ runId: paused.runId!, ctx: { env } });
	assert.equal(run!.status, "waiting");
	const answered = await resumeToolRequest({ token, approved: true, ctx: { env } });
	assert.equal(answered.status, "ok");
});

test("an approval refused at the terminal fails only the question: advance asks again", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-wrong-approver-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "root.lobster");
	const count = path.join(directory, "count");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{
					id: "ship",
					run: `echo x >> ${JSON.stringify(count)}`,
					approval: { prompt: "Ship?", required_approver: "alice" },
				},
			],
		}),
	);
	const answering = (approver: string) => {
		const stdin = Object.assign(new PassThrough(), { isTTY: true });
		const stdout = new PassThrough();
		t.after(() => {
			stdin.destroy();
			stdout.destroy();
		});
		stdout.on("data", () => stdin.write("yes\n"));
		const env = {
			...process.env,
			LOBSTER_STATE_DIR: path.join(directory, "state"),
			LOBSTER_APPROVAL_APPROVED_BY: approver,
		};
		return { ...createToolContext({ env, stdin, stdout }), mode: "human" as const };
	};
	let runId: string | undefined;
	await assert.rejects(
		runWorkflowFile({ filePath, ctx: answering("bob") }).catch((error) => {
			runId = runIdOf(error);
			throw error;
		}),
		/requires approver 'alice'/,
	);
	const retried = await advanceWorkflowRun({ runId: runId!, ctx: answering("alice") });
	assert.equal(retried.status, "ok");
	assert.equal((await fs.readFile(count, "utf8")).trim().split("\n").length, 1);
});

test("a bad answer to a pipeline's question fails only the question: advance asks again", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-bad-answer-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "root.lobster");
	const count = path.join(directory, "count");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{
					id: "draft",
					pipeline: `exec --json --shell 'echo x >> ${count}; echo [1]' | ask --emit --prompt Pick --schema '{"type":"integer"}'`,
				},
			],
		}),
	);
	const answering = (answer: string) => {
		const stdin = Object.assign(new PassThrough(), { isTTY: true });
		const stdout = new PassThrough();
		t.after(() => {
			stdin.destroy();
			stdout.destroy();
		});
		stdout.on("data", () => stdin.write(answer + "\n"));
		const env = { ...process.env, LOBSTER_STATE_DIR: path.join(directory, "state") };
		return { ...createToolContext({ env, stdin, stdout }), mode: "human" as const };
	};
	let runId: string | undefined;
	await assert.rejects(
		runWorkflowFile({ filePath, ctx: answering("notjson") }).catch((error) => {
			runId = runIdOf(error);
			throw error;
		}),
	);
	const retried = await advanceWorkflowRun({ runId: runId!, ctx: answering("7") });
	assert.equal(retried.status, "ok", JSON.stringify(retried));
	assert.equal((await fs.readFile(count, "utf8")).trim().split("\n").length, 1);
});

test("a relative cwd resolves against the run's directory, not the caller's", async (t) => {
	const directory = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "lobster-rel-cwd-")));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	await fs.mkdir(path.join(directory, "sub"));
	const filePath = path.join(directory, "flow.lobster");
	await fs.writeFile(filePath, JSON.stringify({ steps: [{ id: "here", cwd: "sub", run: "pwd" }] }));
	// The run's directory is `directory`; this process runs elsewhere (the repository).
	const ctx = createToolContext({
		cwd: directory,
		env: { LOBSTER_STATE_DIR: path.join(directory, "state") },
	});
	const result = await runWorkflowFile({ filePath, ctx });
	assert.equal(result.status, "ok", JSON.stringify(result));
	assert.deepEqual(result.output, [path.join(directory, "sub") + "\n"]);
});

test("the host's onExecutionStarted hook fires once per dispatch that may act", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-started-hook-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const count = async (step: Record<string, unknown>) => {
		const filePath = path.join(directory, `${step.id}.lobster`);
		await fs.writeFile(filePath, JSON.stringify({ steps: [step] }));
		let started = 0;
		const ctx = {
			...createToolContext({ env: { LOBSTER_STATE_DIR: path.join(directory, "state") } }),
			hooks: { onExecutionStarted: () => void started++ },
		};
		await runWorkflowFile({ filePath, ctx });
		return started;
	};
	assert.equal(await count({ id: "shell", run: "echo hi" }), 1);
	assert.equal(await count({ id: "pipe", pipeline: "exec --json --shell 'echo [1]'" }), 1);
	// Asks, runs nothing (in tool mode it pauses).
	assert.equal(await count({ id: "ask", approval: "Go?" }), 0);
});

test("runWorkflowFile reads a relative filePath from the run's directory", async (t) => {
	const directory = await fs.realpath(
		await fs.mkdtemp(path.join(os.tmpdir(), "lobster-rel-file-")),
	);
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	await fs.writeFile(
		path.join(directory, "flow.lobster"),
		JSON.stringify({ steps: [{ id: "s", run: "echo ok" }] }),
	);
	const ctx = createToolContext({
		cwd: directory,
		env: { LOBSTER_STATE_DIR: path.join(directory, "state") },
	});
	const result = await runWorkflowFile({ filePath: "flow.lobster", ctx });
	assert.equal(result.status, "ok", JSON.stringify(result));
	const planned = await runWorkflowFile({
		filePath: "flow.lobster",
		ctx: { ...ctx, dryRun: true },
	});
	assert.equal(planned.status, "ok");
});

test("an answer for a wait ID that another answer consumed first is a wait conflict", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-lost-race-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "flow.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{ id: "ask", approval: "Go?" },
				{ id: "after", run: "echo ok" },
			],
		}),
	);
	const ctx = createToolContext({ env: { LOBSTER_STATE_DIR: path.join(directory, "state") } });
	const asked = await runWorkflowFile({ filePath, ctx });
	assert.equal(asked.status, "needs_approval");
	const { stateKey } = decodeResumeToken(asked.requiresApproval!.resumeToken!);
	const resume = { stateKey, protocolVersion: 1, v: 1, kind: "workflow-file" } as const;
	// The winner answers first.
	assert.equal((await runWorkflowFile({ filePath, ctx, resume, approved: true })).status, "ok");
	// The loser had passed the revision check before that answer committed.
	const session = await openStoreSession({ env: ctx.env });
	t.after(() => session.close());
	const env = bindResumeGuard(bindStoreSession(ctx.env, session), {
		key: stateKey,
		expectedRevision: 0,
	});
	await assert.rejects(
		runWorkflowFile({ filePath, ctx: { ...ctx, env }, resume, approved: true }),
		WaitConflictError,
	);
});

test("a malformed resume or wait lookup is refused without creating the store", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-no-store-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const state = path.join(directory, "state");
	const ctx = { env: { LOBSTER_STATE_DIR: state } };
	for (const request of [
		{ waitId: "abc", approved: true },
		{ waitId: "abc", expectedRevision: 0, approved: true },
		{ token: "not-a-token", approved: true },
		{ expectedRevision: 0, approved: true },
		{ approved: true },
	]) {
		const envelope = await resumeToolRequest({ ...request, ctx } as any);
		assert.equal(envelope.error?.type, "parse_error", JSON.stringify(request));
	}
	await assert.rejects(getWait({ waitId: "bad", ctx }), /Invalid wait ID/);
	await assert.rejects(fs.access(state), "nothing was written");
});

test("runToolRequest refuses args it would drop or misread, as the CLI does", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-tool-args-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "flow.lobster");
	await fs.writeFile(filePath, JSON.stringify({ steps: [{ id: "s", run: "echo ok" }] }));
	const ctx = { env: { LOBSTER_STATE_DIR: path.join(directory, "state") } };
	for (const request of [
		{ filePath, args: "abc" },
		{ filePath, args: ["z"] },
		{ pipeline: "exec --json --shell 'echo [1]'", args: { n: 1 } },
	]) {
		const envelope = await runToolRequest({ ...request, ctx } as any);
		assert.equal(envelope.error?.type, "parse_error", JSON.stringify(request));
	}
	// Checked before a run is created: a read-only store gets the same refusal.
	const readOnly = await openStoreSession({ env: ctx.env, readOnly: true });
	t.after(() => readOnly.close());
	const refused = await runToolRequest({
		pipeline: "json",
		args: 5 as any,
		ctx: { storeSession: readOnly },
	});
	assert.equal(refused.error?.type, "parse_error", JSON.stringify(refused));
});

test("an approval ID is checked, not cleaned up into another one", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-approval-id-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const ctx = { env: { LOBSTER_STATE_DIR: path.join(directory, "state") } };
	for (const approvalId of ["zz-f446-7885", "ABCD1234", "abc"]) {
		const envelope = await resumeToolRequest({ approvalId, approved: true, ctx });
		assert.equal(envelope.error?.type, "parse_error", approvalId);
		assert.match(envelope.error?.message ?? "", /Invalid approval ID/);
	}
});

test("a resume with more than one decision is refused before any store work, whatever the reference", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-two-decisions-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const state = path.join(directory, "state");
	const ctx = { env: { LOBSTER_STATE_DIR: state } };
	for (const reference of [{ token: "not-checked" }, { approvalId: "abcd1234" }]) {
		const envelope = await resumeToolRequest({
			...reference,
			approved: true,
			cancel: true,
			ctx,
		} as any);
		assert.equal(envelope.error?.type, "parse_error");
		assert.match(
			envelope.error?.message ?? "",
			/resume takes only one of approved, response, or cancel/,
		);
	}
	await assert.rejects(fs.access(state), "nothing was written");
});

test("a pipeline's approve answered at a terminal is the engine's gate: shown, and a no cancels", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-pipeline-gate-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "flow.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{
					id: "send",
					pipeline: `exec --json --shell 'echo "[1]"' | approve --prompt Send?`,
					on_error: "continue",
				},
			],
		}),
	);
	const env = {
		...process.env,
		LOBSTER_STATE_DIR: path.join(directory, "state"),
		// A prompt nobody sees is never answered: fail rather than wait for good.
		LOBSTER_APPROVAL_INPUT_TIMEOUT_MS: "5000",
	};
	const answering = async (answer: string) => {
		const stdin = Object.assign(new PassThrough(), { isTTY: true });
		const stdout = new PassThrough();
		t.after(() => {
			stdin.destroy();
			stdout.destroy();
		});
		let shown = "";
		stdout.on("data", (chunk) => {
			shown += String(chunk);
			if (shown.includes("[y/N]")) stdin.write(`${answer}\n`);
		});
		const ctx = { ...createToolContext({ env, stdin, stdout }), mode: "human" as const };
		return { result: await runWorkflowFile({ filePath, ctx }), shown };
	};

	const yes = await answering("y");
	assert.match(yes.shown, /Send\? \[y\/N\]/, "the prompt is shown to the person");
	assert.equal(yes.result.status, "ok", JSON.stringify(yes.result));
	assert.deepEqual(yes.result.output, [1], "the items, not the prompt, are the step's output");

	const no = await answering("no");
	assert.equal(no.result.status, "cancelled", JSON.stringify(no.result));
	assert.equal(no.result.reason, "Not approved");
});

/** Run `workflow` in human mode, typing `answer` at a terminal once the prompt shows `cue`. */
async function atTerminal(t: test.TestContext, workflow: unknown, answer: string, cue: string) {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-terminal-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "flow.lobster");
	await fs.writeFile(filePath, JSON.stringify(workflow));
	const env = {
		...process.env,
		LOBSTER_STATE_DIR: path.join(directory, "state"),
		TERMINAL_DIR: directory,
		LOBSTER_APPROVAL_INPUT_TIMEOUT_MS: "5000",
	};
	const stdin = Object.assign(new PassThrough(), { isTTY: true });
	const stdout = new PassThrough();
	t.after(() => {
		stdin.destroy();
		stdout.destroy();
	});
	let shown = "";
	stdout.on("data", (chunk) => {
		const before = shown.includes(cue);
		shown += String(chunk);
		if (!before && shown.includes(cue)) stdin.write(`${answer}\n`);
	});
	const ctx = { ...createToolContext({ env, stdin, stdout }), mode: "human" as const };
	const result = await runWorkflowFile({ filePath, ctx }).catch((error: Error) => error);
	const count = async (name: string) =>
		(await fs.readFile(path.join(directory, name), "utf8").catch(() => ""))
			.split("\n")
			.filter(Boolean).length;
	return { result, shown, count };
}

test("an answer typed at a terminal that does not fit stops the run; retry and on_error do not apply", async (t) => {
	const input = await atTerminal(
		t,
		{
			steps: [
				{
					id: "ask",
					input: { prompt: "Count?", responseSchema: { type: "object" } },
					on_error: "continue",
				},
				{ id: "later", run: 'echo x >> "$TERMINAL_DIR/later"' },
			],
		},
		"oops",
		"Enter JSON response:",
	);
	assert.match(String((input.result as Error).message), /Input response must be valid JSON/);
	assert.equal(await input.count("later"), 0, "nobody answered, so the run did not go on");

	const pipeline = await atTerminal(
		t,
		{
			steps: [
				{
					id: "decide",
					pipeline: `exec --json --shell 'echo x >> "$TERMINAL_DIR/ran"; echo "[1]"' | ask --prompt Decide? --schema '{"type":"number"}'`,
					retry: { max: 2, delay_ms: 0 },
				},
			],
		},
		"oops",
		"Decide?",
	);
	assert.match(String((pipeline.result as Error).message), /ask response|must be number/);
	assert.equal(await pipeline.count("ran"), 1, "the stage before the question did not run again");
});

test("a pipeline's ask in a run takes a typed answer as text, as ask does", async (t) => {
	const { result, shown } = await atTerminal(
		t,
		{
			steps: [
				{
					id: "decide",
					pipeline: `exec --json --shell 'echo "[1]"' | ask --prompt Decide?`,
				},
			],
		},
		"approve",
		"Decide?",
	);
	assert.match(shown, /Decide\?\n> /);
	assert.ok(!(result instanceof Error), String(result));
	assert.equal(result.status, "ok", JSON.stringify(result));
	assert.deepEqual(result.output, [{ decision: "approve" }]);
});

test("a typo answering an approval at a terminal fails only the question, as resume refuses it", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-approval-typo-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "flow.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{ id: "make", run: `echo x >> "${path.join(directory, "made")}"; echo ok` },
				{ id: "ship", approval: { prompt: "Ship?" } },
			],
		}),
	);
	const env = {
		...process.env,
		LOBSTER_STATE_DIR: path.join(directory, "state"),
		LOBSTER_APPROVAL_INPUT_TIMEOUT_MS: "5000",
	};
	const typing = (answer: string) => {
		const stdin = Object.assign(new PassThrough(), { isTTY: true });
		const stdout = new PassThrough();
		t.after(() => {
			stdin.destroy();
			stdout.destroy();
		});
		stdout.on("data", (chunk) => {
			if (String(chunk).includes("[y/N]")) stdin.write(`${answer}\n`);
		});
		return { ...createToolContext({ env, stdin, stdout }), mode: "human" as const };
	};

	const typo = await runWorkflowFile({ filePath, ctx: typing("ys") }).catch(
		(error: Error) => error,
	);
	assert.ok(typo instanceof Error);
	assert.match(typo.message, /Answer yes or no \(got "ys"\)/);
	const runId = runIdOf(typo)!;
	assert.ok(runId, "the failure names its run");

	const retried = await advanceWorkflowRun({ runId, ctx: typing("y") });
	assert.equal(retried.status, "ok", JSON.stringify(retried));
	const made = (await fs.readFile(path.join(directory, "made"), "utf8")).trim().split("\n");
	assert.equal(made.length, 1, "the step before the question did not run again");
});

test("the host's approver rules apply to an approve inside a pipeline, as to a step approval", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-pipeline-approver-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const env = {
		...process.env,
		LOBSTER_STATE_DIR: path.join(directory, "state"),
		LOBSTER_APPROVAL_REQUIRED_APPROVER: "alice",
	};
	const paused = await runToolRequest({
		pipeline: "exec --json --shell 'echo [1]' | approve --prompt Ship?",
		ctx: { env },
	});
	assert.equal(paused.status, "needs_approval", JSON.stringify(paused));
	assert.equal(paused.requiresApproval!.requiredApprover, "alice");
	const byBob = await resumeToolRequest({
		token: paused.requiresApproval!.resumeToken,
		approved: true,
		ctx: { env: { ...env, LOBSTER_APPROVAL_APPROVED_BY: "bob" } },
	});
	assert.equal(byBob.ok, false);
	// A standalone pipeline has one step: the message does not name it.
	assert.match(byBob.error!.message, /^Approval requires approver 'alice', got 'bob'/);
	const byAlice = await resumeToolRequest({
		token: paused.requiresApproval!.resumeToken,
		approved: true,
		ctx: { env: { ...env, LOBSTER_APPROVAL_APPROVED_BY: "alice" } },
	});
	assert.equal(byAlice.status, "ok", JSON.stringify(byAlice));
});

test("a standalone pipeline's messages do not name a workflow step", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-standalone-names-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const env = {
		...process.env,
		LOBSTER_STATE_DIR: path.join(directory, "state"),
		LOBSTER_MAX_TOOL_ENVELOPE_BYTES: "1024",
	};
	const result = await runToolRequest({
		pipeline: `ask --prompt '${"x".repeat(1500)}'`,
		ctx: { env },
	});
	assert.equal(result.ok, false);
	assert.match(result.error!.message, /^Input needs_input envelope exceeds 1024 bytes/);
});

test("another caller's approval ID is refused like an unknown one", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-foreign-approval-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const env = { ...process.env, LOBSTER_STATE_DIR: path.join(directory, "state") };
	const as = (callerId: string) => ({ env, runMetadata: { callerId } });
	const paused = await runToolRequest({
		pipeline: "exec --json --shell 'echo [1]' | approve --prompt Ship?",
		ctx: as("a"),
	});
	assert.equal(paused.status, "needs_approval", JSON.stringify(paused));
	const approvalId = paused.requiresApproval!.approvalId!;
	const foreign = await resumeToolRequest({ approvalId, approved: true, ctx: as("b") });
	const unknown = await resumeToolRequest({ approvalId: "0badc0de", approved: true, ctx: as("b") });
	assert.equal(foreign.error?.type, unknown.error?.type);
	assert.equal(
		foreign.error?.message,
		unknown.error?.message.replace("0badc0de", approvalId),
		"nothing tells the two apart",
	);
	const owner = await resumeToolRequest({ approvalId, approved: true, ctx: as("a") });
	assert.equal(owner.status, "ok", JSON.stringify(owner));
});

test("workflow args a run cannot save are refused before the run exists", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-unsavable-args-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "flow.lobster");
	await fs.writeFile(filePath, JSON.stringify({ steps: [{ id: "a", run: "true" }] }));
	const env = { ...process.env, LOBSTER_STATE_DIR: path.join(directory, "state") };
	await assert.rejects(
		runWorkflowFile({ filePath, args: { x: 1n }, ctx: createToolContext({ env }) }),
		/Workflow args cannot be saved/,
	);
	const store = await openStoreSession({ env, readOnly: true });
	try {
		assert.deepEqual((await store.listRuns()).runs, [], "no run is left behind");
	} finally {
		await store.close();
	}
});

test("step output shaped like a saved-record reference comes back as it was", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-record-shaped-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "flow.lobster");
	const shaped = { "$lobster.record": "value/nope" };
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{ id: "a", run: `echo '${JSON.stringify(shaped)}'` },
				{ id: "gate", approval: true },
				{ id: "b", run: "cat", stdin: "$a.json" },
			],
		}),
	);
	const env = { ...process.env, LOBSTER_STATE_DIR: path.join(directory, "state") };
	const paused = await runToolRequest({ filePath, ctx: { env } });
	assert.equal(paused.status, "needs_approval", JSON.stringify(paused));
	const resumed = await resumeToolRequest({
		token: paused.requiresApproval!.resumeToken,
		approved: true,
		ctx: { env },
	});
	assert.equal(resumed.status, "ok", JSON.stringify(resumed));
	assert.deepEqual(resumed.output, [shaped]);
});

test("a resume saves a large output's record once, never a record of its reference", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-records-resume-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "flow.lobster");
	const big = { text: "x".repeat(2000) };
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{ id: "big", run: `echo '${JSON.stringify(big)}'` },
				{ id: "one", approval: "First?" },
				{ id: "two", approval: "Second?" },
				{ id: "use", run: "cat", stdin: "$big.json" },
			],
		}),
	);
	const env = { ...process.env, LOBSTER_STATE_DIR: path.join(directory, "state") };
	const valueRecords = async (runId: string) => {
		const store = await openStoreSession({ env, readOnly: true });
		try {
			return (await store.readRecords(runId, "value/")).map((row) => JSON.parse(row.text).value);
		} finally {
			await store.close();
		}
	};
	const first = await runToolRequest({ filePath, ctx: { env } });
	assert.equal(first.status, "needs_approval", JSON.stringify(first));
	const before = await valueRecords(first.runId!);
	const second = await resumeToolRequest({
		token: first.requiresApproval!.resumeToken,
		approved: true,
		ctx: { env },
	});
	assert.equal(second.status, "needs_approval", JSON.stringify(second));
	const after = await valueRecords(first.runId!);
	assert.equal(after.length, before.length, "the resume wrote no new record");
	assert.ok(
		after.every((value) => !(value && typeof value === "object" && "$lobster.record" in value)),
		"no record holds a reference",
	);
	const done = await resumeToolRequest({
		token: second.requiresApproval!.resumeToken,
		approved: true,
		ctx: { env },
	});
	assert.deepEqual(done.output, [big]);
});

test("a host advances a run with the context it gives runToolRequest", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-host-advance-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "flow.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [
				{ id: "nap", sleep: "1s" },
				{ id: "next", pipeline: "exec --json --shell 'echo [1]'" },
			],
		}),
	);
	const env = {
		...process.env,
		LOBSTER_STATE_DIR: path.join(directory, "state"),
		LOBSTER_INLINE_SLEEP_MS: "0",
	};
	const waiting = await runToolRequest({ filePath, ctx: { env } });
	assert.equal(waiting.status, "waiting", JSON.stringify(waiting));
	await new Promise((resolve) => setTimeout(resolve, 1100));
	const advanced = await core.advanceWorkflowRun({ runId: waiting.runId!, ctx: { env } });
	assert.equal(advanced.status, "ok", JSON.stringify(advanced));
	assert.deepEqual(advanced.output, [1]);
});

test("a workflow refuses an arg it does not take, and a required arg not given", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-args-check-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const marker = path.join(directory, "ran");
	const filePath = path.join(directory, "flow.lobster");
	await fs.writeFile(
		filePath,
		JSON.stringify({
			args: { email: { description: "who" }, tone: { default: "kind" } },
			steps: [{ id: "send", run: `echo "\${email}" > ${JSON.stringify(marker)}` }],
		}),
	);
	const env = { ...process.env, LOBSTER_STATE_DIR: path.join(directory, "state") };
	const typo = await runToolRequest({ filePath, args: { emial: "x" }, ctx: { env } });
	assert.match(typo.error?.message ?? "", /takes no arg "emial" \(did you mean "email"\?\)/);
	const missing = await runToolRequest({ filePath, ctx: { env } });
	assert.match(missing.error?.message ?? "", /requires arg "email"/);
	await assert.rejects(fs.access(marker), "no step ran");
	const ok = await runToolRequest({ filePath, args: { email: "a@b" }, ctx: { env } });
	assert.equal(ok.status, "ok", JSON.stringify(ok));

	const parent = path.join(directory, "parent.lobster");
	await fs.writeFile(
		parent,
		JSON.stringify({
			steps: [{ id: "kid", workflow: "flow.lobster", workflow_args: { emial: "x" } }],
		}),
	);
	const child = await runToolRequest({ filePath: parent, ctx: { env } });
	assert.match(child.error?.message ?? "", /takes no arg "emial"/);
});

test("a host's store handle checks a run listing as core's listRuns does", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-handle-list-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const store = await core.openStoreSession({ directory });
	t.after(() => store.close());
	await assert.rejects(store.listRuns({ status: "bogus" as never }), /Invalid run status/);
	await assert.rejects(store.listRuns({ callerId: "" }), /Invalid caller ID/);
	assert.deepEqual((await store.listRuns()).runs, []);
});

test("an approval a step's output asks for with two spellings of a setting names the step", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-approval-spelling-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "flow.lobster");
	const asked = { requiresApproval: { prompt: "Ship?", initiated_by: "a", initiatedBy: "b" } };
	await fs.writeFile(
		filePath,
		JSON.stringify({
			steps: [{ id: "ship", run: `echo '${JSON.stringify(asked)}'`, approval: "required" }],
		}),
	);
	await assert.rejects(
		runWorkflowFile({
			filePath,
			ctx: createToolContext({
				env: { ...process.env, LOBSTER_STATE_DIR: path.join(directory, "state") },
			}),
		}),
		/Workflow step ship: its output asking for approval takes initiated_by or initiatedBy, not both/,
	);
});
