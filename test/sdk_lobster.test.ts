import test from "node:test";
import assert from "node:assert/strict";
import { promises as fsp } from "node:fs";
import os from "node:os";
import path from "node:path";

import { Lobster } from "../src/sdk/Lobster.js";
import { approve } from "../src/sdk/primitives/approve.js";
import { advanceDueRuns } from "../src/workflows/file.js";
import { resumeToolRequest } from "../src/core/tool_runtime.js";
import { stateSet } from "../src/sdk/primitives/state.js";
import { openStoreSession } from "../src/persistence/session.js";
import { decodeToken, encodeToken } from "../src/resume/token.js";

test("sdk Lobster.resume accepts structured input responses", async () => {
	const workflow = new Lobster().pipe({
		async run() {
			return {
				halt: true,
				output: (async function* () {
					yield {
						type: "input_request",
						prompt: "Decision?",
						responseSchema: {
							type: "object",
							properties: { decision: { type: "string", enum: ["approve", "reject"] } },
							required: ["decision"],
						},
						subject: { text: "draft v1" },
					};
				})(),
			};
		},
	});

	const first = await workflow.run();
	assert.equal(first.ok, true);
	assert.equal(first.status, "needs_input");
	assert.ok(first.requiresInput?.resumeToken);

	const resumed = await workflow.resume(first.requiresInput!.resumeToken, {
		response: { decision: "approve" },
	});
	assert.equal(resumed.ok, true);
	assert.equal(resumed.status, "ok");
	assert.deepEqual(resumed.output, [{ decision: "approve" }]);
});

test("sdk Lobster.resume rejects invalid structured input responses", async () => {
	const workflow = new Lobster().pipe({
		async run() {
			return {
				halt: true,
				output: (async function* () {
					yield {
						type: "input_request",
						prompt: "Decision?",
						responseSchema: {
							type: "object",
							properties: { decision: { type: "string", enum: ["approve", "reject"] } },
							required: ["decision"],
						},
						subject: { text: "draft v1" },
					};
				})(),
			};
		},
	});

	const first = await workflow.run();
	assert.equal(first.status, "needs_input");
	await assert.rejects(
		() => workflow.resume(first.requiresInput!.resumeToken, { response: { decision: "maybe" } }),
		/response failed schema validation at \/decision/,
	);
});

test("sdk Lobster.resume replays command-level requestInput with a fresh instance", async () => {
	const stateDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-sdk-input-"));
	let runs = 0;
	const schema = {
		type: "object",
		properties: { choice: { type: "string", enum: ["red", "blue"] } },
		required: ["choice"],
	};
	const choose = {
		async run({ input, ctx }: any) {
			runs += 1;
			const items = [];
			for await (const item of input) items.push(item);
			const response = await ctx.requestInput({
				prompt: "Pick a color",
				responseSchema: schema,
				suspendedState: { count: items.length },
			});
			return { output: [{ runs, items, choice: response.choice }] };
		},
	};
	const createWorkflow = () => new Lobster({ stateDir }).pipe(choose);

	const first = await createWorkflow().run([{ id: 1 }]);
	assert.equal(first.ok, true);
	assert.equal(first.status, "needs_input");
	assert.ok(first.requiresInput?.resumeToken);

	// The token only names the paused run's saved state.
	const payload = decodeToken(first.requiresInput.resumeToken) as any;
	assert.deepEqual(Object.keys(payload).sort(), ["kind", "protocolVersion", "stateKey", "v"]);

	const resumed = await createWorkflow().resume(first.requiresInput.resumeToken, {
		response: { choice: "red" },
	});
	assert.equal(resumed.ok, true);
	assert.equal(resumed.status, "ok");
	assert.deepEqual(resumed.output, [{ runs: 2, items: [{ id: 1 }], choice: "red" }]);
});

test("sdk Lobster.resume rejects command-level requestInput tampering and metadata drift", async () => {
	const stateDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-sdk-input-tamper-"));
	const schema = {
		type: "object",
		properties: { choice: { type: "string" } },
		required: ["choice"],
	};
	let prompt = "Original";
	const workflow = new Lobster({ stateDir }).pipe({
		async run({ ctx }: any) {
			const response = await ctx.requestInput({ prompt, responseSchema: schema });
			return { output: [response] };
		},
	});

	const first = await workflow.run();
	assert.equal(first.status, "needs_input");
	const payload = decodeToken(first.requiresInput!.resumeToken) as any;
	payload.stateKey = "workflow_resume_forged";
	const forged = encodeToken(payload);

	await assert.rejects(
		() => workflow.resume(forged, { response: { choice: "red" } }),
		/Resume state not found/,
	);

	prompt = "Changed";
	const resumed = await workflow.resume(first.requiresInput!.resumeToken, {
		response: { choice: "red" },
	});
	assert.equal(resumed.ok, false);
	assert.equal(resumed.status, "error");
	assert.match(resumed.error?.message ?? "", /does not match suspended request/);
});

test("sdk Lobster stores chained requestInput history outside the resume token", async () => {
	const stateDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-sdk-input-chain-"));
	const schema = {
		type: "object",
		properties: { choice: { type: "string", enum: ["red", "blue"] } },
		required: ["choice"],
	};
	const createWorkflow = () =>
		new Lobster({ stateDir }).pipe({
			async run({ ctx }: any) {
				const first = await ctx.requestInput({ prompt: "First", responseSchema: schema });
				const second = await ctx.requestInput({
					prompt: `Second after ${first.choice}`,
					responseSchema: schema,
				});
				return { output: [{ first: first.choice, second: second.choice }] };
			},
		});

	const first = await createWorkflow().run();
	assert.equal(first.status, "needs_input");
	const second = await createWorkflow().resume(first.requiresInput!.resumeToken, {
		response: { choice: "red" },
	});
	assert.equal(second.status, "needs_input");

	const payload = decodeToken(second.requiresInput!.resumeToken) as any;
	assert.deepEqual(Object.keys(payload).sort(), ["kind", "protocolVersion", "stateKey", "v"]);

	const final = await createWorkflow().resume(second.requiresInput!.resumeToken, {
		response: { choice: "blue" },
	});
	assert.equal(final.ok, true);
	assert.equal(final.status, "ok");
	assert.deepEqual(final.output, [{ first: "red", second: "blue" }]);
});

test("sdk Lobster preserves custom stateDir while using core runtime", async () => {
	const stateDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-sdk-state-dir-"));
	const workflow = new Lobster({ stateDir })
		.pipe(() => ({ saved: true }))
		.pipe(stateSet("custom-key"));

	const result = await workflow.run();

	assert.equal(result.ok, true);
	assert.equal(result.status, "ok");
	assert.deepEqual(result.output, [{ saved: true }]);
	const store = await openStoreSession({ directory: stateDir, readOnly: true });
	try {
		assert.equal((await store.read("custom-key")).text, '{\n  "saved": true\n}\n');
	} finally {
		await store.close();
	}
});

async function sdkStateDir(t: test.TestContext) {
	const stateDir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-sdk-run-"));
	t.after(() => fsp.rm(stateDir, { recursive: true, force: true }));
	return stateDir;
}

function approvalPipeline(stateDir: string, sent: unknown[]) {
	return new Lobster({ stateDir })
		.pipe(() => [{ to: "a@example.com" }])
		.pipe(approve({ prompt: "Send?" }))
		.pipe((items: unknown[]) => {
			sent.push(...items);
			return items;
		});
}

test("an SDK pipeline is a run: its token resumes once", async (t) => {
	const stateDir = await sdkStateDir(t);
	const sent: unknown[] = [];
	const first = await approvalPipeline(stateDir, sent).run();
	assert.equal(first.status, "needs_approval");
	assert.ok(first.runId);
	const store = await openStoreSession({ directory: stateDir, readOnly: true });
	try {
		assert.equal((await store.getRunState(first.runId!))!.kind, "sdk");
	} finally {
		await store.close();
	}

	// A fresh instance with the same stages continues the run.
	const token = first.requiresApproval!.resumeToken;
	const done = await approvalPipeline(stateDir, sent).resume(token, { approved: true });
	assert.equal(done.status, "ok");
	assert.deepEqual(sent, [{ to: "a@example.com" }]);
	await assert.rejects(
		() => approvalPipeline(stateDir, sent).resume(token, { approved: true }),
		/Resume state not found/,
	);
	assert.equal(sent.length, 1, "a used token never acts again");
});

test("an SDK run is resumed only by an instance with the same stages", async (t) => {
	const stateDir = await sdkStateDir(t);
	const sent: unknown[] = [];
	const first = await approvalPipeline(stateDir, sent).run();
	const token = first.requiresApproval!.resumeToken;
	const shorter = new Lobster({ stateDir }).pipe(() => []).pipe(approve({ prompt: "Send?" }));
	await assert.rejects(
		() => shorter.resume(token, { approved: true }),
		/has 2 stages; the paused run has 3/,
	);
	// Refusing changed nothing: the right instance still resumes it.
	const done = await approvalPipeline(stateDir, sent).resume(token, { approved: true });
	assert.equal(done.status, "ok");
	assert.equal(sent.length, 1);
});

test("SDK runs are not resumed or advanced without their stages", async (t) => {
	const stateDir = await sdkStateDir(t);
	const sent: unknown[] = [];
	const first = await approvalPipeline(stateDir, sent).run();
	const env = { LOBSTER_STATE_DIR: stateDir };
	const tool = await resumeToolRequest({
		token: first.requiresApproval!.resumeToken,
		approved: true,
		ctx: { env },
	});
	assert.equal(tool.ok, false);
	assert.match(tool.error?.message ?? "", /started by the Lobster SDK/);
	const due = await advanceDueRuns({
		ctx: {
			env,
			stdin: process.stdin,
			stdout: process.stdout,
			stderr: process.stderr,
			mode: "tool",
		},
	});
	assert.deepEqual(due.due, []);
	const done = await approvalPipeline(stateDir, sent).resume(first.requiresApproval!.resumeToken, {
		approved: true,
	});
	assert.equal(done.status, "ok");
});

test("tokens from older SDK versions are honoured once, then refused", async (t) => {
	const stateDir = await sdkStateDir(t);
	const sent: unknown[] = [];
	// What older versions handed out: the whole pause inside the token.
	const approvalToken = encodeToken({
		protocolVersion: 1,
		v: 1,
		stageIndex: 1,
		resumeAtIndex: 2,
		items: [{ to: "b@example.com" }],
		prompt: "Send?",
	});
	const approved = await approvalPipeline(stateDir, sent).resume(approvalToken, {
		approved: true,
	});
	assert.equal(approved.status, "ok", JSON.stringify(approved));
	assert.deepEqual(sent, [{ to: "b@example.com" }]);
	await assert.rejects(
		() => approvalPipeline(stateDir, sent).resume(approvalToken, { approved: true }),
		/Resume state not found/,
	);
	assert.equal(sent.length, 1);

	const inputToken = encodeToken({
		protocolVersion: 1,
		v: 1,
		stageIndex: 0,
		resumeAtIndex: 1,
		resumeMode: "next_stage",
		items: [],
		inputSchema: { type: "string" },
	});
	const echo = () => new Lobster({ stateDir }).pipe(() => []).pipe((items: unknown[]) => items);
	await assert.rejects(() => echo().resume(inputToken, { response: 7 }), /schema validation/);
	// A refused answer does not use the token up.
	const answered = await echo().resume(inputToken, { response: "hi" });
	assert.deepEqual(answered.output, ["hi"]);
	await assert.rejects(() => echo().resume(inputToken, { response: "hi" }), /not found/);
});

test("an SDK input pause saved by an older version continues the same stage once", async (t) => {
	const stateDir = await sdkStateDir(t);
	// Keys in sorted order, as the saved request signature has them.
	const schema = { properties: { choice: { type: "string" } }, type: "object" };
	// Saved by older versions under its own key, imported by `lobster upgrade` as it is.
	const key = "sdk_resume_3f0c2a1e-0000-4000-8000-000000000001";
	const store = await openStoreSession({ directory: stateDir });
	try {
		await store.write(
			key,
			JSON.stringify({
				resumeAtIndex: 0,
				items: [{ id: 1 }],
				inputSchema: schema,
				commandInput: {
					pending: {
						requestIndex: 0,
						metadata: {
							prompt: "Pick",
							responseSchema: schema,
							signature: JSON.stringify({ prompt: "Pick", responseSchema: schema }),
						},
					},
					history: [],
				},
			}),
		);
	} finally {
		await store.close();
	}
	const token = encodeToken({
		protocolVersion: 1,
		v: 1,
		stageIndex: 0,
		resumeAtIndex: 0,
		resumeMode: "same_stage",
		stateKey: key,
	});
	let runs = 0;
	const choose = () =>
		new Lobster({ stateDir }).pipe({
			async run({ input, ctx }: any) {
				runs += 1;
				const items = [];
				for await (const item of input) items.push(item);
				const response = await ctx.requestInput({ prompt: "Pick", responseSchema: schema });
				return { output: [{ items, choice: response.choice }] };
			},
		});
	const resumed = await choose().resume(token, { response: { choice: "red" } });
	assert.equal(resumed.status, "ok", JSON.stringify(resumed));
	assert.deepEqual(resumed.output, [{ items: [{ id: 1 }], choice: "red" }]);
	await assert.rejects(() => choose().resume(token, { response: { choice: "red" } }), /not found/);
	assert.equal(runs, 1);
});

test("an older SDK token refused by the wrong instance still resumes on the right one", async (t) => {
	const stateDir = await sdkStateDir(t);
	const sent: unknown[] = [];
	const token = encodeToken({
		protocolVersion: 1,
		v: 1,
		stageIndex: 1,
		resumeAtIndex: 2,
		items: [{ to: "c@example.com" }],
		prompt: "Send?",
	});
	// One stage short: refused, and the token is not used up by the refusal.
	const short = new Lobster({ stateDir }).pipe(() => []);
	await assert.rejects(() => short.resume(token, { approved: true }), /stages/);
	const approved = await approvalPipeline(stateDir, sent).resume(token, { approved: true });
	assert.equal(approved.status, "ok", JSON.stringify(approved));
	assert.deepEqual(sent, [{ to: "c@example.com" }]);
	// Two resumes at once: one runs it, the other is refused, never both.
	const twice = encodeToken({
		protocolVersion: 1,
		v: 1,
		stageIndex: 1,
		resumeAtIndex: 2,
		items: [{ to: "d@example.com" }],
		prompt: "Send?",
	});
	const outcomes = await Promise.allSettled([
		approvalPipeline(stateDir, sent).resume(twice, { approved: true }),
		approvalPipeline(stateDir, sent).resume(twice, { approved: true }),
	]);
	assert.equal(sent.filter((item: any) => item.to === "d@example.com").length, 1);
	assert.equal(
		outcomes.filter((o) => o.status === "fulfilled" && o.value.status === "ok").length,
		1,
	);
});
