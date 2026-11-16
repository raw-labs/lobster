import test from "node:test";
import assert from "node:assert/strict";
import { promises as fsp } from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";

import { decodeResumeToken, resumeToolRequest, runToolRequest } from "../src/core/index.js";
import { openStoreSession } from "../src/persistence/session.js";
import { SqliteBackend } from "../src/persistence/sqlite.js";

/** state.set still serializes on a lock directory next to the key. */
function pauseStateLock(lockPath: string) {
	let release!: () => void;
	let started!: () => void;
	const gate = new Promise<void>((resolve) => {
		release = resolve;
	});
	const lockStarted = new Promise<void>((resolve) => {
		started = resolve;
	});
	const originalMkdir = fsp.mkdir;
	let paused = false;
	Object.defineProperty(fsp, "mkdir", {
		configurable: true,
		writable: true,
		async value(
			pathArg: Parameters<typeof fsp.mkdir>[0],
			options?: Parameters<typeof fsp.mkdir>[1],
		) {
			if (!paused && String(pathArg) === lockPath) {
				paused = true;
				started();
				await gate;
			}
			return originalMkdir(pathArg, options);
		},
	});
	return {
		lockStarted,
		release,
		restore() {
			Object.defineProperty(fsp, "mkdir", {
				configurable: true,
				writable: true,
				value: originalMkdir,
			});
		},
	};
}

type Gate = { started: Promise<void>; release: () => void; restore: () => void };

/**
 * Pause the first SQLite backend call that matches: "before" holds it just before it
 * is sent to the database, "after" holds it once it has committed.
 */
function pauseBackend(
	method: "read" | "write",
	match: (key: string, text?: string | null) => boolean,
	when: "before" | "after",
): Gate {
	let release!: () => void;
	let started!: () => void;
	const gate = new Promise<void>((resolve) => {
		release = resolve;
	});
	const begun = new Promise<void>((resolve) => {
		started = resolve;
	});
	const proto = SqliteBackend.prototype as unknown as Record<
		string,
		(...args: unknown[]) => unknown
	>;
	const original = proto[method];
	let paused = false;
	proto[method] = async function (this: unknown, ...args: unknown[]) {
		const hit = !paused && match(String(args[0]), args[1] as string | null | undefined);
		if (hit) paused = true;
		if (hit && when === "before") {
			started();
			await gate;
		}
		const result = await original.apply(this, args);
		if (hit && when === "after") {
			started();
			await gate;
		}
		return result;
	};
	return {
		started: begun,
		release,
		restore() {
			proto[method] = original;
		},
	};
}

const consumedMarker = (text?: string | null) =>
	typeof text === "string" && text.includes("lobster.consumed-resume-state.v1");

/**
 * Every value saved outside a run, read through the store. A short approval ID is
 * compared by the state it points to: a restored ID gets a new creation time.
 */
async function storeSnapshot(stateDir: string): Promise<Record<string, string>> {
	const store = await openStoreSession({ directory: stateDir, readOnly: true });
	try {
		const { states } = await store.backend.export();
		return Object.fromEntries(
			Object.entries(states).map(([key, text]) => [
				key,
				key.startsWith("approval_") ? JSON.parse(text).stateKey : text,
			]),
		);
	} finally {
		await store.close();
	}
}

async function storedText(stateDir: string, key: string): Promise<string | null> {
	const store = await openStoreSession({ directory: stateDir, readOnly: true });
	try {
		return (await store.read(key)).text;
	} finally {
		await store.close();
	}
}

async function waitForGate(gate: Promise<void>) {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		await Promise.race([
			gate,
			new Promise<never>((_resolve, reject) => {
				timer = setTimeout(() => reject(new Error("authority test gate timed out")), 2_000);
			}),
		]);
	} finally {
		if (timer) clearTimeout(timer);
	}
}

function retireAfterDispatch() {
	let current = true;
	let checks = 0;
	return {
		assertInvocationCurrent() {
			checks++;
			if (!current) throw new Error("host invocation retired");
			if (checks === 1) queueMicrotask(() => (current = false));
		},
		get checks() {
			return checks;
		},
	};
}

function resumeStateKey(token: string): string {
	const stateKey = decodeResumeToken(token).stateKey;
	assert.ok(stateKey, "expected a persisted resume state");
	return stateKey;
}

test("approved pipeline rechecks host authority after awaited resume-state preparation", async () => {
	const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-host-authority-pipeline-"));
	const stateDir = path.join(tmp, "state");
	const env = { ...process.env, LOBSTER_STATE_DIR: stateDir };
	let gate: Gate | undefined;
	try {
		const first = await runToolRequest({
			pipeline: 'approve --prompt "Write?" | state.set committed',
			ctx: { cwd: tmp, env },
		});
		assert.equal(first.status, "needs_approval");
		const token = first.requiresApproval?.resumeToken;
		assert.ok(token);
		const stateKey = resumeStateKey(token);
		gate = pauseBackend("read", (key) => key === stateKey, "before");
		let current = true;
		let checks = 0;
		const pending = resumeToolRequest({
			token,
			approved: true,
			ctx: {
				cwd: tmp,
				env,
				assertInvocationCurrent: () => {
					checks++;
					if (!current) throw new Error("host invocation retired");
				},
			},
		});
		await gate.started;
		current = false;
		gate.release();
		const resumed = await pending;
		assert.equal(resumed.ok, false);
		assert.match(resumed.error?.message ?? "", /host invocation retired/);
		assert.ok(checks >= 1);
		assert.equal(await storedText(stateDir, "committed"), null);
	} finally {
		gate?.release();
		gate?.restore();
		await fsp.rm(tmp, { recursive: true, force: true });
	}
});

test("approved state.set rechecks host authority after its lock wait", async () => {
	const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-host-authority-state-"));
	const stateDir = path.join(tmp, "state");
	const env = { ...process.env, LOBSTER_STATE_DIR: stateDir };
	let gate: ReturnType<typeof pauseStateLock> | undefined;
	try {
		const first = await runToolRequest({
			pipeline: 'approve --prompt "Write?" | state.set committed',
			ctx: { cwd: tmp, env },
		});
		assert.equal(first.status, "needs_approval");
		const token = first.requiresApproval?.resumeToken;
		assert.ok(token);
		const committedPath = path.join(stateDir, "committed.json");
		gate = pauseStateLock(`${committedPath}.lock`);
		let current = true;
		let checks = 0;
		const pending = resumeToolRequest({
			token,
			approved: true,
			ctx: {
				cwd: tmp,
				env,
				assertInvocationCurrent: () => {
					checks++;
					if (!current) throw new Error("host invocation retired");
				},
			},
		});
		await gate.lockStarted;
		current = false;
		gate.release();
		const resumed = await pending;
		assert.equal(resumed.ok, false);
		assert.match(resumed.error?.message ?? "", /host invocation retired/);
		assert.ok(checks >= 2);
		assert.equal(await storedText(stateDir, "committed"), null);
	} finally {
		gate?.release();
		gate?.restore();
		await fsp.rm(tmp, { recursive: true, force: true });
	}
});

test("approved state.set rechecks host authority before atomic publication", async () => {
	const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-host-authority-publish-"));
	const stateDir = path.join(tmp, "state");
	const env = { ...process.env, LOBSTER_STATE_DIR: stateDir };
	let gate: Gate | undefined;
	try {
		const first = await runToolRequest({
			pipeline: 'approve --prompt "Write?" | state.set committed',
			ctx: { cwd: tmp, env },
		});
		assert.equal(first.status, "needs_approval");
		const token = first.requiresApproval?.resumeToken;
		assert.ok(token);
		gate = pauseBackend("write", (key, text) => key === "committed" && text != null, "before");
		let current = true;
		const pending = resumeToolRequest({
			token,
			approved: true,
			ctx: {
				cwd: tmp,
				env,
				assertInvocationCurrent: () => {
					if (!current) throw new Error("host invocation retired");
				},
			},
		});
		await gate.started;
		current = false;
		gate.release();
		const resumed = await pending;
		assert.equal(resumed.ok, false);
		assert.match(resumed.error?.message ?? "", /host invocation retired/);
		assert.equal(await storedText(stateDir, "committed"), null);
	} finally {
		gate?.release();
		gate?.restore();
		await fsp.rm(tmp, { recursive: true, force: true });
	}
});

test("approved exec rechecks host authority after draining input", async () => {
	const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-host-authority-exec-"));
	const stateDir = path.join(tmp, "state");
	const effectPath = path.join(tmp, "committed.txt");
	const env = { ...process.env, LOBSTER_STATE_DIR: stateDir, LOBSTER_EFFECT_FILE: effectPath };
	try {
		const first = await runToolRequest({
			pipeline:
				"approve --prompt \"Run?\" | exec node -e \"require('node:fs').writeFileSync(process.env.LOBSTER_EFFECT_FILE, 'committed')\"",
			ctx: { cwd: tmp, env },
		});
		assert.equal(first.status, "needs_approval");
		const token = first.requiresApproval?.resumeToken;
		assert.ok(token);
		const authority = retireAfterDispatch();
		const resumed = await resumeToolRequest({
			token,
			approved: true,
			ctx: { cwd: tmp, env, assertInvocationCurrent: authority.assertInvocationCurrent },
		});
		assert.equal(resumed.ok, false);
		assert.match(resumed.error?.message ?? "", /host invocation retired/);
		assert.ok(authority.checks >= 2);
		await assert.rejects(fsp.stat(effectPath), { code: "ENOENT" });
	} finally {
		await fsp.rm(tmp, { recursive: true, force: true });
	}
});

test("approved openclaw.invoke rechecks host authority after draining input", async () => {
	const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-host-authority-invoke-"));
	const stateDir = path.join(tmp, "state");
	const env = { ...process.env, LOBSTER_STATE_DIR: stateDir };
	const originalFetch = globalThis.fetch;
	let calls = 0;
	globalThis.fetch = async () => {
		calls++;
		return new Response(JSON.stringify({ ok: true, result: { sent: true } }), {
			status: 200,
			headers: { "content-type": "application/json" },
		});
	};
	try {
		const first = await runToolRequest({
			pipeline:
				'approve --prompt "Invoke?" | openclaw.invoke --url http://127.0.0.1:1 --tool demo --action ping',
			ctx: { cwd: tmp, env },
		});
		assert.equal(first.status, "needs_approval");
		const token = first.requiresApproval?.resumeToken;
		assert.ok(token);
		const authority = retireAfterDispatch();
		const resumed = await resumeToolRequest({
			token,
			approved: true,
			ctx: { cwd: tmp, env, assertInvocationCurrent: authority.assertInvocationCurrent },
		});
		assert.equal(resumed.ok, false);
		assert.match(resumed.error?.message ?? "", /host invocation retired/);
		assert.ok(authority.checks >= 2);
		assert.equal(calls, 0);
	} finally {
		globalThis.fetch = originalFetch;
		await fsp.rm(tmp, { recursive: true, force: true });
	}
});

test("approved openclaw.invoke enforces host authority at real HTTP I/O", async (t) => {
	const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-host-authority-http-"));
	const stateDir = path.join(tmp, "state");
	const env = { ...process.env, LOBSTER_STATE_DIR: stateDir };
	const requests: Array<{ method: string | undefined; path: string | undefined; body: unknown }> =
		[];
	const server = http.createServer(async (request, response) => {
		const chunks: Buffer[] = [];
		for await (const chunk of request) chunks.push(Buffer.from(chunk));
		requests.push({
			method: request.method,
			path: request.url,
			body: JSON.parse(Buffer.concat(chunks).toString("utf8")),
		});
		response.writeHead(200, { "content-type": "application/json" });
		response.end(JSON.stringify({ ok: true, result: { sent: true } }));
	});
	let claimGate: Gate | undefined;
	let dispatchGate: Gate | undefined;
	try {
		await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
		const port = (server.address() as { port: number }).port;
		const invoke = `openclaw.invoke --url http://127.0.0.1:${port} --tool demo --action send`;
		const pipeline = `approve --prompt "Send?" | ${invoke}`;

		const current = await runToolRequest({ pipeline, ctx: { cwd: tmp, env } });
		assert.equal(current.status, "needs_approval");
		assert.ok(current.requiresApproval?.resumeToken);
		const allowed = await resumeToolRequest({
			token: current.requiresApproval.resumeToken,
			approved: true,
			ctx: { cwd: tmp, env, assertInvocationCurrent: () => {} },
		});
		assert.equal(allowed.status, "ok");
		assert.deepEqual(allowed.output, [{ sent: true }]);
		assert.deepEqual(requests, [
			{
				method: "POST",
				path: "/tools/invoke",
				body: { tool: "demo", action: "send", args: {} },
			},
		]);
		const allowedRequests = requests.length;

		const pendingApproval = await runToolRequest({ pipeline, ctx: { cwd: tmp, env } });
		assert.equal(pendingApproval.status, "needs_approval");
		const pendingToken = pendingApproval.requiresApproval?.resumeToken;
		const pendingId = pendingApproval.requiresApproval?.approvalId;
		assert.ok(pendingToken);
		assert.ok(pendingId);
		const stateKey = resumeStateKey(pendingToken);
		const checkpointBefore = await storedText(stateDir, stateKey);
		claimGate = pauseBackend(
			"write",
			(key, text) => key === stateKey && consumedMarker(text),
			"after",
		);
		let claimCurrent = true;
		const retiredBeforeIO = resumeToolRequest({
			token: pendingToken,
			approved: true,
			ctx: {
				cwd: tmp,
				env,
				assertInvocationCurrent: () => {
					if (!claimCurrent) throw new Error("host invocation retired");
				},
			},
		});
		await waitForGate(claimGate.started);
		claimCurrent = false;
		claimGate.release();
		const rejected = await retiredBeforeIO;
		assert.equal(rejected.ok, false);
		assert.match(rejected.error?.message ?? "", /host invocation retired/);
		assert.equal(requests.length, 1, "retired approval must not reach the HTTP server");
		const retiredRequests = requests.length;
		const checkpointRestored = (await storedText(stateDir, stateKey)) === checkpointBefore;
		assert.equal(checkpointRestored, true);
		claimGate.restore();
		claimGate = undefined;

		const recovered = await resumeToolRequest({
			approvalId: pendingId,
			approved: true,
			ctx: { cwd: tmp, env, assertInvocationCurrent: () => {} },
		});
		assert.equal(recovered.status, "ok");
		assert.deepEqual(recovered.output, [{ sent: true }]);
		assert.equal(requests.length, 2, "safe checkpoint must remain actionable");
		const recoveredRequests = requests.length;

		const dispatchedApproval = await runToolRequest({
			pipeline: `${pipeline} | approve --prompt "Next?"`,
			ctx: { cwd: tmp, env },
		});
		assert.equal(dispatchedApproval.status, "needs_approval");
		const dispatchedToken = dispatchedApproval.requiresApproval?.resumeToken;
		const dispatchedId = dispatchedApproval.requiresApproval?.approvalId;
		assert.ok(dispatchedToken);
		assert.ok(dispatchedId);
		dispatchGate = pauseBackend(
			"write",
			(key, text) => key === `approval_${dispatchedId}` && text === null,
			"after",
		);
		let dispatchCurrent = true;
		const retiredAfterIO = resumeToolRequest({
			token: dispatchedToken,
			approved: true,
			ctx: {
				cwd: tmp,
				env,
				assertInvocationCurrent: () => {
					if (!dispatchCurrent) throw new Error("host invocation retired");
				},
			},
		});
		await waitForGate(dispatchGate.started);
		assert.equal(requests.length, 3, "the HTTP effect must precede host retirement");
		const dispatchedRequests = requests.length;
		dispatchCurrent = false;
		dispatchGate.release();
		const interrupted = await retiredAfterIO;
		assert.equal(interrupted.ok, false);
		assert.match(interrupted.error?.message ?? "", /host invocation retired/);
		const replay = await resumeToolRequest({
			token: dispatchedToken,
			approved: true,
			ctx: { cwd: tmp, env, assertInvocationCurrent: () => {} },
		});
		assert.equal(replay.ok, false);
		assert.equal(requests.length, 3, "dispatched HTTP effect must not replay");
		t.diagnostic(
			JSON.stringify({
				transport: "loopback POST /tools/invoke",
				current: allowedRequests,
				retiredBeforeIO: retiredRequests - allowedRequests,
				safeCheckpointRestored: checkpointRestored,
				recovered: recoveredRequests - retiredRequests,
				dispatchedBeforeRetirement: dispatchedRequests - recoveredRequests,
				replay: requests.length - dispatchedRequests,
			}),
		);
	} finally {
		claimGate?.release();
		claimGate?.restore();
		dispatchGate?.release();
		dispatchGate?.restore();
		if (server.listening) {
			server.closeAllConnections();
			await new Promise<void>((resolve) => server.close(() => resolve()));
		}
		await fsp.rm(tmp, { recursive: true, force: true });
	}
});

test("approved llm.invoke rechecks host authority after draining input", async () => {
	const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-host-authority-llm-"));
	const stateDir = path.join(tmp, "state");
	const env = { ...process.env, LOBSTER_STATE_DIR: stateDir };
	let calls = 0;
	const llmAdapters = {
		"authority-test": {
			async invoke() {
				calls++;
				return { ok: true, result: { output: { text: "answered" } } };
			},
		},
	};
	try {
		const first = await runToolRequest({
			pipeline:
				'approve --prompt "Call?" | llm.invoke --provider authority-test --prompt test --disable-cache',
			ctx: { cwd: tmp, env, llmAdapters },
		});
		assert.equal(first.status, "needs_approval");
		const token = first.requiresApproval?.resumeToken;
		assert.ok(token);
		const authority = retireAfterDispatch();
		const resumed = await resumeToolRequest({
			token,
			approved: true,
			ctx: {
				cwd: tmp,
				env,
				llmAdapters,
				assertInvocationCurrent: authority.assertInvocationCurrent,
			},
		});
		assert.equal(resumed.ok, false);
		assert.match(resumed.error?.message ?? "", /host invocation retired/);
		assert.ok(authority.checks >= 2);
		assert.equal(calls, 0);
	} finally {
		await fsp.rm(tmp, { recursive: true, force: true });
	}
});

test("approved pipeline cannot publish a successor approval after host retirement", async () => {
	const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-host-authority-successor-"));
	const env = { ...process.env, LOBSTER_STATE_DIR: tmp };
	try {
		const first = await runToolRequest({
			pipeline: 'approve --prompt "First?" | approve --prompt "Second?"',
			ctx: { cwd: tmp, env },
		});
		assert.equal(first.status, "needs_approval");
		const token = first.requiresApproval?.resumeToken;
		assert.ok(token);
		const before = await storeSnapshot(tmp);
		const authority = retireAfterDispatch();
		const resumed = await resumeToolRequest({
			token,
			approved: true,
			ctx: { cwd: tmp, env, assertInvocationCurrent: authority.assertInvocationCurrent },
		});
		assert.equal(resumed.ok, false);
		assert.match(resumed.error?.message ?? "", /host invocation retired/);
		assert.deepEqual(await storeSnapshot(tmp), before);
	} finally {
		await fsp.rm(tmp, { recursive: true, force: true });
	}
});

test("approved pipeline removes a successor linked before host retirement", async () => {
	const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-host-authority-index-"));
	const env = { ...process.env, LOBSTER_STATE_DIR: tmp };
	let gate: Gate | undefined;
	try {
		const first = await runToolRequest({
			pipeline: 'approve --prompt "First?" | approve --prompt "Second?"',
			ctx: { cwd: tmp, env },
		});
		assert.equal(first.status, "needs_approval");
		const token = first.requiresApproval?.resumeToken;
		assert.ok(token);
		const before = await storeSnapshot(tmp);
		gate = pauseBackend(
			"write",
			(key, text) => key.startsWith("approval_") && text != null,
			"after",
		);
		let current = true;
		const pending = resumeToolRequest({
			token,
			approved: true,
			ctx: {
				cwd: tmp,
				env,
				assertInvocationCurrent: () => {
					if (!current) throw new Error("host invocation retired");
				},
			},
		});
		await gate.started;
		current = false;
		gate.release();
		const resumed = await pending;
		assert.equal(resumed.ok, false);
		assert.match(resumed.error?.message ?? "", /host invocation retired/);
		assert.deepEqual(await storeSnapshot(tmp), before);
	} finally {
		gate?.release();
		gate?.restore();
		await fsp.rm(tmp, { recursive: true, force: true });
	}
});

test("approved pipeline restores the predecessor after retirement during old-index cleanup", async () => {
	const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-host-authority-old-index-"));
	const env = { ...process.env, LOBSTER_STATE_DIR: tmp };
	let gate: Gate | undefined;
	try {
		const first = await runToolRequest({
			pipeline: 'approve --prompt "First?" | approve --prompt "Second?"',
			ctx: { cwd: tmp, env },
		});
		assert.equal(first.status, "needs_approval");
		const token = first.requiresApproval?.resumeToken;
		const approvalId = first.requiresApproval?.approvalId;
		assert.ok(token);
		assert.ok(approvalId);
		const before = await storeSnapshot(tmp);
		gate = pauseBackend(
			"write",
			(key, text) => key === `approval_${approvalId}` && text === null,
			"after",
		);
		let current = true;
		const pending = resumeToolRequest({
			token,
			approved: true,
			ctx: {
				cwd: tmp,
				env,
				assertInvocationCurrent: () => {
					if (!current) throw new Error("host invocation retired");
				},
			},
		});
		await waitForGate(gate.started);
		current = false;
		gate.release();
		const resumed = await pending;
		assert.equal(resumed.ok, false);
		assert.match(resumed.error?.message ?? "", /host invocation retired/);
		assert.deepEqual(await storeSnapshot(tmp), before);
		const retried = await resumeToolRequest({
			approvalId,
			approved: true,
			ctx: { cwd: tmp, env },
		});
		assert.equal(retried.status, "needs_approval");
		assert.equal(retried.requiresApproval?.prompt, "Second?");
	} finally {
		gate?.release();
		gate?.restore();
		await fsp.rm(tmp, { recursive: true, force: true });
	}
});

test("approved pipeline restores the predecessor when a successor input is retired", async () => {
	const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-host-authority-input-"));
	const env = { ...process.env, LOBSTER_STATE_DIR: tmp };
	let gate: Gate | undefined;
	try {
		const first = await runToolRequest({
			pipeline: 'approve --prompt "First?" | ask --prompt "Name?"',
			ctx: { cwd: tmp, env },
		});
		assert.equal(first.status, "needs_approval");
		const token = first.requiresApproval?.resumeToken;
		const approvalId = first.requiresApproval?.approvalId;
		assert.ok(token);
		assert.ok(approvalId);
		const before = await storeSnapshot(tmp);
		gate = pauseBackend(
			"write",
			(key, text) => key === `approval_${approvalId}` && text === null,
			"after",
		);
		let current = true;
		const pending = resumeToolRequest({
			token,
			approved: true,
			ctx: {
				cwd: tmp,
				env,
				assertInvocationCurrent: () => {
					if (!current) throw new Error("host invocation retired");
				},
			},
		});
		await waitForGate(gate.started);
		current = false;
		gate.release();
		const resumed = await pending;
		assert.equal(resumed.ok, false);
		assert.match(resumed.error?.message ?? "", /host invocation retired/);
		assert.deepEqual(await storeSnapshot(tmp), before);
		const retried = await resumeToolRequest({
			approvalId,
			approved: true,
			ctx: { cwd: tmp, env },
		});
		assert.equal(retried.status, "needs_input");
		assert.equal(retried.requiresInput?.prompt, "Name?");
	} finally {
		gate?.release();
		gate?.restore();
		await fsp.rm(tmp, { recursive: true, force: true });
	}
});

for (const kind of ["pipeline"] as const) {
	test(`approved ${kind} restores a pre-dispatch claim when host authority retires`, async () => {
		const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-host-authority-claim-"));
		const stateDir = path.join(tmp, "state");
		const effectPath = path.join(tmp, "committed.txt");
		const env = { ...process.env, LOBSTER_STATE_DIR: stateDir, LOBSTER_EFFECT_FILE: effectPath };
		let gate: Gate | undefined;
		try {
			const filePath = path.join(tmp, "workflow.lobster");
			const first =
				kind === "pipeline"
					? await runToolRequest({
							pipeline:
								"approve --prompt \"Write?\" | exec node -e \"require('node:fs').writeFileSync(process.env.LOBSTER_EFFECT_FILE, 'committed')\"",
							ctx: { cwd: tmp, env },
						})
					: await runToolRequest({ filePath, ctx: { cwd: tmp, env } });
			assert.equal(first.status, "needs_approval");
			const token = first.requiresApproval?.resumeToken;
			const approvalId = first.requiresApproval?.approvalId;
			assert.ok(token);
			assert.ok(approvalId);
			const stateKey = resumeStateKey(token);
			const before = await storedText(stateDir, stateKey);
			gate = pauseBackend(
				"write",
				(key, text) => key === stateKey && consumedMarker(text),
				"after",
			);
			let current = true;
			const pending = resumeToolRequest({
				token,
				approved: true,
				ctx: {
					cwd: tmp,
					env,
					assertInvocationCurrent: () => {
						if (!current) throw new Error("host invocation retired");
					},
				},
			});
			await waitForGate(gate.started);
			current = false;
			gate.release();
			const resumed = await pending;
			assert.equal(resumed.ok, false);
			assert.match(resumed.error?.message ?? "", /host invocation retired/);
			await assert.rejects(fsp.stat(effectPath), { code: "ENOENT" });
			assert.equal(await storedText(stateDir, stateKey), before);
			const cancelled = await resumeToolRequest({
				approvalId,
				cancel: true,
				ctx: { cwd: tmp, env },
			});
			assert.equal(cancelled.status, "cancelled");
		} finally {
			gate?.release();
			gate?.restore();
			await fsp.rm(tmp, { recursive: true, force: true });
		}
	});
}

for (const kind of ["pipeline"] as const) {
	for (const phase of ["marker", "delete"] as const) {
		test(`approved terminal ${kind} preserves its checkpoint after retirement during ${phase}`, async () => {
			const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-host-authority-terminal-"));
			const stateDir = path.join(tmp, "state");
			const env = { ...process.env, LOBSTER_STATE_DIR: stateDir };
			let gate: Gate | undefined;
			try {
				const filePath = path.join(tmp, "workflow.lobster");
				const first =
					kind === "pipeline"
						? await runToolRequest({
								pipeline: 'approve --prompt "Finish?"',
								ctx: { cwd: tmp, env },
							})
						: await runToolRequest({ filePath, ctx: { cwd: tmp, env } });
				assert.equal(first.status, "needs_approval");
				const token = first.requiresApproval?.resumeToken;
				const approvalId = first.requiresApproval?.approvalId;
				assert.ok(token);
				assert.ok(approvalId);
				const stateKey = resumeStateKey(token);
				const before = await storedText(stateDir, stateKey);
				gate = pauseBackend(
					"write",
					(key, text) =>
						key === stateKey && (phase === "marker" ? consumedMarker(text) : text === null),
					"after",
				);
				let current = true;
				const pending = resumeToolRequest({
					token,
					approved: true,
					ctx: {
						cwd: tmp,
						env,
						assertInvocationCurrent: () => {
							if (!current) throw new Error("host invocation retired");
						},
					},
				});
				await waitForGate(gate.started);
				current = false;
				gate.release();
				const resumed = await pending;
				assert.equal(resumed.ok, false);
				assert.match(resumed.error?.message ?? "", /host invocation retired/);
				assert.equal(await storedText(stateDir, stateKey), before);
				const cancelled = await resumeToolRequest({
					approvalId,
					cancel: true,
					ctx: { cwd: tmp, env },
				});
				assert.equal(cancelled.status, "cancelled");
			} finally {
				gate?.release();
				gate?.restore();
				await fsp.rm(tmp, { recursive: true, force: true });
			}
		});
	}
}

for (const kind of ["pipeline"] as const) {
	test(`approved ${kind} does not restore a predecessor after unsafe dispatch`, async () => {
		const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-host-authority-effect-"));
		const stateDir = path.join(tmp, "state");
		const effectPath = path.join(tmp, "committed.txt");
		const env = { ...process.env, LOBSTER_STATE_DIR: stateDir, LOBSTER_EFFECT_FILE: effectPath };
		let gate: Gate | undefined;
		try {
			const command = `"${process.execPath}" -e "require('node:fs').writeFileSync(process.env.LOBSTER_EFFECT_FILE, 'committed')"`;
			const filePath = path.join(tmp, "workflow.lobster");
			const first =
				kind === "pipeline"
					? await runToolRequest({
							pipeline: `approve --prompt "First?" | exec ${command} | approve --prompt "Second?"`,
							ctx: { cwd: tmp, env },
						})
					: await runToolRequest({ filePath, ctx: { cwd: tmp, env } });
			assert.equal(first.status, "needs_approval");
			const token = first.requiresApproval?.resumeToken;
			const approvalId = first.requiresApproval?.approvalId;
			assert.ok(token);
			assert.ok(approvalId);
			gate = pauseBackend(
				"write",
				(key, text) => key === `approval_${approvalId}` && text === null,
				"after",
			);
			let current = true;
			const pending = resumeToolRequest({
				token,
				approved: true,
				ctx: {
					cwd: tmp,
					env,
					assertInvocationCurrent: () => {
						if (!current) throw new Error("host invocation retired");
					},
				},
			});
			await waitForGate(gate.started);
			current = false;
			gate.release();
			const resumed = await pending;
			assert.equal(resumed.ok, false);
			assert.match(resumed.error?.message ?? "", /host invocation retired/);
			assert.equal(await fsp.readFile(effectPath, "utf8"), "committed");
			const keys = Object.keys(await storeSnapshot(stateDir));
			assert.equal(
				keys.some((name) => name.startsWith("approval_")),
				false,
			);
			assert.equal(
				keys.filter((name) => /^(pipeline|workflow)_resume_/.test(name)).length,
				kind === "pipeline" ? 1 : 0,
			);
			const replay = await resumeToolRequest({ token, approved: true, ctx: { cwd: tmp, env } });
			assert.equal(replay.ok, false);
			assert.equal(await fsp.readFile(effectPath, "utf8"), "committed");
		} finally {
			gate?.release();
			gate?.restore();
			await fsp.rm(tmp, { recursive: true, force: true });
		}
	});
}
