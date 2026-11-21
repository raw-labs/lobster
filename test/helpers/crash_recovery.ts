// Kill-and-recover harness: start a workflow in a separate process, SIGKILL it at a
// chosen point, wait for its execution lease to expire, then continue the run with
// `advanceWorkflowRun` in a fresh process. Generalised from the per-node scenario in
// test/execution_machine.test.ts so every node type can be crashed the same way.
//
// The workers register the scripted `fake` command (see fake_command.ts) over the same
// fixture directory, so invocation counts cover all three processes.
import type { TestContext } from "node:test";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { promises as fs } from "node:fs";
import path from "node:path";
import { openStoreSession } from "../../src/persistence/session.js";
import { createFakeCommand } from "./fake_command.js";

const fileUrl = new URL("../../src/engine/runner.js", import.meta.url).href;
const dueUrl = new URL("../../src/engine/due.js", import.meta.url).href;
const sessionUrl = new URL("../../src/persistence/session.js", import.meta.url).href;
const coreUrl = new URL("../../src/core/tool_runtime.js", import.meta.url).href;
const fakeUrl = new URL("./fake_command.js", import.meta.url).href;

/** What a worker printed last: a run result, or the error it failed with. */
export type WorkerOutcome = Record<string, any> & {
	status?: string;
	error?: { code?: string; message: string };
};

export type KillPoint = (progress: {
	calls: (name: string) => number;
	/** How many checkpoints the worker has saved. */
	saves: number;
	/** The last checkpoint the worker saved, as JSON text. */
	saved: string;
}) => boolean;

export function workerSource() {
	return `
// CLOCK_OFFSET_MS shifts this process's clock, as on a machine whose clock is wrong.
if (process.env.CLOCK_OFFSET_MS) {
  const real = Date.now;
  Date.now = () => real() + Number(process.env.CLOCK_OFFSET_MS);
}
import { createToolContext } from ${JSON.stringify(coreUrl)};
import { runWorkflowFile, advanceWorkflowRun } from ${JSON.stringify(fileUrl)};
import { advanceDueRuns } from ${JSON.stringify(dueUrl)};
import { openStoreSession } from ${JSON.stringify(sessionUrl)};
import { createFakeCommand } from ${JSON.stringify(fakeUrl)};
const store = await openStoreSession({ env: process.env });
const ctx = createToolContext({ storeSession: store, env: process.env });
ctx.registry = createFakeCommand(process.env.FAKE_DIR).registry;
const transition = store.backend.transition.bind(store.backend);
store.backend.transition = async (...args) => {
  const result = await transition(...args);
  const request = args[0];
  if (request.action.kind === "execution-save")
    process.stdout.write("SAVED " + JSON.stringify({ runId: request.runId, execution: request.action.execution }) + "\\n");
  return result;
};
const leaseMs = Number(process.env.LEASE_MS);
try {
  const result = process.env.DUE
    ? await advanceDueRuns({ ctx, leaseMs })
    : process.env.RUN_ID
      ? await advanceWorkflowRun({ runId: process.env.RUN_ID, ctx, leaseMs })
      : await runWorkflowFile({ filePath: process.env.FLOW, ctx, leaseMs });
  process.stdout.write("RESULT " + JSON.stringify(result) + "\\n");
} catch (error) {
  process.stdout.write("RESULT " + JSON.stringify({ error: { code: error.code, message: error.message } }) + "\\n");
} finally { await store.close(); }
`;
}

async function runWorker(
	t: TestContext,
	options: {
		dir: string;
		env: Record<string, string | undefined>;
		flowPath?: string;
		runId?: string;
		leaseMs: number;
		killWhen?: KillPoint;
	},
) {
	const worker = path.join(options.dir, "crash-worker.mjs");
	await fs.writeFile(worker, workerSource());
	const fake = createFakeCommand(options.dir);
	const child = spawn(process.execPath, [worker], {
		env: {
			...options.env,
			FAKE_DIR: options.dir,
			LEASE_MS: String(options.leaseMs),
			...(options.flowPath ? { FLOW: options.flowPath } : {}),
			...(options.runId ? { RUN_ID: options.runId } : {}),
		},
		stdio: ["ignore", "pipe", "pipe"],
	});
	t.after(() => {
		if (child.exitCode === null && !child.killed) child.kill("SIGKILL");
	});
	const exited = once(child, "exit");
	let stdout = "",
		stderr = "",
		saved = "",
		saves = 0,
		runId = options.runId,
		outcome: WorkerOutcome | undefined,
		killed = false;
	child.stderr.setEncoding("utf8").on("data", (chunk) => (stderr += chunk));
	child.stdout.setEncoding("utf8").on("data", (chunk) => {
		stdout += chunk;
		let newline: number;
		while ((newline = stdout.indexOf("\n")) >= 0) {
			const line = stdout.slice(0, newline);
			stdout = stdout.slice(newline + 1);
			if (line.startsWith("SAVED ")) {
				const event = JSON.parse(line.slice(6));
				runId ??= event.runId;
				saved = JSON.stringify(event.execution);
				saves++;
			} else if (line.startsWith("RESULT ")) outcome = JSON.parse(line.slice(7));
		}
	});
	const killWhen = options.killWhen;
	if (killWhen) {
		const deadline = Date.now() + 20000;
		while (child.exitCode === null) {
			if (killWhen({ calls: (name) => fake.calls(name), saved, saves })) {
				child.kill("SIGKILL");
				killed = true;
				break;
			}
			if (Date.now() > deadline) {
				child.kill("SIGKILL");
				throw new Error("crash point never reached; worker stderr: " + stderr);
			}
			await new Promise((resolve) => setTimeout(resolve, 10));
		}
	}
	await exited;
	if (!killed && !outcome) throw new Error("worker exited without a result: " + stderr);
	if (!runId) throw new Error("worker saved no checkpoint: " + stderr);
	return { runId, outcome: killed ? undefined : outcome, killed, stderr };
}

/**
 * Start `flowPath` in a worker, kill it once `killWhen` holds (or let it finish when
 * omitted), wait out its lease, and advance the run in a fresh worker.
 */
export async function crashAndRecover(
	t: TestContext,
	options: {
		dir: string;
		env: Record<string, string | undefined>;
		flowPath: string;
		killWhen?: KillPoint;
		leaseMs?: number;
		/**
		 * false: stop once the lease has lapsed and leave recovery to the caller.
		 * "if-killed": recover only a worker that was killed before its run ended (advancing a
		 * run that finished failed would retry it).
		 */
		recover?: boolean | "if-killed";
	},
) {
	const leaseMs = options.leaseMs ?? 1000;
	const first = await runWorker(t, { ...options, leaseMs });
	let finished = false;
	if (first.killed) {
		const session = await openStoreSession({ env: options.env });
		try {
			const run = await session.getRunState(first.runId);
			// Killed after its last save: the run had already ended, and nothing is left to recover.
			finished = ["completed", "failed", "cancelled"].includes(run?.execution?.status ?? "");
			const lease = run?.executionLease;
			const due = lease ? lease.expiresAt - Date.now() + 5 : 0;
			if (due > 0) await new Promise((resolve) => setTimeout(resolve, due));
		} finally {
			await session.close();
		}
	}
	if (options.recover === false || (options.recover === "if-killed" && (!first.killed || finished)))
		return { runId: first.runId, killed: first.killed, first: first.outcome, recovered: undefined };
	const recovered = await runWorker(t, {
		...options,
		runId: first.runId,
		killWhen: undefined,
		leaseMs,
	});
	return {
		runId: first.runId,
		killed: first.killed,
		first: first.outcome,
		recovered: recovered.outcome!,
	};
}
