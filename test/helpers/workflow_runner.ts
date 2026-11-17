// Runs workflow files in-process against a private state directory, with the scripted
// `fake` command registered, and answers approval/input gates on request.
import type { TestContext } from "node:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { runWorkflowFile } from "../../src/workflows/file.js";
import { openStoreSession } from "../../src/persistence/session.js";
import { decodeToken } from "../../src/resume/token.js";
import type { WorkflowRunResult } from "../../src/workflows/types.js";
import type { ExecutionState } from "../../src/engine/state.js";
import { createFakeCommand, type FakeCommand } from "./fake_command.js";

export type RunResult = WorkflowRunResult & { runId?: string };

export type RunOptions = {
	args?: Record<string, unknown>;
	signal?: AbortSignal;
	mode?: "tool" | "human";
	/** Readable used for interactive prompts (human mode). */
	stdin?: NodeJS.ReadableStream;
	dryRun?: boolean;
	env?: Record<string, string>;
};

export type Answers = { approve?: boolean; response?: unknown };

export type WorkflowFixture = Awaited<ReturnType<typeof workflowFixture>>;

/** A stream that the engine treats as a terminal, for interactive prompts. */
export function interactiveStdin() {
	const stream = new PassThrough() as PassThrough & { isTTY: boolean };
	stream.isTTY = true;
	return stream;
}

function capture() {
	const stream = new PassThrough();
	let text = "";
	stream.setEncoding("utf8");
	stream.on("data", (chunk) => (text += chunk));
	return { stream, text: () => text };
}

export async function workflowFixture(t: TestContext, prefix = "lobster-contract-") {
	const dir = await fs.mkdtemp(path.join(os.tmpdir(), prefix));
	t.after(() => fs.rm(dir, { recursive: true, force: true }));
	const env: Record<string, string | undefined> = {
		...process.env,
		LOBSTER_STATE_DIR: path.join(dir, "state"),
	};
	const fake: FakeCommand = createFakeCommand(dir);
	const stderr = capture();

	const fixture = {
		dir,
		env,
		fake,
		/** Everything written to stderr by runs of this fixture (dry-run plans, retries). */
		stderr: stderr.text,
		/** Write a workflow file; returns its path. */
		async write(name: string, workflow: unknown) {
			const file = path.join(dir, name);
			await fs.writeFile(file, JSON.stringify(workflow, null, 2));
			return file;
		},
		ctx(options: RunOptions = {}) {
			return {
				stdin: options.stdin ?? new PassThrough(),
				stdout: new PassThrough().resume(),
				stderr: stderr.stream,
				env: { ...env, ...options.env },
				mode: options.mode ?? "tool",
				registry: fake.registry,
				signal: options.signal,
				dryRun: options.dryRun,
				cwd: dir,
			} as const;
		},
		/** Start a run of a workflow (an object is written to flow.lobster first). */
		async run(workflow: unknown, options: RunOptions = {}): Promise<RunResult> {
			const filePath =
				typeof workflow === "string" ? workflow : await fixture.write("flow.lobster", workflow);
			return runWorkflowFile({ filePath, args: options.args, ctx: fixture.ctx(options) as any });
		},
		/** Answer the gate a run stopped at. */
		async resume(result: RunResult, answers: Answers & { cancel?: boolean } = {}) {
			const token = result.requiresApproval?.resumeToken ?? result.requiresInput?.resumeToken ?? "";
			assert(token, `run did not stop at a gate: ${JSON.stringify(result)}`);
			const resume = decodeToken(token);
			const decision = answers.cancel
				? { cancel: true }
				: result.status === "needs_approval"
					? { approved: answers.approve ?? true }
					: { response: answers.response };
			return runWorkflowFile({ resume, ...decision, ctx: fixture.ctx() as any });
		},
		/** Run to the end, answering every gate with `answers` (approve, or respond). */
		async drive(workflow: unknown, answers: Answers = {}, options: RunOptions = {}) {
			let result = await fixture.run(workflow, options);
			for (let gates = 0; result.status === "needs_approval" || result.status === "needs_input";) {
				assert(++gates < 20, "too many gates");
				result = await fixture.resume(result, answers);
			}
			return result;
		},
		/** The id of the only run this fixture has started. */
		async runId() {
			const session = await openStoreSession({ env });
			try {
				const { runs } = await session.listRuns({ limit: 10 });
				assert(runs.length === 1, `expected one run, found ${runs.length}`);
				return runs[0].id;
			} finally {
				await session.close();
			}
		},
		/** The saved execution checkpoint of a run (the only run by default). */
		async execution(runId?: string): Promise<ExecutionState> {
			const id = runId ?? (await fixture.runId());
			const session = await openStoreSession({ env });
			try {
				const run = await session.getRunState(id);
				assert(run?.execution, `run ${id} has no execution checkpoint`);
				return run.execution as ExecutionState;
			} finally {
				await session.close();
			}
		},
	};
	return fixture;
}

function assert(condition: unknown, message: string): asserts condition {
	if (!condition) throw new Error(message);
}
