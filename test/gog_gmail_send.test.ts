import test from "node:test";
import { waitForPid } from "./helpers/wait_for_pid.js";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { access, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { gogGmailSendCommand } from "../src/packs/google/gmail_send.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

function streamOf(items: unknown[]) {
	return (async function* () {
		for (const item of items) yield item;
	})();
}

async function fileExists(path: string) {
	try {
		await access(path);
		return true;
	} catch {
		return false;
	}
}

function processIsRunning(pid: number) {
	try {
		const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
		const state = stat.slice(stat.lastIndexOf(")") + 2, stat.lastIndexOf(")") + 3);
		if (state === "Z") return false;
	} catch {
		// /proc is unavailable on macOS and Windows.
	}
	try {
		process.kill(pid, 0);
		return true;
	} catch {
		return false;
	}
}

test("aborting gog.gmail.send terminates the sleeper gog child", async () => {
	const dir = await mkdtemp(join(tmpdir(), "lobster-gmail-send-abort-"));
	const controller = new AbortController();
	let run: Promise<any> | undefined;
	try {
		const repoRoot = join(__dirname, "..", "..");
		const mockGog = join(repoRoot, "test", "fixtures", "mock-gog-cancellation.mjs");
		const sendStarted = join(dir, "send-started");
		const sendTerminated = join(dir, "send-terminated");
		const sendCompleted = join(dir, "send-completed");
		const descendantStarted = join(dir, "descendant-started");
		const descendantCompleted = join(dir, "descendant-completed");

		run = gogGmailSendCommand.run({
			input: streamOf([{ to: "user@example.com", subject: "Hello", body: "World" }]),
			args: {},
			ctx: {
				signal: controller.signal,
				env: {
					...process.env,
					GOG_BIN: mockGog,
					MOCK_GOG_SEND_STARTED_FILE: sendStarted,
					MOCK_GOG_SEND_TERMINATED_FILE: sendTerminated,
					MOCK_GOG_SEND_COMPLETED_FILE: sendCompleted,
					MOCK_GOG_DESCENDANT_STARTED_FILE: descendantStarted,
					MOCK_GOG_DESCENDANT_COMPLETED_FILE: descendantCompleted,
					MOCK_GOG_TERMINATION_DELAY_MS: "1000",
					MOCK_GOG_COMPLETION_DELAY_MS: "5000",
				},
			},
		});

		const childPid = await waitForPid(sendStarted);
		const descendantPid = await waitForPid(descendantStarted);
		assert.ok(Number.isInteger(childPid) && childPid > 0);
		assert.ok(Number.isInteger(descendantPid) && descendantPid > 0);
		controller.abort(new Error("abort in-flight gog.gmail.send"));

		await assert.rejects(run, (err: unknown) => {
			assert.ok(err instanceof Error);
			assert.match(err.message, /abort/i);
			return true;
		});

		await new Promise((resolve) => setTimeout(resolve, 700));
		assert.equal(processIsRunning(childPid), false, "gog send child must not remain after abort");
		assert.equal(
			processIsRunning(descendantPid),
			false,
			"gog send descendants must be killed after abort",
		);
		if (process.platform !== "win32") {
			assert.equal(await fileExists(sendTerminated), true);
		}
		assert.equal(await fileExists(sendCompleted), false, "a cancelled send must not finish");
		assert.equal(
			await fileExists(descendantCompleted),
			false,
			"a send descendant must not finish after abort",
		);
	} finally {
		controller.abort();
		await run?.catch(() => {});
		await rm(dir, { recursive: true, force: true });
	}
});
