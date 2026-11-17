import test from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promises as fsp } from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const cli = path.resolve("bin/lobster.js");

async function runWorkflow(dir: string, pipeline: string, env: NodeJS.ProcessEnv = {}) {
	const file = path.join(dir, "workflow.lobster");
	await fsp.writeFile(
		file,
		JSON.stringify({
			cwd: dir,
			steps: [{ id: "dispatch", pipeline, timeout_ms: 1000, retry: { max: 2, delay_ms: 10 } }],
		}),
	);
	try {
		await execFileAsync(process.execPath, [cli, "run", "--mode", "tool", "--file", file], {
			env: { ...process.env, LOBSTER_STATE_DIR: path.join(dir, "state"), ...env },
			timeout: 15_000,
		});
		assert.fail("workflow should fail after dispatch");
	} catch (error: any) {
		assert.equal(error.killed, false, "CLI must settle without the test timeout");
		const envelope = JSON.parse(error.stdout);
		assert.equal(envelope.ok, false);
		assert.doesNotMatch(error.stderr, /\[RETRY\]/);
		return envelope.error.message as string;
	}
}

for (const command of [
	"openclaw.invoke",
	"clawd.invoke",
	"openclaw.invoke --dry-run",
	"openclaw.invoke --dryRun",
]) {
	for (const failure of ["timeout", "http-error", "downstream-error"]) {
		test(`${command} does not repeat a dispatched tool after ${failure}`, async () => {
			const dir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-dispatch-retry-"));
			let hits = 0;
			const server = http.createServer((req, res) => {
				assert.equal(req.method, "POST");
				assert.equal(req.url, "/tools/invoke");
				hits++;
				req.resume();
				if (failure === "http-error") res.writeHead(503).end("unavailable after dispatch");
				if (failure === "downstream-error") res.end(JSON.stringify({ ok: true, result: "done" }));
			});
			try {
				await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
				const port = (server.address() as { port: number }).port;
				const pipeline =
					`${command} --url http://127.0.0.1:${port} --token= --tool demo --action write` +
					(failure === "downstream-error" ? ` | exec --shell "exit 3"` : "");
				const message = await runWorkflow(dir, pipeline);
				assert.match(
					message,
					failure === "timeout"
						? /timed out/
						: failure === "http-error"
							? /503/
							: /exec failed|exit code 3|failed \(3\)/,
				);
				assert.equal(hits, 1, "a failed step must not repeat a potentially completed tool call");
			} finally {
				server.closeAllConnections();
				await new Promise<void>((resolve) => server.close(() => resolve()));
				await fsp.rm(dir, { recursive: true, force: true });
			}
		});
	}
}

for (const failure of ["timeout", "process-error"]) {
	test(`openclaw.agent default process runner dispatches once after ${failure}`, async () => {
		const dir = await fsp.mkdtemp(path.join(os.tmpdir(), "lobster-agent-retry-"));
		try {
			// Node receives "agent" as its script, exercising the default argv/process runner.
			await fsp.writeFile(
				path.join(dir, "agent"),
				`
const fs = require("node:fs");
fs.appendFileSync("calls", String(process.pid) + "\\n");
${failure === "timeout" ? "setInterval(() => {}, 1000);" : 'process.stderr.write("failed after dispatch"); process.exitCode = 1;'}
`,
			);
			const message = await runWorkflow(
				dir,
				'openclaw.agent --agent test --prompt "synthetic proof"',
				{
					LOBSTER_OPENCLAW_BIN: process.execPath,
				},
			);
			assert.match(message, failure === "timeout" ? /timed out/ : /failed after dispatch/);
			const pids = (await fsp.readFile(path.join(dir, "calls"), "utf8")).trim().split("\n");
			assert.equal(pids.length, 1, "the default runner must launch only one child");
			assert.throws(() => process.kill(Number(pids[0]), 0), { code: "ESRCH" });
		} finally {
			await fsp.rm(dir, { recursive: true, force: true });
		}
	});
}
