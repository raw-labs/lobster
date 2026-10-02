import { openStoreSession } from "../../src/persistence/session.js";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
const steps = Number(process.argv[2] ?? 1000);
if (!Number.isInteger(steps) || steps < 1 || steps > 10000)
	throw new Error("steps must be 1..10000");
for (const backend of ["files", "sqlite"] as const) {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-history-benchmark-"));
	const store = await openStoreSession({ directory, backend, history: true });
	let maxDelayMs = 0;
	let previous = performance.now();
	let ticks = 0;
	const interval = setInterval(() => {
		const now = performance.now();
		maxDelayMs = Math.max(maxDelayMs, now - previous);
		previous = now;
		ticks++;
	}, 5);
	const started = performance.now();
	try {
		const recorder = await store.createRun("benchmark");
		for (let n = 0; n < steps; n++) {
			const id = await recorder.start(`step/${n}`, "fixture", n);
			await recorder.finish(id, "completed", [n]);
		}
		await recorder.settle("completed");
		const elapsedMs = performance.now() - started;
		const queryStart = performance.now();
		await store.inspect(recorder.runId, { limit: 50 });
		const queryMs = performance.now() - queryStart;
		await store.close();
		const storage =
			backend === "files"
				? path.join(directory, ".history", `${recorder.runId}.json`)
				: path.join(directory, "lobster.sqlite");
		process.stdout.write(
			JSON.stringify({
				backend,
				steps,
				elapsedMs: Math.round(elapsedMs),
				queryMs: Math.round(queryMs),
				maxCallerIntervalMs: Math.round(maxDelayMs),
				ticks,
				bytes: (await fs.stat(storage)).size,
			}) + "\n",
		);
	} finally {
		clearInterval(interval);
		await store.close();
		await fs.rm(directory, { recursive: true, force: true });
	}
}
