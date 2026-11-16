import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { assertOffline } from "../src/persistence/migrate.js";
import { openStoreSession } from "../src/persistence/session.js";
import { currentSessionMarker } from "../src/persistence/session_marker.js";

async function stateDir(t: test.TestContext) {
	const dir = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-markers-"));
	t.after(() => fs.rm(dir, { recursive: true, force: true }));
	return dir;
}

const write = (dir: string, name: string, value: unknown) =>
	fs.writeFile(path.join(dir, name), typeof value === "string" ? value : JSON.stringify(value));

test("an open session blocks offline maintenance and names its marker", async (t) => {
	const dir = await stateDir(t);
	const session = await openStoreSession({ directory: dir });
	try {
		await assert.rejects(assertOffline(dir), /a session is open \(.*\.session-.*delete that file/s);
	} finally {
		await session.close();
	}
	await assertOffline(dir);
});

test("markers left by dead, reused or foreign PIDs do not block maintenance", async (t) => {
	const dir = await stateDir(t);
	const dead = spawnSync(process.execPath, ["-e", ""]).pid!;
	const hourAgo = currentSessionMarker().startedAt! - 3_600_000;
	await write(dir, `.session-${dead}-a`, { pid: dead });
	// The parent PID is alive but did not start an hour ago: that PID was reused.
	await write(dir, `.session-${process.ppid}-b`, { pid: process.ppid, startedAt: hourAgo });
	// PID 1 belongs to another user (EPERM) and did not start an hour ago either.
	await write(dir, ".session-1-c", { pid: 1, startedAt: hourAgo });
	await write(dir, ".session-0-d", "{half-written");
	await assertOffline(dir);
	assert.deepEqual(
		(await fs.readdir(dir)).filter((name) => name.startsWith(".session-")),
		[],
	);
});

test("a marker from a process that is still running keeps blocking", async (t) => {
	const dir = await stateDir(t);
	await write(dir, `.session-${process.pid}-me`, currentSessionMarker());
	await assert.rejects(assertOffline(dir), /a session is open/);
});
