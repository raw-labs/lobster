import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { once } from "node:events";
import { Worker } from "node:worker_threads";
import { migrateStore, openStoreSession } from "../src/core/index.js";
import { assertSqliteRuntime } from "../src/persistence/sqlite-support.js";

for (const node of ["22.13.0", "22.23.4", "23.4.0", "24.3.0", "24.22.1", "26.0.0"]) {
	test(`SQLite runtime qualification on Node ${node}`, () => {
		for (const sqlite of ["3.44.6", "3.44.7", "3.50.7", "3.50.8", "3.51.3", "3.52.0", "4.0.0"]) {
			assert.doesNotThrow(() => assertSqliteRuntime(node, sqlite));
		}
		for (const sqlite of ["3.44.5", "3.45.3", "3.49.4", "3.50.1", "3.50.6", "3.51.2", "invalid"]) {
			assert.throws(() => assertSqliteRuntime(node, sqlite), /WAL-reset fix/);
		}
	});
}

test("SQLite qualification preserves the native-driver Node minimum", () => {
	for (const node of ["20.19.0", "22.0.0", "22.12.0", "23.3.0", "invalid"]) {
		assert.throws(() => assertSqliteRuntime(node, "3.53.4"), /requires Node/);
	}
});

test("SQLite admission qualifies the connection instead of stale process version metadata", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-sqlite-version-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const descriptor = Object.getOwnPropertyDescriptor(process.versions, "sqlite");
	Object.defineProperty(process.versions, "sqlite", { value: "3.50.1", configurable: true });
	t.after(() => Object.defineProperty(process.versions, "sqlite", descriptor));
	const store = await openStoreSession({ directory, backend: "files" });
	await store.write("saved", "42\n");
	await store.close();
	await migrateStore({ directory, backend: "sqlite", offline: true });
	const sqlite = await openStoreSession({ directory });
	try {
		assert.equal(sqlite.backend.kind, "sqlite");
		assert.equal((await sqlite.read("saved")).text, "42\n");
	} finally {
		await sqlite.close();
	}
});

test("SQLite worker rejects an unpatched connection before publishing readiness or enabling WAL", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-worker-version-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const filePath = path.join(directory, "lobster.sqlite");
	const worker = new Worker(new URL("./fixtures/sqlite-version.js", import.meta.url), {
		workerData: { path: filePath, readOnly: false },
		execArgv: [],
	});
	t.after(() => worker.terminate());
	let ready = false;
	worker.on("message", () => {
		ready = true;
	});
	const exited = new Promise<void>((resolve) => worker.once("exit", () => resolve()));
	const [error] = await once(worker, "error");
	await exited;
	assert.match(error.message, /WAL-reset fix.*found 3.50.1/);
	assert.equal(ready, false);
	await assert.rejects(fs.access(filePath + "-wal"), { code: "ENOENT" });
	const { DatabaseSync } = await import("node:sqlite");
	const db = new DatabaseSync(filePath, { readOnly: true });
	try {
		assert.deepEqual(db.prepare("SELECT name FROM sqlite_schema").all(), []);
	} finally {
		db.close();
	}
});
