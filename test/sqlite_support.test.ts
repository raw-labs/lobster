import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
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

test("unpatched runtimes refuse SQLite opening and migration while files remain usable", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-sqlite-version-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const descriptor = Object.getOwnPropertyDescriptor(process.versions, "sqlite");
	Object.defineProperty(process.versions, "sqlite", { value: "3.50.1", configurable: true });
	t.after(() => Object.defineProperty(process.versions, "sqlite", descriptor));
	await assert.rejects(openStoreSession({ directory, backend: "sqlite" }), /WAL-reset fix/);
	await fs.writeFile(path.join(directory, "saved.json"), "42\n");
	await assert.rejects(
		migrateStore({ directory, backend: "sqlite", offline: true }),
		/WAL-reset fix/,
	);
	assert.equal(await fs.readFile(path.join(directory, "saved.json"), "utf8"), "42\n");
	await assert.rejects(fs.access(path.join(directory, ".backend")), { code: "ENOENT" });
	await assert.rejects(fs.access(path.join(directory, "lobster.sqlite")), { code: "ENOENT" });
	const store = await openStoreSession({ directory, backend: "files" });
	try {
		assert.equal((await store.read("saved")).text, "42\n");
	} finally {
		await store.close();
	}
});
