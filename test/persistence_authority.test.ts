import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
	getRun,
	getWait,
	openStoreSession,
	resumeToolRequest,
	runToolRequest,
} from "../src/core/index.js";

for (const backend of ["files", "sqlite"] as const) {
	test(`${backend}: retirement during state preparation prevents the write`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-store-authority-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const store = await openStoreSession({ directory, backend, trackRuns: true });
		t.after(() => store.close());
		const write = store.backend.write.bind(store.backend);
		let current = true;
		store.backend.write = async (...args) => {
			if (args[0] === "blocked") current = false;
			return write(...args);
		};
		const result = await runToolRequest({
			pipeline: "state.set blocked",
			ctx: {
				storeSession: store,
				assertInvocationCurrent() {
					if (!current) throw new Error("Host invocation retired");
				},
			},
		});
		assert.equal(result.ok, false);
		assert.match(result.error.message, /Host invocation retired/);
		assert.equal((await store.read("blocked")).text, null);
	});

	for (const boundary of ["before", "after"] as const) {
		for (const history of [false, true]) {
			test(`${backend}: host retirement ${boundary} a handoff preserves the committed wait, history=${history}`, async (t) => {
				const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-handoff-authority-"));
				t.after(() => fs.rm(directory, { recursive: true, force: true }));
				const store = await openStoreSession({ directory, backend, trackRuns: true, history });
				t.after(() => store.close());
				const ctx = { storeSession: store };
				const first = await runToolRequest({
					pipeline:
						'ask --emit --prompt First --schema \'{"type":"integer"}\' | ask --emit --prompt Second --schema \'{"type":"integer"}\'',
					ctx,
				});
				const run = await getRun({ runId: first.runId, ctx });
				const wait = await getWait({ waitId: run.pendingWaits[0].id, ctx });
				const transition = store.backend.transition.bind(store.backend);
				let current = true;
				store.backend.transition = async (...args) => {
					if (args[0].action.kind === "handoff" && boundary === "before") current = false;
					const result = await transition(...args);
					if (args[0].action.kind === "handoff" && boundary === "after") current = false;
					return result;
				};
				const result = await resumeToolRequest({
					waitId: wait.id,
					expectedRevision: wait.revision,
					response: 1,
					ctx: {
						...ctx,
						assertInvocationCurrent() {
							if (!current) throw new Error("Host invocation retired");
						},
					},
				});
				assert.equal(result.ok, false);
				assert.match(result.error.message, /Host invocation retired/);
				const updated = await getRun({ runId: first.runId, ctx });
				assert.equal(updated.status, "waiting");
				assert.equal(updated.pendingWaits.length, 1);
				const remaining = await getWait({ waitId: updated.pendingWaits[0].id, ctx });
				assert.equal(remaining.request.prompt, boundary === "before" ? "First" : "Second");
				assert.equal((await getWait({ waitId: wait.id, ctx })) !== null, boundary === "before");
			});
		}
	}
}
