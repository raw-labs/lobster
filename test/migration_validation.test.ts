import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { migrateStore, openStoreSession } from "../src/core/index.js";

const pipeline = {
	pipeline: [{ name: "approve", args: {}, raw: "approve" }],
	items: [],
	resumeAtIndex: 1,
};
const workflow = { filePath: "/saved/workflow.lobster", steps: {}, args: {}, resumeAtIndex: 1 };
const invalid = [
	{
		name: "saved command input",
		states: {
			pipeline_resume_bad: {
				...pipeline,
				haltType: "input_request",
				resumeMode: "same_stage",
				inputSchema: { type: "integer" },
				commandInput: {},
			},
		},
		error: /Invalid saved input/,
	},
	{
		name: "input schema format",
		states: {
			pipeline_resume_bad: { ...pipeline, haltType: "input_request", inputSchema: "invalid" },
		},
		error: /Invalid saved input/,
	},
	{
		name: "workflow command frame",
		states: {
			workflow_resume_bad: {
				...workflow,
				inputKind: "pipeline_command",
				inputStepId: "question",
				inputSchema: { type: "integer" },
				pipelineInput: {},
			},
		},
		error: /Invalid workflow/,
	},
	{
		name: "SDK input schema",
		states: { sdk_resume_bad: { resumeAtIndex: 0, items: [], commandInput: {} } },
		error: /Invalid SDK/,
	},
	{ name: "pipeline shape", states: { pipeline_resume_bad: {} }, error: /Invalid pipeline/ },
	{
		name: "pipeline cursor",
		states: { pipeline_resume_bad: { ...pipeline, resumeAtIndex: 2 } },
		error: /Invalid pipeline/,
	},
	{
		name: "input schema",
		states: { pipeline_resume_bad: { ...pipeline, haltType: "input_request" } },
		error: /Invalid pipeline/,
	},
	{ name: "SDK frame", states: { sdk_resume_bad: { resumeAtIndex: 0 } }, error: /Invalid SDK/ },
	{
		name: "SDK state reference",
		states: {
			sdk_resume_bad: {
				type: "lobster.sdk-tracked.v1",
				commandInput: {},
				payload: {
					protocolVersion: 1,
					v: 1,
					resumeAtIndex: 0,
					resumeMode: "same_stage",
					stateKey: "sdk_resume_other",
				},
			},
		},
		error: /Invalid SDK/,
	},
	{
		name: "workflow shape",
		states: { workflow_resume_bad: { ...workflow, args: [] } },
		error: /Invalid workflow/,
	},
	{
		name: "consumption marker",
		states: {
			pipeline_resume_bad: {
				type: "lobster.consumed-resume-state.v1",
				claimId: "",
				consumedAt: "invalid",
			},
		},
		error: /Invalid consumed/,
	},
	{
		name: "workflow aliases",
		states: { workflow_resume_bad: workflow, "workflow-resume_bad": workflow },
		error: /Conflicting workflow checkpoint aliases/,
	},
	{
		name: "approval reference",
		states: { approval_abcdef: { stateKey: "ordinary_value" }, ordinary_value: 42 },
		error: /Invalid approval checkpoint reference/,
	},
	{
		name: "superseded pending checkpoint",
		states: {
			pipeline_resume_previous: pipeline,
			pipeline_resume_next: {
				...pipeline,
				supersededResumeStateKeys: ["pipeline_resume_previous"],
			},
		},
		error: /Invalid superseded checkpoint reference/,
	},
];

for (const source of ["files", "sqlite"] as const) {
	for (const fixture of invalid) {
		test(`${source}: migration rejects invalid legacy ${fixture.name} before activation`, async (t) => {
			const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-invalid-migration-"));
			t.after(() => fs.rm(directory, { recursive: true, force: true }));
			const store = await openStoreSession({ directory, backend: source });
			for (const [key, value] of Object.entries(fixture.states))
				await store.write(key, JSON.stringify(value));
			const before = await store.backend.export();
			await store.close();
			await assert.rejects(
				migrateStore({
					directory,
					backend: source === "files" ? "sqlite" : "files",
					offline: true,
				}),
				fixture.error,
			);
			const reopened = await openStoreSession({ directory });
			try {
				assert.equal(reopened.backend.kind, source);
				assert.deepEqual(await reopened.backend.export(), before);
			} finally {
				await reopened.close();
			}
			assert.equal(
				(await fs.readdir(directory)).some((name) => name.startsWith(".backup-")),
				false,
			);
		});
	}
}

test("migration preserves legacy aliases, consumed predecessors, and already-expired optional IDs", async (t) => {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-valid-migration-"));
	t.after(() => fs.rm(directory, { recursive: true, force: true }));
	const store = await openStoreSession({ directory });
	await store.write("workflow-resume_alias", JSON.stringify(workflow));
	await store.write(
		"pipeline_resume_old",
		JSON.stringify({
			type: "lobster.consumed-resume-state.v1",
			claimId: "old-owner",
			consumedAt: "2026-01-01T00:00:00Z",
		}),
	);
	await store.write(
		"pipeline_resume_next",
		JSON.stringify({
			...pipeline,
			supersededResumeStateKeys: ["pipeline_resume_old", "pipeline_resume_already_deleted"],
		}),
	);
	await store.write("approval_abcdef", JSON.stringify({ stateKey: "workflow_resume_alias" }));
	await store.write("approval_123abc", JSON.stringify({ stateKey: "pipeline_resume_expired" }));
	await store.write("workflow_resume_pair", JSON.stringify(workflow));
	await store.write(
		"workflow-resume_pair",
		JSON.stringify({
			type: "lobster.consumed-resume-state.v1",
			claimId: "already-consumed",
			consumedAt: "2026-01-01T00:00:00Z",
		}),
	);
	const before = await store.backend.export();
	await store.close();
	await migrateStore({ directory, backend: "sqlite", offline: true });
	await migrateStore({ directory, backend: "files", offline: true });
	const reopened = await openStoreSession({ directory });
	try {
		assert.deepEqual(await reopened.backend.export(), before);
	} finally {
		await reopened.close();
	}
});

test("opening and migration reject malformed backend selection without reinterpreting stored state", async (t) => {
	const root = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-invalid-selection-"));
	t.after(() => fs.rm(root, { recursive: true, force: true }));
	const invalidSelections = [
		null,
		false,
		[],
		{ version: 2, backend: "files" },
		{ version: 1, backend: "other" },
		{ version: 1, backend: ["files"] },
	];
	for (const [index, value] of invalidSelections.entries()) {
		const directory = path.join(root, String(index));
		await fs.mkdir(directory);
		const selection = JSON.stringify(value);
		await fs.writeFile(path.join(directory, ".backend"), selection);
		await fs.writeFile(path.join(directory, "saved.json"), "42\n");
		await assert.rejects(openStoreSession({ directory }), /Unsupported backend selection/);
		await assert.rejects(
			migrateStore({ directory, backend: "sqlite", offline: true }),
			/Unsupported backend selection/,
		);
		assert.equal(await fs.readFile(path.join(directory, ".backend"), "utf8"), selection);
		assert.equal(await fs.readFile(path.join(directory, "saved.json"), "utf8"), "42\n");
		await assert.rejects(fs.access(path.join(directory, "lobster.sqlite")), { code: "ENOENT" });
	}
});
