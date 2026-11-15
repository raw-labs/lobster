import test from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { createToolContext } from "../src/core/tool_runtime.js";
import { runWorkflowFile } from "../src/workflows/file.js";

for (const backend of ["files", "sqlite"]) {
	test(`machine ${backend}: parent budget stops further child and parent effects`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-budget-regression-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const marker = path.join(directory, "effect");
		const effect = {
			id: "effect",
			run: `node -e 'require("fs").writeFileSync(process.env.MARKER,"executed")'`,
		};
		await fs.writeFile(
			path.join(directory, "child.lobster"),
			JSON.stringify({
				steps: [
					{
						id: "paid",
						run: `node -e 'console.log(JSON.stringify({model:"gpt-4o",usage:{inputTokens:1000,outputTokens:1000}}))'`,
					},
					effect,
				],
			}),
		);
		const filePath = path.join(directory, "parent.lobster");
		await fs.writeFile(
			filePath,
			JSON.stringify({
				cost_limit: { max_usd: 0.00001, action: "stop" },
				steps: [{ id: "child", workflow: "child.lobster" }, effect],
			}),
		);
		await assert.rejects(
			runWorkflowFile({
				filePath,
				ctx: createToolContext({
					env: {
						...process.env,
						MARKER: marker,
						LOBSTER_BACKEND: backend,
						LOBSTER_STATE_DIR: path.join(directory, "state"),
					},
				}),
			}),
			/Cost limit exceeded/,
		);
		await assert.rejects(fs.access(marker), { code: "ENOENT" });
	});

	for (const parallel of [false, true]) {
		test(`machine ${backend}: input subject follows ${parallel ? "parallel aggregate" : "last successful step"}`, async (t) => {
			const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-subject-regression-"));
			t.after(() => fs.rm(directory, { recursive: true, force: true }));
			const filePath = path.join(directory, "flow.lobster");
			const data = { id: "data", run: `echo '{"value":42}'` };
			await fs.writeFile(
				filePath,
				JSON.stringify({
					steps: [
						parallel
							? {
									id: "group",
									parallel: { branches: [data, { id: "other", run: `echo '{"value":7}'` }] },
								}
							: data,
						{ id: "skipped", when: false, run: "exit 99" },
						{ id: "question", input: { prompt: "Review?", responseSchema: { type: "object" } } },
					],
				}),
			);
			const result = await runWorkflowFile({
				filePath,
				ctx: createToolContext({
					env: {
						...process.env,
						LOBSTER_BACKEND: backend,
						LOBSTER_STATE_DIR: path.join(directory, "state"),
					},
				}),
			});
			assert.equal(result.status, "needs_input");
			assert.deepEqual(
				result.requiresInput!.subject,
				parallel ? { data: { value: 42 }, other: { value: 7 } } : { value: 42 },
			);
		});
	}

	test(`machine ${backend}: child inherits resolved parent environment`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-env-regression-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		await fs.writeFile(
			path.join(directory, "child.lobster"),
			JSON.stringify({
				args: { target: { default: "child" } },
				env: { CHILD: "${target}" },
				steps: [
					{
						id: "show",
						run: `node -e 'console.log(JSON.stringify({parent:process.env.PARENT,child:process.env.CHILD,step:process.env.STEP,source:process.env.SOURCE}))'`,
					},
				],
			}),
		);
		const filePath = path.join(directory, "parent.lobster");
		await fs.writeFile(
			filePath,
			JSON.stringify({
				args: { target: { default: "parent" } },
				env: { PARENT: "${target}" },
				steps: [
					{ id: "data", run: `echo '{"value":42}'` },
					{
						id: "child",
						workflow: "child.lobster",
						env: { STEP: "${target}", SOURCE: "$data.json.value" },
					},
				],
			}),
		);
		const result = await runWorkflowFile({
			filePath,
			ctx: createToolContext({
				env: {
					...process.env,
					LOBSTER_BACKEND: backend,
					LOBSTER_STATE_DIR: path.join(directory, "state"),
				},
			}),
		});
		assert.deepEqual(result.output, [
			{ parent: "parent", child: "child", step: "parent", source: "42" },
		]);
	});

	test(`machine ${backend}: retries preserve activity ID and increment dispatch attempt`, async (t) => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "lobster-attempt-regression-"));
		t.after(() => fs.rm(directory, { recursive: true, force: true }));
		const filePath = path.join(directory, "flow.lobster");
		await fs.writeFile(
			filePath,
			JSON.stringify({
				steps: [{ id: "work", pipeline: "probe", retry: { max: 2, delay_ms: 0 } }],
			}),
		);
		const seen: Array<{ id: string; attempt: string }> = [];
		const ctx = createToolContext({
			env: {
				...process.env,
				LOBSTER_BACKEND: backend,
				LOBSTER_STATE_DIR: path.join(directory, "state"),
			},
		});
		const original = ctx.registry;
		ctx.registry = {
			get: (name) =>
				name === "probe"
					? {
							name: "probe",
							meta: { argsSchema: { type: "object" } },
							async run({ ctx }: { ctx: { env: Record<string, string> } }) {
								seen.push({
									id: ctx.env.LOBSTER_ACTIVITY_ID,
									attempt: ctx.env.LOBSTER_ACTIVITY_ATTEMPT,
								});
								if (seen.length === 1) throw new Error("transient");
								return {
									output: (async function* () {
										yield "ok";
									})(),
								};
							},
						}
					: original.get(name),
		};
		const result = await runWorkflowFile({ filePath, ctx });
		assert.equal(result.status, "ok");
		assert.deepEqual(
			seen.map((entry) => entry.attempt),
			["1", "2"],
		);
		assert.equal(seen[0].id, seen[1].id);
	});
}
