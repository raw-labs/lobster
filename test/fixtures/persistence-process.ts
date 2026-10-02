import { runToolRequest, resumeToolRequest } from "../../src/core/tool_runtime.js";

const [mode, value] = process.argv.slice(2);

if (mode === "cli-fault") {
	const { runCli } = await import("../../src/cli.js");
	const { PersistenceOutcomeUnknown } = await import("../../src/persistence/backend.js");
	const Backend =
		process.env.LOBSTER_BACKEND === "files"
			? (await import("../../src/persistence/files.js")).FileBackend
			: (await import("../../src/persistence/sqlite.js")).SqliteBackend;
	const transition = Backend.prototype.transition;
	Backend.prototype.transition = async function (request, signal) {
		const result = await transition.call(this, request, signal);
		if (request.action.kind === "settle") {
			throw new PersistenceOutcomeUnknown(
				request.operationId,
				new Error("fixture lost terminal reply"),
				request.runId,
			);
		}
		return result;
	};
	await runCli(["run", "--mode", "tool", "--file", value]);
} else {
	const result =
		mode === "run"
			? await runToolRequest({ filePath: value, ctx: { env: process.env } })
			: await resumeToolRequest({ token: value, approved: true, ctx: { env: process.env } });
	process.stdout.write(JSON.stringify(result));
}
