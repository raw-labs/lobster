import { runToolRequest, resumeToolRequest } from "../../src/core/tool_runtime.js";
const [mode, value] = process.argv.slice(2);
const result =
	mode === "run"
		? await runToolRequest({ filePath: value, ctx: { env: process.env } })
		: await resumeToolRequest({ token: value, approved: true, ctx: { env: process.env } });
process.stdout.write(JSON.stringify(result));
