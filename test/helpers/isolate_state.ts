// Loaded with `node --import` into every test process (see the `test` script).
// Each test file gets a private state directory and home directory, so no test can
// read or write the developer's real ~/.lobster/state, even one that passes an env
// without LOBSTER_STATE_DIR or never sets it. Removed when the process exits.
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";

if (/\.test\.[cm]?js$/.test(process.argv[1] ?? "")) {
	const root = mkdtempSync(path.join(os.tmpdir(), "lobster-test-"));
	process.env.HOME = root;
	process.env.LOBSTER_STATE_DIR = path.join(root, ".lobster", "state");
	process.on("exit", () => rmSync(root, { recursive: true, force: true }));
}
