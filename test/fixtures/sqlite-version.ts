import { DatabaseSync } from "node:sqlite";

// Simulate an older linked library at the connection boundary, not by changing
// process metadata that can disagree with a dynamically selected library.
const prepare = DatabaseSync.prototype.prepare;
let installed = false;
DatabaseSync.prototype.prepare = function (...args) {
	if (!installed) {
		this.function("sqlite_version", () => "3.50.1");
		installed = true;
	}
	return prepare.apply(this, args);
};
await import("../../src/persistence/sqlite-worker.js");
