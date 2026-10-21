import os from "node:os";
import path from "node:path";

export function resolveStateDirectory(env: Record<string, string | undefined>): string {
	return (
		(env?.LOBSTER_STATE_DIR && String(env.LOBSTER_STATE_DIR).trim()) ||
		path.join(os.homedir(), ".lobster", "state")
	);
}

export function normalizeStateKey(key: unknown): string {
	let safe = String(key)
		.toLowerCase()
		.replace(/[^a-z0-9._-]+/g, "_")
		.replace(/_+/g, "_");
	if (safe.startsWith("_")) {
		safe = safe.slice(1);
	}
	if (safe.endsWith("_")) {
		safe = safe.slice(0, -1);
	}
	if (!safe) {
		throw new Error("state key is empty/invalid");
	}
	return safe;
}

export function keyToPath(stateDir: string, key: unknown): string {
	return path.join(stateDir, `${normalizeStateKey(key)}.json`);
}
