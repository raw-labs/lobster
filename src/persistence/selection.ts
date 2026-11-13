import { promises as fs } from "node:fs";
import path from "node:path";
import type { BackendKind } from "./backend.js";

type BackendSelection = { version: 1; backend: BackendKind };

export async function readBackendSelection(
	directory: string,
): Promise<BackendSelection | undefined> {
	let selected: unknown;
	try {
		selected = JSON.parse(await fs.readFile(path.join(directory, ".backend"), "utf8"));
	} catch (error) {
		if (error.code === "ENOENT") return undefined;
		throw error;
	}
	if (
		!selected ||
		typeof selected !== "object" ||
		!("version" in selected) ||
		selected.version !== 1 ||
		!("backend" in selected) ||
		typeof selected.backend !== "string" ||
		!["files", "sqlite"].includes(selected.backend)
	) {
		throw new Error("Unsupported backend selection");
	}
	return selected as BackendSelection;
}
