import { execFileSync } from "node:child_process";
import { promises as fs } from "node:fs";
import path from "node:path";

/**
 * A `.session-<pid>-<uuid>` file marks a process with persistence open. A process
 * that is killed leaves its marker behind, and its PID can later belong to an
 * unrelated (or another user's) process, so a marker records its process's start
 * time as well: the PID plus start time identify the process, not the PID alone.
 */
export type SessionMarker = { pid: number; startedAt?: number };

export function currentSessionMarker(): SessionMarker {
	return { pid: process.pid, startedAt: Math.round(Date.now() - process.uptime() * 1000) };
}

function processStartTime(pid: number): number | undefined {
	if (process.platform === "win32") return undefined;
	try {
		const text = execFileSync("ps", ["-o", "lstart=", "-p", String(pid)], {
			encoding: "utf8",
			stdio: ["ignore", "pipe", "ignore"],
			env: { ...process.env, LC_ALL: "C" },
			timeout: 2000,
		}).trim();
		const time = Date.parse(text);
		return Number.isFinite(time) ? time : undefined;
	} catch {
		return undefined;
	}
}

/** Whether the process that wrote this marker may still be running. */
export function markerIsLive(marker: SessionMarker): boolean {
	if (!Number.isSafeInteger(marker?.pid) || marker.pid <= 0) return false;
	try {
		process.kill(marker.pid, 0);
	} catch (error) {
		if (error.code === "ESRCH") return false;
		// EPERM: a process with this PID exists but belongs to someone else.
	}
	if (marker.pid === process.pid) return true;
	if (marker.startedAt === undefined) return true;
	const started = processStartTime(marker.pid);
	// `ps` reports whole seconds; a different start time means the PID was reused.
	return started === undefined || Math.abs(started - marker.startedAt) < 2000;
}

/** Remove markers of processes that are gone; return the ones that still look live. */
export async function liveSessionMarkers(directory: string): Promise<string[]> {
	const live: string[] = [];
	for (const name of await fs.readdir(directory)) {
		if (!name.startsWith(".session-")) continue;
		const file = path.join(directory, name);
		let marker: SessionMarker;
		try {
			marker = JSON.parse(await fs.readFile(file, "utf8"));
		} catch (error) {
			if (error.code === "ENOENT") continue;
			marker = { pid: -1 }; // unreadable: written by a process that died mid-write
		}
		if (markerIsLive(marker)) live.push(file);
		else await fs.rm(file, { force: true });
	}
	return live;
}
