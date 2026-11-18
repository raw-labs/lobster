// One-off upgrade from Lobster versions that kept state as loose JSON files in the
// state directory. Everything moves into the run store (`lobster.sqlite`) under
// the same keys, so tokens and short approval IDs handed out before keep working:
//
// - paused workflow files and paused pipelines become waiting runs of the state machine;
// - everything else (paused SDK steps, approval IDs, `state.set` values, consumed-token
//   markers) is imported as it is. A paused SDK step needs the host's stages, so it
//   becomes a run the first time its host resumes it.
//
// The originals are moved to `.pre-upgrade/<time>/`; pauses that cannot be
// converted go to `.upgrade-skipped/` and are listed in the report.
import { messageOf } from "../util/errors.js";
import { promises as fs } from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { openStoreSession } from "../persistence/session.js";
import { ensureDirectory } from "../state/atomic_file.js";
import { reclaimOrphanedFileLock } from "../state/file_lock.js";
import { keyToPath } from "../state/paths.js";
import {
	convertPipelinePause,
	convertWorkflowPause,
	isOldPipelinePause,
	isOldWorkflowPause,
	type OldPipelinePause,
	type OldWorkflowPause,
} from "./legacy_pauses.js";

export type UpgradeReport = {
	dryRun: boolean;
	/** Paused workflows and pipelines that became waiting runs. `workflow` is the file, or the pipeline text. */
	converted: { checkpointId: string; workflow: string; runId?: string }[];
	/** Other saved values imported as they are. */
	imported: string[];
	skipped: {
		checkpointId: string;
		workflow?: string;
		reason: string;
		movedTo?: string;
	}[];
	/** Where the original files were moved. */
	backup?: string;
};

/** Files an older Lobster kept in the state directory: one `<key>.json` per value. */
const OLD_FILE = /^[a-z0-9][a-z0-9._-]*\.json$/;
const SKIPPED_DIRECTORY = ".upgrade-skipped";

/** A pause an older Lobster saved, of a standalone pipeline or of a workflow file. */
type OldPause =
	| { kind: "pipeline"; pause: OldPipelinePause }
	| { kind: "workflow"; pause: OldWorkflowPause };

/** Saved values from an older Lobster that still need `lobster upgrade`. Cheap per command. */
export async function pendingUpgrade(directory: string): Promise<string[]> {
	try {
		return (await fs.readdir(directory))
			.filter((name) => OLD_FILE.test(name))
			.map((name) => name.slice(0, -5))
			.sort();
	} catch {
		return [];
	}
}

/** True when the state directory still holds a value an older Lobster saved under this key. */
export async function savedByOlderVersion(directory: string, key: string): Promise<boolean> {
	// Keys can come from a caller's token: normalize so the name never leaves the directory.
	let file: string;
	try {
		file = keyToPath(directory, key);
	} catch {
		return false;
	}
	return fs
		.access(file)
		.then(() => true)
		.catch(() => false);
}

async function assertNoOlderWriter(directory: string) {
	for (const name of await fs.readdir(directory)) {
		if (
			name.endsWith(".lock") &&
			name !== ".backend-selection.lock" &&
			!(await reclaimOrphanedFileLock(path.join(directory, name)))
		)
			throw new Error("Stop other running Lobster processes first: state is locked");
	}
}

export async function upgradeStore({
	directory,
	dryRun = false,
}: {
	directory: string;
	dryRun?: boolean;
}): Promise<UpgradeReport> {
	directory = path.resolve(directory);
	const report: UpgradeReport = {
		dryRun,
		converted: [],
		imported: [],
		skipped: [],
	};
	const keys = await pendingUpgrade(directory);
	if (!keys.length) return report;
	// An older binary writes the files being imported; it must not be running.
	if (!dryRun) await assertNoOlderWriter(directory);
	// A dry run never creates the database; with none yet, nothing is stored under any key.
	const hasDatabase = await fs
		.access(path.join(directory, "lobster.sqlite"))
		.then(() => true)
		.catch(() => false);
	const session =
		dryRun && !hasDatabase
			? undefined
			: await openStoreSession({
					directory,
					trackRuns: true,
					readOnly: dryRun,
				});
	const backup = path.join(directory, ".pre-upgrade", new Date().toISOString().replace(/:/g, "-"));
	const keep = async (key: string) => {
		if (dryRun) return;
		await ensureDirectory(backup);
		await fs.rename(path.join(directory, `${key}.json`), path.join(backup, `${key}.json`));
		report.backup = backup;
	};
	try {
		for (const key of keys) {
			let text: string;
			let value: unknown;
			try {
				text = await fs.readFile(path.join(directory, `${key}.json`), "utf8");
				value = JSON.parse(text);
			} catch (error) {
				if (error?.code === "ENOENT") continue;
				await skip(report, directory, dryRun, key, "Saved value is not valid JSON");
				continue;
			}
			const existing = session ? await session.read(key) : { text: null, runId: undefined };
			const old: OldPause | undefined = isOldPipelinePause(key, value)
				? { kind: "pipeline", pause: value }
				: isOldWorkflowPause(key, value)
					? { kind: "workflow", pause: value }
					: undefined;
			if (old) {
				const workflow =
					old.kind === "pipeline"
						? (old.pause.pipeline ?? []).map((stage) => stage?.raw ?? stage?.name).join(" | ")
						: String(old.pause.filePath);
				if (existing.runId) {
					// Converted by an earlier upgrade that stopped before moving the file.
					await keep(key);
					continue;
				}
				let converted: Awaited<ReturnType<typeof convertWorkflowPause>>;
				try {
					converted =
						old.kind === "pipeline"
							? convertPipelinePause(key, old.pause)
							: await convertWorkflowPause(key, old.pause);
				} catch (error) {
					await skip(report, directory, dryRun, key, messageOf(error), workflow);
					continue;
				}
				if (dryRun) {
					report.converted.push({ checkpointId: key, workflow });
					continue;
				}
				const runId = await session!.createWaitingRun({
					id: randomUUID(),
					kind: old.kind,
					key,
					execution: converted.execution,
					continuation: converted.continuation,
				});
				report.converted.push({ checkpointId: key, workflow, runId });
				await keep(key);
				continue;
			}
			if (existing.text !== null && existing.text !== text) {
				await skip(
					report,
					directory,
					dryRun,
					key,
					"A different value is already stored under this key",
				);
				continue;
			}
			if (existing.text === null && !dryRun) await session!.write(key, text, { exclusive: true });
			report.imported.push(key);
			await keep(key);
		}
	} finally {
		await session?.close();
	}
	return report;
}

async function skip(
	report: UpgradeReport,
	directory: string,
	dryRun: boolean,
	key: string,
	reason: string,
	workflow?: string,
) {
	const skipped: UpgradeReport["skipped"][number] = {
		checkpointId: key,
		...(workflow ? { workflow } : {}),
		reason,
	};
	if (!dryRun) {
		const target = path.join(directory, SKIPPED_DIRECTORY);
		await ensureDirectory(target);
		skipped.movedTo = path.join(target, `${key}.json`);
		await fs.rename(path.join(directory, `${key}.json`), skipped.movedTo);
	}
	report.skipped.push(skipped);
}
