import { promises as fs } from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { openStoreSession } from "../persistence/session.js";
import { ensureDirectory } from "../state/atomic_file.js";
import { reclaimOrphanedFileLock } from "../state/file_lock.js";
import { keyToPath } from "../state/paths.js";
import { snapshotDefinition } from "./definition.js";
import { resolveWorkflowArgs } from "./args.js";
import { extractApprovalRequest } from "../engine/gates.js";
import { occurrence, type Activation, type ExecutionState } from "../engine/state.js";
import type { WaitRequest } from "../persistence/waits.js";
import { resolveTemplate } from "./expressions.js";
import { getStepExecution, isApprovalStep, isInputStep } from "./load.js";
import { createRecord, mergeEnv, resolveCwd, resolveInputValue } from "./values.js";
import type { WorkflowStepResult } from "./types.js";

/**
 * One-off upgrade from Lobster versions that kept state as loose JSON files in the
 * state directory. Everything moves into the run store (`lobster.sqlite`) under
 * the same keys, so tokens and short approval IDs handed out before keep working:
 *
 * - paused workflow files become waiting runs of the workflow state machine;
 * - everything else (paused pipelines and SDK steps, approval IDs, `state.set`
 *   values, consumed-token markers) is imported as it is.
 *
 * The originals are moved to `.pre-upgrade/<time>/`; pauses that cannot be
 * converted go to `.upgrade-skipped/` and are listed in the report.
 */

export type UpgradeReport = {
	dryRun: boolean;
	/** Paused workflows that became waiting runs. */
	converted: { checkpointId: string; workflow: string; runId?: string }[];
	/** Other saved values imported as they are. */
	imported: string[];
	skipped: { checkpointId: string; workflow?: string; reason: string; movedTo?: string }[];
	/** Where the original files were moved. */
	backup?: string;
};

/** Files an older Lobster kept in the state directory: one `<key>.json` per value. */
const OLD_FILE = /^[a-z0-9][a-z0-9._-]*\.json$/;
const SKIPPED_DIRECTORY = ".upgrade-skipped";

type OldWorkflowPause = {
	filePath: string;
	resumeAtIndex: number;
	steps: Record<string, WorkflowStepResult>;
	args?: Record<string, unknown>;
	approvalStepId?: string;
	approvalIdentity?: Record<string, unknown>;
	inputStepId?: string;
	inputKind?: "workflow_step" | "pipeline_command";
	inputSchema?: unknown;
	inputSubject?: unknown;
	pipelineInput?: {
		resumeAtIndex: number;
		commandInput: {
			pending: {
				requestIndex: number;
				metadata: {
					prompt: string;
					responseSchema: unknown;
					defaults?: unknown;
					subject?: unknown;
				};
			};
		};
	};
	cost?: ExecutionState["cost"];
	llmSpend?: ExecutionState["llmSpend"];
	llmBilled?: ExecutionState["llmBilled"];
};

function isOldWorkflowPause(key: string, value: unknown): value is OldWorkflowPause {
	const item = value as Record<string, unknown> | null;
	return (
		/^workflow[-_]resume_/.test(key) &&
		item !== null &&
		typeof item === "object" &&
		item.type !== "lobster.consumed-resume-state.v1" &&
		item.machineVersion === undefined
	);
}

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
	const report: UpgradeReport = { dryRun, converted: [], imported: [], skipped: [] };
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
			: await openStoreSession({ directory, trackRuns: true, readOnly: dryRun });
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
			if (isOldWorkflowPause(key, value)) {
				const workflow = String(value.filePath);
				if (existing.runId) {
					// Converted by an earlier upgrade that stopped before moving the file.
					await keep(key);
					continue;
				}
				let converted: Awaited<ReturnType<typeof convertWorkflowPause>>;
				try {
					converted = await convertWorkflowPause(key, value);
				} catch (error) {
					await skip(report, directory, dryRun, key, error?.message ?? String(error), workflow);
					continue;
				}
				if (dryRun) {
					report.converted.push({ checkpointId: key, workflow });
					continue;
				}
				const runId = await session!.createWaitingRun({
					id: randomUUID(),
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

/** Rebuild the state machine's view of a pause written by the old workflow executor. */
export async function convertWorkflowPause(key: string, old: OldWorkflowPause) {
	if (
		typeof old.filePath !== "string" ||
		!old.filePath ||
		!Number.isSafeInteger(old.resumeAtIndex) ||
		!old.steps ||
		typeof old.steps !== "object" ||
		(old.approvalStepId === undefined) === (old.inputStepId === undefined)
	) {
		throw new Error("Unrecognized saved workflow state");
	}
	let definition: ExecutionState["definition"];
	try {
		definition = await snapshotDefinition(path.resolve(old.filePath), old.args ?? {});
	} catch (error) {
		throw new Error(
			error?.code === "ENOENT"
				? `Workflow file not found: ${old.filePath}`
				: `Workflow file cannot be loaded: ${error?.message ?? String(error)}`,
		);
	}
	const steps = definition.workflow.steps;
	const args = resolveWorkflowArgs(definition.workflow.args, old.args ?? {});
	const results = createRecord<WorkflowStepResult>(structuredClone(old.steps));
	const stepId = (old.approvalStepId ?? old.inputStepId)!;
	const index = steps.findIndex((step) => step.id === stepId);
	const step = steps[index];
	// The old executor saved the index to continue from: past an approval or input
	// step, but at a pipeline step that is waiting inside one of its commands.
	const pipelineCommand = old.inputKind === "pipeline_command";
	if (!step || old.resumeAtIndex !== (pipelineCommand ? index : index + 1)) {
		throw new Error(`Workflow file changed since the pause: step '${stepId}' moved or was removed`);
	}

	const stepKey = occurrence("root", step.id);
	const attemptKey = stepKey + "/attempt-1";
	const activations: Record<string, Activation> = {};
	let gateKey: string;
	let wait: WaitRequest;

	if (old.approvalStepId !== undefined) {
		if (!isApprovalStep(step.approval))
			throw new Error(
				`Workflow file changed since the pause: step '${stepId}' is no longer an approval`,
			);
		const subject = results[step.id];
		if (!subject) throw new Error("Unrecognized saved workflow state");
		delete results[step.id];
		// Approver rules from the workflow file stay in force; the identity saved at the
		// pause (which also captured environment-provided values) takes precedence.
		const approval = extractApprovalRequest(step, subject, {});
		Object.assign(approval, old.approvalIdentity ?? {});
		gateKey = attemptKey + "/approval";
		wait = {
			kind: "approval",
			prompt: approval.prompt,
			items: approval.items,
			...(approval.preview ? { preview: approval.preview } : {}),
		};
		// The step already ran and was charged; only the decision is outstanding.
		activations[stepKey] = { phase: "started", attempt: 1, accounted: true };
		activations[gateKey] = {
			phase: "waiting",
			attempt: 1,
			approval,
			result: {
				...subject,
				...(approval.initiatedBy ? { initiatedBy: approval.initiatedBy } : {}),
			},
			wait,
			waitKey: key,
		};
	} else if (!pipelineCommand) {
		if (!isInputStep(step.input))
			throw new Error(
				`Workflow file changed since the pause: step '${stepId}' is no longer an input`,
			);
		gateKey = attemptKey;
		wait = {
			kind: "input",
			prompt: step.input!.prompt,
			responseSchema: old.inputSchema ?? step.input!.responseSchema,
			...(step.input!.defaults !== undefined ? { defaults: step.input!.defaults } : {}),
			subject: old.inputSubject ?? null,
		};
		activations[stepKey] = { phase: "started", attempt: 1 };
		activations[gateKey] = { phase: "waiting", attempt: 1, stepId: step.id, wait, waitKey: key };
	} else {
		const execution = getStepExecution(step);
		const pending = old.pipelineInput?.commandInput?.pending;
		if (execution.kind !== "pipeline")
			throw new Error(
				`Workflow file changed since the pause: step '${stepId}' is no longer a pipeline`,
			);
		if (!pending?.metadata || !Number.isSafeInteger(old.pipelineInput!.resumeAtIndex))
			throw new Error("Unrecognized saved workflow state");
		const request = pending.metadata;
		gateKey =
			attemptKey +
			"/pipeline-input-" +
			old.pipelineInput!.resumeAtIndex +
			"-" +
			pending.requestIndex;
		wait = {
			kind: "input",
			prompt: request.prompt,
			responseSchema: old.inputSchema ?? request.responseSchema,
			...(request.defaults !== undefined ? { defaults: request.defaults } : {}),
			subject: old.inputSubject ?? request.subject ?? null,
		};
		const cwd = resolveCwd(step.cwd ?? definition.workflow.cwd, args);
		activations[stepKey] = { phase: "started", attempt: 1, dispatchAttempts: 1 };
		activations[attemptKey + "/activity"] = {
			phase: "waiting",
			attempt: 1,
			pipelineInput: old.pipelineInput as Activation["pipelineInput"],
			wait,
			input: {
				kind: "pipeline",
				text: resolveTemplate(execution.value, args, results),
				input: resolveInputValue(step.stdin, args, results),
				env: mergeEnv({}, definition.workflow.env, step.env, args, results),
				...(cwd ? { cwd } : {}),
			},
		};
		activations[gateKey] = { phase: "waiting", attempt: 1, stepId: step.id, wait, waitKey: key };
	}

	let lastStepId: string | undefined;
	for (let i = index - 1; i >= 0; i--) {
		const result = results[steps[i].id];
		if (result && !result.skipped && !result.error) {
			lastStepId = steps[i].id;
			break;
		}
	}
	activations.root = {
		phase: "started",
		attempt: 1,
		cursor: index,
		results,
		...(lastStepId ? { lastStepId } : {}),
	};

	// No cwd: the old executor ran steps without one in the directory of whoever resumed.
	const execution: ExecutionState = {
		version: 1,
		revision: 1,
		definition,
		args,
		activations,
		status: "waiting",
		...(old.cost ? { cost: old.cost } : {}),
		...(old.llmSpend?.length ? { llmSpend: old.llmSpend } : {}),
		...(old.llmBilled?.length ? { llmBilled: old.llmBilled } : {}),
	};
	const continuation = (runId: string) =>
		JSON.stringify({
			machineVersion: 1,
			runId,
			activation: gateKey,
			wait,
			...(wait.kind === "input" ? { inputStepId: gateKey } : { approvalStepId: gateKey }),
		});
	return { execution, continuation };
}
