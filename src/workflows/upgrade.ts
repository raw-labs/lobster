import { promises as fs } from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { assertOffline } from "../persistence/migrate.js";
import { readBackendSelection } from "../persistence/selection.js";
import { assertSqliteNodeSupport } from "../persistence/sqlite-support.js";
import { FileBackend } from "../persistence/files.js";
import { StoreSession } from "../persistence/session.js";
import { fingerprint } from "../persistence/model.js";
import type { Backend } from "../persistence/backend.js";
import { ensureDirectory, writeFileAtomic } from "../state/atomic_file.js";
import { withFileLock } from "../state/file_lock.js";
import { snapshotDefinition } from "../definition/normalize.js";
import { resolveWorkflowArgs } from "../definition/args.js";
import { extractApprovalRequest } from "../engine/gates.js";
import { occurrence, type Activation, type ExecutionState } from "../engine/state.js";
import type { WaitRequest } from "../persistence/waits.js";
import { resolveTemplate } from "./expressions.js";
import { getStepExecution, isApprovalStep, isInputStep } from "./load.js";
import { createRecord, mergeEnv, resolveCwd, resolveInputValue } from "./values.js";
import type { WorkflowStepResult } from "./types.js";

/**
 * One-off upgrade of state written by Lobster versions before the workflow state machine.
 *
 * Only paused workflow files changed format. Paused pipelines, SDK pauses and short
 * approval IDs are read as they are and need nothing. Each converted pause keeps its
 * state key, so tokens and approval IDs handed out before the upgrade keep working.
 */

export type UpgradeReport = {
	dryRun: boolean;
	converted: { checkpointId: string; workflow: string; runId?: string }[];
	skipped: { checkpointId: string; workflow?: string; reason: string; movedTo?: string }[];
	backup?: string;
};

const OLD_WORKFLOW_FILE = /^workflow[-_]resume_[a-z0-9._-]+\.json$/;
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

function isOldWorkflowPause(value: unknown): value is OldWorkflowPause {
	const item = value as Record<string, unknown> | null;
	return (
		item !== null &&
		typeof item === "object" &&
		item.type !== "lobster.consumed-resume-state.v1" &&
		item.machineVersion === undefined
	);
}

/** Paused workflows on disk that still need `lobster upgrade`. Cheap enough to call per command. */
export async function pendingUpgrade(directory: string): Promise<string[]> {
	if ((await readBackendSelection(directory).catch(() => null))?.backend === "sqlite") return [];
	let names: string[];
	try {
		names = await fs.readdir(directory);
	} catch {
		return [];
	}
	const pending: string[] = [];
	for (const name of names.sort()) {
		if (!OLD_WORKFLOW_FILE.test(name)) continue;
		try {
			if (isOldWorkflowPause(JSON.parse(await fs.readFile(path.join(directory, name), "utf8"))))
				pending.push(name.slice(0, -5));
		} catch {
			// Unreadable or half-written files are reported by the upgrade itself.
			pending.push(name.slice(0, -5));
		}
	}
	return pending;
}

export async function upgradeStore({
	directory,
	dryRun = false,
}: {
	directory: string;
	dryRun?: boolean;
}): Promise<UpgradeReport> {
	directory = path.resolve(directory);
	const report: UpgradeReport = { dryRun, converted: [], skipped: [] };
	try {
		await fs.access(directory);
	} catch {
		return report;
	}
	return withFileLock({
		filePath: path.join(directory, ".backend-selection"),
		task: async () => {
			if (!dryRun) await assertOffline(directory);
			const selected = await readBackendSelection(directory);
			const kind = selected?.backend ?? "files";
			let backend: Backend;
			if (kind === "sqlite") {
				assertSqliteNodeSupport();
				const { SqliteBackend } = await import("../persistence/sqlite.js");
				backend = new SqliteBackend(path.join(directory, "lobster.sqlite"), dryRun);
			} else {
				backend = new FileBackend(directory, dryRun);
			}
			// The selection lock is held here, so the session must not try to take it again.
			const session = new StoreSession(directory, backend, false, undefined, dryRun, true);
			try {
				const keys = Object.keys({
					...(await backend.listState("workflow_resume_")),
					...(await backend.listState("workflow-resume_")),
				}).sort();
				const pending: { key: string; value: OldWorkflowPause | null; error?: string }[] = [];
				for (const key of keys) {
					const record = await session.read(key);
					if (record.text === null) continue;
					if (record.runId) {
						// Converted by an earlier, interrupted upgrade; drop the shadowed original.
						if (!dryRun && kind === "files") await removeTopLevel(directory, key);
						continue;
					}
					try {
						const value = JSON.parse(record.text);
						if (isOldWorkflowPause(value)) pending.push({ key, value });
					} catch {
						pending.push({ key, value: null, error: "Saved state is not valid JSON" });
					}
				}
				// Take the backup before anything changes, including setting files aside.
				if (!dryRun && pending.length) {
					report.backup = await writeBackup(directory, backend);
					if (!selected) {
						await writeFileAtomic(
							path.join(directory, ".backend"),
							JSON.stringify({ version: 1, backend: kind }) + "\n",
						);
					}
				}
				for (const { key, value, error } of pending) {
					const workflow = typeof value?.filePath === "string" ? value.filePath : undefined;
					let converted: Awaited<ReturnType<typeof convertWorkflowPause>>;
					try {
						if (!value) throw new Error(error);
						converted = await convertWorkflowPause(key, value);
					} catch (error) {
						const skipped: UpgradeReport["skipped"][number] = {
							checkpointId: key,
							...(workflow ? { workflow } : {}),
							reason: error?.message ?? String(error),
						};
						if (!dryRun && kind === "files") skipped.movedTo = await setAside(directory, key);
						report.skipped.push(skipped);
						continue;
					}
					if (dryRun) {
						report.converted.push({ checkpointId: key, workflow: workflow! });
						continue;
					}
					const runId = await session.createWaitingRun({
						id: randomUUID(),
						key,
						execution: converted.execution,
						continuation: converted.continuation,
					});
					if (kind === "files") await removeTopLevel(directory, key);
					report.converted.push({ checkpointId: key, workflow: workflow!, runId });
				}
			} finally {
				await session.close();
			}
			return report;
		},
	});
}

async function removeTopLevel(directory: string, key: string) {
	await fs.unlink(path.join(directory, `${key}.json`)).catch((error) => {
		if (error.code !== "ENOENT") throw error;
	});
}

async function setAside(directory: string, key: string): Promise<string> {
	const target = path.join(directory, SKIPPED_DIRECTORY);
	await ensureDirectory(target);
	const destination = path.join(target, `${key}.json`);
	await fs.rename(path.join(directory, `${key}.json`), destination);
	return destination;
}

async function writeBackup(directory: string, backend: Backend): Promise<string> {
	const archive = await backend.export();
	const file = path.join(directory, `.backup-${fingerprint(archive)}.archive`);
	await writeFileAtomic(file, JSON.stringify(archive) + "\n");
	return file;
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
		const approval = extractApprovalRequest(step, subject, {});
		delete approval.initiatedBy;
		delete approval.requiredApprover;
		delete approval.requireDifferentApprover;
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
		...(lastStepId ? { lastStepId, result: results[lastStepId] } : {}),
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
