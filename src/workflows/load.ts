import { promises as fsp } from "node:fs";
import path from "node:path";
import { parse as parseYaml } from "yaml";
import { compileCached } from "../validation.js";
import type { WorkflowFile, WorkflowStep, ParallelConfig } from "./types.js";

export async function loadWorkflowFile(filePath: string): Promise<WorkflowFile> {
	const text = await fsp.readFile(filePath, "utf8");
	const ext = path.extname(filePath).toLowerCase();
	const parsed = ext === ".json" ? JSON.parse(text) : parseYaml(text);

	if (!parsed || typeof parsed !== "object") {
		throw new Error("Workflow file must be a JSON/YAML object");
	}

	const steps = (parsed as WorkflowFile).steps;
	if (!Array.isArray(steps) || steps.length === 0) {
		throw new Error("Workflow file requires a non-empty steps array");
	}
	rejectUnknownKeys(parsed as Record<string, unknown>);

	const costLimit = (parsed as WorkflowFile).cost_limit;
	if (costLimit !== undefined) {
		if (!costLimit || typeof costLimit !== "object" || Array.isArray(costLimit)) {
			throw new Error("Workflow cost_limit must be an object");
		}
		if (!Number.isFinite(Number(costLimit.max_usd)) || Number(costLimit.max_usd) < 0) {
			throw new Error("Workflow cost_limit.max_usd must be a non-negative number");
		}
		if (
			costLimit.action !== undefined &&
			costLimit.action !== "warn" &&
			costLimit.action !== "stop"
		) {
			throw new Error('Workflow cost_limit.action must be "warn" or "stop"');
		}
	}

	const seen = new Set<string>();
	for (const step of steps) {
		if (!step || typeof step !== "object") {
			throw new Error("Workflow step must be an object");
		}
		validateRecovery(step);
		if (!step.id || typeof step.id !== "string") {
			throw new Error("Workflow step requires an id");
		}
		if (step.workflow !== undefined && typeof step.workflow !== "string") {
			throw new Error(`Workflow step ${step.id} workflow must be a string (file path)`);
		}
		if (typeof step.workflow === "string" && !step.workflow.trim()) {
			throw new Error(`Workflow step ${step.id} workflow path cannot be blank`);
		}
		if (step.workflow_args !== undefined) {
			if (
				!step.workflow_args ||
				typeof step.workflow_args !== "object" ||
				Array.isArray(step.workflow_args)
			) {
				throw new Error(`Workflow step ${step.id} workflow_args must be a plain object`);
			}
		}
		if (
			step.parallel !== undefined &&
			(!step.parallel || typeof step.parallel !== "object" || Array.isArray(step.parallel))
		) {
			throw new Error(`Workflow step ${step.id} parallel must be an object`);
		}
		const isParallel = Boolean(
			step.parallel && typeof step.parallel === "object" && !Array.isArray(step.parallel),
		);
		if (isParallel) {
			const parallel = step.parallel as ParallelConfig;
			if (!Array.isArray(parallel.branches) || parallel.branches.length === 0) {
				throw new Error(`Workflow step ${step.id} parallel requires a non-empty branches array`);
			}
			if (parallel.wait !== undefined && parallel.wait !== "all" && parallel.wait !== "any") {
				throw new Error(`Workflow step ${step.id} parallel wait must be "all" or "any"`);
			}
			if (
				parallel.timeout_ms !== undefined &&
				(typeof parallel.timeout_ms !== "number" ||
					!Number.isFinite(parallel.timeout_ms) ||
					!Number.isInteger(parallel.timeout_ms) ||
					parallel.timeout_ms < 1 ||
					parallel.timeout_ms > 2_147_483_647)
			) {
				throw new Error(
					`Workflow step ${step.id} parallel timeout_ms must be a positive integer between 1 and 2147483647`,
				);
			}

			const branchIds = new Set<string>();
			for (const branch of parallel.branches) {
				if (!branch || typeof branch !== "object") {
					throw new Error(`Workflow step ${step.id} parallel branches must be objects`);
				}
				validateRecovery(branch);
				if (!branch.id || typeof branch.id !== "string") {
					throw new Error(`Workflow step ${step.id} parallel branch requires an id`);
				}
				if (branch.id === step.id) {
					throw new Error(`Workflow step ${step.id} parallel branch id cannot match the step id`);
				}
				if (branchIds.has(branch.id)) {
					throw new Error(`Workflow step ${step.id} duplicate parallel branch id: ${branch.id}`);
				}
				if (seen.has(branch.id)) {
					throw new Error(`Duplicate workflow id across steps/parallel branches: ${branch.id}`);
				}
				branchIds.add(branch.id);
				const branchShell = typeof branch.run === "string" ? branch.run : branch.command;
				const branchPipeline = typeof branch.pipeline === "string" ? branch.pipeline : undefined;
				const branchExecCount = Number(Boolean(branchShell)) + Number(Boolean(branchPipeline));
				if (branchExecCount === 0) {
					throw new Error(
						`Workflow step ${step.id} parallel branch ${branch.id} requires run, command, or pipeline`,
					);
				}
				if (branchExecCount > 1) {
					throw new Error(
						`Workflow step ${step.id} parallel branch ${branch.id} can only define one of run, command, or pipeline`,
					);
				}
				if (branch.run !== undefined && typeof branch.run !== "string") {
					throw new Error(
						`Workflow step ${step.id} parallel branch ${branch.id} run must be a string`,
					);
				}
				if (branch.command !== undefined && typeof branch.command !== "string") {
					throw new Error(
						`Workflow step ${step.id} parallel branch ${branch.id} command must be a string`,
					);
				}
				if (branch.pipeline !== undefined && typeof branch.pipeline !== "string") {
					throw new Error(
						`Workflow step ${step.id} parallel branch ${branch.id} pipeline must be a string`,
					);
				}
			}
		}
		if (step.for_each !== undefined && typeof step.for_each !== "string") {
			throw new Error(
				`Workflow step ${step.id} for_each must be a string (step reference expression)`,
			);
		}
		const isForEach = typeof step.for_each === "string";
		if (isForEach) {
			if (!Array.isArray(step.steps) || step.steps.length === 0) {
				throw new Error(`Workflow step ${step.id} for_each requires a non-empty steps array`);
			}
			if (
				step.batch_size !== undefined &&
				(typeof step.batch_size !== "number" ||
					!Number.isInteger(step.batch_size) ||
					step.batch_size < 1)
			) {
				throw new Error(`Workflow step ${step.id} batch_size must be a positive integer`);
			}
			if (
				step.pause_ms !== undefined &&
				(typeof step.pause_ms !== "number" || !Number.isFinite(step.pause_ms) || step.pause_ms < 0)
			) {
				throw new Error(`Workflow step ${step.id} pause_ms must be a finite non-negative number`);
			}
			if (isApprovalStep(step.approval)) {
				throw new Error(
					`Workflow step ${step.id} for_each steps cannot define approval (use a separate step after the loop)`,
				);
			}
			if (isInputStep(step.input)) {
				throw new Error(
					`Workflow step ${step.id} for_each steps cannot define input (use a separate step after the loop)`,
				);
			}
			if (step.stdin !== undefined && step.stdin !== null) {
				throw new Error(
					`Workflow step ${step.id} for_each steps cannot define stdin (loop input comes from the for_each expression)`,
				);
			}
			const loopShell = typeof step.run === "string" ? step.run : step.command;
			const loopPipeline = typeof step.pipeline === "string" ? step.pipeline : undefined;
			if (loopShell || loopPipeline || step.workflow || step.parallel) {
				throw new Error(
					`Workflow step ${step.id} for_each cannot also define run, command, pipeline, workflow, or parallel`,
				);
			}
			if (step.item_var !== undefined && typeof step.item_var !== "string") {
				throw new Error(`Workflow step ${step.id} item_var must be a string`);
			}
			if (step.index_var !== undefined && typeof step.index_var !== "string") {
				throw new Error(`Workflow step ${step.id} index_var must be a string`);
			}
			const loopItemVar = step.item_var ?? "item";
			const loopIndexVar = step.index_var ?? "index";
			if (loopItemVar === loopIndexVar) {
				throw new Error(`Workflow step ${step.id} item_var and index_var cannot be the same`);
			}
			const subStepIds = new Set<string>();
			for (const sub of step.steps) {
				if (!sub || typeof sub !== "object" || !sub.id || typeof sub.id !== "string") {
					throw new Error(`Workflow step ${step.id} for_each sub-step requires an id`);
				}
				validateRecovery(sub);
				validateStepOptions(sub, `Workflow step ${step.id} for_each sub-step ${sub.id}`);
				if (sub.id === loopItemVar || sub.id === loopIndexVar) {
					throw new Error(
						`Workflow step ${step.id} for_each sub-step id '${sub.id}' conflicts with loop variable`,
					);
				}
				if (subStepIds.has(sub.id)) {
					throw new Error(`Workflow step ${step.id} duplicate for_each sub-step id: ${sub.id}`);
				}
				subStepIds.add(sub.id);
				if (isApprovalStep(sub.approval) || isInputStep(sub.input)) {
					throw new Error(
						`Workflow step ${step.id} for_each sub-steps cannot contain approval or input steps`,
					);
				}
				if (sub.run !== undefined && typeof sub.run !== "string") {
					throw new Error(
						`Workflow step ${step.id} for_each sub-step ${sub.id} run must be a string`,
					);
				}
				if (sub.command !== undefined && typeof sub.command !== "string") {
					throw new Error(
						`Workflow step ${step.id} for_each sub-step ${sub.id} command must be a string`,
					);
				}
				if (sub.pipeline !== undefined && typeof sub.pipeline !== "string") {
					throw new Error(
						`Workflow step ${step.id} for_each sub-step ${sub.id} pipeline must be a string`,
					);
				}
				if (sub.workflow || sub.parallel || sub.for_each) {
					throw new Error(
						`Workflow step ${step.id} for_each sub-step ${sub.id} cannot define workflow, parallel, or for_each`,
					);
				}
				const subShell =
					typeof sub.run === "string" && sub.run.trim()
						? sub.run
						: typeof sub.command === "string" && sub.command.trim()
							? sub.command
							: undefined;
				const subPipeline =
					typeof sub.pipeline === "string" && sub.pipeline.trim() ? sub.pipeline : undefined;
				if (!subShell && !subPipeline) {
					throw new Error(
						`Workflow step ${step.id} for_each sub-step ${sub.id} requires run, command, or pipeline`,
					);
				}
				if (Number(Boolean(subShell)) + Number(Boolean(subPipeline)) > 1) {
					throw new Error(
						`Workflow step ${step.id} for_each sub-step ${sub.id} can only define one of run, command, or pipeline`,
					);
				}
			}
		}
		const shellCommand = typeof step.run === "string" ? step.run : step.command;
		const pipeline = typeof step.pipeline === "string" ? step.pipeline : undefined;
		const workflowRef =
			typeof step.workflow === "string" && step.workflow.trim() ? step.workflow : undefined;
		const executionCount =
			Number(Boolean(shellCommand)) +
			Number(Boolean(pipeline)) +
			Number(Boolean(workflowRef)) +
			Number(isParallel) +
			Number(isForEach);
		if (executionCount === 0 && !isApprovalStep(step.approval) && !isInputStep(step.input)) {
			throw new Error(
				`Workflow step ${step.id} requires run, command, pipeline, workflow, parallel, for_each, approval, or input`,
			);
		}
		if (executionCount > 1) {
			throw new Error(
				`Workflow step ${step.id} can only define one of run, command, pipeline, workflow, parallel, or for_each`,
			);
		}
		if (executionCount > 0 && isInputStep(step.input)) {
			throw new Error(
				`Workflow step ${step.id} input steps cannot define run, command, pipeline, workflow, parallel, or for_each`,
			);
		}
		if (isApprovalStep(step.approval) && isInputStep(step.input)) {
			throw new Error(`Workflow step ${step.id} cannot define both approval and input`);
		}
		if (step.run !== undefined && typeof step.run !== "string") {
			throw new Error(`Workflow step ${step.id} run must be a string`);
		}
		if (step.command !== undefined && typeof step.command !== "string") {
			throw new Error(`Workflow step ${step.id} command must be a string`);
		}
		if (step.pipeline !== undefined && typeof step.pipeline !== "string") {
			throw new Error(`Workflow step ${step.id} pipeline must be a string`);
		}
		if (step.input !== undefined && !isInputStep(step.input)) {
			throw new Error(`Workflow step ${step.id} input must be an object`);
		}
		if (step.input && typeof step.input.prompt !== "string") {
			throw new Error(`Workflow step ${step.id} input.prompt must be a string`);
		}
		if (
			step.input &&
			(step.input.responseSchema === undefined || typeof step.input.responseSchema !== "object")
		) {
			throw new Error(`Workflow step ${step.id} input.responseSchema must be an object`);
		}
		if (step.input) {
			try {
				compileCached(step.input.responseSchema as any);
			} catch (err: any) {
				throw new Error(
					`Workflow step ${step.id} input.responseSchema is invalid: ${err?.message ?? String(err)}`,
				);
			}
		}
		// Anything else would silently drop the gate and run the step unapproved.
		if (
			step.approval !== undefined &&
			typeof step.approval !== "boolean" &&
			typeof step.approval !== "string" &&
			!isPlainObject(step.approval)
		) {
			throw new Error(
				`Workflow step ${step.id} approval must be true, a prompt string, or an object`,
			);
		}
		if (typeof step.approval === "string" && !step.approval.trim()) {
			throw new Error(`Workflow step ${step.id} approval prompt cannot be blank`);
		}
		if (step.approval && typeof step.approval === "object" && !Array.isArray(step.approval)) {
			const approval = step.approval as Record<string, unknown>;
			for (const key of ["prompt", "preview"])
				if (approval[key] !== undefined && typeof approval[key] !== "string")
					throw new Error(`Workflow step ${step.id} approval.${key} must be a string`);
			if (approval.items !== undefined && !Array.isArray(approval.items)) {
				throw new Error(`Workflow step ${step.id} approval.items must be a list`);
			}
			if (approval.initiated_by !== undefined && typeof approval.initiated_by !== "string") {
				throw new Error(`Workflow step ${step.id} approval.initiated_by must be a string`);
			}
			if (approval.initiatedBy !== undefined && typeof approval.initiatedBy !== "string") {
				throw new Error(`Workflow step ${step.id} approval.initiatedBy must be a string`);
			}
			if (
				approval.required_approver !== undefined &&
				typeof approval.required_approver !== "string"
			) {
				throw new Error(`Workflow step ${step.id} approval.required_approver must be a string`);
			}
			if (
				approval.requiredApprover !== undefined &&
				typeof approval.requiredApprover !== "string"
			) {
				throw new Error(`Workflow step ${step.id} approval.requiredApprover must be a string`);
			}
			if (
				approval.require_different_approver !== undefined &&
				typeof approval.require_different_approver !== "boolean"
			) {
				throw new Error(
					`Workflow step ${step.id} approval.require_different_approver must be a boolean`,
				);
			}
			if (
				approval.requireDifferentApprover !== undefined &&
				typeof approval.requireDifferentApprover !== "boolean"
			) {
				throw new Error(
					`Workflow step ${step.id} approval.requireDifferentApprover must be a boolean`,
				);
			}
		}
		validateStepOptions(step, `Workflow step ${step.id}`);
		if (seen.has(step.id)) {
			throw new Error(`Duplicate workflow step id: ${step.id}`);
		}
		if (isParallel) {
			const parallel = step.parallel as ParallelConfig;
			for (const branch of parallel.branches) {
				seen.add(branch.id);
			}
		}
		seen.add(step.id);
	}

	return parsed as WorkflowFile;
}

export function isApprovalStep(approval: WorkflowStep["approval"]) {
	if (approval === true) return true;
	if (typeof approval === "string" && approval.trim().length > 0) return true;
	if (approval && typeof approval === "object" && !Array.isArray(approval)) return true;
	return false;
}

export function isInputStep(input: WorkflowStep["input"]) {
	return Boolean(input && typeof input === "object" && !Array.isArray(input));
}

export function getStepExecution(step: WorkflowStep) {
	if (step.parallel && typeof step.parallel === "object" && !Array.isArray(step.parallel)) {
		return { kind: "parallel" as const, value: step.parallel };
	}

	if (typeof step.workflow === "string" && step.workflow.trim()) {
		return { kind: "workflow" as const, value: step.workflow };
	}

	if (typeof step.pipeline === "string" && step.pipeline.trim()) {
		return { kind: "pipeline" as const, value: step.pipeline };
	}

	const shellCommand = typeof step.run === "string" ? step.run : step.command;
	if (typeof shellCommand === "string" && shellCommand.trim()) {
		return { kind: "shell" as const, value: shellCommand };
	}

	return { kind: "none" as const };
}

// Options every step accepts, wherever it is nested.
function validateStepOptions(step: WorkflowStep, name: string) {
	if (
		step.timeout_ms !== undefined &&
		(typeof step.timeout_ms !== "number" ||
			!Number.isFinite(step.timeout_ms) ||
			!Number.isInteger(step.timeout_ms) ||
			step.timeout_ms < 1 ||
			step.timeout_ms > 2_147_483_647)
	) {
		throw new Error(`${name} timeout_ms must be a positive integer between 1 and 2147483647`);
	}
	if (
		step.on_error !== undefined &&
		step.on_error !== "stop" &&
		step.on_error !== "continue" &&
		step.on_error !== "skip_rest"
	) {
		throw new Error(`${name} on_error must be "stop", "continue", or "skip_rest"`);
	}
	if (step.retry !== undefined) {
		if (!step.retry || typeof step.retry !== "object" || Array.isArray(step.retry)) {
			throw new Error(`${name} retry must be an object`);
		}
		const r = step.retry;
		if (
			r.max !== undefined &&
			(typeof r.max !== "number" || !Number.isInteger(r.max) || r.max < 1)
		) {
			throw new Error(`${name} retry.max must be a positive integer`);
		}
		if (r.backoff !== undefined && r.backoff !== "fixed" && r.backoff !== "exponential") {
			throw new Error(`${name} retry.backoff must be "fixed" or "exponential"`);
		}
		if (
			r.delay_ms !== undefined &&
			(typeof r.delay_ms !== "number" || !Number.isFinite(r.delay_ms) || r.delay_ms < 0)
		) {
			throw new Error(`${name} retry.delay_ms must be a finite non-negative number`);
		}
		if (
			r.max_delay_ms !== undefined &&
			(typeof r.max_delay_ms !== "number" || !Number.isFinite(r.max_delay_ms) || r.max_delay_ms < 0)
		) {
			throw new Error(`${name} retry.max_delay_ms must be a finite non-negative number`);
		}
		if (r.jitter !== undefined && typeof r.jitter !== "boolean") {
			throw new Error(`${name} retry.jitter must be a boolean`);
		}
	}
	validateCondition(step, name);
}

// Checked here, not when the step is reached: by then earlier steps have had their effects.
function validateCondition(step: WorkflowStep, name: string) {
	for (const key of ["when", "condition"] as const) {
		const value = step[key];
		if (
			value !== undefined &&
			value !== null &&
			typeof value !== "boolean" &&
			typeof value !== "string"
		)
			throw new Error(`${name} ${key} must be a boolean or an expression string`);
	}
}

function validateRecovery(step: WorkflowStep) {
	if (step.recovery !== undefined && step.recovery !== "repeat" && step.recovery !== "manual")
		throw new Error("Workflow recovery must be repeat or manual");
}

// A misspelled key must not be ignored: `wehn:` would otherwise run its step unconditionally.
const WORKFLOW_KEYS = ["name", "description", "args", "env", "cwd", "steps", "cost_limit"];
const STEP_KEYS = [
	"id",
	"recovery",
	"command",
	"run",
	"pipeline",
	"workflow",
	"workflow_args",
	"env",
	"cwd",
	"stdin",
	"approval",
	"input",
	"condition",
	"when",
	"parallel",
	"for_each",
	"item_var",
	"index_var",
	"batch_size",
	"pause_ms",
	"steps",
	"timeout_ms",
	"on_error",
	"retry",
];
const BRANCH_KEYS = ["id", "recovery", "run", "command", "pipeline", "env", "cwd", "stdin"];
const PARALLEL_KEYS = ["wait", "timeout_ms", "branches"];
const RETRY_KEYS = ["max", "backoff", "delay_ms", "max_delay_ms", "jitter"];
const COST_LIMIT_KEYS = ["max_usd", "action"];
const INPUT_KEYS = ["prompt", "responseSchema", "defaults"];
const APPROVAL_KEYS = [
	"prompt",
	"items",
	"preview",
	"initiated_by",
	"initiatedBy",
	"required_approver",
	"requiredApprover",
	"require_different_approver",
	"requireDifferentApprover",
];

function rejectUnknownKeys(workflow: Record<string, unknown>) {
	checkKeys(workflow, WORKFLOW_KEYS, "Workflow file");
	checkKeys(workflow.cost_limit, COST_LIMIT_KEYS, "Workflow cost_limit");
	const visit = (step: unknown, where: string) => {
		if (!isPlainObject(step)) return;
		const name = typeof step.id === "string" ? `${where} ${step.id}` : where;
		checkKeys(step, STEP_KEYS, name);
		checkKeys(step.retry, RETRY_KEYS, `${name} retry`);
		checkKeys(step.input, INPUT_KEYS, `${name} input`);
		checkKeys(step.approval, APPROVAL_KEYS, `${name} approval`);
		if (isPlainObject(step.parallel)) {
			checkKeys(step.parallel, PARALLEL_KEYS, `${name} parallel`);
			if (Array.isArray(step.parallel.branches)) {
				for (const branch of step.parallel.branches) {
					const id = isPlainObject(branch) && typeof branch.id === "string" ? ` ${branch.id}` : "";
					checkKeys(branch, BRANCH_KEYS, `${name} parallel branch${id}`);
				}
			}
		}
		if (Array.isArray(step.steps)) for (const sub of step.steps) visit(sub, `${name} sub-step`);
	};
	for (const step of workflow.steps as unknown[]) visit(step, "Workflow step");
}

function checkKeys(value: unknown, allowed: string[], where: string) {
	if (!isPlainObject(value)) return;
	for (const key of Object.keys(value)) {
		if (allowed.includes(key)) continue;
		const near = allowed.find((candidate) => editDistance(key, candidate) <= 2);
		throw new Error(
			`${where} has unknown key "${key}"${near ? ` (did you mean "${near}"?)` : ""}. Allowed: ${allowed.join(", ")}`,
		);
	}
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
	return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function editDistance(a: string, b: string): number {
	let previous = Array.from({ length: b.length + 1 }, (_, i) => i);
	for (let i = 1; i <= a.length; i++) {
		const current = [i];
		for (let j = 1; j <= b.length; j++) {
			current[j] = Math.min(
				previous[j] + 1,
				current[j - 1] + 1,
				previous[j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1),
			);
		}
		previous = current;
	}
	return previous[b.length];
}
