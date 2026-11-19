import { checkCondition } from "./conditions.js";
import { parseStepRef } from "./expressions.js";
import { isApprovalStep, isInputStep, nonBlank, shellCommandOf } from "./steps.js";
import { isPlainObject } from "../util/objects.js";
import { MAX_TIMER_MS, parseDuration } from "../util/duration.js";
import { closestMatch } from "../util/suggest.js";
import { promises as fsp } from "node:fs";
import path from "node:path";
import { parse as parseYaml } from "yaml";
import { prepareSchema } from "../util/validation.js";
import type {
	WorkflowFile,
	WorkflowStep,
	ParallelConfig,
	RepeatConfig,
	SwitchConfig,
} from "./types.js";
import { messageOf } from "../util/errors.js";

export const WORKFLOW_FILE_EXTENSIONS = [".lobster", ".yaml", ".yml", ".json"];

/** The absolute path of a workflow file named on a command line or in a tool request. */
export async function resolveWorkflowFile(candidate: string, cwd = process.cwd()) {
	const resolved = path.resolve(cwd, candidate);
	if (!(await fsp.stat(resolved)).isFile()) throw new Error("Workflow path is not a file");
	if (!WORKFLOW_FILE_EXTENSIONS.includes(path.extname(resolved).toLowerCase()))
		throw new Error("Workflow file must end in .lobster, .yaml, .yml, or .json");
	return resolved;
}

export async function loadWorkflowFile(filePath: string): Promise<WorkflowFile> {
	const text = await fsp.readFile(filePath, "utf8");
	const ext = path.extname(filePath).toLowerCase();
	let parsed: unknown;
	try {
		parsed = ext === ".json" ? JSON.parse(text) : parseYaml(text);
	} catch (error) {
		throw new Error(
			`Workflow file ${filePath} is not valid ${ext === ".json" ? "JSON" : "YAML"}: ${messageOf(error)}`,
		);
	}

	if (!parsed || typeof parsed !== "object") {
		throw new Error("Workflow file must be a JSON/YAML object");
	}

	const steps = (parsed as WorkflowFile).steps;
	if (!Array.isArray(steps) || steps.length === 0) {
		throw new Error("Workflow file requires a non-empty steps array");
	}
	rejectUnknownKeys(parsed as Record<string, unknown>);
	validateEnvAndCwd(parsed as Record<string, unknown>, "Workflow");
	for (const field of ["name", "description"] as const) {
		const value: unknown = (parsed as WorkflowFile)[field];
		if (value !== undefined && typeof value !== "string")
			throw new Error(`Workflow ${field} must be a string`);
	}
	validateArgDefinitions((parsed as WorkflowFile).args);

	const costLimit = (parsed as WorkflowFile).cost_limit;
	if (costLimit !== undefined) {
		if (!isPlainObject(costLimit)) {
			throw new Error("Workflow cost_limit must be an object");
		}
		const maxUsd: unknown = costLimit.max_usd;
		if (typeof maxUsd !== "number" || !Number.isFinite(maxUsd) || maxUsd < 0) {
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

	validateSteps(steps, new Set<string>(), new Set<string>());
	return parsed as WorkflowFile;
}

/** A workflow's `args`: each name maps to an object with an optional default and description. */
function validateArgDefinitions(args: unknown) {
	if (args === undefined) return;
	if (!isPlainObject(args)) throw new Error("Workflow args must be an object");
	for (const [name, definition] of Object.entries(args)) {
		const where = `Workflow arg ${name}`;
		if (!isPlainObject(definition))
			throw new Error(`${where} must be an object (with a default and a description)`);
		checkKeys(definition, ARG_KEYS, where);
		if (definition.description !== undefined && typeof definition.description !== "string")
			throw new Error(`${where} description must be a string`);
	}
}

/**
 * Validates steps that run in order. `seen` holds every step id so far, which must be unique;
 * `known` holds the steps whose results exist when these start, and gains each step as it is
 * validated, so a condition can only read a step that runs before it.
 */
function validateSteps(steps: WorkflowStep[], seen: Set<string>, known: Set<string>) {
	for (const step of steps) {
		if (!step || typeof step !== "object") {
			throw new Error("Workflow step must be an object");
		}
		if (!step.id || typeof step.id !== "string") {
			throw new Error("Workflow step requires an id");
		}
		validateRecovery(step, `Workflow step ${step.id}`);
		if (step.workflow !== undefined && typeof step.workflow !== "string") {
			throw new Error(`Workflow step ${step.id} workflow must be a string (file path)`);
		}
		if (typeof step.workflow === "string" && !step.workflow.trim()) {
			throw new Error(`Workflow step ${step.id} workflow path cannot be blank`);
		}
		if (typeof step.workflow === "string" && step.workflow.includes("\0"))
			throw new Error(`Workflow step ${step.id} workflow cannot contain a NUL character`);
		validateEnvAndCwd(step as Record<string, unknown>, `Workflow step ${step.id}`);
		if (step.workflow_args !== undefined) {
			if (!isPlainObject(step.workflow_args)) {
				throw new Error(`Workflow step ${step.id} workflow_args must be a plain object`);
			}
		}
		if (step.parallel !== undefined && !isPlainObject(step.parallel)) {
			throw new Error(`Workflow step ${step.id} parallel must be an object`);
		}
		const isParallel = Boolean(isPlainObject(step.parallel));
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
					parallel.timeout_ms > MAX_TIMER_MS)
			) {
				throw new Error(
					`Workflow step ${step.id} parallel timeout_ms must be a positive integer between 1 and ${MAX_TIMER_MS}`,
				);
			}

			const branchIds = new Set<string>();
			for (const branch of parallel.branches) {
				if (!branch || typeof branch !== "object") {
					throw new Error(`Workflow step ${step.id} parallel branches must be objects`);
				}
				if (!branch.id || typeof branch.id !== "string") {
					throw new Error(`Workflow step ${step.id} parallel branch requires an id`);
				}
				validateRecovery(branch, `Workflow step ${step.id} parallel branch ${branch.id}`);
				validateEnvAndCwd(
					branch as Record<string, unknown>,
					`Workflow step ${step.id} parallel branch ${branch.id}`,
				);
				checkStepRefs(
					branch.stdin,
					`Workflow step ${step.id} parallel branch ${branch.id} stdin`,
					known,
				);
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
				if (branch.run !== undefined && branch.command !== undefined)
					throw new Error(
						`Workflow step ${step.id} parallel branch ${branch.id} takes run or command, not both`,
					);
				const branchShell = shellCommandOf(branch);
				const branchPipeline = nonBlank(branch.pipeline);
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
				(typeof step.pause_ms !== "number" ||
					!Number.isFinite(step.pause_ms) ||
					step.pause_ms < 0 ||
					step.pause_ms > MAX_TIMER_MS)
			) {
				throw new Error(
					`Workflow step ${step.id} pause_ms must be a number from 0 to ${MAX_TIMER_MS}`,
				);
			}
			if (
				step.concurrency !== undefined &&
				(typeof step.concurrency !== "number" ||
					!Number.isInteger(step.concurrency) ||
					step.concurrency < 1 ||
					step.concurrency > 1000)
			) {
				throw new Error(`Workflow step ${step.id} concurrency must be an integer from 1 to 1000`);
			}
			if (
				(step.concurrency ?? 1) > 1 &&
				(step.pause_ms !== undefined || step.batch_size !== undefined)
			) {
				throw new Error(
					`Workflow step ${step.id} for_each cannot combine concurrency with pause_ms or batch_size (they pace items one at a time)`,
				);
			}
			if (step.batch_size !== undefined && step.pause_ms === undefined)
				throw new Error(
					`Workflow step ${step.id} batch_size applies only with pause_ms (it sets how many items run between pauses)`,
				);
			if (isApprovalStep(step.approval)) {
				throw new Error(
					`Workflow step ${step.id} for_each steps cannot define approval (put it on a step inside the loop, or after it)`,
				);
			}
			if (isInputStep(step.input)) {
				throw new Error(
					`Workflow step ${step.id} for_each steps cannot define input (put it on a step inside the loop, or after it)`,
				);
			}
			const loopShell = shellCommandOf(step);
			const loopPipeline = nonBlank(step.pipeline);
			if (
				loopShell ||
				loopPipeline ||
				step.workflow ||
				step.parallel ||
				step.repeat !== undefined ||
				step.switch !== undefined
			) {
				throw new Error(
					`Workflow step ${step.id} for_each cannot also define run, command, pipeline, workflow, parallel, repeat, or switch`,
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
			// The body is steps like any other, in a scope of its own that starts with the steps
			// before the loop and the loop variables: no body step, however nested, may take one of
			// their names and hide it. Its ids stay its own: each item's results are gone after
			// the item, and the loop's result carries what later steps can read.
			const inside = new Set([...known, loopItemVar, loopIndexVar]);
			validateSteps(step.steps, new Set(inside), inside);
			refuseGatesUnderRetry(step, "for_each");
			if ((step.concurrency ?? 1) > 1 && containsGate(step.steps))
				throw new Error(
					`Workflow step ${step.id} for_each with concurrency cannot contain approval, input, sleep or wait steps: several items would wait at once`,
				);
		}
		// Only `run` is read: a `command` beside it would be dropped without a word.
		if (step.run !== undefined && step.command !== undefined)
			throw new Error(`Workflow step ${step.id} takes run or command, not both`);
		const shellCommand = shellCommandOf(step);
		const pipeline = nonBlank(step.pipeline);
		const workflowRef = nonBlank(step.workflow);
		const isSleep = step.sleep !== undefined;
		const isWait = step.wait !== undefined;
		const isSwitch = step.switch !== undefined;
		const isRepeat = step.repeat !== undefined;
		const executionCount =
			Number(Boolean(shellCommand)) +
			Number(Boolean(pipeline)) +
			Number(Boolean(workflowRef)) +
			Number(isParallel) +
			Number(isForEach) +
			Number(isSwitch) +
			Number(isRepeat) +
			Number(isSleep) +
			Number(isWait);
		if (executionCount === 0 && !isApprovalStep(step.approval) && !isInputStep(step.input)) {
			throw new Error(
				`Workflow step ${step.id} requires run, command, pipeline, workflow, parallel, for_each, repeat, switch, sleep, wait, approval, or input`,
			);
		}
		for (const key of ["concurrency", "pause_ms", "batch_size", "item_var", "index_var"] as const)
			if (step[key] !== undefined && !isForEach)
				throw new Error(`Workflow step ${step.id} ${key} applies only to for_each`);
		if (step.steps !== undefined && !isForEach && !isRepeat) {
			throw new Error(
				`Workflow step ${step.id} has steps, which only for_each and repeat take (group steps with switch cases or a child workflow)`,
			);
		}
		if (executionCount > 1) {
			throw new Error(
				`Workflow step ${step.id} can only define one of run, command, pipeline, workflow, parallel, for_each, repeat, switch, sleep, or wait`,
			);
		}
		if (executionCount > 0 && isInputStep(step.input)) {
			throw new Error(
				`Workflow step ${step.id} input steps cannot define run, command, pipeline, workflow, parallel, for_each, repeat, switch, sleep, or wait`,
			);
		}
		if (isSwitch && isApprovalStep(step.approval)) {
			throw new Error(
				`Workflow step ${step.id} switch steps cannot define approval (put it on a step in a case)`,
			);
		}
		if (isRepeat && isApprovalStep(step.approval)) {
			throw new Error(
				`Workflow step ${step.id} repeat steps cannot define approval (put it on a step inside)`,
			);
		}
		// Only steps that run a command, or ask (no execution kind), read their input.
		const readsStdin = Boolean(shellCommand || pipeline) || executionCount === 0;
		if (step.stdin !== undefined && step.stdin !== null && !readsStdin)
			throw new Error(
				`Workflow step ${step.id} stdin applies only to run, command, pipeline, input and approval steps (give it to the steps inside)`,
			);
		if (step.workflow_args !== undefined && !workflowRef)
			throw new Error(`Workflow step ${step.id} workflow_args applies only to workflow steps`);
		// A command or the steps inside use env and cwd; a gate or an approval alone would drop them.
		const usesEnv = executionCount > 0 && !isSleep && !isWait;
		for (const key of ["env", "cwd"] as const)
			if (step[key] !== undefined && !usesEnv)
				throw new Error(
					`Workflow step ${step.id} ${key} applies only to steps that run a command or contain steps`,
				);
		if ((isSleep || isWait) && isApprovalStep(step.approval)) {
			throw new Error(`Workflow step ${step.id} sleep and wait steps cannot define approval`);
		}
		if (isSleep) parseDuration(step.sleep, `Workflow step ${step.id} sleep`);
		if (isWait) validateEventWait(step, `Workflow step ${step.id}`);
		if (step.heartbeat_timeout_ms !== undefined && !shellCommand && !pipeline && !isForEach) {
			throw new Error(
				`Workflow step ${step.id} heartbeat_timeout_ms applies only to run, command, pipeline and for_each steps`,
			);
		}
		if (isInputStep(step.input)) validateWaitTimeout(step.input, `Workflow step ${step.id} input`);
		if (step.approval && typeof step.approval === "object")
			validateWaitTimeout(step.approval, `Workflow step ${step.id} approval`);
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
		if (step.input && !isPlainObject(step.input.responseSchema)) {
			throw new Error(`Workflow step ${step.id} input.responseSchema must be an object`);
		}
		if (step.input) {
			try {
				prepareSchema(step.input.responseSchema);
			} catch (err) {
				throw new Error(
					`Workflow step ${step.id} input.responseSchema is invalid: ${messageOf(err)}`,
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
		if (isPlainObject(step.approval)) {
			const approval = step.approval as Record<string, unknown>;
			for (const key of ["prompt", "preview"])
				if (approval[key] !== undefined && typeof approval[key] !== "string")
					throw new Error(`Workflow step ${step.id} approval.${key} must be a string`);
			if (approval.items !== undefined && !Array.isArray(approval.items)) {
				throw new Error(`Workflow step ${step.id} approval.items must be a list`);
			}
			for (const [snake, camel, type] of APPROVAL_IDENTITY_SPELLINGS) {
				for (const key of [snake, camel])
					if (approval[key] !== undefined && typeof approval[key] !== type)
						throw new Error(`Workflow step ${step.id} approval.${key} must be a ${type}`);
				// Only one is read: the other would be dropped without a word.
				if (approval[snake] !== undefined && approval[camel] !== undefined)
					throw new Error(`Workflow step ${step.id} approval takes ${snake} or ${camel}, not both`);
			}
			if (
				approval.on_reject !== undefined &&
				approval.on_reject !== "cancel" &&
				approval.on_reject !== "continue"
			)
				throw new Error(
					`Workflow step ${step.id} approval.on_reject must be "cancel" or "continue"`,
				);
		}
		validateStepOptions(step, `Workflow step ${step.id}`, known);
		if (seen.has(step.id)) {
			throw new Error(
				`Duplicate workflow step id: ${step.id} (a step or loop variable in scope already has it)`,
			);
		}
		// Taken before its switch cases or repeat steps are validated: none of them may reuse it.
		seen.add(step.id);
		if (isSwitch) {
			// Validated after the switch's own condition: its cases run after that.
			const before = new Set(seen);
			validateSwitch(step, seen, known);
			for (const id of seen) if (!before.has(id)) known.add(id);
		}
		if (isRepeat) {
			const before = new Set(seen);
			validateRepeat(step, seen, known);
			for (const id of seen) if (!before.has(id)) known.add(id);
		}
		if (isParallel) {
			const parallel = step.parallel as ParallelConfig;
			for (const branch of parallel.branches) {
				seen.add(branch.id);
				known.add(branch.id);
			}
		}
		known.add(step.id);
	}
}

/**
 * A switch runs the steps of its first case whose `when` holds, or its `else`. Every case's
 * steps are known after it: those of the cases that did not run count as skipped.
 */
function validateSwitch(step: WorkflowStep, seen: Set<string>, known: Set<string>) {
	const name = `Workflow step ${step.id} switch`;
	const config = step.switch as SwitchConfig;
	if (!isPlainObject(config)) throw new Error(`${name} must be an object with cases`);
	if (!Array.isArray(config.cases) || config.cases.length === 0)
		throw new Error(`${name} requires a non-empty cases array`);
	config.cases.forEach((entry, index) => {
		const where = `${name} case ${index + 1}`;
		if (!isPlainObject(entry)) throw new Error(`${where} must be an object with when and steps`);
		if (typeof entry.when !== "string" && typeof entry.when !== "boolean")
			throw new Error(`${where} requires when: a condition, or true`);
		try {
			checkCondition(entry.when, known);
		} catch (error) {
			throw new Error(`${where} when is not a valid condition: ${messageOf(error)}`);
		}
		if (!Array.isArray(entry.steps) || entry.steps.length === 0)
			throw new Error(`${where} requires a non-empty steps array`);
		validateSteps(entry.steps, seen, new Set(known));
	});
	if (config.else !== undefined) {
		if (!Array.isArray(config.else))
			throw new Error(`${name} else must be a list of steps (empty for none)`);
		validateSteps(config.else, seen, new Set(known));
	}
	refuseGatesUnderRetry(step, "switch", [
		...config.cases.flatMap((entry) => entry.steps),
		...(config.else ?? []),
	]);
}

/**
 * A repeat runs its steps in rounds until `until` holds after one. Its steps are known after
 * it (the last round's results), and `until` reads them.
 */
function validateRepeat(step: WorkflowStep, seen: Set<string>, known: Set<string>) {
	const name = `Workflow step ${step.id} repeat`;
	const config = step.repeat as RepeatConfig;
	if (!isPlainObject(config)) throw new Error(`${name} must be an object with until and max`);
	if (!Array.isArray(step.steps) || step.steps.length === 0)
		throw new Error(`${name} requires a non-empty steps array`);
	if (
		typeof config.max !== "number" ||
		!Number.isInteger(config.max) ||
		config.max < 1 ||
		config.max > 100_000
	)
		throw new Error(`${name} requires max: the most rounds it may run, from 1 to 100000`);
	if (config.on_max !== undefined && config.on_max !== "fail" && config.on_max !== "continue")
		throw new Error(`${name} on_max must be "fail" or "continue"`);
	if (typeof config.until !== "string" && typeof config.until !== "boolean")
		throw new Error(`${name} requires until: a condition that ends the loop`);
	const inside = new Set(known);
	validateSteps(step.steps, seen, inside);
	try {
		checkCondition(config.until, inside);
	} catch (error) {
		throw new Error(`${name} until is not a valid condition: ${messageOf(error)}`);
	}
	refuseGatesUnderRetry(step, "repeat");
}

/** A retry runs a loop again from the start, so it would ask its questions again. */
function refuseGatesUnderRetry(step: WorkflowStep, kind: string, inside = step.steps ?? []) {
	if ((step.retry?.max ?? 1) > 1 && containsGate(inside))
		throw new Error(
			`Workflow step ${step.id} ${kind} with retry cannot contain approval, input, sleep or wait steps: a retry would run it again and ask again`,
		);
}

/** Whether any of these steps, or a step inside them, pauses the run. */
function containsGate(steps: WorkflowStep[]): boolean {
	return steps.some(
		(step) =>
			isApprovalStep(step.approval) ||
			isInputStep(step.input) ||
			step.sleep !== undefined ||
			step.wait !== undefined ||
			(Array.isArray(step.steps) && containsGate(step.steps)) ||
			Boolean(
				step.switch &&
				(step.switch.cases.some((entry) => containsGate(entry.steps)) ||
					containsGate(step.switch.else ?? [])),
			),
	);
}

// Options every step accepts, wherever it is nested.
function validateStepOptions(step: WorkflowStep, name: string, known: ReadonlySet<string>) {
	checkStepRefs(step.stdin, `${name} stdin`, known);
	checkStepRefs(step.for_each, `${name} for_each`, known);
	if (isPlainObject(step.wait)) checkStepRefs(step.wait.match, `${name} wait.match`, known);
	if (
		step.timeout_ms !== undefined &&
		(typeof step.timeout_ms !== "number" ||
			!Number.isFinite(step.timeout_ms) ||
			!Number.isInteger(step.timeout_ms) ||
			step.timeout_ms < 1 ||
			step.timeout_ms > MAX_TIMER_MS)
	) {
		throw new Error(`${name} timeout_ms must be a positive integer between 1 and ${MAX_TIMER_MS}`);
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
		if (!isPlainObject(step.retry)) {
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
			(typeof r.delay_ms !== "number" ||
				!Number.isFinite(r.delay_ms) ||
				r.delay_ms < 0 ||
				r.delay_ms > MAX_TIMER_MS)
		) {
			throw new Error(`${name} retry.delay_ms must be a number from 0 to ${MAX_TIMER_MS}`);
		}
		if (
			r.max_delay_ms !== undefined &&
			(typeof r.max_delay_ms !== "number" ||
				!Number.isFinite(r.max_delay_ms) ||
				r.max_delay_ms < 0 ||
				r.max_delay_ms > MAX_TIMER_MS)
		) {
			throw new Error(`${name} retry.max_delay_ms must be a number from 0 to ${MAX_TIMER_MS}`);
		}
		if (r.jitter !== undefined && typeof r.jitter !== "boolean") {
			throw new Error(`${name} retry.jitter must be a boolean`);
		}
	}
	if (
		step.heartbeat_timeout_ms !== undefined &&
		(typeof step.heartbeat_timeout_ms !== "number" ||
			!Number.isInteger(step.heartbeat_timeout_ms) ||
			step.heartbeat_timeout_ms < 1 ||
			step.heartbeat_timeout_ms > MAX_TIMER_MS)
	) {
		throw new Error(
			`${name} heartbeat_timeout_ms must be a positive integer between 1 and ${MAX_TIMER_MS}`,
		);
	}
	validateCondition(step, name, known);
}

function validateWaitTimeout(config: Record<string, unknown>, name: string) {
	if (config.timeout !== undefined) parseDuration(config.timeout, `${name}.timeout`);
	if (
		config.on_timeout !== undefined &&
		config.on_timeout !== "cancel" &&
		config.on_timeout !== "continue"
	)
		throw new Error(`${name}.on_timeout must be "cancel" or "continue"`);
	if (config.on_timeout !== undefined && config.timeout === undefined)
		throw new Error(`${name}.on_timeout needs a timeout`);
}

/** What an event name may contain: `wait.event` and `lobster signal` check the same rule. */
export const EVENT_NAME = /^[A-Za-z0-9_.:/-]+$/;
export const EVENT_NAME_CHARS = "letters, digits and . _ : / -";

/**
 * `env` (an object of strings) and `cwd` (a string without NUL) on a workflow, step or branch.
 * A YAML number or flag must be quoted: anything but a string would be dropped from the env.
 */
function validateEnvAndCwd(holder: Record<string, unknown>, name: string) {
	const { env, cwd } = holder;
	if (env !== undefined) {
		if (!isPlainObject(env)) throw new Error(`${name} env must be an object of strings`);
		for (const [key, value] of Object.entries(env)) {
			if (typeof value !== "string")
				throw new Error(`${name} env.${key} must be a string${envValueHint(value)}`);
			if (key.includes("\0") || value.includes("\0"))
				throw new Error(`${name} env.${key} cannot contain a NUL character`);
		}
	}
	if (cwd !== undefined) {
		if (typeof cwd !== "string") throw new Error(`${name} cwd must be a string`);
		if (cwd.includes("\0")) throw new Error(`${name} cwd cannot contain a NUL character`);
	}
}

/** How to fix a YAML value that is not a string: quote a number or flag; "" for empty. */
function envValueHint(value: unknown) {
	if (typeof value === "number" || typeof value === "boolean") return ` (quote it: "${value}")`;
	if (value === null) return ' (use "" for empty)';
	return "";
}

function validateEventWait(step: WorkflowStep, name: string) {
	const wait = step.wait as unknown;
	if (!isPlainObject(wait)) throw new Error(`${name} wait must be an object with an event name`);
	if (typeof wait.event !== "string" || !EVENT_NAME.test(wait.event))
		throw new Error(`${name} wait.event must be an event name (${EVENT_NAME_CHARS})`);
	if (wait.match !== undefined && !isPlainObject(wait.match))
		throw new Error(`${name} wait.match must be an object`);
	validateWaitTimeout(wait, `${name} wait`);
}

/**
 * An exact step reference (`$id.path`, alone) is read strictly: the step it names must run
 * before this one, as a condition's must. Checked here, before earlier steps have acted.
 * Strings inside objects (a wait's `match`) count too.
 */
function checkStepRefs(value: unknown, name: string, known: ReadonlySet<string>) {
	if (isPlainObject(value)) {
		for (const [key, item] of Object.entries(value)) checkStepRefs(item, `${name}.${key}`, known);
		return;
	}
	if (typeof value !== "string") return;
	const ref = parseStepRef(value.trim());
	if (ref && !known.has(ref.id))
		throw new Error(`${name} reads ${value.trim()}, but no step "${ref.id}" runs before it`);
}

// Checked here, not when the step is reached: by then earlier steps have had their effects.
function validateCondition(step: WorkflowStep, name: string, known: ReadonlySet<string>) {
	// Only one is read (`when` first): the other would be dropped without a word.
	if (step.when != null && step.condition != null)
		throw new Error(`${name} takes when or condition, not both`);
	for (const key of ["when", "condition"] as const) {
		const value = step[key];
		if (
			value !== undefined &&
			value !== null &&
			typeof value !== "boolean" &&
			typeof value !== "string"
		)
			throw new Error(`${name} ${key} must be a boolean or an expression string`);
		if (typeof value === "string") {
			try {
				checkCondition(value, known);
			} catch (error) {
				throw new Error(`${name} ${key} is not a valid condition: ${messageOf(error)}`);
			}
		}
	}
}

function validateRecovery(step: { recovery?: unknown }, name: string) {
	if (step.recovery !== undefined && step.recovery !== "repeat" && step.recovery !== "manual")
		throw new Error(`${name} recovery must be "repeat" or "manual"`);
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
	"switch",
	"repeat",
	"concurrency",
	"item_var",
	"index_var",
	"batch_size",
	"pause_ms",
	"steps",
	"timeout_ms",
	"heartbeat_timeout_ms",
	"sleep",
	"wait",
	"on_error",
	"retry",
];
const WAIT_KEYS = ["event", "match", "timeout", "on_timeout"];
const BRANCH_KEYS = ["id", "recovery", "run", "command", "pipeline", "env", "cwd", "stdin"];
const PARALLEL_KEYS = ["wait", "timeout_ms", "branches"];
const SWITCH_KEYS = ["cases", "else"];
const REPEAT_KEYS = ["until", "max", "on_max"];
const CASE_KEYS = ["when", "steps"];
const RETRY_KEYS = ["max", "backoff", "delay_ms", "max_delay_ms", "jitter"];
const COST_LIMIT_KEYS = ["max_usd", "action"];
const ARG_KEYS = ["default", "description"];
const INPUT_KEYS = ["prompt", "responseSchema", "defaults", "timeout", "on_timeout"];
/** The approval identity settings, each accepted in either spelling (but not both). */
const APPROVAL_IDENTITY_SPELLINGS = [
	["initiated_by", "initiatedBy", "string"],
	["required_approver", "requiredApprover", "string"],
	["require_different_approver", "requireDifferentApprover", "boolean"],
] as const;
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
	"timeout",
	"on_timeout",
	"on_reject",
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
		checkKeys(step.wait, WAIT_KEYS, `${name} wait`);
		checkKeys(step.repeat, REPEAT_KEYS, `${name} repeat`);
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
		if (isPlainObject(step.switch)) {
			checkKeys(step.switch, SWITCH_KEYS, `${name} switch`);
			const cases = Array.isArray(step.switch.cases) ? step.switch.cases : [];
			cases.forEach((entry, index) => {
				const where = `${name} switch case ${index + 1}`;
				checkKeys(entry, CASE_KEYS, where);
				if (isPlainObject(entry) && Array.isArray(entry.steps))
					for (const sub of entry.steps) visit(sub, `${where} step`);
			});
			if (Array.isArray(step.switch.else))
				for (const sub of step.switch.else) visit(sub, `${name} switch else step`);
		}
	};
	for (const step of workflow.steps as unknown[]) visit(step, "Workflow step");
}

function checkKeys(value: unknown, allowed: string[], where: string) {
	if (!isPlainObject(value)) return;
	for (const key of Object.keys(value)) {
		if (allowed.includes(key)) continue;
		const near = closestMatch(key, allowed);
		throw new Error(
			`${where} has unknown key "${key}"${near ? ` (did you mean "${near}"?)` : ""}. Allowed: ${allowed.join(", ")}`,
		);
	}
}
