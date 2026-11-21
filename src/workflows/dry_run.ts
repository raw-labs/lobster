// `lobster run --dry-run`: the plan of a workflow, step by step, without running anything.
// Conditions, templates and pipelines are checked as a run would check them; what depends on
// output the plan does not have is marked as such.
import { checkedStages } from "./pipeline_check.js";
import { resolveRetryConfig } from "./retry.js";
import type { CommandLookup } from "../commands/types.js";
import { hasStepRef, resolveArgsTemplate } from "./expressions.js";
import { conditionRefs, evaluateCondition } from "./conditions.js";
import { createRecord } from "./values.js";
import { getStepExecution, isApprovalStep, isInputStep, stepIds } from "./steps.js";
import type { WorkflowStep, WorkflowStepResult, WorkflowRunResult } from "./types.js";

/** A step's stdin for the plan, when it is a step's output (which the plan does not have). */
function dryRunStdinNote(stdin: unknown): string | null {
	if (typeof stdin !== "string") return null;
	const trimmed = stdin.trim();
	// Exactly one step's output: '$step-id.stdout' or '$step-id.json'.
	if (/^\$[A-Za-z0-9_-]+\.(stdout|json)$/.test(trimmed))
		return `${trimmed}  [output unknown at plan time]`;
	return hasStepRef(trimmed) ? annotated(trimmed) : null;
}

/** `text` for the plan, marked when it holds step references the plan cannot fill in. */
function annotated(text: string) {
	return hasStepRef(text) ? `${text}  [contains step output refs — unknown at plan time]` : text;
}

/** `input` with its args filled in, and the approvals of steps the plan has decided. */
function resolveDryRunTemplate(
	input: string,
	args: Record<string, unknown>,
	results: Record<string, WorkflowStepResult>,
) {
	const withArgs = resolveArgsTemplate(input, args);
	return withArgs.replace(/\$([A-Za-z0-9_-]+)\.approved/g, (match, id) => {
		const step = results[id];
		if (!step) return match;
		return step.approved === true ? "true" : "false";
	});
}

/** Whether an approval may complete without a yes, so the run goes on either way. */
function mayContinueUnapproved(approval: WorkflowStep["approval"]) {
	return (
		typeof approval === "object" &&
		approval !== null &&
		(approval.on_reject === "continue" || approval.on_timeout === "continue")
	);
}

// A plan has no step outputs: a condition that reads one is neither true nor false yet.
// Only `approved` and `skipped` are modelled by the plan, and only for steps whose outcome
// was decided (`undecided` holds the others). A gate is planned as granted: a "no" cancels
// the run, so the steps after it run only after a yes, unless the gate continues without one.
function dependsOnStepOutput(condition: unknown, undecided: Set<string>) {
	return conditionRefs(condition).some(({ id, path }) => {
		const field = path.split(".")[0];
		return (field !== "approved" && field !== "skipped") || undecided.has(id);
	});
}

function conditionNote(condition: unknown) {
	const text = String(condition).replace(/\s+/g, " ").trim();
	return `when: ${text}  [depends on step output — unknown at plan time]`;
}

export function dryRunWorkflow({
	steps,
	resolvedArgs,
	ctx,
}: {
	steps: WorkflowStep[];
	resolvedArgs: Record<string, unknown>;
	ctx: { stderr: NodeJS.WritableStream; registry?: CommandLookup };
}): WorkflowRunResult {
	const lines = [
		`[DRY RUN] Would execute ${steps.length} step${steps.length !== 1 ? "s" : ""}:\n`,
		...planSteps(steps, resolvedArgs, {}, ctx, new Set<string>()),
		"",
	];
	const plan = lines.join("\n");
	ctx.stderr.write(plan);
	return { status: "ok", output: [], plan };
}

type PlanContext = Parameters<typeof dryRunWorkflow>[0]["ctx"];

/**
 * The plan of `steps`, one numbered entry each. `undecided` holds the steps whose outcome
 * depends on output the plan does not have; it grows as steps are planned.
 */
function planSteps(
	steps: WorkflowStep[],
	resolvedArgs: Record<string, unknown>,
	results: Record<string, WorkflowStepResult>,
	ctx: PlanContext,
	undecided: Set<string>,
): string[] {
	const lines: string[] = [];
	for (const [idx, step] of steps.entries()) {
		const num = idx + 1;

		const condition = step.when ?? step.condition;
		const unknownCondition = dependsOnStepOutput(condition, undecided);
		// A condition on output the plan does not have (checked when the file loaded) is decided
		// only when the run gets there.
		const holds = unknownCondition || evaluateCondition(condition, results);
		// Whether it runs is not known, so neither is anything inside it.
		if (unknownCondition) for (const id of stepIds([step])) undecided.add(id);
		if (!unknownCondition && !holds) {
			// Skipped with everything inside it, as a run skips them.
			for (const id of stepIds([step])) results[id] = { id, skipped: true };
			lines.push(`  ${num}. ${step.id}  [skipped — condition: false]`);
			continue;
		}
		// Shown under the step's first line, whichever kind of step it is.
		const header = lines.length + 1;
		const noteCondition = () => {
			if (unknownCondition) lines.splice(header, 0, `     ${conditionNote(condition)}`);
		};

		if (step.sleep !== undefined) {
			lines.push(`  ${num}. ${step.id}  [sleep]`);
			lines.push(`     sleep: ${step.sleep}`);
			results[step.id] = { id: step.id };
			noteCondition();
			continue;
		}

		if (step.wait) {
			lines.push(`  ${num}. ${step.id}  [wait]`);
			lines.push(`     event: ${step.wait.event}`);
			if (step.wait.match) lines.push(`     match: ${JSON.stringify(step.wait.match)}`);
			if (step.wait.timeout !== undefined)
				lines.push(`     timeout: ${step.wait.timeout} (then ${step.wait.on_timeout ?? "cancel"})`);
			lines.push(`     [event required]`);
			results[step.id] = { id: step.id, json: { pending: true } };
			noteCondition();
			continue;
		}

		if (isInputStep(step.input)) {
			lines.push(`  ${num}. ${step.id}  [input]`);
			lines.push(`     prompt: ${step.input.prompt}`);
			lines.push(`     [input required]`);
			results[step.id] = { id: step.id, response: { pending: true } };
			noteCondition();
			continue;
		}

		if (step.switch) {
			lines.push(`  ${num}. ${step.id}  [switch]`);
			planSwitch(step, resolvedArgs, results, ctx, undecided, lines, unknownCondition);
			lines.push(...stepSettingLines(step));
			noteCondition();
			continue;
		}

		if (step.repeat && Array.isArray(step.steps)) {
			lines.push(`  ${num}. ${step.id}  [repeat]`);
			const until = String(step.repeat.until).replace(/\s+/g, " ").trim();
			lines.push(
				`     until: ${until}  [at most ${step.repeat.max} round${step.repeat.max === 1 ? "" : "s"}${step.repeat.on_max === "continue" ? ", then continue" : ""}]`,
			);
			lines.push(`     each round:`);
			// Planned once; the last round's steps are what later steps see.
			for (const line of planSteps(step.steps, resolvedArgs, results, ctx, undecided))
				lines.push(`     ${line}`);
			lines.push(...stepSettingLines(step));
			// How many rounds run, and what the last one produced, is known only when it runs.
			undecided.add(step.id);
			results[step.id] = { id: step.id };
			noteCondition();
			continue;
		}

		if (typeof step.for_each === "string" && Array.isArray(step.steps)) {
			lines.push(`  ${num}. ${step.id}  [for_each]`);
			const forEachRef = step.for_each;
			lines.push(`     for_each: ${annotated(forEachRef)}`);
			const dryItemVar = step.item_var ?? "item";
			const dryIndexVar = step.index_var ?? "index";
			lines.push(`     item_var: ${dryItemVar}, index_var: ${dryIndexVar}`);
			if ((step.concurrency ?? 1) > 1) lines.push(`     concurrency: ${step.concurrency} at once`);
			if (step.batch_size) lines.push(`     batch_size: ${step.batch_size}`);
			if (step.pause_ms) lines.push(`     pause_ms: ${step.pause_ms}`);
			lines.push(`     sub-steps: ${step.steps.length}`);

			const loopScopedResults = createRecord(results);
			loopScopedResults[dryItemVar] = { id: dryItemVar, json: { _placeholder: true } };
			loopScopedResults[dryIndexVar] = { id: dryIndexVar, json: 0 };
			// The body is planned once, for an item the plan does not know.
			for (const line of planSteps(step.steps, resolvedArgs, loopScopedResults, ctx, undecided))
				lines.push(`     ${line}`);
			lines.push(...stepSettingLines(step));
			results[step.id] = { id: step.id };
			noteCondition();
			continue;
		}

		const execution = getStepExecution(step);

		// Annotate when the resolved command/pipeline references a prior step's output.
		// Since dry-run placeholders have no actual stdout/json, note it explicitly
		// rather than silently collapsing the reference to an empty string.
		const stdinNote = dryRunStdinNote(step.stdin);

		if (execution.kind === "parallel") {
			lines.push(`  ${num}. ${step.id}  [parallel]`);
			lines.push(`     wait: ${step.parallel?.wait ?? "all"}`);
			if (step.parallel?.timeout_ms) {
				lines.push(`     branches timeout: ${step.parallel.timeout_ms}ms`);
			}
			for (const branch of step.parallel?.branches ?? []) {
				const branchExecution = getStepExecution(branch);
				if (branchExecution.kind === "pipeline") {
					const pipelineText = resolveDryRunTemplate(branchExecution.value, resolvedArgs, results);
					checkedStages(pipelineText, ctx.registry, `Parallel branch ${branch.id}`);
					lines.push(`     branch ${branch.id}: [pipeline] ${annotated(pipelineText)}`);
				} else if (branchExecution.kind === "shell") {
					const command = resolveDryRunTemplate(branchExecution.value, resolvedArgs, results);
					lines.push(`     branch ${branch.id}: [shell] ${annotated(command)}`);
				}
				results[branch.id] = { id: branch.id };
			}
		} else if (execution.kind === "workflow") {
			const workflowPath = resolveDryRunTemplate(execution.value, resolvedArgs, results);
			lines.push(`  ${num}. ${step.id}  [workflow]`);
			lines.push(`     workflow: ${annotated(workflowPath)}`);
			if (step.workflow_args && typeof step.workflow_args === "object") {
				const argKeys = Object.keys(step.workflow_args);
				if (argKeys.length) {
					lines.push(`     args: ${argKeys.join(", ")}`);
				}
			}
		} else if (execution.kind === "shell") {
			const command = resolveDryRunTemplate(execution.value, resolvedArgs, results);
			lines.push(`  ${num}. ${step.id}  [shell]`);
			lines.push(`     run: ${annotated(command)}`);
		} else if (execution.kind === "pipeline") {
			const pipelineText = resolveDryRunTemplate(execution.value, resolvedArgs, results);
			const stages = checkedStages(pipelineText, ctx.registry, `Workflow step ${step.id}`);
			lines.push(`  ${num}. ${step.id}  [pipeline]`);
			lines.push(`     pipeline: ${annotated(pipelineText)}`);
			if (stages.some((stage) => hasStepRef(stage.name))) {
				lines.push("     [command validation deferred — stage name depends on step output]");
			}
		} else {
			lines.push(`  ${num}. ${step.id}  [${isApprovalStep(step.approval) ? "approval" : "no-op"}]`);
		}

		if (stdinNote) lines.push(`     stdin: ${stdinNote}`);
		lines.push(...stepSettingLines(step));
		if (isApprovalStep(step.approval)) {
			lines.push(`     [approval required]`);
		}

		noteCondition();

		// Record a placeholder result so later steps can reference this step in conditions.
		// For approval steps, model approval as granted so downstream conditions like
		// $step.approved evaluate correctly in the plan (rather than always being false).
		// We intentionally omit stdout/json — dryRunStdinNote() marks that gap.
		results[step.id] = isApprovalStep(step.approval)
			? { id: step.id, approved: true }
			: { id: step.id };
		// A "no" or a timeout that lets the run go on (`on_reject` / `on_timeout: continue`)
		// leaves the answer open: conditions on it are decided only when the run gets there.
		if (mayContinueUnapproved(step.approval)) undecided.add(step.id);
	}
	return lines;
}

/**
 * A switch's cases, in order. A case decided from what the plan knows either runs (and the
 * plan follows it) or does not; once a case depends on step output, every case from there
 * may run, so each is planned and its steps count as undecided.
 */
function planSwitch(
	step: WorkflowStep,
	resolvedArgs: Record<string, unknown>,
	results: Record<string, WorkflowStepResult>,
	ctx: PlanContext,
	undecided: Set<string>,
	lines: string[],
	/** The switch itself may not run: its own `when` depends on step output. */
	mayNotRun = false,
) {
	const config = step.switch!;
	let chosen = false;
	let open = false;
	const MAY_RUN = "may run — depends on step output";
	const branch = (label: string, steps: WorkflowStep[], state: string) => {
		lines.push(`     ${label}  [${state}]`);
		if (state !== "runs" && state !== MAY_RUN) {
			for (const id of stepIds(steps)) results[id] ??= { id, skipped: true };
			return;
		}
		for (const line of planSteps(steps, resolvedArgs, results, ctx, undecided))
			lines.push(`     ${line}`);
		// Whether these steps run is not known, so neither is anything that reads them.
		if (state === MAY_RUN) for (const id of stepIds(steps)) undecided.add(id);
	};
	config.cases.forEach((entry, index) => {
		const label = `case ${index + 1}: when ${String(entry.when).replace(/\s+/g, " ").trim()}`;
		if (chosen) return branch(label, entry.steps, "not reached");
		if (open || dependsOnStepOutput(entry.when, undecided)) {
			open = true;
			return branch(label, entry.steps, MAY_RUN);
		}
		if (evaluateCondition(entry.when, results)) {
			chosen = true;
			return branch(label, entry.steps, "runs");
		}
		branch(label, entry.steps, "condition: false");
	});
	if (config.else) {
		branch("else", config.else, chosen ? "not reached" : open ? MAY_RUN : "runs");
	} else if (!chosen && !open && !mayNotRun) {
		throw new Error(`Workflow step ${step.id} switch: no case matched and there is no else`);
	}
	if (open) undecided.add(step.id);
	results[step.id] = { id: step.id };
}

/** The lines of a step's plan for its timeout, on_error and retry settings. */
function stepSettingLines(step: WorkflowStep): string[] {
	const lines: string[] = [];
	if (step.timeout_ms) lines.push(`     timeout: ${step.timeout_ms}ms`);
	if (step.on_error && step.on_error !== "stop") lines.push(`     on_error: ${step.on_error}`);
	const retry = resolveRetryConfig(step.retry);
	if (retry.max > 1)
		lines.push(
			`     retry: up to ${retry.max} attempts, ${retry.backoff} backoff (base: ${retry.delay_ms}ms${retry.jitter ? ", jitter" : ""})`,
		);
	return lines;
}
