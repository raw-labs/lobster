import { parsePipeline } from "../pipeline/parser.js";
import { resolveRetryConfig, type RetryConfig } from "../engine/retry.js";
import { resolveArgsTemplate } from "./expressions.js";
import { checkConditionSyntax, conditionRefs, evaluateCondition } from "./conditions.js";
import { createRecord, resolveInputValue } from "./values.js";
import { getStepExecution, isApprovalStep, isInputStep } from "./load.js";
import type { WorkflowStep, WorkflowStepResult, WorkflowRunResult } from "./types.js";

function dryRunStdinNote(stdin: unknown): string | null {
	if (stdin === null || stdin === undefined) return null;
	if (typeof stdin !== "string") return null;
	const trimmed = stdin.trim();
	// Strict step ref: '$step-id.stdout' or '$step-id.json'
	if (/^\$[A-Za-z0-9_-]+\.(stdout|json)$/.test(trimmed)) {
		return `${trimmed}  [output unknown at plan time]`;
	}
	// Inline template ref: contains '$stepid.stdout' or '$stepid.json'
	if (/\$[A-Za-z0-9_-]+\.(stdout|json)/.test(trimmed)) {
		return `${trimmed}  [contains step output refs — unknown at plan time]`;
	}
	return null;
}

function dryRunTemplateNote(input: string): string | null {
	if (/\$[A-Za-z0-9_-]+\.(stdout|json)/.test(input)) {
		return "[contains step output refs — unknown at plan time]";
	}
	return null;
}

function hasDeferredDryRunStageName(input: string) {
	return /\$[A-Za-z0-9_-]+\.(stdout|json)/.test(input);
}

function resolveDryRunTemplate(
	input: string,
	args: Record<string, unknown>,
	results: Record<string, WorkflowStepResult>,
) {
	const withArgs = resolveArgsTemplate(input, args);
	return withArgs.replace(/\$([A-Za-z0-9_-]+)\.(stdout|json|approved)/g, (match, id, field) => {
		if (field === "approved") {
			const step = results[id];
			if (!step) return match;
			return step.approved === true ? "true" : "false";
		}
		return match;
	});
}

// A plan has no step outputs: a condition that reads one is neither true nor false yet.
// Only `approved` (gates are assumed granted) and `skipped` are modelled by the plan, and
// only for steps whose own condition was decided (`undecided` holds the others).
function dependsOnStepOutput(condition: unknown, undecided: Set<string>) {
	let refs: { id: string; path: string }[];
	try {
		refs = conditionRefs(condition);
	} catch {
		return false; // Not a valid condition: evaluating it reports why.
	}
	return refs.some(({ id, path }) => {
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
	results,
	startIndex,
	ctx,
}: {
	steps: WorkflowStep[];
	resolvedArgs: Record<string, unknown>;
	results: Record<string, WorkflowStepResult>;
	startIndex: number;
	ctx: { stderr: NodeJS.WritableStream; registry?: { get: (name: string) => unknown } };
}): WorkflowRunResult {
	const totalSteps = steps.length - startIndex;
	const lines = [
		`[DRY RUN] Would execute ${totalSteps} step${totalSteps !== 1 ? "s" : ""}:\n`,
		...planSteps(steps, resolvedArgs, results, startIndex, ctx, new Set<string>()),
		"",
	];
	ctx.stderr.write(lines.join("\n"));
	return { status: "ok", output: [] };
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
	startIndex: number,
	ctx: PlanContext,
	undecided: Set<string>,
): string[] {
	const lines: string[] = [];
	for (let idx = startIndex; idx < steps.length; idx++) {
		const step = steps[idx];
		const num = idx - startIndex + 1;

		const condition = step.when ?? step.condition;
		const unknownCondition = dependsOnStepOutput(condition, undecided);
		// A condition on output the plan does not have is only checked for syntax: its value,
		// and whether its comparisons are valid, are known only when the run gets there.
		const holds = unknownCondition
			? (checkConditionSyntax(condition), true)
			: evaluateCondition(condition, results);
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
			if (step.timeout_ms) lines.push(`     timeout: ${step.timeout_ms}ms`);
			if (step.on_error && step.on_error !== "stop") lines.push(`     on_error: ${step.on_error}`);
			noteCondition();
			continue;
		}

		if (step.repeat && Array.isArray(step.steps)) {
			lines.push(`  ${num}. ${step.id}  [repeat]`);
			const until = String(step.repeat.until).replace(/\s+/g, " ").trim();
			checkConditionSyntax(step.repeat.until);
			lines.push(
				`     until: ${until}  [at most ${step.repeat.max} round${step.repeat.max === 1 ? "" : "s"}${step.repeat.on_max === "continue" ? ", then continue" : ""}]`,
			);
			lines.push(`     each round:`);
			// Planned once; the last round's steps are what later steps see.
			for (const line of planSteps(step.steps, resolvedArgs, results, 0, ctx, undecided))
				lines.push(`     ${line}`);
			if (step.timeout_ms) lines.push(`     timeout: ${step.timeout_ms}ms`);
			if (step.on_error && step.on_error !== "stop") lines.push(`     on_error: ${step.on_error}`);
			// How many rounds run, and what the last one produced, is known only when it runs.
			undecided.add(step.id);
			results[step.id] = { id: step.id };
			noteCondition();
			continue;
		}

		if (typeof step.for_each === "string" && Array.isArray(step.steps)) {
			lines.push(`  ${num}. ${step.id}  [for_each]`);
			const forEachRef = step.for_each;
			const forEachNote = dryRunTemplateNote(forEachRef);
			lines.push(`     for_each: ${forEachRef}${forEachNote ? `  ${forEachNote}` : ""}`);
			if (forEachRef.trim().startsWith("$")) {
				try {
					resolveInputValue(forEachRef, resolvedArgs, results);
				} catch (err: any) {
					throw new Error(`Workflow step ${step.id} for_each: ${err?.message ?? String(err)}`);
				}
			}
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
			for (const line of planSteps(step.steps, resolvedArgs, loopScopedResults, 0, ctx, undecided))
				lines.push(`     ${line}`);
			if (step.timeout_ms) lines.push(`     timeout: ${step.timeout_ms}ms`);
			if (step.on_error && step.on_error !== "stop") lines.push(`     on_error: ${step.on_error}`);
			if (step.retry && typeof step.retry === "object") {
				const rc = resolveRetryConfig(step.retry as RetryConfig);
				if (rc.max > 1) {
					lines.push(
						`     retry: up to ${rc.max} attempts, ${rc.backoff} backoff (base: ${rc.delay_ms}ms${rc.jitter ? ", jitter" : ""})`,
					);
				}
			}
			results[step.id] = { id: step.id };
			noteCondition();
			continue;
		}

		// Validate stdin refs early — throws if a strict ref like '$missing.stdout'
		// points to a step that doesn't exist at all (real execution would also fail).
		// We call resolveInputValue with the current results so refs to steps we've
		// already visited (placeholders) are accepted without throwing.
		if (step.stdin !== undefined && step.stdin !== null) {
			try {
				resolveInputValue(step.stdin, resolvedArgs, results);
			} catch (err: any) {
				throw new Error(`Workflow step ${step.id} stdin: ${err?.message ?? String(err)}`);
			}
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
				lines.push(`     timeout: ${step.parallel.timeout_ms}ms`);
			}
			for (const branch of step.parallel?.branches ?? []) {
				const branchShell = typeof branch.run === "string" ? branch.run : branch.command;
				if (typeof branch.pipeline === "string" && branch.pipeline.trim()) {
					const pipelineText = resolveDryRunTemplate(branch.pipeline, resolvedArgs, results);
					const pipelineNote = dryRunTemplateNote(pipelineText);
					if (!ctx.registry) {
						throw new Error(
							`Parallel branch ${branch.id} requires a command registry for pipeline execution`,
						);
					}
					const stages = parsePipeline(pipelineText, ctx.registry as any);
					for (const stage of stages) {
						if (hasDeferredDryRunStageName(stage.name)) continue;
						if (!ctx.registry.get(stage.name)) {
							throw new Error(
								`Parallel branch ${branch.id} pipeline references unknown command: ${stage.name}`,
							);
						}
					}
					lines.push(
						`     branch ${branch.id}: [pipeline] ${pipelineText}${pipelineNote ? `  ${pipelineNote}` : ""}`,
					);
				} else if (typeof branchShell === "string" && branchShell.trim()) {
					const command = resolveDryRunTemplate(branchShell, resolvedArgs, results);
					const commandNote = dryRunTemplateNote(command);
					lines.push(
						`     branch ${branch.id}: [shell] ${command}${commandNote ? `  ${commandNote}` : ""}`,
					);
				} else {
					lines.push(`     branch ${branch.id}: [no-op]`);
				}
				results[branch.id] = { id: branch.id };
			}
		} else if (execution.kind === "workflow") {
			const workflowPath = resolveDryRunTemplate(execution.value, resolvedArgs, results);
			const pathNote = dryRunTemplateNote(workflowPath);
			lines.push(`  ${num}. ${step.id}  [workflow]`);
			lines.push(`     workflow: ${workflowPath}${pathNote ? `  ${pathNote}` : ""}`);
			if (step.workflow_args && typeof step.workflow_args === "object") {
				const argKeys = Object.keys(step.workflow_args);
				if (argKeys.length) {
					lines.push(`     args: ${argKeys.join(", ")}`);
				}
			}
		} else if (execution.kind === "shell") {
			const command = resolveDryRunTemplate(execution.value, resolvedArgs, results);
			const commandNote = dryRunTemplateNote(command);
			lines.push(`  ${num}. ${step.id}  [shell]`);
			lines.push(`     run: ${command}${commandNote ? `  ${commandNote}` : ""}`);
		} else if (execution.kind === "pipeline") {
			const pipelineText = resolveDryRunTemplate(execution.value, resolvedArgs, results);
			const pipelineNote = dryRunTemplateNote(pipelineText);
			// Validate pipeline syntax and registry even in dry-run so errors surface early.
			if (!ctx.registry) {
				throw new Error(
					`Workflow step ${step.id} requires a command registry for pipeline execution`,
				);
			}
			// Validate that every stage name is a known command.
			const stages = parsePipeline(pipelineText, ctx.registry as any);
			for (const stage of stages) {
				if (hasDeferredDryRunStageName(stage.name)) {
					continue;
				}
				if (!ctx.registry.get(stage.name)) {
					throw new Error(
						`Workflow step ${step.id} pipeline references unknown command: ${stage.name}`,
					);
				}
			}
			lines.push(`  ${num}. ${step.id}  [pipeline]`);
			lines.push(`     pipeline: ${pipelineText}${pipelineNote ? `  ${pipelineNote}` : ""}`);
			if (stages.some((stage) => hasDeferredDryRunStageName(stage.name))) {
				lines.push("     [command validation deferred — stage name depends on step output]");
			}
		} else {
			lines.push(`  ${num}. ${step.id}  [no-op]`);
		}

		if (stdinNote) lines.push(`     stdin: ${stdinNote}`);
		if (step.timeout_ms) lines.push(`     timeout: ${step.timeout_ms}ms`);
		if (step.on_error && step.on_error !== "stop") lines.push(`     on_error: ${step.on_error}`);
		if (step.retry && typeof step.retry === "object") {
			const rc = resolveRetryConfig(step.retry as RetryConfig);
			if (rc.max > 1) {
				lines.push(
					`     retry: up to ${rc.max} attempts, ${rc.backoff} backoff (base: ${rc.delay_ms}ms${rc.jitter ? ", jitter" : ""})`,
				);
			}
		}
		if (isApprovalStep(step.approval)) {
			lines.push(`     [approval required]`);
		}

		noteCondition();

		// Record a placeholder result so later steps can reference this step in conditions.
		// For approval steps, model approval as granted so downstream conditions like
		// $step.approved evaluate correctly in the plan (rather than always being false).
		// We intentionally omit stdout/json — dryRunStdinNote() surfaces that gap.
		results[step.id] = isApprovalStep(step.approval)
			? { id: step.id, approved: true }
			: { id: step.id };
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
		for (const line of planSteps(steps, resolvedArgs, results, 0, ctx, undecided))
			lines.push(`     ${line}`);
		// Whether these steps run is not known, so neither is anything that reads them.
		if (state === MAY_RUN) for (const id of stepIds(steps)) undecided.add(id);
	};
	config.cases.forEach((entry, index) => {
		const label = `case ${index + 1}: when ${String(entry.when).replace(/\s+/g, " ").trim()}`;
		if (chosen) return branch(label, entry.steps, "not reached");
		if (open || dependsOnStepOutput(entry.when, undecided)) {
			checkConditionSyntax(entry.when);
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

/** Every step id a list of steps can produce, nested switches included. */
function stepIds(steps: WorkflowStep[]): string[] {
	return steps.flatMap((step) => [
		step.id,
		...(step.parallel?.branches ?? []).map((branch) => branch.id),
		...(step.repeat ? stepIds(step.steps ?? []) : []),
		...(step.switch
			? [
					...step.switch.cases.flatMap((entry) => stepIds(entry.steps)),
					...stepIds(step.switch.else ?? []),
				]
			: []),
	]);
}
