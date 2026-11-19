// Starting, continuing, stopping and waking runs, as hosts call it: with the same context they
// give runToolRequest, defaults filled in the same way (createToolContext).
import {
	abandonWorkflowRun as abandonRun,
	advanceWorkflowRun as advanceRun,
	runWorkflowFile as runFile,
	type WorkflowRunOptions as EngineRunOptions,
} from "../engine/runner.js";
import { advanceDueRuns as advanceDue, signalEvent as signal } from "../engine/due.js";
import { createToolContext, type ToolRunContext } from "./tool_runtime.js";

/** An engine entry point's options, with the host's context in place of the engine's. */
type HostOptions<T> = Omit<T, "ctx" | "sdk"> & { ctx?: ToolRunContext };

/** Start a workflow file, or answer a run's question (`resume` with a decision). */
export type WorkflowRunOptions = HostOptions<Omit<EngineRunOptions, "definition" | "kind">>;

/** Start a workflow file, or answer a run's question; the result as the engine reports it. */
export function runWorkflowFile(options: WorkflowRunOptions) {
	return runFile({ ...options, ctx: createToolContext(options.ctx) });
}

/** Continue a run: one waiting for time or an event, or a failed one from the step that failed. */
export function advanceWorkflowRun(options: HostOptions<Parameters<typeof advanceRun>[0]>) {
	return advanceRun({ ...options, ctx: createToolContext(options.ctx) });
}

/** Cancel a run for good, withdrawing what it asked. */
export function abandonWorkflowRun(options: HostOptions<Parameters<typeof abandonRun>[0]>) {
	return abandonRun({ ...options, ctx: createToolContext(options.ctx) });
}

/** Advance every run whose wake time has passed. */
export function advanceDueRuns(options: HostOptions<Parameters<typeof advanceDue>[0]> = {}) {
	return advanceDue({ ...options, ctx: createToolContext(options.ctx) });
}

/** Deliver an event, and advance the runs waiting for it. */
export function signalEvent(options: HostOptions<Parameters<typeof signal>[0]>) {
	return signal({ ...options, ctx: createToolContext(options.ctx) });
}
