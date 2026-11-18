// The life of a wait: published when a step pauses, answered by a person, ended by its
// deadline, resolved in place, or withdrawn when its step or run fails. Each change is saved
// together with the state it causes.
import { messageOf } from "../util/errors.js";
import { randomUUID } from "node:crypto";
import type { WorkflowStepResult, WorkflowWaitTimeout } from "../workflows/types.js";
import { WaitConflictError } from "../persistence/backend.js";
import { resumeGuardFromEnv } from "../persistence/context.js";
import { cleanupApprovalIndexByStateKey } from "../state/store.js";
import { parseDuration } from "../util/duration.js";
import {
	activationsBelow,
	clearTimeoutsAbove,
	continuationText,
	deadlineOf,
	isPending,
	isFinished,
	type Activation,
	type WaitDeadline,
	type WaitRequest,
} from "./checkpoint.js";
import type { Machine } from "./machine.js";
import { decideApproval, readAnswer, validateInputResponse } from "./asking.js";
import { isInteractive } from "../io/terminal.js";
import type { Scope } from "./scope.js";
import {
	ExecutionConflictError,
	ResumeRefusedError,
	RunCancelledError,
	RunSuspendedError,
	InvalidAnswerError,
} from "./errors.js";

type Decision = { approved?: boolean; response?: unknown; cancel?: boolean };

type WaitOf<K extends WaitRequest["kind"]> = Extract<WaitRequest, { kind: K }>;

/**
 * The wait of `frame`, created by `create` on first entry and saved with it: a resumed or
 * recovered step keeps the same question, wake time or deadline.
 */
export function waitOf<K extends WaitRequest["kind"]>(
	frame: Activation,
	kind: K,
	stepId: string,
	create: () => WaitOf<K>,
): WaitOf<K> {
	frame.wait ??= { request: create(), stepId };
	const { request } = frame.wait;
	if (request.kind !== kind)
		throw new Error(`Saved wait is a ${request.kind} wait, not a ${kind} wait`);
	return request as WaitOf<K>;
}

/** The deadline a wait gets when it starts, from its `timeout`/`on_timeout`. */
export function waitDeadline(config: WorkflowWaitTimeout | undefined, what: string): WaitDeadline {
	if (!config || config.timeout === undefined) return {};
	return {
		deadline: Date.now() + parseDuration(config.timeout, `${what} timeout`),
		onTimeout: config.on_timeout ?? "cancel",
	};
}

/**
 * Save the wait of `frame` as pending, in the same save that marks the run waiting.
 * Returns false when it was published before (a question asked again, e.g. on advance).
 */
export async function publishWait(machine: Machine, key: string, frame: Activation) {
	machine.checkAuthority();
	const wait = frame.wait!;
	if (wait.key) {
		if (machine.state.status !== "waiting") {
			machine.state.status = "waiting";
			await machine.save();
		}
		return false;
	}
	wait.key = "workflow_resume_" + randomUUID();
	frame.phase = "waiting";
	machine.state.status = "waiting";
	await machine.save({
		publish: { key: wait.key, text: continuationText(machine.store.runId, key, wait.request) },
	});
	return true;
}

/** Pause the run at `frame`: publish its wait and stop. */
export async function suspend(machine: Machine, key: string, frame: Activation): Promise<never> {
	// A step's timeout covers its work, not a pause: the steps enclosing the wait start over.
	clearTimeoutsAbove(machine.state, key);
	const fresh = await publishWait(machine, key, frame);
	machine.ctx.signal?.throwIfAborted();
	throw new RunSuspendedError(key, fresh);
}

/** Complete a waiting step with `result`, consuming its published wait in the same save. */
export async function resolveWait(machine: Machine, frame: Activation, result: WorkflowStepResult) {
	machine.checkAuthority();
	frame.result = result;
	frame.phase = "completed";
	machine.state.status = "running";
	await saveConsumed(machine, frame);
	return result;
}

/** Whether a question or event wait is past its deadline. */
export function expired(frame: Activation, now = Date.now()) {
	const deadline = deadlineOf(frame.wait?.request);
	return deadline !== undefined && now >= deadline;
}

/**
 * Apply a deadline that has passed: `on_timeout: continue` completes the step with
 * `timedOut: true`; `cancel` (the default) cancels the run like a rejection.
 */
export async function timeOut(machine: Machine, frame: Activation, result: WorkflowStepResult) {
	const wait = frame.wait!.request;
	if ("onTimeout" in wait && wait.onTimeout === "continue")
		return resolveWait(machine, frame, { ...result, timedOut: true });
	return cancelRun(machine, frame, `Step ${result.id} timed out waiting`);
}

/** Cancel the run at `frame` (a rejection, a deadline), consuming its wait in the same save. */
export async function cancelRun(
	machine: Machine,
	frame: Activation,
	message: string,
): Promise<never> {
	cancelled(machine, message);
	await saveConsumed(machine, frame);
	throw new RunCancelledError(message);
}

/**
 * Ask the person at the terminal for `frame`'s answer, after writing `prompt`; pause the run
 * when nobody can be asked here (tool mode, no terminal). A deadline that has passed, or that
 * passes while asking, times the step out with `unanswered` as its result.
 */
export async function askAtTerminal(
	machine: Machine,
	key: string,
	frame: Activation,
	scope: Scope,
	prompt: string,
	unanswered: WorkflowStepResult,
): Promise<{ answer: string } | { timedOut: WorkflowStepResult }> {
	if (expired(frame)) return { timedOut: await timeOut(machine, frame, unanswered) };
	if (scope.ctx.mode === "tool" || !isInteractive(scope.ctx.stdin))
		await suspend(machine, key, frame);
	scope.ctx.stdout.write(prompt);
	const read = await readAnswer(scope.ctx, deadlineOf(frame.wait?.request));
	if ("expired" in read) return { timedOut: await timeOut(machine, frame, unanswered) };
	return { answer: read.answer.trim() };
}

/**
 * Withdraw the waits published at or below `key` (anywhere, without one): nobody may answer
 * them any more, because the step that asked failed or the run ended.
 */
export async function withdrawWaits(machine: Machine, key?: string) {
	const frames = key
		? activationsBelow(machine.state, key, true).map(([, frame]) => frame)
		: Object.values(machine.state.activations);
	for (const frame of frames) {
		if (!isPending(frame)) continue;
		const published = { ...frame.wait! };
		delete frame.wait!.key;
		await saveConsumed(machine, { wait: published });
	}
}

/**
 * Cancel the whole run with `reason` (`lobster runs abandon`), withdrawing every wait it still
 * has published. Each withdrawal is saved together with the cancelled state, so a crash in
 * between never leaves a run that waits for nothing.
 */
export async function cancelWithdrawing(machine: Machine, reason: string) {
	const published = Object.values(machine.state.activations)
		.filter(isPending)
		.map((frame) => ({ wait: { ...frame.wait! } }));
	cancelled(machine, reason);
	// Only waits the store still holds open: one already consumed cannot be consumed again.
	const run = await machine.store.session.getRunState(machine.store.runId);
	const open = published.filter((frame) => run?.waits[frame.wait.key!]?.status === "pending");
	if (!open.length) return machine.save();
	for (const frame of open) await saveConsumed(machine, frame);
}

/** Apply a person's answer to the wait published under `waitKey`. */
export async function answer(machine: Machine, waitKey: string, decision: Decision) {
	machine.ctx.signal?.throwIfAborted();
	machine.checkAuthority();
	const entry = Object.entries(machine.state.activations).find(
		([, frame]) => isPending(frame) && frame.wait!.key === waitKey,
	);
	if (!entry || isFinished(machine.state)) throw new ResumeRefusedError();
	const [path, frame] = entry;
	const wait = frame.wait!.request;
	if (wait.kind === "timer")
		throw new InvalidAnswerError(
			"This run is sleeping; `lobster runs advance --due` continues it when the time comes",
		);
	if (wait.kind === "event")
		throw new InvalidAnswerError(
			`This run waits for the event "${wait.event}"; deliver it with \`lobster signal ${wait.event}\``,
		);
	if (expired(frame))
		throw new InvalidAnswerError(
			`This question expired at ${new Date(wait.deadline!).toISOString()}; \`lobster runs advance --due\` applies its timeout`,
		);
	clearTimeoutsAbove(machine.state, path);
	if (decision.cancel) cancelled(machine, "Cancelled");
	else if (wait.kind === "approval") {
		if (typeof decision.approved !== "boolean" || decision.response !== undefined)
			throw new InvalidAnswerError(
				"Workflow resume requires --approve yes|no for approval requests",
			);
		if (!decision.approved && wait.onReject !== "continue") cancelled(machine, "Not approved");
		else {
			frame.result = {
				...frame.result!,
				...decideApproval(frame.wait!.stepId, wait, decision.approved, machine.ctx.env),
			};
			frame.phase = "completed";
			machine.state.status = "running";
		}
	} else {
		if (decision.response === undefined || decision.approved !== undefined)
			throw new InvalidAnswerError("Workflow resume requires --response-json for input requests");
		try {
			validateInputResponse({
				schema: wait.responseSchema,
				response: decision.response,
				stepId: frame.wait!.stepId,
				standalone: machine.state.definition.standalone,
			});
		} catch (error) {
			throw new InvalidAnswerError(messageOf(error));
		}
		frame.result = {
			id: frame.wait!.stepId,
			subject: wait.subject,
			response: decision.response,
		};
		frame.phase = "completed";
		machine.state.status = "running";
	}
	const guard = resumeGuardFromEnv(machine.ctx.env);
	const expectedWaitRevision = guard?.key === waitKey ? guard.expectedRevision : undefined;
	try {
		await saveConsumed(machine, frame, expectedWaitRevision);
	} catch (error) {
		// The answer was for a revision of the wait that is no longer current.
		if (error instanceof ExecutionConflictError && expectedWaitRevision !== undefined) {
			const run = await machine.store.session.getRunState(machine.store.runId);
			const saved = run?.waits[waitKey];
			if (saved?.status !== "pending" || (saved.revision ?? 0) !== expectedWaitRevision)
				throw new WaitConflictError();
		}
		throw error;
	}
}

/** Mark the run cancelled, with why; saved by the caller. */
function cancelled(machine: Machine, reason: string) {
	machine.finish("cancelled");
	machine.state.error = reason;
}

/** Save the state with `frame`'s published wait consumed, and withdraw its approval ID. */
async function saveConsumed(
	machine: Machine,
	frame: Pick<Activation, "wait">,
	expectedWaitRevision?: number,
) {
	const waitKey = frame.wait?.key;
	if (!waitKey) return machine.save();
	await machine.save({
		consume: {
			key: waitKey,
			...(expectedWaitRevision !== undefined ? { expectedWaitRevision } : {}),
		},
	});
	if (frame.wait?.request.kind === "approval")
		await cleanupApprovalIndexByStateKey({ env: machine.ctx.env, stateKey: waitKey });
}
