import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { RunContext } from "./context.js";
import type { WorkflowStep, WorkflowStepResult, WorkflowRunResult } from "../workflows/types.js";
import type { LlmSpendLedger } from "../llm/accounting.js";
import { createLlmSpendLedger, restoreLlmProvenance } from "../llm/accounting.js";
import { CostTracker, CostLimitExceededError, type CostLimit } from "../llm/cost_tracker.js";
import { resolveRetryConfig, computeDelay } from "./retry.js";
import { PersistenceError, WaitConflictError } from "../persistence/backend.js";
import { RequestInputResumeError } from "../pipeline/input_request.js";
import {
	createApprovalIndex,
	cleanupApprovalIndexByStateKey,
	findApprovalIdByStateKey,
} from "../state/store.js";
import { resumeGuardFromEnv } from "../persistence/context.js";
import { combineSignals } from "../process/owner_signal.js";
import { encodeToken } from "../resume/token.js";
import { enter } from "./nodes/condition.js";
import { switchStep } from "./nodes/switch.js";
import { repeat } from "./nodes/repeat.js";
import { isApprovalStep } from "../workflows/load.js";
import {
	activation,
	nodeKind,
	InvocationRetiredError,
	InterruptedActivityError,
	ExecutionConflictError,
	RunCancelled,
	type Activation,
	type Definition,
	type ExecutionState,
} from "./state.js";
import { ExecutionStore } from "./store.js";
import { sequence } from "./nodes/sequence.js";
import { activity } from "./nodes/activity.js";
import { forEach } from "./nodes/for-each.js";
import { parallel } from "./nodes/parallel.js";
import { childWorkflow } from "./nodes/child-workflow.js";
import { WorkflowPipelineHaltError } from "./activities/pipeline.js";
import { input } from "./nodes/input.js";
import { approval } from "./nodes/approval.js";
import { sleep } from "./nodes/sleep.js";
import { eventWait } from "./nodes/event.js";
import type { InboxEvent } from "../persistence/backend.js";
import { toOutputItems } from "./values.js";
import {
	trackStepCost,
	settleDeferredCosts,
	settleUnbilledCharges,
	type DeferredCharge,
} from "./accounting.js";
import {
	buildNeedsInputRequest,
	resolveToolEnvelopeMaxBytes,
	validateInputResponse,
	WorkflowResumeArgumentError,
	ResumeRefusedError,
	enforceApprovalIdentity,
	approvalIdentityFromRequest,
} from "./gates.js";

export type Scope = {
	definition: Definition;
	definitionPath?: string;
	args: Record<string, unknown>;
	results: Record<string, WorkflowStepResult>;
	lastStepId?: string;
	baseEnv?: Record<string, string | undefined>;
	parentLimits?: Array<{ limit: CostLimit; start: number }>;
	costStart?: number;
	env?: Record<string, string>;
	cwd?: string;
	ancestors: string[];
	ctx: RunContext;
	ledger: LlmSpendLedger;
	costPrefix: string;
	accountAs?: string;
	/** Why steps here cannot pause (inside a child workflow, a parallel branch, a retried loop). */
	gatesRefused?: string;
	timeoutSignal?: AbortSignal;
	/** Start the enclosing step's timeout; activities call it right before dispatch. */
	startTimeout?: () => void;
};

export class Suspension extends Error {
	constructor(
		readonly key: string,
		readonly frame: Activation,
		/** False when the question was already asked by an earlier invocation. */
		readonly fresh = true,
	) {
		super("Workflow suspended");
	}
}

export class Machine {
	readonly ledger: LlmSpendLedger;
	readonly cost: CostTracker;
	readonly ctx: RunContext;

	constructor(
		readonly state: ExecutionState,
		readonly store: ExecutionStore,
		ctx: RunContext,
	) {
		this.ctx = {
			...ctx,
			cwd: state.cwd,
			signal: combineSignals(ctx.signal, store.lost.signal),
		};
		this.cost = new CostTracker(CostTracker.parsePricingFromEnv(ctx.env, ctx.stderr), ctx.stderr);
		this.cost.restore(state.cost?.byStep);
		this.ledger = createLlmSpendLedger(ctx._llmSpendLedger);
		this.ledger.restore(state.llmSpend);
		this.ledger.restoreSettled(state.llmBilled);
		restoreLlmProvenance(state.activations, state.llmBilled);
	}

	/**
	 * Record bookkeeping that is recomputed from saved results after a restart
	 * (cursors, completion markers, evaluated conditions). It is written with the
	 * next save. Saves guarding an effect (before dispatch, results, waits) stay
	 * immediate; each save writes the whole snapshot, so nothing is lost.
	 */
	defer() {}

	/**
	 * A completed or cancelled run never runs again, so its step records (every intermediate
	 * output) are dropped; status, error, final output, cost and definition remain. A failed
	 * run keeps them: it can be retried from the step that failed.
	 */
	private finish(status: "completed" | "failed" | "cancelled") {
		this.state.status = status;
		if (status !== "failed") this.state.activations = {};
	}

	/**
	 * Prepare a failed run to continue from the step that failed: that step starts over (with
	 * its retry budget), the steps that completed keep their results, and an enclosing step
	 * that failed because of it (a child workflow, a loop) continues where it was. A step that
	 * reported dispatch of a command that may not be repeated is never run again.
	 */
	retryFailed() {
		if (this.state.status !== "failed") throw new Error("Only a failed run can be retried");
		const failed = Object.keys(this.state.activations).filter(
			(path) => this.state.activations[path].phase === "failed",
		);
		if (!failed.length)
			throw new Error(
				"This run failed before Lobster kept what a retry needs; start it again instead",
			);
		const below = (path: string) =>
			Object.keys(this.state.activations).filter((item) => item.startsWith(path + "/"));
		const deepest = failed.filter(
			(path) => !below(path).some((item) => this.state.activations[item].phase === "failed"),
		);
		for (const path of deepest) {
			const acted = [path, ...below(path)].find(
				(item) => this.state.activations[item].nonRetryable,
			);
			if (acted)
				throw new Error(
					`Step ${path.split("/").map(decodeURIComponent).slice(1).join("/")} may have acted before it failed (it ran a command that may not be repeated); Lobster will not run it again. Run \`lobster runs abandon ${this.store.runId}\` to close the run.`,
				);
		}
		for (const path of deepest) {
			for (const item of below(path)) delete this.state.activations[item];
			delete this.state.activations[path];
		}
		for (const path of failed) {
			const frame = this.state.activations[path];
			if (!frame) continue;
			frame.phase = "started";
			delete frame.error;
		}
		this.state.status = "running";
		delete this.state.error;
	}

	/**
	 * Forget the activations below a completed node. Its own frame keeps the result,
	 * and both changes reach storage in the same snapshot, so recovery never needs them.
	 */
	forgetBelow(key: string, inclusive: boolean, survivor: Activation) {
		for (const [path, item] of Object.entries(this.state.activations))
			if ((inclusive && path === key) || path.startsWith(key + "/")) {
				// An effect that must not repeat keeps blocking retries of every enclosing step.
				if (item.nonRetryable) survivor.nonRetryable = true;
				delete this.state.activations[path];
			}
	}

	save(extras: Parameters<ExecutionStore["save"]>[1] = {}) {
		this.state.cost = this.cost.hasUsage() ? this.cost.getSummary() : undefined;
		this.state.llmSpend = this.ledger.outstanding();
		this.state.llmBilled = this.ledger.settled();
		return this.store.save(
			this.state,
			extras,
			extras.publish || extras.consume ? () => this.checkAuthority() : undefined,
		);
	}

	checkAuthority(ctx = this.ctx) {
		try {
			ctx.assertInvocationCurrent?.();
		} catch (error) {
			throw new InvocationRetiredError(error);
		}
	}

	checkCost(scope: Scope) {
		for (const { limit, start } of scope.parentLimits ?? []) {
			this.cost.checkLimit(limit, scope.ctx.stderr, start);
		}
		if (scope.definition.workflow.cost_limit) {
			this.cost.checkLimit(scope.definition.workflow.cost_limit, scope.ctx.stderr, scope.costStart);
		}
	}

	fatal(error: unknown): boolean {
		return (
			error instanceof InvocationRetiredError ||
			error instanceof WorkflowPipelineHaltError ||
			error instanceof PersistenceError ||
			error instanceof ExecutionConflictError ||
			error instanceof InterruptedActivityError ||
			error instanceof Suspension ||
			error instanceof RequestInputResumeError ||
			error instanceof CostLimitExceededError ||
			error instanceof WorkflowResumeArgumentError ||
			error instanceof ResumeRefusedError ||
			error instanceof WaitConflictError ||
			error instanceof RunCancelled
		);
	}

	async delay(until: number, signal?: AbortSignal) {
		while (Date.now() < until) {
			signal?.throwIfAborted();
			await new Promise<void>((resolve, reject) => {
				const done = () => {
					signal?.removeEventListener("abort", aborted);
					resolve();
				};
				const timer = setTimeout(done, Math.min(until - Date.now(), 2147483647));
				const aborted = () => {
					clearTimeout(timer);
					reject(signal!.reason);
				};
				if (signal?.aborted) aborted();
				else signal?.addEventListener("abort", aborted, { once: true });
			});
		}
	}

	async visit(
		step: WorkflowStep,
		key: string,
		scope: Scope,
		account = true,
	): Promise<WorkflowStepResult> {
		this.ctx.signal?.throwIfAborted();
		this.checkAuthority();
		const frame = activation(this.state, key);
		if (frame.phase === "completed") return frame.result!;
		if (frame.phase === "failed") throw new Error(frame.error);
		if (await enter(this, step, frame, scope)) return frame.result!;
		// Refused before dispatch: the command of a gated step must not run when nobody can be asked.
		if (
			scope.gatesRefused &&
			(isApprovalStep(step.approval) || ["input", "sleep", "event"].includes(nodeKind(step)))
		)
			throw new Error(`Workflow step ${step.id}: ${scope.gatesRefused}`);
		const config = resolveRetryConfig(step.retry);
		while (true) {
			if (frame.retryAt) {
				await this.delay(frame.retryAt, scope.ctx.signal);
				delete frame.retryAt;
			}
			const attemptKey = key + "/attempt-" + frame.attempt;
			const timeout = new AbortController();
			let timer: ReturnType<typeof setTimeout> | undefined;
			// The clock measures the step's own work. An activity starts it once its
			// pre-dispatch checkpoint is written, so slow storage never eats the budget;
			// the deadline is persisted by the next save and survives a crash.
			// A step with heartbeat_timeout_ms must show progress: a shell command touches
			// LOBSTER_HEARTBEAT_FILE, a pipeline command calls ctx.heartbeat(). Silence aborts it
			// like a timeout, so retry, on_error and the repeatable rules apply.
			const heartbeatFile = step.heartbeat_timeout_ms
				? path.join(os.tmpdir(), `lobster-heartbeat-${randomUUID()}`)
				: undefined;
			let lastBeat = Date.now();
			const startHeartbeat = () => {
				if (!heartbeatFile || watchdog) return;
				lastBeat = Date.now();
				const limit = step.heartbeat_timeout_ms!;
				watchdog = setInterval(
					() => {
						let beat = lastBeat;
						try {
							beat = Math.max(beat, fs.statSync(heartbeatFile).mtimeMs);
						} catch {
							// Not touched yet.
						}
						if (Date.now() - beat > limit)
							timeout.abort(
								new Error(`Step '${step.id}' showed no progress for ${limit}ms (heartbeat)`),
							);
					},
					Math.max(25, Math.min(1000, Math.floor(limit / 4))),
				);
			};
			let watchdog: ReturnType<typeof setInterval> | undefined;
			const startTimeout = () => {
				startHeartbeat();
				if (!step.timeout_ms || timer || timeout.signal.aborted) return;
				frame.deadline ??= Date.now() + step.timeout_ms;
				if (frame.deadline <= Date.now()) {
					timeout.abort(new Error("Step timeout expired during interruption"));
					return;
				}
				timer = setTimeout(
					() => timeout.abort(new Error(`Step '${step.id}' timed out after ${step.timeout_ms}ms`)),
					frame.deadline - Date.now(),
				);
			};
			const local: Scope = {
				...scope,
				timeoutSignal: timeout.signal,
				startTimeout,
				ctx: {
					...scope.ctx,
					signal: combineSignals(scope.ctx.signal, timeout.signal),
					...(heartbeatFile
						? {
								env: { ...scope.ctx.env, LOBSTER_HEARTBEAT_FILE: heartbeatFile },
								_heartbeat: () => {
									lastBeat = Date.now();
								},
							}
						: {}),
				},
			};
			// Control nodes are timed as a whole. Gates wait for people, time or events, not work.
			if (["for-each", "repeat", "parallel", "child-workflow", "switch"].includes(nodeKind(step)))
				startTimeout();
			let decidingApproval = false;
			try {
				let result: WorkflowStepResult;
				// Once the approval gate exists the node has finished and been charged; its
				// result is the gate's subject. Never dispatch the node again to ask.
				const gate = this.state.activations[attemptKey + "/approval"];
				if (gate && isApprovalStep(step.approval) && !scope.gatesRefused) {
					decidingApproval = true;
					result = await approval(this, step, attemptKey, scope, gate.result!);
					frame.result = result;
					frame.phase = "completed";
					this.forgetBelow(key, false, frame);
					this.defer();
					return result;
				}
				switch (nodeKind(step)) {
					case "for-each":
						result = await forEach(this, step, attemptKey, local);
						break;
					case "parallel":
						result = await parallel(this, step, attemptKey, local);
						frame.results = activation(this.state, attemptKey).results;
						break;
					case "child-workflow":
						result = await childWorkflow(this, step, attemptKey, local);
						break;
					case "switch":
						result = await switchStep(this, step, attemptKey, local);
						// Its case's steps, and those that did not run as skipped, for later steps.
						frame.results = activation(this.state, attemptKey).results;
						break;
					case "repeat":
						result = await repeat(this, step, attemptKey, local);
						// The last round's steps, for later steps.
						frame.results = activation(this.state, attemptKey).results;
						break;
					case "input":
						result = await input(this, step, attemptKey, local);
						break;
					case "sleep":
						result = await sleep(this, step, attemptKey, local);
						break;
					case "event":
						result = await eventWait(this, step, attemptKey, local);
						break;
					default:
						result = await activity(this, step, attemptKey, local);
				}
				// The steps of a child workflow, a switch and a repeat were charged as they ran.
				if (
					account &&
					!frame.accounted &&
					!["child-workflow", "switch", "repeat"].includes(nodeKind(step))
				) {
					const deferred: DeferredCharge[] = [];
					if (frame.results)
						for (const [id, branch] of Object.entries(frame.results))
							trackStepCost(
								this.cost,
								scope.accountAs ?? scope.costPrefix + id,
								branch,
								scope.ledger,
								deferred,
							);
					trackStepCost(
						this.cost,
						scope.accountAs ?? scope.costPrefix + step.id,
						result,
						scope.ledger,
						deferred,
					);
					settleDeferredCosts(this.cost, deferred, scope.ledger);
					settleUnbilledCharges(this.cost, scope.ledger);
				}
				if (account) this.checkCost(scope);
				frame.accounted = account || frame.accounted;
				// An approval is a separate activation: the preceding command is never rerun to ask again.
				if (isApprovalStep(step.approval)) {
					// A completed activity must not be retried for a failed human decision/read.
					decidingApproval = true;
					result = await approval(this, step, attemptKey, scope, result);
				}
				frame.result = result;
				frame.phase = "completed";
				this.forgetBelow(key, false, frame);
				this.defer();
				return result;
			} catch (error) {
				this.checkAuthority(scope.ctx);
				if (decidingApproval || this.fatal(error) || scope.ctx.signal?.aborted) throw error;
				// Includes the attempt's own frame: a loop keeps the mark of iterations it forgot.
				const nonRetryable = Object.entries(this.state.activations).some(
					([path, item]) =>
						(path === attemptKey || path.startsWith(attemptKey + "/")) && item.nonRetryable,
				);
				settleUnbilledCharges(this.cost, scope.ledger);
				this.checkCost(scope);
				if (
					!nonRetryable &&
					frame.attempt < config.max &&
					!/halted (for approval inside|before completion at) pipeline/.test(error?.message ?? "")
				) {
					const delay = computeDelay(config, frame.attempt - 1);
					scope.ctx.stderr.write(
						`[RETRY] Step '${step.id}' failed (attempt ${frame.attempt}/${config.max}): ${error?.message ?? String(error)}. Retrying in ${delay}ms...\n`,
					);
					frame.attempt++;
					frame.retryAt = Date.now() + delay;
					delete frame.deadline;
					await this.save();
					continue;
				}
				frame.error = error?.message ?? String(error);
				if (!step.on_error || step.on_error === "stop") {
					frame.phase = "failed";
					await this.save();
					throw error;
				}
				frame.result = { id: step.id, error: true, errorMessage: frame.error };
				frame.phase = "completed";
				this.forgetBelow(key, false, frame);
				this.defer();
				return frame.result;
			} finally {
				if (timer) clearTimeout(timer);
				if (watchdog) clearInterval(watchdog);
				if (heartbeatFile) fs.rmSync(heartbeatFile, { force: true });
			}
		}
	}

	/**
	 * Save the wait of `frame` as pending, in the same save that marks the run waiting.
	 * Returns false when it was published before (a question asked again, e.g. on advance).
	 */
	async publishWait(key: string, frame: Activation): Promise<boolean> {
		this.checkAuthority();
		if (frame.waitKey) {
			if (this.state.status !== "waiting") {
				this.state.status = "waiting";
				await this.save();
			}
			return false;
		}
		frame.waitKey = "workflow_resume_" + randomUUID();
		frame.phase = "waiting";
		this.state.status = "waiting";
		const kind = frame.wait!.kind;
		await this.save({
			publish: {
				key: frame.waitKey,
				text: JSON.stringify({
					machineVersion: 1,
					runId: this.store.runId,
					activation: key,
					wait: frame.wait,
					...(kind === "input"
						? { inputStepId: key }
						: kind === "approval"
							? { approvalStepId: key }
							: { haltType: kind }),
				}),
			},
		});
		return true;
	}

	async suspend(key: string, frame: Activation): Promise<never> {
		// A step's timeout covers its work, not a pause for a person, a time or an event: the
		// steps enclosing this wait get a fresh budget when the run continues.
		for (const [ancestor, item] of Object.entries(this.state.activations))
			if (key.startsWith(ancestor + "/")) delete item.deadline;
		const fresh = await this.publishWait(key, frame);
		this.ctx.signal?.throwIfAborted();
		throw new Suspension(key, frame, fresh);
	}

	/** Complete a waiting step with `result`, consuming its published wait in the same save. */
	async resolveWait(key: string, frame: Activation, result: WorkflowStepResult) {
		this.checkAuthority();
		frame.result = result;
		frame.phase = "completed";
		this.state.status = "running";
		const waitKey = frame.waitKey;
		await this.save(waitKey ? { consume: { key: waitKey } } : {});
		if (waitKey && frame.wait?.kind === "approval")
			await cleanupApprovalIndexByStateKey({ env: this.ctx.env, stateKey: waitKey });
		return result;
	}

	/** Whether a question or event wait is past its deadline. */
	expired(frame: Activation, now = Date.now()) {
		const deadline = frame.wait && "deadline" in frame.wait ? frame.wait.deadline : undefined;
		return deadline !== undefined && now >= deadline;
	}

	/**
	 * Apply a deadline that has passed: `on_timeout: continue` completes the step with
	 * `timedOut: true`; `cancel` (the default) cancels the run like a rejection.
	 */
	async timeOut(key: string, frame: Activation, result: WorkflowStepResult) {
		const wait = frame.wait!;
		const onTimeout = "onTimeout" in wait ? wait.onTimeout : undefined;
		if (onTimeout === "continue")
			return this.resolveWait(key, frame, { ...result, timedOut: true });
		const waitKey = frame.waitKey;
		const message = `Step ${result.id} timed out waiting`;
		this.finish("cancelled");
		this.state.error = message;
		await this.save(waitKey ? { consume: { key: waitKey } } : {});
		if (waitKey && wait.kind === "approval")
			await cleanupApprovalIndexByStateKey({ env: this.ctx.env, stateKey: waitKey });
		throw new RunCancelled(message);
	}

	/** The oldest inbox event matching an event wait that this run has not consumed yet. */
	async findEvent(wait: Extract<NonNullable<Activation["wait"]>, { kind: "event" }>) {
		// Only unscoped events and events from the caller that owns this run.
		const run = await this.store.session.getRunState(this.store.runId);
		const owner = run?.metadata?.callerId ?? "";
		const consumed = new Set(this.state.events ?? []);
		const events = await this.store.session.findEvents(wait.event, wait.since, owner);
		// An event received after the deadline does not count, however late the run is advanced.
		return events.find(
			(event) =>
				!consumed.has(eventKey(event)) &&
				(wait.deadline === undefined || event.at <= wait.deadline) &&
				matchesEvent(wait.match, event.data),
		) as InboxEvent | undefined;
	}

	/** Record that this run consumed an inbox event; saved with the step's result. */
	consumeEvent(event: InboxEvent) {
		(this.state.events ??= []).push(eventKey(event));
	}

	async answer(
		key: string,
		decision: { approved?: boolean; response?: unknown; cancel?: boolean },
	) {
		this.ctx.signal?.throwIfAborted();
		this.checkAuthority();
		const entry = Object.entries(this.state.activations).find(
			([, frame]) => frame.waitKey === key && frame.phase === "waiting",
		);
		if (!entry) throw new ResumeRefusedError();
		const [path, frame] = entry;
		const wait = frame.wait!;
		if (wait.kind === "timer" || wait.kind === "event")
			throw new WorkflowResumeArgumentError(
				wait.kind === "timer"
					? "This run is sleeping; `lobster runs advance --due` continues it when the time comes"
					: `This run waits for the event "${wait.event}"; deliver it with \`lobster signal ${wait.event}\``,
			);
		if (this.expired(frame))
			throw new WorkflowResumeArgumentError(
				`This question expired at ${new Date(wait.deadline!).toISOString()}; \`lobster runs advance --due\` applies its timeout`,
			);
		// A step's timeout covers its work, not the time people take to answer it:
		// the steps enclosing this gate get a fresh budget when it is answered.
		for (const [ancestor, item] of Object.entries(this.state.activations))
			if (path.startsWith(ancestor + "/")) delete item.deadline;
		const rejectionContinues =
			wait.kind === "approval" && decision.approved === false && wait.onReject === "continue";
		if (
			decision.cancel ||
			(wait.kind === "approval" && decision.approved === false && !rejectionContinues)
		)
			this.finish("cancelled");
		else if (wait.kind === "approval") {
			if (typeof decision.approved !== "boolean" || decision.response !== undefined)
				throw new WorkflowResumeArgumentError(
					"Workflow resume requires --approve yes|no for approval requests",
				);
			const approvedBy =
				String(this.ctx.env.LOBSTER_APPROVAL_APPROVED_BY ?? "").trim() || undefined;
			// A rejection that lets the run go on is a decision too: only an allowed approver makes it.
			enforceApprovalIdentity({
				stepId: path,
				identity: approvalIdentityFromRequest(frame.approval),
				approvedBy,
			});
			frame.result = {
				...frame.result!,
				approved: decision.approved,
				...(approvedBy ? { approvedBy } : {}),
			};
			frame.phase = "completed";
			this.state.status = "running";
		} else {
			if (decision.response === undefined || decision.approved !== undefined)
				throw new WorkflowResumeArgumentError(
					"Workflow resume requires --response-json for input requests",
				);
			try {
				validateInputResponse({
					schema: wait.responseSchema,
					response: decision.response,
					...(this.state.definition.standalone ? {} : { stepId: path }),
				});
			} catch (error) {
				throw new WorkflowResumeArgumentError(error.message);
			}
			frame.result = {
				id: frame.stepId ?? path.split("/").at(-1)!,
				subject: wait.subject,
				response: decision.response,
			};
			frame.phase = "completed";
			this.state.status = "running";
		}
		const guard = resumeGuardFromEnv(this.ctx.env);
		try {
			await this.save({
				consume: {
					key,
					...(guard?.key === key ? { expectedWaitRevision: guard.expectedRevision } : {}),
				},
			});
		} catch (error) {
			// The answer was for a revision of the wait that is no longer current.
			if (error instanceof ExecutionConflictError && guard?.key === key) {
				const wait = (await this.store.session.getRunState(this.store.runId))?.waits[key];
				if (wait?.status !== "pending" || (wait.revision ?? 0) !== guard.expectedRevision)
					throw new WaitConflictError();
			}
			throw error;
		}
		await cleanupApprovalIndexByStateKey({ env: this.ctx.env, stateKey: key });
	}

	async resultForSuspension(frame: Activation, fresh = true): Promise<WorkflowRunResult> {
		const pending = frame.wait!;
		if (pending.kind === "timer" || pending.kind === "event") {
			const stepId = frame.stepId ?? "";
			return {
				status: "waiting",
				output: [],
				waitingFor:
					pending.kind === "timer"
						? { kind: "timer", stepId, until: new Date(pending.until).toISOString() }
						: {
								kind: "event",
								stepId,
								event: pending.event,
								...(pending.deadline !== undefined
									? { until: new Date(pending.deadline).toISOString() }
									: {}),
							},
			};
		}
		const resumeToken = encodeToken({
			protocolVersion: 1,
			v: 1,
			kind: "workflow-file",
			stateKey: frame.waitKey,
		});
		if (frame.wait!.kind === "approval") {
			// A question asked before keeps its short ID instead of minting another.
			const approvalId =
				(!fresh &&
					(await findApprovalIdByStateKey({ env: this.ctx.env, stateKey: frame.waitKey! }))) ||
				(await createApprovalIndex({ env: this.ctx.env, stateKey: frame.waitKey! }));
			return {
				status: "needs_approval",
				output: [],
				requiresApproval: { ...frame.approval!, resumeToken, approvalId },
			};
		}
		const wait = frame.wait!;
		if (wait.kind !== "input") throw new Error("Invalid wait kind");
		const request = buildNeedsInputRequest({
			stepId: frame.waitKey!,
			prompt: wait.prompt,
			responseSchema: wait.responseSchema,
			defaults: wait.defaults,
			subject: wait.subject,
			maxEnvelopeBytes: resolveToolEnvelopeMaxBytes(this.ctx.env),
		});
		return { status: "needs_input", output: [], requiresInput: { ...request, resumeToken } };
	}

	async run(): Promise<WorkflowRunResult> {
		if (this.state.status === "completed")
			return {
				status: "ok",
				output: this.state.output ?? [],
				...(this.state.cost ? { _meta: { cost: this.state.cost } } : {}),
			};
		if (this.state.status === "cancelled") return { status: "cancelled", output: [] };
		if (this.state.status === "failed") throw new Error(this.state.error ?? "Workflow failed");
		this.state.status = "running";
		delete this.state.error;
		try {
			const result = await sequence(this, this.state.definition.workflow.steps, "root", {
				definition: this.state.definition,
				args: this.state.args,
				results: {},
				env: this.state.definition.workflow.env,
				cwd: this.state.definition.workflow.cwd,
				ancestors: [this.state.definition.filePath],
				ctx: this.ctx,
				ledger: this.ledger,
				costPrefix: "",
			});
			settleUnbilledCharges(this.cost, this.ledger);
			// A standalone pipeline's output is its items as they are.
			const standalone = this.state.definition.standalone && Array.isArray(result.items);
			this.state.output = standalone ? result.items! : toOutputItems(result);
			this.finish("completed");
			await this.save();
			return {
				status: "ok",
				output: this.state.output,
				...(standalone && result.rendered ? { rendered: true } : {}),
				...(this.cost.hasUsage() ? { _meta: { cost: this.cost.getSummary() } } : {}),
			};
		} catch (error) {
			if (error instanceof Suspension) return this.resultForSuspension(error.frame, error.fresh);
			// A gate's deadline cancelled the run; it was saved as cancelled already.
			if (error instanceof RunCancelled) return { status: "cancelled", output: [] };
			if (
				error instanceof InvocationRetiredError ||
				error instanceof PersistenceError ||
				error instanceof ExecutionConflictError
			)
				throw error;
			// Stopped (Ctrl-C, SIGTERM, host abort) while a command was running: its
			// effects are unknown, so the run is blocked until someone advances it
			// (repeatable steps run again) or abandons it.
			const running = this.ctx.signal?.aborted
				? Object.entries(this.state.activations).find(
						([path, frame]) => path.endsWith("/activity") && frame.phase === "started",
					)?.[0]
				: undefined;
			this.state.status =
				error instanceof InterruptedActivityError || running
					? "blocked"
					: this.ctx.signal?.aborted
						? Object.values(this.state.activations).some(
								(frame) => frame.phase === "waiting" && frame.waitKey,
							)
							? "waiting"
							: "running"
						: "failed";
			this.state.error = running
				? `Interrupted while ${running} was running; its outcome is unknown. Check it, then run \`lobster runs advance ${this.store.runId}\` to continue or \`lobster runs abandon ${this.store.runId}\` to stop the run.`
				: (error?.message ?? String(error));
			if (this.state.status === "failed") this.finish("failed");
			await this.save();
			throw error;
		}
	}
}

/** Delivery IDs are unique per sender, so a consumed event is remembered by both. */
function eventKey(event: InboxEvent) {
	return JSON.stringify([event.owner, event.id]);
}

/** Every field of `expected` is in `actual`; nested objects match as subsets. */
export function matchesEvent(expected: unknown, actual: unknown): boolean {
	if (expected === undefined) return true;
	if (expected && typeof expected === "object" && !Array.isArray(expected)) {
		if (!actual || typeof actual !== "object" || Array.isArray(actual)) return false;
		return Object.entries(expected).every(([key, value]) =>
			matchesEvent(value, (actual as Record<string, unknown>)[key]),
		);
	}
	return JSON.stringify(expected) === JSON.stringify(actual);
}
