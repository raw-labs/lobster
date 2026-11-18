// The machine that runs a saved execution: it visits steps (conditions, attempts, retries,
// on_error, timeouts, approvals after a step), charges their cost, and saves the checkpoint.
// What each kind of step does is in nodes/; waits, records and reporting have modules of
// their own.
import { messageOf } from "../util/errors.js";
import type { WorkflowStep, WorkflowStepResult, WorkflowRunResult } from "../workflows/types.js";
import { isApprovalStep } from "../workflows/steps.js";
import { computeDelay, resolveRetryConfig } from "../workflows/retry.js";
import {
	createLlmSpendLedger,
	restoreLlmProvenance,
	type LlmSpendLedger,
} from "../llm/accounting.js";
import { CostTracker } from "../llm/cost_tracker.js";
import { combineSignals } from "../process/owner_signal.js";
import { sleepUntil } from "../util/duration.js";
import {
	activation,
	activationsBelow,
	clearTimeoutsAbove,
	keys,
	isPending,
	unresolvedActivities,
	type Activation,
	type ExecutionState,
} from "./checkpoint.js";
import type { RunContext } from "./context.js";
import { ExecutionStore } from "./store.js";
import { CheckpointRecords } from "./records.js";
import { StepClock } from "./step_clock.js";
import type { Scope } from "./scope.js";
import { chargeStep, settleUnbilledCharges } from "./accounting.js";
import { pausedResult } from "./outcome.js";
import { withdrawWaits } from "./waits.js";
import { toOutputItems } from "./results.js";
import {
	InterruptedActivityError,
	InvocationRetiredError,
	RunCancelledError,
	RunSuspendedError,
	lostRun,
	stopsRun,
} from "./errors.js";
import { nodeFor, type NodeHandler } from "./nodes/index.js";
import { skippedByCondition } from "./nodes/condition.js";
import { sequence } from "./nodes/sequence.js";
import { approval } from "./nodes/approval.js";

export class Machine {
	readonly ctx: RunContext;
	readonly cost: CostTracker;
	readonly ledger: LlmSpendLedger;
	readonly records: CheckpointRecords;

	constructor(
		readonly state: ExecutionState,
		readonly store: ExecutionStore,
		ctx: RunContext,
	) {
		this.ctx = { ...ctx, cwd: state.cwd, signal: combineSignals(ctx.signal, store.lost.signal) };
		this.cost = new CostTracker(CostTracker.parsePricingFromEnv(ctx.env, ctx.stderr), ctx.stderr);
		this.cost.restore(state.cost?.byStep);
		this.ledger = createLlmSpendLedger();
		this.ledger.restore(state.llmSpend);
		this.ledger.restoreSettled(state.llmBilled);
		restoreLlmProvenance(state.activations, state.llmBilled);
		this.records = new CheckpointRecords(store.session, store.runId);
	}

	/** Run the workflow from where it stands until it finishes, pauses or stops. */
	async run(): Promise<WorkflowRunResult> {
		const { state } = this;
		if (state.status === "completed")
			return {
				status: "ok",
				output: state.output ?? [],
				...(state.cost ? { _meta: { cost: state.cost } } : {}),
			};
		if (state.status === "cancelled")
			return { status: "cancelled", output: [], ...(state.error ? { reason: state.error } : {}) };
		if (state.status === "failed") throw new Error(state.error ?? "Workflow failed");
		state.status = "running";
		delete state.error;
		try {
			await this.records.hydrate(state);
			const result = await sequence(this, state.definition.workflow.steps, "root", {
				definition: state.definition,
				args: state.args,
				results: {},
				env: state.definition.workflow.env,
				cwd: state.definition.workflow.cwd,
				ancestors: [state.definition.filePath],
				ctx: this.ctx,
				ledger: this.ledger,
				costPrefix: "",
			});
			settleUnbilledCharges(this.cost, this.ledger);
			// A standalone pipeline's output is its items as they are.
			const standalone = state.definition.standalone && Array.isArray(result.items);
			state.output = standalone ? result.items! : toOutputItems(result);
			this.finish("completed");
			await this.save();
			return {
				status: "ok",
				output: state.output,
				...(standalone && result.rendered ? { rendered: true } : {}),
				...(this.cost.hasUsage() ? { _meta: { cost: this.cost.getSummary() } } : {}),
			};
		} catch (error) {
			if (error instanceof RunSuspendedError)
				return pausedResult(state.activations[error.key], this.ctx.env, error.fresh);
			// A rejection or a deadline cancelled the run; it was saved as cancelled already.
			if (error instanceof RunCancelledError)
				return { status: "cancelled", output: [], reason: error.message };
			// Nothing more may be saved: another owner, the host or storage stopped this one.
			if (lostRun(error)) throw error;
			await this.#saveStopped(error);
			throw error;
		}
	}

	/**
	 * Visit `step` at `key`: decide its condition, run its attempts (retrying them as its
	 * `retry` allows), ask its approval, and apply `on_error`. `account: false` leaves the
	 * charging to the caller (a parallel step charges its branches).
	 */
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
		if (skippedByCondition(step, frame, scope)) return frame.result!;
		const node = nodeFor(step);
		// Refused before dispatch: the command of a gated step must not run when nobody can be asked.
		if (scope.gatesRefused && (isApprovalStep(step.approval) || node.role === "gate"))
			throw new Error(`Workflow step ${step.id}: ${scope.gatesRefused}`);
		frame.attempt ??= 1;
		while (true) {
			if (frame.retryAt) {
				await sleepUntil(frame.retryAt, scope.ctx.signal);
				delete frame.retryAt;
			}
			const attemptKey = keys.attempt(key, frame.attempt);
			// Once the approval after the step exists, the node has finished and been charged: its
			// result is the question's subject, and it never runs again to ask.
			const asked = this.state.activations[keys.approval(attemptKey)];
			let result: WorkflowStepResult;
			if (asked) result = asked.result!;
			else
				try {
					result = await this.#runNode(step, node, frame, attemptKey, scope, account);
				} catch (error) {
					this.checkAuthority(scope.ctx);
					if (stopsRun(error) || scope.ctx.signal?.aborted) throw error;
					const outcome = await this.#attemptFailed(step, key, frame, attemptKey, scope, error);
					if (outcome === "retry") continue;
					return outcome;
				}
			// Outside the retry: a failed decision never runs the command it is about again.
			if (isApprovalStep(step.approval))
				try {
					result = await approval(this, step, attemptKey, scope, result);
				} catch (error) {
					this.checkAuthority(scope.ctx);
					throw error;
				}
			return this.#complete(key, frame, result);
		}
	}

	/** Run the step's node for one attempt, timed by its clock, and charge what it spent. */
	async #runNode(
		step: WorkflowStep,
		node: NodeHandler,
		frame: Activation,
		attemptKey: string,
		scope: Scope,
		account: boolean,
	) {
		const clock = new StepClock(step, frame);
		let result: WorkflowStepResult;
		try {
			// Control nodes are timed as a whole; activities from dispatch; gates not at all.
			if (node.role === "control") clock.start();
			result = await node.run(this, step, attemptKey, timedScope(scope, clock));
		} finally {
			clock.stop();
		}
		const inner = this.state.activations[attemptKey]?.results;
		if (inner) frame.results = inner;
		if (account && !frame.accounted && !node.chargesInner)
			chargeStep(this.cost, scope, step.id, result, frame.results);
		if (account) this.checkCost(scope);
		frame.accounted ||= account;
		return result;
	}

	/**
	 * An attempt failed with `error`: retry the step later when its `retry` allows and nothing
	 * unrepeatable ran, otherwise fail it, or complete it with the error under `on_error`.
	 */
	async #attemptFailed(
		step: WorkflowStep,
		key: string,
		frame: Activation,
		attemptKey: string,
		scope: Scope,
		error: unknown,
	): Promise<"retry" | WorkflowStepResult> {
		// A question the attempt published can no longer be answered.
		await withdrawWaits(this, attemptKey);
		settleUnbilledCharges(this.cost, scope.ledger);
		this.checkCost(scope);
		const message = messageOf(error);
		const retry = resolveRetryConfig(step.retry);
		const attempt = frame.attempt!;
		const used = attempt - (frame.budgetFrom ?? 1) + 1;
		if (used < retry.max && !this.#actedBelow(attemptKey)) {
			const delay = computeDelay(retry, used - 1);
			scope.ctx.stderr.write(
				`[RETRY] Step '${step.id}' failed (attempt ${used}/${retry.max}): ${message}. Retrying in ${delay}ms...\n`,
			);
			frame.attempt = attempt + 1;
			frame.retryAt = Date.now() + delay;
			delete frame.timeoutAt;
			await this.save();
			return "retry";
		}
		frame.error = message;
		if (!step.on_error || step.on_error === "stop") {
			frame.phase = "failed";
			await this.save();
			throw error;
		}
		return this.#complete(key, frame, { id: step.id, error: true, errorMessage: message });
	}

	/** Complete the step with `result`; what ran below it is no longer needed. */
	#complete(key: string, frame: Activation, result: WorkflowStepResult) {
		frame.result = result;
		frame.phase = "completed";
		this.forgetBelow(key, false, frame);
		return result;
	}

	/** Whether a command that may not be repeated was dispatched at or below `key`. */
	#actedBelow(key: string) {
		return activationsBelow(this.state, key, true).some(([, item]) => item.nonRetryable);
	}

	/**
	 * Prepare a failed run to continue from the step that failed: that step starts over (with
	 * its retry budget), the steps that completed keep their results, and an enclosing step
	 * that failed because of it (a child workflow, a loop) continues where it was. A step that
	 * reported dispatch of a command that may not be repeated is never run again.
	 */
	retryFailed() {
		const { activations } = this.state;
		if (this.state.status !== "failed") throw new Error("Only a failed run can be retried");
		const failed = Object.keys(activations).filter((path) => activations[path].phase === "failed");
		if (!failed.length)
			throw new Error(
				`No step of this run failed, so there is no step to retry from: it stopped on "${this.state.error}". Start it again instead.`,
			);
		const below = (path: string) => activationsBelow(this.state, path).map(([item]) => item);
		const deepest = failed.filter(
			(path) => !below(path).some((item) => activations[item].phase === "failed"),
		);
		for (const path of deepest)
			if (this.#actedBelow(path))
				throw new Error(
					`Step ${keys.describe(path)} may have acted before it failed (it ran a command that may not be repeated); Lobster will not run it again. Run \`lobster runs abandon ${this.store.runId}\` to close the run.`,
				);
		for (const path of deepest) {
			for (const item of below(path)) delete activations[item];
			delete activations[path];
			clearTimeoutsAbove(this.state, path);
		}
		for (const path of failed) {
			const frame = activations[path];
			if (!frame) continue;
			frame.phase = "started";
			delete frame.error;
			// The step runs again under its current attempt (its failed record is gone above),
			// with its whole retry budget.
			if (frame.attempt !== undefined) frame.budgetFrom = frame.attempt;
		}
		this.state.status = "running";
		delete this.state.error;
	}

	/**
	 * Forget the activations below a completed node. Its own frame keeps the result, and both
	 * changes reach storage in the same snapshot, so recovery never needs them.
	 */
	forgetBelow(key: string, inclusive: boolean, survivor: Activation) {
		for (const [path, item] of activationsBelow(this.state, key, inclusive)) {
			// An effect that must not repeat keeps blocking retries of every enclosing step.
			if (item.nonRetryable) survivor.nonRetryable = true;
			delete this.state.activations[path];
		}
	}

	/**
	 * End the run. A completed or cancelled run never runs again, so its activations (every
	 * intermediate output) are dropped; status, error, final output, cost and definition remain.
	 * A failed run keeps them: it can be retried from the step that failed.
	 */
	finish(status: "completed" | "failed" | "cancelled") {
		this.state.status = status;
		if (status !== "failed") this.state.activations = {};
	}

	/**
	 * Save the checkpoint. Bookkeeping that is recomputed from saved results after a restart
	 * (cursors, completion markers, evaluated conditions) is not saved on its own: it is
	 * written with the next save, which writes the whole snapshot. Saves that guard an effect
	 * (before dispatch, results, waits) are made at once.
	 */
	save(extras: Parameters<ExecutionStore["save"]>[1] = {}) {
		this.state.cost = this.cost.hasUsage() ? this.cost.getSummary() : undefined;
		this.state.llmSpend = this.ledger.outstanding();
		this.state.llmBilled = this.ledger.settled();
		return this.store.save(
			this.state,
			extras,
			extras.publish || extras.consume ? () => this.checkAuthority() : undefined,
			(state) => this.records.prepare(state),
		);
	}

	/** Throw InvocationRetiredError once the host has retired this invocation. */
	checkAuthority(ctx = this.ctx) {
		try {
			ctx.assertInvocationCurrent?.();
		} catch (error) {
			throw new InvocationRetiredError(error);
		}
	}

	/** Throw once spend passes this workflow's `cost_limit`, or one of the workflows above it. */
	checkCost(scope: Scope) {
		for (const { limit, start } of scope.parentLimits ?? [])
			this.cost.checkLimit(limit, scope.ctx.stderr, start);
		const own = scope.definition.workflow.cost_limit;
		if (own) this.cost.checkLimit(own, scope.ctx.stderr, scope.costStart);
	}

	/**
	 * Save how the run stopped on `error`. A command running when the host stopped it (Ctrl-C,
	 * SIGTERM, an abort) has unknown effects: the run is blocked until someone advances it
	 * (repeatable steps run again) or abandons it. Otherwise a stopped run stays waiting or
	 * running, to continue later, and any other error fails it.
	 */
	async #saveStopped(error: unknown) {
		const { state } = this;
		const stopped = Boolean(this.ctx.signal?.aborted);
		const running = stopped ? unresolvedActivities(state)[0] : undefined;
		if (error instanceof InterruptedActivityError || running) state.status = "blocked";
		else if (!stopped) state.status = "failed";
		else state.status = Object.values(state.activations).some(isPending) ? "waiting" : "running";
		state.error = running
			? `Interrupted while ${keys.describe(running)} was running; its outcome is unknown. Check it, then run \`lobster runs advance ${this.store.runId}\` to continue or \`lobster runs abandon ${this.store.runId}\` to stop the run.`
			: messageOf(error);
		if (state.status !== "failed") return this.save();
		this.finish("failed");
		// A failed run is not answered any more: withdraw what it asked (it asks again on retry).
		await withdrawWaits(this);
		await this.save();
	}
}

/**
 * The scope an attempt runs in: its clock's signal aborts it, and with a heartbeat limit its
 * commands get the file to touch (LOBSTER_HEARTBEAT_FILE) and the callback to call.
 */
function timedScope(scope: Scope, clock: StepClock): Scope {
	const ctx = scope.ctx;
	return {
		...scope,
		timeoutSignal: clock.signal,
		startTimeout: clock.start,
		ctx: {
			...ctx,
			signal: combineSignals(ctx.signal, clock.signal),
			...(clock.heartbeatFile
				? {
						env: { ...ctx.env, LOBSTER_HEARTBEAT_FILE: clock.heartbeatFile },
						hooks: { ...ctx.hooks, heartbeat: clock.beat },
					}
				: {}),
		},
	};
}
