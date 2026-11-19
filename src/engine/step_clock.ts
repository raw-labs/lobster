// The clock of one step attempt: its `timeout_ms` and its `heartbeat_timeout_ms`. Either one
// aborts the attempt like a failure, so retry, on_error and the repeatable rules apply. A
// deliberate wait inside the step holds the clock: waiting is neither work nor a hang.
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { WorkflowStep } from "../workflows/types.js";
import type { Activation } from "./checkpoint.js";

/**
 * Hold the clocks of the steps around a wait. The result releases them: `finished` when the
 * wait ended normally, false when it threw (a stop), which keeps their deadlines unsaved.
 */
export type HoldClocks = () => (finished: boolean) => void;

/**
 * Run `wait`, a deliberate wait (a person answering at the terminal, a batch pause, a sleep
 * waited in place, a retry delay), with the clocks of the steps around it held: like a pause,
 * it counts against neither their `timeout_ms` nor their `heartbeat_timeout_ms`, and a crash or
 * a stop during it leaves them a fresh budget. `wait` saves the run first, inside the hold, so
 * the save records no deadline. Branches that run at once (parallel, for_each with concurrency)
 * hold nothing above them: while one waits, the others are still the step's work.
 */
export async function deliberateWait<T>(
	holdClocks: HoldClocks | undefined,
	wait: () => Promise<T>,
): Promise<T> {
	const release = holdClocks?.();
	let finished = false;
	try {
		const result = await wait();
		finished = true;
		return result;
	} finally {
		release?.(finished);
	}
}

export class StepClock {
	readonly #timeout = new AbortController();
	/** Aborts when the attempt times out or stops showing progress. */
	readonly signal: AbortSignal = this.#timeout.signal;
	#timer?: ReturnType<typeof setTimeout>;
	#watchdog?: ReturnType<typeof setInterval>;
	#lastBeat = Date.now();
	/** When the step times out (the clock's own copy of the frame's `timeoutAt`). */
	#deadline?: number;
	#held = 0;
	/** The time left before the deadline, while held. */
	#remaining?: number;
	/**
	 * With `heartbeat_timeout_ms`, the file a shell command touches to show progress
	 * (`LOBSTER_HEARTBEAT_FILE`); a pipeline command calls `ctx.heartbeat()` instead.
	 */
	readonly heartbeatFile?: string;

	constructor(
		private readonly step: WorkflowStep,
		/** The step's frame: its deadline is saved there, so it survives a crash. */
		private readonly frame: Activation,
		/** The enclosing step's heartbeat: progress here is progress there too. */
		private readonly outerBeat?: () => void,
		/** The enclosing step's hold: a wait here is a wait there too. */
		private readonly outerHold?: HoldClocks,
	) {
		if (step.heartbeat_timeout_ms)
			this.heartbeatFile = path.join(os.tmpdir(), `lobster-heartbeat-${randomUUID()}`);
	}

	/** Report progress (a pipeline command's `ctx.heartbeat()`). */
	beat = () => {
		this.#lastBeat = Date.now();
		this.outerBeat?.();
	};

	/**
	 * Start measuring. The clock measures the step's own work: an activity starts it once its
	 * pre-dispatch checkpoint is written, so slow storage never eats the budget; a control node
	 * starts it on entry. The deadline is saved with the next save, and a pause for a person, a
	 * time or an event clears it (the step gets a fresh budget when the run continues).
	 */
	start = () => {
		this.#startHeartbeat();
		const { timeout_ms } = this.step;
		if (!timeout_ms || this.#timer || this.#timeout.signal.aborted) return;
		this.#deadline = this.frame.timeoutAt ??= Date.now() + timeout_ms;
		if (this.#deadline <= Date.now()) {
			this.#timeout.abort(new Error("Step timeout expired during interruption"));
			return;
		}
		this.#arm();
	};

	/**
	 * Hold the clock, and the enclosing steps' clocks, during a deliberate wait. The deadline is
	 * taken off the frame meanwhile, so a save records none and a crash gives the step a fresh
	 * budget, as a pause does. Releasing a finished wait puts it back, moved by the time held,
	 * and restarts the heartbeat's count; after a wait that threw (a stop) the frame keeps no
	 * deadline, so the stop's save records none either.
	 */
	hold: HoldClocks = () => {
		const releaseOuter = this.outerHold?.();
		if (this.#held++ === 0 && this.#timer) {
			clearTimeout(this.#timer);
			this.#timer = undefined;
			this.#remaining = this.#deadline! - Date.now();
			delete this.frame.timeoutAt;
		}
		let released = false;
		return (finished) => {
			if (released) return;
			released = true;
			if (--this.#held === 0) {
				this.#lastBeat = Date.now();
				if (this.#remaining !== undefined) {
					this.#deadline = Date.now() + this.#remaining;
					this.#remaining = undefined;
					if (finished) this.frame.timeoutAt = this.#deadline;
					this.#arm();
				}
			}
			releaseOuter?.(finished);
		};
	};

	#arm() {
		if (this.#timeout.signal.aborted) return;
		const { timeout_ms } = this.step;
		this.#timer = setTimeout(
			() =>
				this.#timeout.abort(new Error(`Step '${this.step.id}' timed out after ${timeout_ms}ms`)),
			this.#deadline! - Date.now(),
		);
	}

	stop() {
		if (this.#timer) clearTimeout(this.#timer);
		if (this.#watchdog) clearInterval(this.#watchdog);
		if (this.heartbeatFile) fs.rmSync(this.heartbeatFile, { force: true });
	}

	#startHeartbeat() {
		const limit = this.step.heartbeat_timeout_ms;
		if (!limit || !this.heartbeatFile || this.#watchdog) return;
		this.#lastBeat = Date.now();
		const file = this.heartbeatFile;
		this.#watchdog = setInterval(
			() => {
				try {
					const touched = fs.statSync(file).mtimeMs;
					if (touched > this.#lastBeat) {
						this.#lastBeat = touched;
						this.outerBeat?.();
					}
				} catch {
					// Not touched yet.
				}
				if (this.#held === 0 && Date.now() - this.#lastBeat > limit)
					this.#timeout.abort(
						new Error(`Step '${this.step.id}' showed no progress for ${limit}ms (heartbeat)`),
					);
			},
			Math.max(25, Math.min(1000, Math.floor(limit / 4))),
		);
	}
}
