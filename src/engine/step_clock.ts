// The clock of one step attempt: its `timeout_ms` and its `heartbeat_timeout_ms`. Either one
// aborts the attempt like a failure, so retry, on_error and the repeatable rules apply.
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { WorkflowStep } from "../workflows/types.js";
import type { Activation } from "./checkpoint.js";

export class StepClock {
	readonly #timeout = new AbortController();
	/** Aborts when the attempt times out or stops showing progress. */
	readonly signal: AbortSignal = this.#timeout.signal;
	#timer?: ReturnType<typeof setTimeout>;
	#watchdog?: ReturnType<typeof setInterval>;
	#lastBeat = Date.now();
	/**
	 * With `heartbeat_timeout_ms`, the file a shell command touches to show progress
	 * (`LOBSTER_HEARTBEAT_FILE`); a pipeline command calls `ctx.heartbeat()` instead.
	 */
	readonly heartbeatFile?: string;

	constructor(
		private readonly step: WorkflowStep,
		/** The step's frame: its deadline is saved there, so it survives a crash. */
		private readonly frame: Activation,
	) {
		if (step.heartbeat_timeout_ms)
			this.heartbeatFile = path.join(os.tmpdir(), `lobster-heartbeat-${randomUUID()}`);
	}

	/** Report progress (a pipeline command's `ctx.heartbeat()`, a person being asked). */
	beat = () => {
		this.#lastBeat = Date.now();
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
		this.frame.timeoutAt ??= Date.now() + timeout_ms;
		if (this.frame.timeoutAt <= Date.now()) {
			this.#timeout.abort(new Error("Step timeout expired during interruption"));
			return;
		}
		this.#timer = setTimeout(
			() =>
				this.#timeout.abort(new Error(`Step '${this.step.id}' timed out after ${timeout_ms}ms`)),
			this.frame.timeoutAt - Date.now(),
		);
	};

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
				let beat = this.#lastBeat;
				try {
					beat = Math.max(beat, fs.statSync(file).mtimeMs);
				} catch {
					// Not touched yet.
				}
				if (Date.now() - beat > limit)
					this.#timeout.abort(
						new Error(`Step '${this.step.id}' showed no progress for ${limit}ms (heartbeat)`),
					);
			},
			Math.max(25, Math.min(1000, Math.floor(limit / 4))),
		);
	}
}
