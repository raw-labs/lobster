import { randomUUID } from "node:crypto";
import type { StoreSession } from "../persistence/session.js";
import type { RunAction } from "../persistence/model.js";
import { PersistenceError } from "../persistence/backend.js";
import { ExecutionConflictError, InvocationRetiredError, type ExecutionState } from "./state.js";

type SaveExtras = Pick<
	Extract<RunAction, { kind: "execution-save" }>,
	"publish" | "consume" | "records"
>;

/** Serialize branch checkpoints; ownership is checked inside the backend transaction. */
export class ExecutionStore {
	readonly owner = randomUUID();
	private epoch = 0;
	private tail: Promise<unknown> = Promise.resolve();
	private failure?: unknown;
	private closed = false;
	private heartbeat?: ReturnType<typeof setInterval>;
	readonly lost = new AbortController();

	constructor(
		readonly session: StoreSession,
		readonly runId: string,
		readonly leaseMs = 30000,
	) {
		if (!Number.isSafeInteger(leaseMs) || leaseMs < 100)
			throw new Error("Execution lease must be at least 100ms");
	}

	private async transition(action: RunAction, assertCurrent?: () => void) {
		if (this.closed) throw new ExecutionConflictError();
		if (this.failure) throw this.failure;
		const operationId = randomUUID();
		try {
			const result = await this.session.transition(
				{ runId: this.runId, operationId, action },
				undefined,
				assertCurrent,
			);
			if (result.status !== "committed") throw new ExecutionConflictError();
		} catch (error) {
			if (error instanceof InvocationRetiredError) throw error;
			this.failure =
				error instanceof PersistenceError || error instanceof ExecutionConflictError
					? error
					: new PersistenceError(operationId, this.runId, error);
			this.lost.abort(this.failure);
			throw this.failure;
		}
	}

	async claim() {
		const run = await this.session.getRunState(this.runId);
		if (!run) throw new Error("Workflow run not found");
		this.epoch = (run.executionLease?.epoch ?? 0) + 1;
		await this.transition({
			kind: "execution-claim",
			owner: this.owner,
			epoch: this.epoch,
			expiresAt: Date.now() + this.leaseMs,
		});
		this.heartbeat = setInterval(
			() => {
				void this.enqueue(() =>
					this.transition({
						kind: "execution-renew",
						owner: this.owner,
						epoch: this.epoch,
						expiresAt: Date.now() + this.leaseMs,
					}),
				).catch(() => {});
			},
			Math.floor(this.leaseMs / 3),
		);
		this.heartbeat.unref();
		// Read after admission: a checkpoint read before the claim could be stale.
		return (await this.session.getRunState(this.runId))!.execution;
	}

	private enqueue<T>(task: () => Promise<T>): Promise<T> {
		const next = this.tail.then(task);
		this.tail = next.catch(() => {});
		return next;
	}

	/**
	 * Save `state` after every save queued before it. `prepare`, called when this save's turn
	 * comes, may give a smaller checkpoint to write instead, plus records to write with it.
	 */
	save(
		state: ExecutionState,
		extras: SaveExtras = {},
		assertCurrent?: () => void,
		prepare?: (state: ExecutionState) => {
			execution: ExecutionState;
			put?: Record<string, string>;
		},
	) {
		return this.enqueue(async () => {
			const prepared = prepare?.(state);
			const next = structuredClone(prepared?.execution ?? state);
			next.revision = state.revision + 1;
			const put = { ...extras.records?.put, ...prepared?.put };
			const records =
				extras.records?.drop?.length || Object.keys(put).length
					? { records: { drop: extras.records?.drop ?? [], put } }
					: {};
			await this.transition(
				{
					kind: "execution-save",
					owner: this.owner,
					epoch: this.epoch,
					expectedVersion: state.revision,
					execution: next,
					...extras,
					...records,
				},
				assertCurrent,
			);
			state.revision = next.revision;
		});
	}

	async close() {
		if (this.heartbeat) clearInterval(this.heartbeat);
		await this.tail;
		const release = { kind: "execution-release" as const, owner: this.owner, epoch: this.epoch };
		if (!this.failure) await this.transition(release);
		else if (this.failure instanceof ExecutionConflictError)
			// A refused save (a stale answer, say) need not mean the lease was lost: hand it back
			// so the run is not locked until it expires. Without the lease, this changes nothing.
			await this.session
				.transition({ runId: this.runId, operationId: randomUUID(), action: release })
				.catch(() => {});
		this.closed = true;
	}
}
