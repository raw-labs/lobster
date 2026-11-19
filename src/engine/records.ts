// Values kept beside the checkpoint, each written once: a loop's items and outputs, and step
// outputs too large to rewrite with every save. Otherwise every save rewrites every large
// output of every step before it, and a loop over a big list costs the square of its length.
import { randomUUID } from "node:crypto";
import type { WorkflowStepResult } from "../workflows/types.js";
import type { StoreSession } from "../persistence/session.js";
import type { Activation, ExecutionState } from "./checkpoint.js";

/** Step outputs longer than this (in JSON characters) are kept beside the checkpoint. */
const OUTLINE_CHARS = 1024;
const OUTLINED_FIELDS = ["json", "stdout", "response"] as const;
type OutlinedField = (typeof OUTLINED_FIELDS)[number];
/** How a checkpoint refers to a value kept beside it. */
const RECORD_REF = "$lobster.record";

type RecordRef = { [RECORD_REF]: string };

function isRecordRef(value: unknown): value is RecordRef {
	return (
		Boolean(value) &&
		typeof value === "object" &&
		typeof (value as Record<string, unknown>)[RECORD_REF] === "string"
	);
}

/** The records a save writes and drops, and the checkpoint it writes with them. */
export type PreparedSave = {
	execution: ExecutionState;
	put: Record<string, string>;
	drop: string[];
};

/**
 * The checkpoint of a run that does not exist yet (a pause an older version saved, converted),
 * written as every save writes one: its large outputs as `records`, created with the run.
 */
export function newRunCheckpoint(state: ExecutionState) {
	const records: Record<string, string> = {};
	return { execution: new Outliner().checkpoint(state, records), records };
}

export class CheckpointRecords {
	#put = new Map<string, string>();
	#drop = new Set<string>();
	#outliner = new Outliner();

	constructor(
		private readonly session: StoreSession,
		private readonly runId: string,
	) {}

	/**
	 * Keep `value` under `key`, written with the next save. Keys start with the activation's
	 * path, so a node's records are found and dropped by that prefix.
	 */
	put(key: string, value: unknown) {
		this.#put.set(key, JSON.stringify(value));
	}

	/** Forget every record under `prefix`, with the next save. */
	drop(prefix: string) {
		for (const key of this.#put.keys()) if (key.startsWith(prefix)) this.#put.delete(key);
		this.#drop.add(prefix);
	}

	/** The records under `prefix`, as saved and as changed since, by key. */
	async read(prefix: string): Promise<Map<string, unknown>> {
		const found = new Map<string, unknown>();
		const dropped = (key: string) => [...this.#drop].some((drop) => key.startsWith(drop));
		if (!dropped(prefix))
			for (const row of await this.session.readRecords(this.runId, prefix))
				if (!dropped(row.key)) found.set(row.key, JSON.parse(row.text));
		for (const [key, text] of this.#put)
			if (key.startsWith(prefix)) found.set(key, JSON.parse(text));
		return found;
	}

	/**
	 * What to write for `state` now: the records changed since the last save, and the checkpoint
	 * with step outputs over OUTLINE_CHARS replaced by references to records. Called when the
	 * save's turn comes, so the records go with the state they belong to.
	 */
	prepare(state: ExecutionState): PreparedSave {
		const put = Object.fromEntries(this.#put);
		const drop = [...this.#drop];
		this.#put = new Map();
		this.#drop = new Set();
		return { execution: this.#outliner.checkpoint(state, put), put, drop };
	}

	/** Put the outputs a checkpoint refers to back in place; done before the run continues. */
	async hydrate(state: ExecutionState) {
		const refs: Array<[WorkflowStepResult, OutlinedField, string]> = [];
		const collect = (result: WorkflowStepResult | undefined) => {
			if (!result) return;
			for (const field of OUTLINED_FIELDS) {
				const value = result[field];
				if (isRecordRef(value)) refs.push([result, field, value[RECORD_REF]]);
			}
		};
		for (const frame of Object.values(state.activations)) {
			collect(frame.result);
			for (const entry of Object.values(frame.results ?? {})) collect(entry);
		}
		// Only the outputs referred to: a long run may have saved many that it no longer needs.
		const values = new Map<string, unknown>();
		for (const key of new Set(refs.map(([, , key]) => key))) {
			const found = await this.read(key);
			if (found.has(key)) values.set(key, found.get(key));
		}
		for (const [result, field, key] of refs) {
			if (!values.has(key)) throw new Error(`Saved step output ${key} is missing`);
			const value = values.get(key);
			result[field] = value as never;
			this.#outliner.remember(value, key);
		}
	}
}

/**
 * Writes checkpoints with step outputs over OUTLINE_CHARS (and any value shaped like a
 * reference) kept as records, each written once: it remembers the key of every value saved.
 */
class Outliner {
	#objectKeys = new WeakMap<object, string | false>();
	#textKeys = new Map<string, string>();

	/**
	 * `state` as written: its outputs replaced by references to records (added to `put`). A
	 * finished run keeps no records (and few activations): its checkpoint stays whole.
	 */
	checkpoint(state: ExecutionState, put: Record<string, string>): ExecutionState {
		if (state.status === "completed" || state.status === "cancelled") return state;
		const activations: ExecutionState["activations"] = {};
		for (const [path, frame] of Object.entries(state.activations))
			activations[path] = this.#frame(frame, put);
		return { ...state, activations };
	}

	/** `value` is saved under `key` already: a later save refers to it instead of writing it. */
	remember(value: unknown, key: string) {
		if (typeof value === "string") this.#textKeys.set(value, key);
		else if (value && typeof value === "object") this.#objectKeys.set(value, key);
	}

	#frame(frame: Activation, put: Record<string, string>): Activation {
		let copy: Activation | undefined;
		const result = this.#result(frame.result, put);
		if (result !== frame.result) (copy ??= { ...frame }).result = result;
		if (frame.results) {
			let results: Record<string, WorkflowStepResult> | undefined;
			for (const [id, entry] of Object.entries(frame.results)) {
				const outlined = this.#result(entry, put);
				if (outlined !== entry) (results ??= { ...frame.results })[id] = outlined!;
			}
			if (results) (copy ??= { ...frame }).results = results;
		}
		return copy ?? frame;
	}

	#result(result: WorkflowStepResult | undefined, put: Record<string, string>) {
		if (!result) return result;
		let copy: WorkflowStepResult | undefined;
		for (const field of OUTLINED_FIELDS) {
			if (result[field] === undefined) continue;
			const outlined = this.#value(result[field], put);
			if (outlined !== result[field]) (copy ??= { ...result })[field] = outlined as never;
		}
		return copy ?? result;
	}

	#value(value: unknown, put: Record<string, string>): unknown {
		if (typeof value === "string") {
			if (value.length <= OUTLINE_CHARS) return value;
			let key = this.#textKeys.get(value);
			if (!key) {
				key = "value/" + randomUUID();
				this.#textKeys.set(value, key);
				put[key] = JSON.stringify(value);
			}
			return { [RECORD_REF]: key };
		}
		if (!value || typeof value !== "object") return value;
		let key = this.#objectKeys.get(value);
		if (key === undefined) {
			const text = JSON.stringify(value);
			// A value shaped like a reference is kept as a record too: left inline, it would be
			// read back as a reference to another value.
			key = text.length > OUTLINE_CHARS || isRecordRef(value) ? "value/" + randomUUID() : false;
			if (key) put[key] = JSON.stringify(value);
			this.#objectKeys.set(value, key);
		}
		return key ? { [RECORD_REF]: key } : value;
	}
}
