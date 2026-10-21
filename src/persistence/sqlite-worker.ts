// This is the only module that loads node:sqlite. Files remain usable on Node 22.0.
import { WaitConflictError } from "./backend.js";
import { DatabaseSync } from "node:sqlite";
import { parentPort, workerData } from "node:worker_threads";
import {
	applyTransition,
	canRetain,
	historyPage,
	runSummary,
	SCHEMA_VERSION,
	validateArchive,
	type Attempt,
	type LogicalArchive,
	type OperationReceipt,
	type RunEvent,
	type RunRecord,
	type Transition,
	type TransitionResult,
	type WaitRecord,
} from "./model.js";
import type { WorkerMethod, WorkerRequest, WorkerResult } from "./sqlite-protocol.js";

type RecordRow = { record: string };

type StateRow = { key: string; text: string | null; run_id: string | null };

type RunMetadataRecord = Omit<RunRecord, "attempts" | "waits" | "events" | "receipts" | "states">;

const db = new DatabaseSync(workerData.path, { readOnly: workerData.readOnly });

const statements = new Map<string, ReturnType<DatabaseSync["prepare"]>>();

function prepare(sql: string): ReturnType<DatabaseSync["prepare"]> {
	let statement = statements.get(sql);
	if (!statement) {
		statement = db.prepare(sql);
		statements.set(sql, statement);
	}
	return statement;
}

db.exec("PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;");

const version = Number(prepare("PRAGMA user_version").get().user_version);

if (version !== 0 && version !== SCHEMA_VERSION) {
	throw new Error(`Unsupported persistence schema ${version}`);
}

if (!workerData.readOnly) {
	db.exec(`
		PRAGMA journal_mode=WAL;
		PRAGMA synchronous=FULL;

		CREATE TABLE IF NOT EXISTS state (
			key TEXT PRIMARY KEY,
			text TEXT,
			run_id TEXT
		);
		CREATE TABLE IF NOT EXISTS runs (
			id TEXT PRIMARY KEY,
			record TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS attempts (
			run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
			id TEXT NOT NULL,
			path TEXT NOT NULL,
			record TEXT NOT NULL,
			PRIMARY KEY(run_id, id)
		);
		CREATE INDEX IF NOT EXISTS attempt_paths ON attempts(run_id, path);
		CREATE TABLE IF NOT EXISTS waits (
			run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
			key TEXT NOT NULL,
			record TEXT NOT NULL,
			PRIMARY KEY(run_id, key)
		);
		CREATE TABLE IF NOT EXISTS events (
			run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
			sequence INTEGER NOT NULL,
			record TEXT NOT NULL,
			PRIMARY KEY(run_id, sequence)
		);
		CREATE TABLE IF NOT EXISTS receipts (
			run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
			operation_id TEXT NOT NULL,
			record TEXT NOT NULL,
			PRIMARY KEY(run_id, operation_id)
		);
		PRAGMA user_version=${SCHEMA_VERSION};
	`);
}

function metadata(id: string): RunRecord | null {
	const row = prepare("SELECT record FROM runs WHERE id=?").get(id) as RecordRow | undefined;
	if (!row) {
		return null;
	}

	const record = JSON.parse(row.record) as RunMetadataRecord;
	return { ...record, attempts: [], waits: {}, events: [], receipts: {}, states: {} };
}

function getRun(id: string): RunRecord | null {
	const run = metadata(id);
	if (!run) {
		return null;
	}

	const attempts = prepare("SELECT record FROM attempts WHERE run_id=? ORDER BY rowid").all(
		id,
	) as RecordRow[];
	run.attempts = attempts.map((row) => JSON.parse(row.record) as Attempt);

	const events = prepare("SELECT record FROM events WHERE run_id=? ORDER BY sequence").all(
		id,
	) as RecordRow[];
	run.events = events.map((row) => JSON.parse(row.record) as RunEvent);

	const waits = prepare("SELECT key,record FROM waits WHERE run_id=?").all(id) as (RecordRow & {
		key: string;
	})[];
	for (const row of waits) {
		run.waits[row.key] = JSON.parse(row.record) as WaitRecord;
	}

	const receipts = prepare("SELECT operation_id,record FROM receipts WHERE run_id=?").all(
		id,
	) as (RecordRow & { operation_id: string })[];
	for (const row of receipts) {
		run.receipts[row.operation_id] = JSON.parse(row.record) as OperationReceipt;
	}

	const states = prepare("SELECT key,text FROM state WHERE run_id=?").all(id) as StateRow[];
	for (const row of states) {
		run.states[row.key] = row.text;
	}
	return run;
}

function listRuns(): RunRecord[] {
	const rows = prepare("SELECT id FROM runs ORDER BY id").all() as { id: string }[];
	return rows.map((row) => getRun(row.id)!);
}

function transaction<T>(task: () => T, write = true): T {
	db.exec(write ? "BEGIN IMMEDIATE" : "BEGIN");
	try {
		const value = task();
		db.exec("COMMIT");
		return value;
	} catch (error) {
		db.exec("ROLLBACK");
		throw error;
	}
}

function saveRun(run: RunRecord): void {
	const { attempts, waits, events, receipts, states, ...record } = run;
	prepare(
		"INSERT INTO runs(id,record) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET record=excluded.record",
	).run(run.id, JSON.stringify(record));

	for (const attempt of attempts) {
		prepare(
			"INSERT INTO attempts(run_id,id,path,record) VALUES(?,?,?,?) ON CONFLICT(run_id,id) DO UPDATE SET record=excluded.record",
		).run(run.id, attempt.id, attempt.path, JSON.stringify(attempt));
	}
	for (const [key, wait] of Object.entries(waits)) {
		prepare(
			"INSERT INTO waits(run_id,key,record) VALUES(?,?,?) ON CONFLICT(run_id,key) DO UPDATE SET record=excluded.record",
		).run(run.id, key, JSON.stringify(wait));
	}
	for (const event of events) {
		prepare(
			"INSERT INTO events(run_id,sequence,record) VALUES(?,?,?) ON CONFLICT(run_id,sequence) DO NOTHING",
		).run(run.id, event.sequence, JSON.stringify(event));
	}
	for (const [id, receipt] of Object.entries(receipts)) {
		prepare(
			"INSERT INTO receipts(run_id,operation_id,record) VALUES(?,?,?) ON CONFLICT(run_id,operation_id) DO NOTHING",
		).run(run.id, id, JSON.stringify(receipt));
	}
	for (const [key, text] of Object.entries(states)) {
		prepare(
			"INSERT INTO state(key,text,run_id) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET text=excluded.text,run_id=excluded.run_id",
		).run(key, text, run.id);
	}
}

function transition(request: Transition): TransitionResult {
	const current = metadata(request.runId);
	let previousAttempts: number | undefined;
	let successorExists = false;
	if (current) {
		const receipt = prepare("SELECT record FROM receipts WHERE run_id=? AND operation_id=?").get(
			current.id,
			request.operationId,
		) as RecordRow | undefined;
		if (receipt) {
			current.receipts[request.operationId] = JSON.parse(receipt.record) as OperationReceipt;
		}

		if (request.action.kind === "start" && !request.action.attempt.attempt) {
			const row = prepare("SELECT COUNT(*) AS count FROM attempts WHERE run_id=? AND path=?").get(
				current.id,
				request.action.attempt.path,
			);
			// Supply the count separately so the request fingerprint stays stable on replay.
			previousAttempts = Number(row.count);
		}
		if (request.action.kind === "finish") {
			const row = prepare("SELECT record FROM attempts WHERE run_id=? AND id=?").get(
				current.id,
				request.action.id,
			) as RecordRow | undefined;
			if (row) {
				current.attempts = [JSON.parse(row.record) as Attempt];
			}
		}
		const keys =
			request.action.kind === "adopt"
				? [request.action.checkpoint.key]
				: request.action.kind === "state"
					? [request.action.key]
					: request.action.kind === "handoff"
						? [request.action.predecessor.key, request.action.successor.key]
						: [];
		for (const key of keys) {
			const state = prepare("SELECT text FROM state WHERE run_id=? AND key=?").get(
				current.id,
				key,
			) as Pick<StateRow, "text"> | undefined;
			const wait = prepare("SELECT record FROM waits WHERE run_id=? AND key=?").get(
				current.id,
				key,
			) as RecordRow | undefined;
			if (state) {
				current.states[key] = state.text;
			}
			if (wait) {
				current.waits[key] = JSON.parse(wait.record) as WaitRecord;
			}
		}
	}

	if (request.action.kind === "handoff") {
		successorExists = Boolean(
			prepare("SELECT key FROM state WHERE key=?").get(request.action.successor.key),
		);
	}
	const applied = applyTransition(current, request, { previousAttempts, successorExists });
	if (applied.status === "committed" && applied.run !== current) {
		saveRun(applied.run);
	}
	return { status: applied.status, revision: applied.revision };
}

function handle(request: WorkerRequest): WorkerResult<WorkerMethod> {
	const { method, args } = request;
	switch (method) {
		case "listState": {
			const values: Record<string, string> = {};
			const rows = prepare("SELECT key,text FROM state WHERE run_id IS NULL").all() as StateRow[];
			for (const row of rows) {
				if (row.key.startsWith(args.prefix) && row.text !== null) {
					values[row.key] = row.text;
				}
			}
			return values;
		}
		case "read": {
			const row = prepare("SELECT text,run_id FROM state WHERE key=?").get(args.key) as
				| StateRow
				| undefined;
			return { text: row?.text ?? null, ...(row?.run_id ? { runId: row.run_id } : {}) };
		}
		case "write":
			return transaction(() => {
				const row = prepare("SELECT run_id FROM state WHERE key=?").get(args.key) as
					| Pick<StateRow, "run_id">
					| undefined;
				const runId = row?.run_id ?? args.runId;
				if (runId) {
					const result = transition({
						operationId: args.operationId,
						runId,
						action: {
							kind: "state",
							key: args.key,
							text: args.text,
							expectedWaitRevision: args.expectedWaitRevision,
							...(args.decision ? { decision: args.decision } : {}),
						},
					});
					if (result.status === "conflict") {
						throw new WaitConflictError();
					}
					return {
						kind: "run",
						runId,
						operationId: args.operationId,
						revision: result.revision,
					} satisfies WorkerResult<"write">;
				}

				if (args.exclusive && prepare("SELECT key FROM state WHERE key=?").get(args.key)) {
					throw Object.assign(new Error("State already exists"), { code: "EEXIST" });
				}
				if (args.text === null) {
					prepare("DELETE FROM state WHERE key=?").run(args.key);
				} else {
					prepare(
						"INSERT INTO state(key,text) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET text=excluded.text",
					).run(args.key, args.text);
				}
				return { kind: "state" } satisfies WorkerResult<"write">;
			});
		case "transition":
			return transaction(() => transition(args));
		case "acknowledge":
			return transaction(() => {
				const row = prepare("SELECT record FROM receipts WHERE run_id=? AND operation_id=?").get(
					args.runId,
					args.operationId,
				) as RecordRow | undefined;
				if (row) {
					const receipt = JSON.parse(row.record) as OperationReceipt;
					receipt.acknowledged = true;
					prepare("UPDATE receipts SET record=? WHERE run_id=? AND operation_id=?").run(
						JSON.stringify(receipt),
						args.runId,
						args.operationId,
					);
				}
			});
		case "getRun":
			return getRun(args.id);
		case "getRunState":
			return transaction(() => {
				const run = metadata(args.id);
				if (!run) return null;
				for (const row of prepare("SELECT key,record FROM waits WHERE run_id=?").all(
					args.id,
				) as (RecordRow & { key: string })[]) {
					run.waits[row.key] = JSON.parse(row.record) as WaitRecord;
				}
				for (const row of prepare("SELECT key,text FROM state WHERE run_id=?").all(
					args.id,
				) as StateRow[]) {
					run.states[row.key] = row.text;
				}
				const { attempts: _attempts, events: _events, receipts: _receipts, ...state } = run;
				return state;
			}, false);
		case "listRunSummaries": {
			const filter = args.filter ?? {};
			const rows = prepare(`SELECT record FROM runs WHERE id > ?
				AND (? IS NULL OR json_extract(record, '$.status') = ?)
				AND (? IS NULL OR json_extract(record, '$.metadata.callerId') = ?)
				ORDER BY id LIMIT ?`).all(
				args.after,
				filter.status ?? null,
				filter.status ?? null,
				filter.callerId ?? null,
				filter.callerId ?? null,
				args.limit,
			) as RecordRow[];
			return rows.map((row) => runSummary(JSON.parse(row.record) as RunMetadataRecord));
		}
		case "inspectRun":
			return transaction(() => {
				const run = metadata(args.id);
				if (!run) {
					return null;
				}

				const rows = prepare(
					"SELECT record FROM events WHERE run_id=? AND sequence>? ORDER BY sequence LIMIT ?",
				).all(args.id, args.afterSequence, args.limit) as RecordRow[];
				run.events = rows.map((row) => JSON.parse(row.record) as RunEvent);
				for (const event of run.events) {
					const payload = event.payload;
					if (payload?.id) {
						const row = prepare("SELECT record FROM attempts WHERE run_id=? AND id=?").get(
							args.id,
							payload.id,
						) as RecordRow | undefined;
						if (row && !run.attempts.some((a) => a.id === payload.id)) {
							run.attempts.push(JSON.parse(row.record) as Attempt);
						}
					}
					for (const key of [payload?.key, payload?.previousKey]) {
						if (!key) continue;
						const row = prepare("SELECT record FROM waits WHERE run_id=? AND key=?").get(
							args.id,
							key,
						) as RecordRow | undefined;
						if (row) {
							run.waits[key] = JSON.parse(row.record) as WaitRecord;
						}
					}
				}
				return historyPage(run, args.afterSequence, args.limit);
			}, false);
		case "listRuns":
			return listRuns();
		case "export":
			return transaction(() => {
				const states: Record<string, string> = {};
				const rows = prepare(
					"SELECT key,text FROM state WHERE run_id IS NULL ORDER BY key",
				).all() as StateRow[];
				for (const row of rows) {
					if (row.text !== null) {
						states[row.key] = row.text;
					}
				}
				return { version: 1, states, runs: listRuns() } satisfies LogicalArchive;
			}, false);
		case "import":
			return transaction(() => {
				validateArchive(args);
				if (
					prepare("SELECT key FROM state LIMIT 1").get() ||
					prepare("SELECT id FROM runs LIMIT 1").get()
				) {
					throw new Error("Import destination must be empty");
				}
				for (const [key, text] of Object.entries(args.states)) {
					prepare("INSERT INTO state(key,text) VALUES(?,?)").run(key, text);
				}
				for (const run of args.runs) {
					saveRun(run);
				}
			});
		case "removeRun":
			return transaction(() => {
				const run = getRun(args.id);
				if (!run || !canRetain(run, args.before)) {
					return false;
				}
				prepare("DELETE FROM state WHERE run_id=?").run(args.id);
				prepare("DELETE FROM runs WHERE id=?").run(args.id);
				return true;
			});
		case "close":
			db.close();
			return;
	}
}

parentPort!.on("message", (request: WorkerRequest) => {
	try {
		const result = handle(request);
		parentPort!.postMessage({ id: request.id, result });
		if (request.method === "close") {
			parentPort!.close();
		}
	} catch (error: unknown) {
		const message = error instanceof Error ? error.message : String(error);
		const code = error && typeof error === "object" && "code" in error ? error.code : undefined;
		parentPort!.postMessage({ id: request.id, error: { message, code } });
	}
});

parentPort!.postMessage({ ready: true });
