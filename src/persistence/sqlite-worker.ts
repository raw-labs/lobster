// This is the only module that loads node:sqlite. Files remain usable on Node 22.0.
import { parentPort, workerData } from "node:worker_threads";
import { DatabaseSync } from "node:sqlite";
import {
	applyTransition,
	canRetain,
	runSummary,
	historyPage,
	SCHEMA_VERSION,
	validateArchive,
	type LogicalArchive,
	type RunRecord,
} from "./model.js";
const db = new DatabaseSync(workerData.path, { readOnly: workerData.readOnly });
db.exec("PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;");
const statements = new Map<string, ReturnType<DatabaseSync["prepare"]>>();
function prepare(sql: string) {
	let statement = statements.get(sql);
	if (!statement) {
		statement = db.prepare(sql);
		statements.set(sql, statement);
	}
	return statement;
}
const version = Number((prepare("PRAGMA user_version").get() as any).user_version);
if (version !== 0 && version !== SCHEMA_VERSION)
	throw new Error(`Unsupported persistence schema ${version}`);
if (!workerData.readOnly) {
	db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
 CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, text TEXT, run_id TEXT);
 CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, record TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS attempts (run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE, id TEXT NOT NULL, path TEXT NOT NULL, record TEXT NOT NULL, PRIMARY KEY(run_id,id));
 CREATE INDEX IF NOT EXISTS attempt_paths ON attempts(run_id,path);
 CREATE TABLE IF NOT EXISTS waits (run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE, key TEXT NOT NULL, record TEXT NOT NULL, PRIMARY KEY(run_id,key));
 CREATE TABLE IF NOT EXISTS events (run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE, sequence INTEGER NOT NULL, record TEXT NOT NULL, PRIMARY KEY(run_id,sequence));
 CREATE TABLE IF NOT EXISTS receipts (run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE, operation_id TEXT NOT NULL, record TEXT NOT NULL, PRIMARY KEY(run_id,operation_id));
 PRAGMA user_version=${SCHEMA_VERSION};`);
}
const metadata = (id: string): RunRecord | null => {
	const row = prepare("SELECT record FROM runs WHERE id=?").get(id) as any;
	return row
		? { ...JSON.parse(row.record), attempts: [], waits: {}, events: [], receipts: {}, states: {} }
		: null;
};
const getRun = (id: string): RunRecord | null => {
	const run = metadata(id);
	if (!run) return null;
	run.attempts = (
		prepare("SELECT record FROM attempts WHERE run_id=? ORDER BY rowid").all(id) as any[]
	).map((row) => JSON.parse(row.record));
	run.events = (
		prepare("SELECT record FROM events WHERE run_id=? ORDER BY sequence").all(id) as any[]
	).map((row) => JSON.parse(row.record));
	for (const row of prepare("SELECT key,record FROM waits WHERE run_id=?").all(id) as any[])
		run.waits[row.key] = JSON.parse(row.record);
	for (const row of db
		.prepare("SELECT operation_id,record FROM receipts WHERE run_id=?")
		.all(id) as any[])
		run.receipts[row.operation_id] = JSON.parse(row.record);
	for (const row of prepare("SELECT key,text FROM state WHERE run_id=?").all(id) as any[])
		run.states[row.key] = row.text;
	return run;
};
const listRuns = () =>
	(prepare("SELECT id FROM runs ORDER BY id").all() as any[]).map((row) => getRun(row.id)!);
function transaction(task: () => unknown, write = true) {
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
function saveRun(run: RunRecord) {
	const { attempts, waits, events, receipts, states, ...record } = run;
	prepare(
		"INSERT INTO runs(id,record) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET record=excluded.record",
	).run(run.id, JSON.stringify(record));
	for (const attempt of attempts)
		prepare(
			"INSERT INTO attempts(run_id,id,path,record) VALUES(?,?,?,?) ON CONFLICT(run_id,id) DO UPDATE SET record=excluded.record",
		).run(run.id, attempt.id, attempt.path, JSON.stringify(attempt));
	for (const [key, wait] of Object.entries(waits))
		prepare(
			"INSERT INTO waits(run_id,key,record) VALUES(?,?,?) ON CONFLICT(run_id,key) DO UPDATE SET record=excluded.record",
		).run(run.id, key, JSON.stringify(wait));
	for (const event of events)
		prepare(
			"INSERT INTO events(run_id,sequence,record) VALUES(?,?,?) ON CONFLICT(run_id,sequence) DO NOTHING",
		).run(run.id, event.sequence, JSON.stringify(event));
	for (const [id, receipt] of Object.entries(receipts))
		prepare(
			"INSERT INTO receipts(run_id,operation_id,record) VALUES(?,?,?) ON CONFLICT(run_id,operation_id) DO NOTHING",
		).run(run.id, id, JSON.stringify(receipt));
	for (const [key, text] of Object.entries(states))
		prepare(
			"INSERT INTO state(key,text,run_id) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET text=excluded.text,run_id=excluded.run_id",
		).run(key, text, run.id);
}
function transition(request: any) {
	const current = metadata(request.runId);
	if (current) {
		const receipt = db
			.prepare("SELECT record FROM receipts WHERE run_id=? AND operation_id=?")
			.get(current.id, request.operationId) as any;
		if (receipt) current.receipts[request.operationId] = JSON.parse(receipt.record);
		if (request.action.kind === "start" && !request.action.attempt.attempt) {
			const count = (
				db
					.prepare("SELECT COUNT(*) AS count FROM attempts WHERE run_id=? AND path=?")
					.get(current.id, request.action.attempt.path) as any
			).count;
			// Set the computed number only in the reducer's projection, leaving the request
			// fingerprint stable across retries.
			current.attempts = Array.from(
				{ length: count },
				() => ({ path: request.action.attempt.path }) as any,
			);
		}
		if (request.action.kind === "finish") {
			const row = db
				.prepare("SELECT record FROM attempts WHERE run_id=? AND id=?")
				.get(current.id, request.action.id) as any;
			if (row) current.attempts = [JSON.parse(row.record)];
		}
		if (request.action.kind === "state") {
			const key = request.action.key;
			const state = db
				.prepare("SELECT text FROM state WHERE run_id=? AND key=?")
				.get(current.id, key) as any;
			const wait = db
				.prepare("SELECT record FROM waits WHERE run_id=? AND key=?")
				.get(current.id, key) as any;
			if (state) current.states[key] = state.text;
			if (wait) current.waits[key] = JSON.parse(wait.record);
		}
	}
	const { run, result } = applyTransition(current, request);
	if (!result.conflict && run !== current) {
		if (request.action.kind === "start")
			run.attempts = run.attempts.filter((attempt) => attempt.id);
		saveRun(run);
	}
	return result;
}
function handle(method: string, args: any) {
	switch (method) {
		case "listState": {
			const values: Record<string, string> = {};
			for (const row of prepare("SELECT key,text FROM state WHERE run_id IS NULL").all() as any[])
				if (row.key.startsWith(args.prefix) && row.text !== null) values[row.key] = row.text;
			return values;
		}
		case "read": {
			const row = prepare("SELECT text,run_id FROM state WHERE key=?").get(args.key) as any;
			return { text: row?.text ?? null, ...(row?.run_id ? { runId: row.run_id } : {}) };
		}
		case "write":
			return transaction(() => {
				const row = prepare("SELECT run_id FROM state WHERE key=?").get(args.key) as any;
				const runId = row?.run_id ?? args.runId;
				if (runId) {
					return transition({
						operationId: args.operationId,
						runId,
						action: {
							kind: "state",
							key: args.key,
							text: args.text,
							...(args.decision ? { decision: args.decision } : {}),
						},
					});
				}
				if (args.exclusive) {
					if (prepare("SELECT key FROM state WHERE key=?").get(args.key)) {
						const error = new Error("State already exists");
						(error as any).code = "EEXIST";
						throw error;
					}
				}
				if (args.text === null) prepare("DELETE FROM state WHERE key=?").run(args.key);
				else
					prepare(
						"INSERT INTO state(key,text) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET text=excluded.text",
					).run(args.key, args.text);
				return {};
			});
		case "transition":
			return transaction(() => {
				return transition(args);
			});
		case "getRun":
			return getRun(args.id);
		case "listRunSummaries":
			return (
				prepare("SELECT record FROM runs WHERE id > ? ORDER BY id LIMIT ?").all(
					args.after,
					args.limit,
				) as any[]
			).map((row) => runSummary(JSON.parse(row.record)));
		case "inspectRun":
			return transaction(() => {
				const run = metadata(args.id);
				if (!run) return null;
				run.events = (
					prepare(
						"SELECT record FROM events WHERE run_id=? AND sequence>? ORDER BY sequence LIMIT ?",
					).all(args.id, args.afterSequence, args.limit) as any[]
				).map((row) => JSON.parse(row.record));
				for (const event of run.events) {
					const payload = event.payload as any;
					if (payload?.id) {
						const row = prepare("SELECT record FROM attempts WHERE run_id=? AND id=?").get(
							args.id,
							payload.id,
						) as any;
						if (row && !run.attempts.some((a) => a.id === payload.id))
							run.attempts.push(JSON.parse(row.record));
					}
					if (payload?.key) {
						const row = prepare("SELECT record FROM waits WHERE run_id=? AND key=?").get(
							args.id,
							payload.key,
						) as any;
						if (row) run.waits[payload.key] = JSON.parse(row.record);
					}
				}
				return historyPage(run, args.afterSequence, args.limit);
			}, false);
		case "listRuns":
			return listRuns();
		case "export":
			return transaction(() => {
				const states: Record<string, string> = {};
				for (const row of db
					.prepare("SELECT key,text FROM state WHERE run_id IS NULL ORDER BY key")
					.all() as any[]) {
					if (row.text !== null) states[row.key] = row.text;
				}
				return { version: 1, states, runs: listRuns() };
			}, false);
		case "import":
			return transaction(() => {
				const data = args as LogicalArchive;
				validateArchive(data);
				if (
					prepare("SELECT key FROM state LIMIT 1").get() ||
					prepare("SELECT id FROM runs LIMIT 1").get()
				)
					throw new Error("Import destination must be empty");
				for (const [key, text] of Object.entries(data.states))
					prepare("INSERT INTO state(key,text) VALUES(?,?)").run(key, text);
				for (const run of data.runs) saveRun(run);
			});
		case "removeRun":
			return transaction(() => {
				const run = getRun(args.id);
				if (!run || !canRetain(run, args.before)) return false;
				prepare("DELETE FROM state WHERE run_id=?").run(args.id);
				prepare("DELETE FROM runs WHERE id=?").run(args.id);
				return true;
			});
		case "close":
			db.close();
			return;
		default:
			throw new Error("Unknown persistence operation");
	}
}
parentPort!.on("message", ({ id, method, args }) => {
	try {
		const result = handle(method, args);
		parentPort!.postMessage({ id, result });
		if (method === "close") parentPort!.close();
	} catch (error) {
		parentPort!.postMessage({ id, error: { message: error.message, code: error.code } });
	}
});
parentPort!.postMessage({ ready: true });
