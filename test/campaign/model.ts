// Random workflows and a driver that runs them to the end, with or without interference.
//
// The generator builds a valid workflow from every node type, with seeded choices. Every
// piece of work is a scripted fake (see ../helpers/fake_command.ts) whose output is fixed
// by the script, so running a step again produces the same value: a run that is
// interrupted and recovered must end exactly like one that was not.
//
// The driver answers every question with a decision drawn from the seed and the question
// (not from when it is asked), waits out timers and delivers awaited events, so two runs
// of the same workflow meet the same decisions whatever happens in between.
import { AsyncLocalStorage } from "node:async_hooks";
import { openStoreSession } from "../../src/persistence/session.js";
import { ExecutionConflictError } from "../../src/engine/errors.js";
import { advanceWorkflowRun, runWorkflowFile } from "../../src/engine/runner.js";
import { signalEvent } from "../../src/engine/due.js";
import { decodeResumeToken } from "../../src/resume/resume.js";
import type { WorkflowStep } from "../../src/workflows/types.js";
import type { ExecutionState } from "../../src/engine/checkpoint.js";
import type { FakeBehavior } from "../helpers/fake_command.js";
import type { WorkflowFixture } from "../helpers/workflow_runner.js";
import { random, type Random } from "./settings.js";

// ---------------------------------------------------------------------------------------
// Generator

export type Generated = {
	workflow: { steps: WorkflowStep[] };
	/** Child workflow files, by name. */
	children: Record<string, { steps: WorkflowStep[] }>;
	/** The script of every fake. */
	scripts: Record<string, FakeBehavior[]>;
	/** Fakes on steps with `recovery: repeat` (they may legitimately run again after a crash). */
	repeatable: Set<string>;
	/**
	 * How many copies of each fake's step can run at once (the product of the concurrency of
	 * the loops around it): one interruption can cut that many short, and each runs again.
	 */
	inFlight: Record<string, number>;
	/** Node kinds used, for coverage reports. */
	kinds: Set<string>;
};

type Scope = {
	/** Steps whose `json` has v, ok and list (fakes), visible to conditions here. */
	values: string[];
	/** Approval steps visible here. */
	gates: string[];
	/** Inside a for_each: its item is a number. */
	item?: boolean;
	/** Where gates are refused (parallel branches and child workflows have none). */
	noGates?: boolean;
	inLoop?: boolean;
	/**
	 * Inside a for_each that runs items at once: a step that always fails may not stop the
	 * loop, since how many other items had started by then depends on timing.
	 */
	noStop?: boolean;
	/** The product of the concurrency of the for_each loops around these steps. */
	atOnce?: number;
};

type Gen = Generated & { r: Random; n: number; shell: (name: string) => string };

const KINDS = [
	["activity", 30],
	["gated activity", 8],
	["approval", 5],
	["input", 5],
	["sleep", 5],
	["wait", 5],
	["switch", 10],
	["for_each", 8],
	["repeat", 7],
	["parallel", 5],
	["child", 4],
] as const;

export function generate(
	seed: number,
	shell: (name: string) => string,
	options: { gates?: boolean } = {},
): Generated {
	const g: Gen = {
		r: random(seed),
		n: 0,
		shell,
		workflow: { steps: [] },
		children: {},
		scripts: {},
		repeatable: new Set(),
		inFlight: {},
		kinds: new Set(),
	};
	g.workflow.steps = steps(
		g,
		{ values: [], gates: [], noGates: options.gates === false },
		0,
		g.r.int(2, 6),
	);
	return g;
}

function steps(g: Gen, scope: Scope, depth: number, count: number): WorkflowStep[] {
	const list: WorkflowStep[] = [];
	const local: Scope = { ...scope, values: [...scope.values], gates: [...scope.gates] };
	for (let i = 0; i < count; i++) list.push(...step(g, local, depth));
	return list;
}

function pickKind(g: Gen, scope: Scope, depth: number) {
	const allowed = KINDS.filter(([kind]) => {
		if (scope.noGates && ["gated activity", "approval", "input", "sleep", "wait"].includes(kind))
			return false;
		if (depth >= 2 && ["switch", "for_each", "repeat", "parallel", "child"].includes(kind))
			return false;
		return true;
	});
	const total = allowed.reduce((sum, [, weight]) => sum + weight, 0);
	let roll = g.r.next() * total;
	for (const [kind, weight] of allowed) if ((roll -= weight) < 0) return kind;
	return "activity";
}

/**
 * The value a fake returns: a number, a flag and a short list of numbers; sometimes padded
 * past 1 KB, so it is kept beside the checkpoint instead of in it.
 */
function value(g: Gen) {
	return {
		v: g.r.int(0, 9),
		ok: g.r.chance(0.5),
		list: Array.from({ length: g.r.int(1, 3) }, () => g.r.int(0, 4)),
		...(g.r.chance(0.2) ? { pad: "p".repeat(g.r.int(1100, 3000)) } : {}),
	};
}

function condition(g: Gen, scope: Scope): string | undefined {
	const options: string[] = [];
	for (const id of scope.values) {
		options.push(`$${id}.json.v != null && $${id}.json.v > ${g.r.int(0, 8)}`);
		options.push(`$${id}.json.ok == true`);
		options.push(`$${id}.json.v in [${g.r.int(0, 9)}, ${g.r.int(0, 9)}, ${g.r.int(0, 9)}]`);
		options.push(`!$${id}.skipped`);
	}
	for (const id of scope.gates) options.push(`$${id}.approved`, `!$${id}.approved`);
	if (scope.item) options.push(`$item.json >= ${g.r.int(0, 4)}`);
	if (!options.length) return undefined;
	const one = () => g.r.pick(options);
	const roll = g.r.next();
	if (roll < 0.6) return one();
	if (roll < 0.8) return `${one()} && ${one()}`;
	return `(${one()}) || (${one()})`;
}

/** A fake-backed step: pipeline or shell, with a script and optional failure handling. */
function work(g: Gen, scope: Scope, prefix = "a"): WorkflowStep {
	const id = `${prefix}${++g.n}`;
	const output = value(g);
	const step: WorkflowStep = g.r.chance(0.7)
		? { id, pipeline: `fake --name ${id}` }
		: { id, run: g.shell(id) };
	let script: FakeBehavior[] = [{ output }];
	const roll = g.r.next();
	if (roll < 0.08) {
		// Always fails: on_error decides what happens.
		script = ["fail"];
		step.on_error = scope.noStop
			? g.r.pick(["continue", "skip_rest"] as const)
			: g.r.pick(["stop", "continue", "skip_rest"] as const);
	} else if (roll < 0.16 && !scope.inLoop) {
		// Fails once, then a retry succeeds.
		script = ["fail", { output }];
		step.retry = { max: 2, delay_ms: 0 };
	}
	if (g.r.chance(0.5)) {
		step.recovery = "repeat";
		g.repeatable.add(id);
	}
	g.inFlight[id] = scope.atOnce ?? 1;
	g.scripts[id] = script;
	return step;
}

function step(g: Gen, scope: Scope, depth: number): WorkflowStep[] {
	const kind = pickKind(g, scope, depth);
	g.kinds.add(kind);
	const when = g.r.chance(0.3) ? condition(g, scope) : undefined;
	const withWhen = (s: WorkflowStep) => (when ? { ...s, when } : s);
	switch (kind) {
		case "activity": {
			const s = work(g, scope);
			if (s.on_error !== "skip_rest" && s.on_error !== "continue") scope.values.push(s.id);
			return [withWhen(s)];
		}
		case "gated activity": {
			const s = work(g, scope);
			s.approval = {
				prompt: `Approve ${s.id}?`,
				...(g.r.chance(0.6) ? { on_reject: "continue" as const } : {}),
			};
			scope.gates.push(s.id);
			return [withWhen(s)];
		}
		case "approval": {
			const id = `g${++g.n}`;
			scope.gates.push(id);
			return [
				withWhen({
					id,
					approval: {
						prompt: `Approve ${id}?`,
						...(g.r.chance(0.6) ? { on_reject: "continue" as const } : {}),
					},
				}),
			];
		}
		case "input": {
			const id = `i${++g.n}`;
			return [
				withWhen({
					id,
					input: {
						prompt: `Answer ${id}?`,
						responseSchema: {
							type: "object",
							properties: { ok: { type: "boolean" } },
							required: ["ok"],
						},
					},
				}),
			];
		}
		case "sleep":
			return [withWhen({ id: `s${++g.n}`, sleep: `${g.r.int(5, 30)}ms` })];
		case "wait": {
			const id = `w${++g.n}`;
			return [withWhen({ id, wait: { event: `ev.${id}`, match: { k: id } } })];
		}
		case "switch": {
			const id = `sw${++g.n}`;
			const cases = Array.from({ length: g.r.int(1, 3) }, () => ({
				when: condition(g, scope) ?? g.r.chance(0.5),
				steps: steps(g, scope, depth + 1, g.r.int(1, 2)),
			}));
			const s: WorkflowStep = {
				id,
				switch: {
					cases,
					...(g.r.chance(0.7) ? { else: steps(g, scope, depth + 1, g.r.int(0, 2)) } : {}),
				},
			};
			return [withWhen(s)];
		}
		case "for_each": {
			const source = work(g, scope, "src");
			delete source.on_error;
			delete source.retry;
			g.scripts[source.id] = [{ output: value(g) }];
			const id = `fe${++g.n}`;
			const concurrent = g.r.chance(0.4);
			if (concurrent) g.kinds.add("for_each (concurrency)");
			const body = steps(
				g,
				{
					...scope,
					item: true,
					inLoop: true,
					...(concurrent ? { noGates: true, noStop: true } : {}),
					atOnce: (scope.atOnce ?? 1) * (concurrent ? 3 : 1),
				},
				depth + 1,
				g.r.int(1, 3),
			);
			return [
				source,
				withWhen({
					id,
					for_each: `$${source.id}.json.list`,
					...(concurrent ? { concurrency: g.r.int(2, 3) } : {}),
					steps: body,
				}),
			];
		}
		case "repeat": {
			const id = `rp${++g.n}`;
			const inner: Scope = {
				...scope,
				values: [...scope.values],
				gates: [...scope.gates],
				inLoop: true,
			};
			const first = work(g, inner);
			delete first.on_error;
			delete first.retry;
			g.scripts[first.id] = [{ output: value(g) }];
			inner.values.push(first.id);
			const body = [first, ...steps(g, inner, depth + 1, g.r.int(0, 2))];
			const max = g.r.int(1, 3);
			// Never holds (runs max rounds), or holds after the first round.
			const until = g.r.chance(0.5) ? `$${first.id}.json.v > 100` : `$${first.id}.json.v >= 0`;
			const onMax = g.r.chance(0.8) ? { on_max: "continue" as const } : {};
			return [withWhen({ id, repeat: { until, max, ...onMax }, steps: body })];
		}
		case "parallel": {
			const id = `p${++g.n}`;
			const branches = Array.from({ length: g.r.int(2, 3) }, () => {
				const b = work(g, { ...scope, noGates: true });
				g.scripts[b.id] = [{ output: value(g) }];
				return {
					id: b.id,
					...(b.pipeline ? { pipeline: b.pipeline } : { run: b.run }),
					...(b.recovery ? { recovery: b.recovery } : {}),
				};
			});
			return [withWhen({ id, parallel: { branches } })];
		}
		case "child": {
			const id = `c${++g.n}`;
			const file = `child-${id}.lobster`;
			g.children[file] = {
				steps: steps(
					g,
					{ values: [], gates: [], noGates: true, atOnce: scope.atOnce },
					depth + 1,
					g.r.int(1, 3),
				),
			};
			return [withWhen({ id, workflow: file })];
		}
	}
	return [];
}

// ---------------------------------------------------------------------------------------
// Driver

export type Outcome = {
	status: string;
	output?: unknown;
	error?: string;
	runId: string;
	/** Questions answered and events delivered, in order. */
	answered: string[];
	interruptions: number;
};

export type Interference = {
	/** Abort the invocation that makes the Nth save (counted over the whole run), like Ctrl-C. */
	abortAtSaves: number[];
	/**
	 * Crash the invocation right before its Nth save: that save and everything it would write
	 * afterwards is lost, as if the process died. Its lease lapses instead of being released.
	 */
	crashAtSaves?: number[];
	/** The Nth save fails once with a write error (a full disk, say); nothing is written. */
	failAtSaves?: number[];
	/** Answer every question a second time with its old token: it must be refused. */
	replayAnswers?: boolean;
	/** Advance due timers twice at once: exactly one may proceed. */
	raceAdvances?: boolean;
};

/** A decision for a question, from the seed and the question alone. */
function decide(seed: number, key: string) {
	let h = seed >>> 0;
	for (const char of key) h = Math.imul(h ^ char.charCodeAt(0), 2654435761) >>> 0;
	return h % 10 < 8;
}

const DEBUG = Boolean(process.env.LOBSTER_CAMPAIGN_DEBUG);
function trace(...parts: unknown[]) {
	process.stderr.write(`[drive] ${parts.join(" ")}\n`);
}
function summary(execution: any) {
	if (!execution) return "";
	const frames = Object.entries(execution.activations ?? {})
		.filter(([path]) => path.endsWith("/activity"))
		.map(([path, frame]: [string, any]) => `${path.replace(/\/attempt-1/g, "")}:${frame.phase}`);
	return `${execution.status} ${frames.join(" ")}`;
}

export async function drive(
	fx: WorkflowFixture,
	seed: number,
	interference: Interference = { abortAtSaves: [] },
): Promise<Outcome> {
	const session = await openStoreSession({ env: fx.env });
	let runId: string | undefined;
	const answered: string[] = [];
	let saves = 0;
	let interruptions = 0;
	let controller = new AbortController();
	const aborts = new Set(interference.abortAtSaves);
	const crashes = new Set(interference.crashAtSaves ?? []);
	const failures = new Set(interference.failAtSaves ?? []);
	// Which invocation a write comes from, so a crashed one (timers included) writes nothing more.
	const invocation = new AsyncLocalStorage<{ dead: boolean }>();
	const transition = session.backend.transition.bind(session.backend);
	session.backend.transition = (async (...args: any[]) => {
		const current = invocation.getStore();
		if (current?.dead) throw new Error("campaign: this invocation has crashed");
		const isSave = args[0]?.action?.kind === "execution-save";
		if (isSave) saves++;
		// The run being driven is the one created last (a run whose first save failed never
		// started any work, so it is started again).
		if (args[0]?.action?.kind === "create" && args[0].runId) runId = args[0].runId;
		if (DEBUG)
			trace(
				isSave ? `save ${saves}` : `write ${args[0]?.action?.kind}`,
				isSave ? summary(args[0].action.execution) : "",
			);
		if (isSave && current && crashes.has(saves)) {
			current.dead = true;
			interruptions++;
			controller.abort(new Error(`campaign crash before save ${saves}`));
			throw new Error(`campaign crash before save ${saves}`);
		}
		if (isSave && failures.has(saves)) {
			failures.delete(saves);
			interruptions++;
			throw new Error(`campaign write error at save ${saves}`);
		}
		const result = await (transition as any)(...args);
		if (isSave && aborts.has(saves)) {
			interruptions++;
			controller.abort(new Error(`campaign interruption at save ${saves}`));
		}
		return result;
	}) as typeof transition;
	const LEASE_MS = 200;
	const ctx = () => ({ ...(fx.ctx() as any), storeSession: session, signal: controller.signal });
	// Each question is decided once, by its prompt and how many different questions with that
	// prompt came before it; asking the same question again gets the same answer.
	const seen = new Map<string, string[]>();
	const decision = (prompt: string, identity: string) => {
		const list = seen.get(prompt) ?? [];
		if (!list.includes(identity)) list.push(identity);
		seen.set(prompt, list);
		return decide(seed, `${prompt}#${list.indexOf(identity)}`);
	};
	const attempt = async <T>(task: () => Promise<T>): Promise<T | undefined> => {
		for (let tries = 0; ; tries++) {
			controller = new AbortController();
			try {
				return await invocation.run({ dead: false }, task);
			} catch (error) {
				if (controller.signal.aborted) return undefined;
				// A failed write stops the invocation; continuing it is the operator's next move.
				if (/campaign write error/.test(errorChain(error))) return undefined;
				// A crashed invocation still holds the lease until it lapses.
				if (error instanceof ExecutionConflictError && tries < 20) {
					await new Promise((resolve) => setTimeout(resolve, LEASE_MS));
					continue;
				}
				throw error;
			}
		}
	};
	const state = async () => {
		const run = runId ? await session.getRunState(runId) : null;
		return run?.execution as ExecutionState | undefined;
	};
	try {
		let result: any = await attempt(() =>
			runWorkflowFile({ filePath: fx.dir + "/flow.lobster", ctx: ctx(), leaseMs: LEASE_MS }),
		);
		for (let steps = 0; ; steps++) {
			if (steps > 400) throw new Error("driver: too many steps");
			if (DEBUG)
				trace(
					"result",
					result ? `${result.status}` : "interrupted",
					"calls so far:",
					fx.fake.dir && "",
				);
			if (result?.runId) runId = result.runId;
			if (!result) {
				// Interrupted: continue from what was saved, as an operator would.
				const saved = await state();
				if (!saved) {
					result = await attempt(() =>
						runWorkflowFile({ filePath: fx.dir + "/flow.lobster", ctx: ctx(), leaseMs: LEASE_MS }),
					);
					continue;
				}
				if (["completed", "cancelled", "failed"].includes(saved.status))
					return finish(saved.status, saved);
				result = await attempt(() =>
					advanceWorkflowRun({ runId: runId!, ctx: ctx(), leaseMs: LEASE_MS }),
				);
				if (result === undefined) continue;
				if (result && (await state())?.status === "blocked")
					return finish("blocked", await state());
				continue;
			}
			switch (result.status) {
				case "ok":
					return {
						status: "completed",
						output: result.output,
						runId: runId!,
						answered,
						interruptions,
					};
				case "cancelled":
					return { status: "cancelled", runId: runId!, answered, interruptions };
				case "needs_approval": {
					const request = result.requiresApproval;
					const approved = decision(request.prompt, request.approvalId);
					answered.push(`${request.prompt}=${approved}`);
					const resume = decodeResumeToken(request.resumeToken);
					result = await attempt(() =>
						runWorkflowFile({ resume, approved, ctx: ctx(), leaseMs: LEASE_MS } as any),
					);
					if (interference.replayAnswers) await refusesReplay({ resume, approved }, ctx());
					break;
				}
				case "needs_input": {
					const request = result.requiresInput;
					const resume = decodeResumeToken(request.resumeToken);
					const response = { ok: decision(request.prompt, String(resume.stateKey)) };
					answered.push(`${request.prompt}=${response.ok}`);
					result = await attempt(() =>
						runWorkflowFile({ resume, response, ctx: ctx(), leaseMs: LEASE_MS } as any),
					);
					if (interference.replayAnswers) await refusesReplay({ resume, response }, ctx());
					break;
				}
				case "waiting": {
					const waiting = result.waitingFor;
					if (waiting.kind === "timer") {
						const due = Date.parse(waiting.until) - Date.now();
						if (due > 0) await new Promise((resolve) => setTimeout(resolve, due + 2));
						if (interference.raceAdvances) {
							const both = await Promise.allSettled([
								advanceWorkflowRun({
									runId: runId!,
									ctx: { ...ctx(), signal: undefined },
									leaseMs: LEASE_MS,
								}),
								advanceWorkflowRun({
									runId: runId!,
									ctx: { ...ctx(), signal: undefined },
									leaseMs: LEASE_MS,
								}),
							]);
							// Either may fail like any advance (the run failed, a write failed, the
							// other held the lease); what matters is the run as it is afterwards.
							void both;
							// Whatever either did, look at the run as it is now (advancing a failed run
							// would retry it, which is an operator's choice, not the driver's).
							const after = await state();
							if (after && ["completed", "cancelled", "failed", "blocked"].includes(after.status))
								return finish(after.status, after);
							result = await attempt(() =>
								advanceWorkflowRun({ runId: runId!, ctx: ctx(), leaseMs: LEASE_MS }),
							);
							break;
						}
					} else {
						answered.push(`event ${waiting.event}`);
						// Delivering advances the waiting run; however that ends, look at the run after.
						await signalEvent({
							name: waiting.event,
							data: { k: waiting.event.slice(3), v: 1 },
							ctx: { ...ctx(), signal: undefined },
							leaseMs: LEASE_MS,
						}).catch(() => undefined);
					}
					const saved = await state();
					if (saved && ["completed", "cancelled", "failed", "blocked"].includes(saved.status))
						return finish(saved.status, saved);
					result = await attempt(() =>
						advanceWorkflowRun({ runId: runId!, ctx: ctx(), leaseMs: LEASE_MS }),
					);
					break;
				}
				default:
					throw new Error(`driver: unexpected status ${result.status}`);
			}
		}
	} catch (error: any) {
		const saved = await state().catch(() => undefined);
		if (saved && ["failed", "blocked", "cancelled", "completed"].includes(saved.status))
			return finish(saved.status, saved, error?.message);
		throw error;
	} finally {
		await session.close();
	}

	function finish(status: string, saved?: ExecutionState, error?: string): Outcome {
		return {
			status,
			...(status === "completed" ? { output: saved?.output } : {}),
			error: saved?.error ?? error,
			runId: runId!,
			answered,
			interruptions,
		};
	}
}

function errorChain(error: any): string {
	const parts: string[] = [];
	for (let e = error; e && parts.length < 5; e = e.cause) parts.push(String(e.message ?? e));
	return parts.join(" <- ");
}

/** Answering a question again with its old token must be refused, and change nothing. */
async function refusesReplay(answer: Record<string, unknown>, ctx: any) {
	let accepted = false;
	try {
		await runWorkflowFile({ ...answer, ctx: { ...ctx, signal: undefined } } as any);
		accepted = true;
	} catch {
		/* refused, as it must be */
	}
	if (accepted) throw new Error("an answered question accepted a second answer");
}

/** Timestamps and generated ids differ between runs; the rest of an output must not. */
export function normalize(value: unknown): unknown {
	return JSON.parse(
		JSON.stringify(value ?? null)
			.replace(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z/g, "<time>")
			.replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g, "<id>"),
	);
}
