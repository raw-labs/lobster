import { CostTracker } from "../core/cost_tracker.js";
import { llmProvenanceOf, markLlmProvenanceReplayed } from "../core/llm_accounting.js";
import type { LlmChargeCost, LlmSpendLedger } from "../core/llm_accounting.js";
import type { WorkflowStepResult } from "../workflows/types.js";

const PIPELINE_SOURCE_ITEMS = Symbol("lobster.workflow.pipelineSourceItems");

export function attachPipelineSourceItems<T extends object>(target: T, items: unknown[]) {
	Object.defineProperty(target, PIPELINE_SOURCE_ITEMS, {
		value: items,
		enumerable: false,
		configurable: true,
	});
	return target;
}

export function pipelineSourceItems(target: object | undefined): unknown[] | undefined {
	if (!target) return undefined;
	const items = (target as Record<symbol, unknown>)[PIPELINE_SOURCE_ITEMS];
	return Array.isArray(items) ? items : undefined;
}

// An item this step held back: it may only be read against the ledger once every marked live
// result accounted alongside it has settled the charge it opened. A `replay` carries the mark of
// the call it stands in for; a `copy` lost the mark on its way through a stage that rebuilt it,
// and sometimes the cache key with it.
export type DeferredCharge = {
	kind: "replay" | "copy";
	stepId: string;
	cacheKey: string | null;
	model: string | null;
	usage: Record<string, unknown>;
};

export type BranchSpendBuffer = LlmSpendLedger & { release: () => void };

/**
 * One `wait: "any"` branch's view of the run ledger. The race returns as soon as a branch
 * answers and leaves the others running, so a losing branch can pay a provider long after the
 * step settled. Writing that charge straight to the run ledger makes the workflow's own total
 * depend on how long the run happened to live afterwards: land before the run ends and the
 * discarded branch's spend is billed, land after and it is billed nowhere.
 *
 * So a branch opens its charges here instead, and only the winner releases them into the run.
 * The output of a losing branch is thrown away; its accounting goes with it. Reads pass
 * through, because they settle charges the run opened before the race and are made by the
 * workflow after a branch has returned, never from inside one.
 */
export function branchSpendBuffer(ledger: LlmSpendLedger): BranchSpendBuffer {
	const held: Array<{ cacheKey: string; charge?: LlmChargeCost }> = [];
	let released = false;
	return {
		record: (cacheKey, charge) => {
			if (released) ledger.record(cacheKey, charge);
			else held.push({ cacheKey, charge });
		},
		claim: (cacheKey, cost) => ledger.claim(cacheKey, cost),
		billCopy: (cacheKey, model, usage) => ledger.billCopy(cacheKey, model, usage),
		outstanding: () => ledger.outstanding(),
		restore: (charges) => ledger.restore(charges),
		settled: () => ledger.settled(),
		restoreSettled: (charges) => ledger.restoreSettled(charges),
		release: () => {
			if (released) return;
			released = true;
			for (const { cacheKey, charge } of held) ledger.record(cacheKey, charge);
			held.length = 0;
		},
	};
}

/**
 * The run ledger seen by one step, so a charge opened inside it knows where it came from. A
 * composed run's charges reach the composing run through this too, which is why the step id is
 * overwritten rather than kept: in the parent's totals the spend belongs to the `workflow:`
 * step, not to whatever the child called its own.
 */
export function stepScopedLedger(ledger: LlmSpendLedger, stepId: string): LlmSpendLedger {
	return {
		record: (cacheKey, charge) => ledger.record(cacheKey, { ...charge, stepId }),
		claim: (cacheKey, cost) => ledger.claim(cacheKey, cost),
		billCopy: (cacheKey, model, usage) => ledger.billCopy(cacheKey, model, usage),
		outstanding: () => ledger.outstanding(),
		restore: (charges) => ledger.restore(charges),
		settled: () => ledger.settled(),
		restoreSettled: (charges) => ledger.restoreSettled(charges),
	};
}

/**
 * Bills the calls that ended the run unclaimed. An item is the usual carrier of usage, but a
 * renderer can consume the pipeline before a later stage emits items of its own, an `ask` gate
 * can swallow the only item, and a composed run can end on a step that carries none. The
 * charge outlives all three, and dropping it would leave real spend out of `_meta.cost` and
 * out of `cost_limit`.
 */
export function settleUnbilledCharges(costTracker: CostTracker, llmSpendLedger: LlmSpendLedger) {
	for (const charge of llmSpendLedger.outstanding()) {
		if (!charge.usage) continue;
		if (!llmSpendLedger.claim(charge.cacheKey)) continue;
		costTracker.recordUsage(charge.stepId ?? "llm", charge.model ?? null, charge.usage);
	}
}

export function trackStepCost(
	costTracker: CostTracker,
	stepId: string,
	result: WorkflowStepResult,
	llmSpendLedger: LlmSpendLedger,
	deferredCharges?: DeferredCharge[],
) {
	// Bill the items the pipeline produced rather than the JSON re-read from its stdout: a
	// renderer serializes them, and replay provenance does not survive that round trip. Steps
	// without a pipeline (shell commands) have no such items and are read from `json` as before.
	const json = pipelineSourceItems(result) ?? result.json;
	if (!json || typeof json !== "object") return;

	// Replays wait for the live results accounted with them. Branches of one parallel step are
	// billed in declaration order, so a cached replay declared before an identical `--refresh`
	// branch would otherwise claim the charge that branch had just opened — and the live branch,
	// billed whatever its own claim returns, would be billed for it too. One provider call, two
	// charges, and a `cost_limit` tripped on spend that never happened.
	const deferred = deferredCharges ?? [];
	const items = Array.isArray(json) ? json : [json];
	for (const item of items) {
		if (!item || typeof item !== "object") continue;
		const record = item as Record<string, unknown>;
		const usage = record.usage;
		if (!usage || typeof usage !== "object") continue;
		const modelValue = record.model;
		const model = typeof modelValue === "string" ? modelValue : null;
		// Provenance the LLM commands attach in-process, never a field in the JSON: a step that
		// prints a replay-shaped object stays billed, so `cost_limit` cannot be evaded. A
		// projection can drop the item's own mark while carrying its usage record across, so the
		// record is read too — both marks are symbol keys built in this process.
		const provenance = llmProvenanceOf(record) ?? llmProvenanceOf(usage);
		if (provenance) {
			if (provenance.replayed) {
				deferred.push({
					kind: "replay",
					stepId,
					cacheKey: provenance.cacheKey,
					model,
					usage: usage as Record<string, unknown>,
				});
				continue;
			}
			// Settles the charge this live call opened -- the one that cost what this item cost,
			// not merely the first under the key. A step retried after an attempt that failed
			// after paying leaves more than one charge open, and they did not cost the same.
			const settled = llmSpendLedger.claim(provenance.cacheKey, {
				model,
				usage: usage as Record<string, unknown>,
			});
			// Bill what the provider was really asked for, not what the item says by the time it
			// gets here. A pipeline can drop the model on the way -- `| pick usage` hands on the
			// marked usage record without it -- or rewrite it, and the step is then priced at the
			// wrong rate or at nothing at all while `cost_limit` reads the difference as room left.
			// The charge the call opened is the same witness a replay is already billed from. A
			// charge restored from resume state written before it carried a cost falls back.
			if (settled?.usage) {
				costTracker.recordUsage(stepId, settled.model ?? null, settled.usage);
				markLlmProvenanceReplayed(record);
				markLlmProvenanceReplayed(usage);
				continue;
			}
		} else {
			// No mark, but numbers: a stage that JSON round-trips the stream hands on a copy that
			// kept `model` and `usage` and lost everything this process attached -- sometimes the
			// cache key with it. Held back like a replay, and for the same reason: a copy that
			// lost its key is read by cost alone, and until the marked results have settled their
			// own charges an identical parallel call's charge is still open to be mistaken for
			// the one this copy came from.
			deferred.push({
				kind: "copy",
				stepId,
				cacheKey: typeof record.cacheKey === "string" && record.cacheKey ? record.cacheKey : null,
				model,
				usage: usage as Record<string, unknown>,
			});
			continue;
		}
		costTracker.recordUsage(stepId, model, usage as Record<string, unknown>);
		markLlmProvenanceReplayed(record);
		markLlmProvenanceReplayed(usage);
	}

	if (!deferredCharges) settleDeferredCosts(costTracker, deferred, llmSpendLedger);
}

/**
 * Bills what a step held back, now that every marked live result accounted alongside them has
 * settled its own charge. Replays go first: one carries the mark of the call it stands in for,
 * which is better evidence than the cost a copy carries on its own.
 *
 * A replay carries the usage of the call that produced it, so counting it again charges a run
 * for tokens no provider was asked to spend. It is billed only when the call it replays is
 * still unpaid for: a step is costed once it succeeds, so a step that failed after its LLM
 * call and was retried leaves its real spend riding on the replay the retry produced.
 *
 * A copy is billed either way — it is a record of a call, and nothing in a JSON payload can be
 * trusted to say otherwise — but the ledger still decides whether it settles a charge, so the
 * step settlement does not add a second record of the same call.
 */
export function settleDeferredCosts(
	costTracker: CostTracker,
	deferred: DeferredCharge[],
	llmSpendLedger: LlmSpendLedger,
) {
	for (const replay of deferred) {
		if (replay.kind !== "replay" || replay.cacheKey === null) continue;
		const settled = llmSpendLedger.claim(replay.cacheKey);
		if (!settled) continue;
		// Bill what the call this settles actually cost, not what the replay carries. Identical
		// calls that raced on a cold cache are separate charges under one key, and every replay
		// of them repeats whichever single answer was stored -- so costing each replay by its
		// own copy reports all of those calls at the price of the one that won the cache write.
		// A charge restored from resume state written before it carried a cost falls back.
		const cost = typeof settled === "object" && settled.usage ? settled : null;
		costTracker.recordUsage(
			replay.stepId,
			cost ? (cost.model ?? null) : replay.model,
			cost?.usage ?? replay.usage,
		);
	}
	for (const copy of deferred) {
		if (copy.kind !== "copy") continue;
		if (llmSpendLedger.billCopy(copy.cacheKey, copy.model, copy.usage)) {
			costTracker.recordUsage(copy.stepId, copy.model, copy.usage);
		}
	}
	deferred.length = 0;
}
