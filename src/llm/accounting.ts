import { billableTokens } from "./cost_tracker.js";

// Only in-process objects carry this symbol. JSON fields cannot grant a replay exemption.
const LLM_PROVENANCE = Symbol("lobster.llm.provenance");
// Object spreads share this private identity; WeakSet tracking also works for frozen items.
const replayedCalls = new WeakSet<object>();

export type LlmProvenance = { cacheKey: string; replayed: boolean };

/**
 * Stamps provenance on a value this module is emitting. The property is enumerable so a
 * downstream `{ ...value }` keeps it, and `JSON.stringify` ignores symbol keys, so serialized
 * output — including what is written to the cache — is unchanged.
 */
function markProvenance<T extends object>(value: T, provenance: LlmProvenance): T {
	return Object.defineProperty(value, LLM_PROVENANCE, {
		value: provenance,
		enumerable: true,
		configurable: true,
	});
}

/** Mark both the item and usage so projections such as `pick model,usage` preserve provenance. */
export function markLlmItem<T extends { usage?: Record<string, unknown> | null }>(
	item: T,
	provenance: LlmProvenance,
): T {
	if (item.usage && typeof item.usage === "object") {
		markProvenance(item.usage as object, provenance);
	}
	return markProvenance(item, provenance);
}

/** Public cache keys and `replayed` fields are not trusted as billing provenance. */
export function llmProvenanceOf(value: unknown): LlmProvenance | null {
	if (!value || typeof value !== "object") return null;
	const provenance = (value as Record<symbol, unknown>)[LLM_PROVENANCE];
	if (!provenance || typeof provenance !== "object") return null;
	const { cacheKey, replayed } = provenance as LlmProvenance;
	if (typeof cacheKey !== "string" || typeof replayed !== "boolean") return null;
	return { cacheKey, replayed: replayed || replayedCalls.has(provenance) };
}

/** A billed live result becomes a replay wherever that same call identity is re-emitted. */
export function markLlmProvenanceReplayed(value: unknown) {
	if (!llmProvenanceOf(value)) return;
	replayedCalls.add((value as Record<symbol, object>)[LLM_PROVENANCE]);
}

/** Carry provenance only across a JSON round trip whose source is still held in-process. */
export function carryLlmProvenance(source: unknown, target: unknown) {
	if (!source || typeof source !== "object" || !target || typeof target !== "object") return;
	const provenance = llmProvenanceOf(source);
	if (provenance)
		markProvenance(target as object, { cacheKey: provenance.cacheKey, replayed: true });
	if (Array.isArray(source) || Array.isArray(target)) {
		if (!Array.isArray(source) || !Array.isArray(target)) return;
		for (let index = 0; index < Math.min(source.length, target.length); index++) {
			carryLlmProvenance(source[index], target[index]);
		}
		return;
	}
	for (const key of Object.keys(source)) {
		carryLlmProvenance(
			(source as Record<string, unknown>)[key],
			(target as Record<string, unknown>)[key],
		);
	}
}

/** Where the marks of model calls sit in a value: the path of keys to each marked object. */
export type LlmMarks = Array<[path: Array<string | number>, mark: LlmProvenance]>;

/**
 * The marks of model calls in `value`, to save with it: JSON drops them. Read from the
 * in-process marks only, so nothing a command prints can add one.
 */
export function llmMarksOf(value: unknown): LlmMarks {
	const marks: LlmMarks = [];
	const visit = (node: unknown, path: Array<string | number>) => {
		if (!node || typeof node !== "object") return;
		const mark = llmProvenanceOf(node);
		if (mark) marks.push([path, mark]);
		if (Array.isArray(node)) node.forEach((item, index) => visit(item, [...path, index]));
		else for (const [key, child] of Object.entries(node)) visit(child, [...path, key]);
	};
	visit(value, []);
	return marks;
}

/**
 * Put `marks` (llmMarksOf) back on `value`, read back from Lobster's own saved state: exactly
 * where they were, through its own keys only, and nowhere else. A saved live mark stays live
 * only where `mayBeLive` says its call may still be unbilled: a value saved before its step was
 * billed is a replay once the step is.
 */
export function restoreLlmMarks(
	value: unknown,
	marks: LlmMarks | undefined,
	mayBeLive: (path: Array<string | number>, cacheKey: string) => boolean,
) {
	for (const [path, { cacheKey, replayed }] of marks ?? []) {
		let node: unknown = value;
		for (const key of path)
			node =
				node && typeof node === "object" && Object.hasOwn(node, key)
					? (node as Record<string, unknown>)[key]
					: undefined;
		if (node && typeof node === "object")
			markProvenance(node, { cacheKey, replayed: replayed || !mayBeLive(path, cacheKey) });
	}
}

// Track provider charges separately from output: failed or suspended steps can lose their items.
export type LlmSpendLedger = {
	record: (cacheKey: string, charge?: LlmChargeCost) => void;
	claim: (cacheKey: string, cost?: LlmChargeCost) => LlmChargeCost | null;
	settleCopy: (
		cacheKey: string | null,
		model: string | null,
		usage: Record<string, unknown>,
	) => void;
	outstanding: () => LlmOutstandingCharge[];
	/** Whether a charge under `cacheKey` is still open (made, not yet billed). */
	isOpen: (cacheKey: string) => boolean;
	restore: (charges: readonly LlmOutstandingCharge[] | undefined) => void;
};

// Retain cost even if a renderer, input gate, or composed workflow consumes the item.
export type LlmChargeCost = {
	stepId?: string;
	model?: string | null;
	usage?: Record<string, unknown>;
};

// A charge a run has opened and not yet billed. Carried in the run's own resume state so a
// workflow that pauses mid-pipeline keeps it, and only that workflow can settle it.
export type LlmOutstandingCharge = { cacheKey: string; count: number } & LlmChargeCost;

// A call nothing ever bills — a step that failed outright — leaves its key behind, so the
// oldest are dropped to keep a long run's ledger bounded.
const MAX_UNBILLED_LIVE_INVOCATIONS = 256;

/** A run's ledger: each charge is settled once. */
export function createLlmSpendLedger(): LlmSpendLedger {
	// Outstanding charges per cache key, held one entry per call rather than as a count: two
	// identical calls that race on a cold cache are two provider charges under one key, the
	// replays that later stand in for them have to be able to settle both, and each carries the
	// cost of the call that opened it.
	const unbilled = new Map<string, LlmChargeCost[]>();
	return {
		record(cacheKey: string, charge?: LlmChargeCost) {
			if (!cacheKey) return;
			const open = unbilled.get(cacheKey) ?? [];
			open.push({ ...charge });
			unbilled.set(cacheKey, open);
			for (const oldest of unbilled.keys()) {
				if (unbilled.size <= MAX_UNBILLED_LIVE_INVOCATIONS) break;
				unbilled.delete(oldest);
			}
		},
		/**
		 * Settles one outstanding charge and hands back what that call cost, or null for the
		 * caller that must not bill anything. A live item and every replay of it draw on the same
		 * charges, so a provider call is billed exactly once however many steps re-emit its
		 * answer — and N calls under one key can be billed N times, never fewer.
		 *
		 * The cost comes back because a replay is not a reliable witness of it: identical calls
		 * that raced on a cold cache each paid their own way, and every replay of them carries
		 * whichever single answer was stored.
		 */
		claim(cacheKey: string, cost?: LlmChargeCost) {
			const open = unbilled.get(cacheKey);
			if (!open?.length) return null;
			// A caller that knows what its own call cost settles that call's charge. A step
			// retried after an attempt that failed *after* paying has more than one charge under
			// the key, and they did not cost the same: settling the wrong one leaves the other to
			// be billed at this call's price instead of its own.
			const own = cost
				? open.findIndex(
						(charge) =>
							(charge.model ?? null) === (cost.model ?? null) &&
							sameBillableUsage(charge.usage, cost.usage),
					)
				: -1;
			// Otherwise: a charge that records no cost settles nothing anyone can bill, so it
			// must not be the one handed to a caller with a real call to account for. Charges
			// restored from resume state written before they carried a cost are the ones this
			// can be.
			const index =
				own >= 0
					? own
					: Math.max(
							open.findIndex((charge) => charge.usage !== undefined),
							0,
						);
			const [charge] = open.splice(index, 1);
			if (!open.length) unbilled.delete(cacheKey);
			return charge ?? {};
		},
		/**
		 * An item carrying a call's numbers but not its in-process mark is always billed: once
		 * no charge is open, public fields can never suppress usage, as only the private
		 * provenance symbol can prove an item is a replay. A matching open charge is settled
		 * here, so end-of-step cleanup does not bill the same provider call again.
		 */
		settleCopy(cacheKey: string | null, model: string | null, usage: Record<string, unknown>) {
			const isSameCall = (charge: LlmChargeCost) =>
				(charge.model ?? null) === model && sameBillableUsage(charge.usage, usage);
			// A copy that still names a cache key is read against that key alone: the key is
			// evidence of which call it came from, and honoring it keeps one call's copy from
			// settling another call's charge. A transform can emit `{ model, usage }` and drop
			// the key with the symbols, leaving the cost as the only evidence there is.
			for (const key of cacheKey === null ? [...unbilled.keys()] : [cacheKey]) {
				const open = unbilled.get(key);
				const index = open?.findIndex(isSameCall) ?? -1;
				if (!open || index < 0) continue;
				open.splice(index, 1);
				if (!open.length) unbilled.delete(key);
				return;
			}
		},
		isOpen(cacheKey: string) {
			return unbilled.has(cacheKey);
		},
		outstanding() {
			const charges: LlmOutstandingCharge[] = [];
			for (const [cacheKey, open] of unbilled) {
				for (const charge of open) charges.push({ cacheKey, count: 1, ...charge });
			}
			return charges;
		},
		/**
		 * Reopens charges a paused run had not billed. A pipeline can suspend mid-step — at an
		 * `ask` gate — after its LLM call has been paid for but before the step succeeded, so
		 * without this the charge would exist in no run: the paused one never billed it, and the
		 * resumed one would exempt the replay that stands in for it.
		 */
		restore(charges: readonly LlmOutstandingCharge[] | undefined) {
			if (!Array.isArray(charges)) return;
			for (const charge of charges) {
				if (!charge || typeof charge !== "object") continue;
				if (typeof charge.cacheKey !== "string" || !charge.cacheKey) continue;
				const count = Math.floor(Number(charge.count ?? 0));
				if (!Number.isFinite(count) || count < 1) continue;
				// State written before this field existed carries no cost, and a hand-edited file
				// should not be able to invent one: only a plain object of numbers is taken.
				const { cacheKey, count: _count, ...cost } = charge;
				const restored = sanitizeChargeCost(cost);
				for (let i = 0; i < count; i++) this.record(cacheKey, restored);
			}
		},
	};
}

/** Compare billable token counts, ignoring provenance and unbilled response metadata. */
function sameBillableUsage(left: unknown, right: unknown) {
	if (!left || typeof left !== "object" || !right || typeof right !== "object") return false;
	const billed = billableTokens(left as Record<string, unknown>);
	const other = billableTokens(right as Record<string, unknown>);
	return billed.inputTokens === other.inputTokens && billed.outputTokens === other.outputTokens;
}

function sanitizeChargeCost(cost: Record<string, unknown>): LlmChargeCost {
	const usage = cost.usage;
	if (!usage || typeof usage !== "object" || Array.isArray(usage)) return {};
	const numbers: Record<string, unknown> = {};
	for (const [key, value] of Object.entries(usage)) {
		if (typeof value === "number" && Number.isFinite(value)) numbers[key] = value;
	}
	if (!Object.keys(numbers).length) return {};
	return {
		...(typeof cost.stepId === "string" ? { stepId: cost.stepId } : null),
		model: typeof cost.model === "string" ? cost.model : null,
		usage: numbers,
	};
}
