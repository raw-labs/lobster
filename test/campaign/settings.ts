// Settings shared by the campaign tests (randomized, chaos, fuzz, load).
//
// LOBSTER_CAMPAIGN_SCALE=quick (default) runs a few cases of each, enough to keep the
// harness honest on a laptop; =nightly runs many. LOBSTER_CAMPAIGN_SEED fixes the base
// seed (default: 1 for quick, today's date for nightly), and every failure names the seed
// of its case, so `LOBSTER_CAMPAIGN_SEED=<seed> LOBSTER_CAMPAIGN_CASES=1` replays it.

export const NIGHTLY = process.env.LOBSTER_CAMPAIGN_SCALE === "nightly";

export const BASE_SEED = process.env.LOBSTER_CAMPAIGN_SEED
	? Number(process.env.LOBSTER_CAMPAIGN_SEED)
	: NIGHTLY
		? Number(new Date().toISOString().slice(0, 10).replaceAll("-", ""))
		: 1;

/** How many cases to run: `quick` for a laptop, `nightly` for CI, or the override. */
export function cases(quick: number, nightly: number) {
	const override = Number(process.env.LOBSTER_CAMPAIGN_CASES);
	return override > 0 ? override : NIGHTLY ? nightly : quick;
}

/** A small, fast, seeded generator (mulberry32). */
export function random(seed: number) {
	let state = seed | 0;
	const next = () => {
		state = (state + 0x6d2b79f5) | 0;
		let t = Math.imul(state ^ (state >>> 15), 1 | state);
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
	return {
		next,
		/** An integer in [min, max]. */
		int: (min: number, max: number) => min + Math.floor(next() * (max - min + 1)),
		pick: <T>(list: readonly T[]) => list[Math.floor(next() * list.length)],
		chance: (p: number) => next() < p,
	};
}

export type Random = ReturnType<typeof random>;
