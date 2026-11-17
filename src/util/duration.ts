const UNITS: Record<string, number> = { ms: 1, s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000 };
const MAX_MS = 366 * UNITS.d;

/** Milliseconds from a number (taken as ms) or a string like "500ms", "30s", "5m", "2h", "3d". */
export function parseDuration(value: unknown, what = "duration"): number {
	const ms =
		typeof value === "number"
			? value
			: typeof value === "string" && /^\d+(\.\d+)?(ms|s|m|h|d)$/.test(value.trim())
				? Number(value.trim().replace(/(ms|s|m|h|d)$/, "")) *
					UNITS[value.trim().match(/(ms|s|m|h|d)$/)![1]]
				: Number.NaN;
	if (!Number.isFinite(ms) || ms < 0 || ms > MAX_MS)
		throw new Error(
			`${what} must be a duration such as 500ms, 30s, 5m, 2h or 3d (at most 366d), got ${JSON.stringify(value)}`,
		);
	return Math.round(ms);
}
