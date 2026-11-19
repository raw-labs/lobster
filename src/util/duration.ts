const UNITS: Record<string, number> = { ms: 1, s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000 };
const MAX_MS = 366 * UNITS.d;

/** Milliseconds from a number (taken as ms) or a string like "500ms", "30s", "5m", "2h", "3d". */
export function parseDuration(value: unknown, what: string): number {
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

/** The longest delay a timer can wait; beyond it, setTimeout fires at once (about 24.8 days). */
export const MAX_TIMER_MS = 2_147_483_647;

/** Call `fire` at `until` (epoch ms), however far off: the timer re-arms past MAX_TIMER_MS. */
export function timerUntil(until: number, fire: () => void): () => void {
	let timer: ReturnType<typeof setTimeout> | undefined;
	const arm = () => {
		const left = until - Date.now();
		if (left <= 0) fire();
		else timer = setTimeout(arm, Math.min(left, MAX_TIMER_MS));
	};
	arm();
	return () => clearTimeout(timer);
}

/** Wait until `until` (epoch ms), however far off; `signal` stops the wait with its reason. */
export async function sleepUntil(until: number, signal?: AbortSignal): Promise<void> {
	signal?.throwIfAborted();
	if (Date.now() >= until) return;
	await new Promise<void>((resolve, reject) => {
		const aborted = () => {
			cancel();
			reject(signal!.reason);
		};
		const cancel = timerUntil(until, () => {
			signal?.removeEventListener("abort", aborted);
			resolve();
		});
		signal?.addEventListener("abort", aborted, { once: true });
	});
}
