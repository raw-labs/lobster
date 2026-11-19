/**
 * Lobster combines the host's abort signal with its own (a lost lease, a step timeout), so a
 * command always receives a signal. When the host gave none, the process that runs Lobster owns
 * its children: on Ctrl-C they are stopped and the process exits as it would have. A combined
 * signal remembers that the host gave none, so commands still see it that way.
 */
const HOSTLESS = Symbol.for("lobster.hostless-signal");

/** Combine signals; the result is "hostless" when the host signal behind them was absent. */
export function combineSignals(
	host: AbortSignal | undefined,
	...own: (AbortSignal | undefined)[]
): AbortSignal {
	const combined = AbortSignal.any(
		[host, ...own].filter((signal): signal is AbortSignal => Boolean(signal)),
	);
	if (!host || isHostless(host)) (combined as any)[HOSTLESS] = true;
	return combined;
}

/** True when no host signal stands behind this one (or there is no signal at all). */
export function isHostless(signal: AbortSignal | undefined): boolean {
	return signal === undefined || (signal as any)[HOSTLESS] === true;
}
