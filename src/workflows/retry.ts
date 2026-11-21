// A step's `retry:` setting, and how long to wait before each attempt.
export type RetryConfig = {
	max?: number;
	backoff?: "fixed" | "exponential";
	delay_ms?: number;
	max_delay_ms?: number;
	jitter?: boolean;
};

const DEFAULTS = {
	max: 1,
	backoff: "fixed" as const,
	delay_ms: 1000,
	max_delay_ms: 30000,
	jitter: false,
};

export function resolveRetryConfig(raw: RetryConfig | undefined): Required<RetryConfig> {
	if (!raw) return { ...DEFAULTS };
	return {
		max: raw.max ?? DEFAULTS.max,
		backoff: raw.backoff ?? DEFAULTS.backoff,
		delay_ms: raw.delay_ms ?? DEFAULTS.delay_ms,
		max_delay_ms: raw.max_delay_ms ?? DEFAULTS.max_delay_ms,
		jitter: raw.jitter ?? DEFAULTS.jitter,
	};
}

export function computeDelay(config: Required<RetryConfig>, attempt: number): number {
	let delay: number;
	if (config.backoff === "exponential") {
		delay = Math.min(config.delay_ms * Math.pow(2, attempt), config.max_delay_ms);
	} else {
		delay = config.delay_ms;
	}
	if (config.jitter) {
		// +/- 10% randomization, clamped to max_delay_ms
		const jitterRange = delay * 0.1;
		delay += (Math.random() * 2 - 1) * jitterRange;
		delay = Math.min(delay, config.max_delay_ms);
	}
	return Math.max(0, Math.round(delay));
}
