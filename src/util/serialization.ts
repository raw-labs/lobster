import { createHash } from "node:crypto";

/** JSON with object keys sorted, so equal values give equal text (for hashes and comparisons). */
export function stableStringify(value: unknown): string {
	return JSON.stringify(value, (_k, v) => {
		if (v && typeof v === "object" && !Array.isArray(v)) {
			return Object.fromEntries(
				Object.keys(v)
					.sort()
					.map((k) => [k, v[k]]),
			);
		}
		return v;
	});
}

/** A hash of `value` that equal values share whatever their key order (sha256 of stableStringify). */
export function fingerprint(value: unknown): string {
	return createHash("sha256").update(stableStringify(value)).digest("hex");
}

/**
 * Throw unless `value` can be kept in a run's checkpoint, which is cloned and written as JSON:
 * a value that cannot be would fail every later save, and leave the run stuck.
 */
export function assertSavable(value: unknown, what: string) {
	try {
		structuredClone(value);
		JSON.stringify(value);
	} catch (error) {
		throw new Error(
			`${what} cannot be saved: ${error instanceof Error ? error.message : String(error)}`,
		);
	}
}
