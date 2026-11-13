export function stableStringify(value) {
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
