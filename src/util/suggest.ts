/**
 * The candidate nearest to `name`, if it is within two edits: the intended spelling of a typo.
 * Of candidates equally near, the first.
 */
export function closestMatch(name: string, candidates: readonly string[]): string | undefined {
	let best: { candidate: string; distance: number } | undefined;
	for (const candidate of candidates) {
		const distance = editDistance(name, candidate);
		if (distance <= 2 && (!best || distance < best.distance)) best = { candidate, distance };
	}
	return best?.candidate;
}

function editDistance(a: string, b: string): number {
	let previous = Array.from({ length: b.length + 1 }, (_, i) => i);
	for (let i = 1; i <= a.length; i++) {
		const current = [i];
		for (let j = 1; j <= b.length; j++) {
			current[j] = Math.min(
				previous[j] + 1,
				current[j - 1] + 1,
				previous[j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1),
			);
		}
		previous = current;
	}
	return previous[b.length];
}
