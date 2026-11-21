/** "true"/"yes"/"y"/"1" and "false"/"no"/"n"/"0" (any case) as a boolean; anything else undefined. */
export function parseBoolLike(value: unknown): boolean | undefined {
	if (typeof value === "boolean") return value;
	if (value === undefined || value === null) return undefined;
	const normalized = String(value).trim().toLowerCase();
	if (["1", "true", "yes", "y"].includes(normalized)) return true;
	if (["0", "false", "no", "n"].includes(normalized)) return false;
	return undefined;
}
