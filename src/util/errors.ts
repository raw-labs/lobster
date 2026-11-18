/** The message of a thrown value: an error's message, or the value as text. */
export function messageOf(error: unknown): string {
	const message = (error as { message?: unknown } | null | undefined)?.message;
	return typeof message === "string" ? message : String(error);
}
