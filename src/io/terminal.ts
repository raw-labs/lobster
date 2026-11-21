// Asking a person: whether one is at the terminal, how long a terminal question waits, how a
// typed answer is read, and how long a question's preview is.

/** Whether a person is at the other end of `stdin` (a terminal), so a question can be asked here. */
export function isInteractive(stdin: unknown) {
	return Boolean((stdin as NodeJS.ReadStream | undefined)?.isTTY);
}

/** How long a question asked on the terminal waits for its answer (0: no limit). */
export function approvalInputTimeoutMs(env: Record<string, string | undefined>) {
	const value = Number(env.LOBSTER_APPROVAL_INPUT_TIMEOUT_MS);
	if (!Number.isFinite(value) || value <= 0) return 0;
	return Math.floor(value);
}

/**
 * A yes or a no, from a person: "y"/"yes" or "n"/"no", in any case. Anything else is
 * neither (undefined), whether typed at a terminal or sent with `resume --approve`.
 */
export function yesOrNo(answer: string): boolean | undefined {
	const word = answer.trim().toLowerCase();
	if (word === "y" || word === "yes") return true;
	if (word === "n" || word === "no") return false;
	return undefined;
}

/** A line typed at a `[y/N]` prompt: yes or no, an empty line being the "N" it offers. */
export function yesOrNoPrompt(line: string): boolean | undefined {
	return line.trim() === "" ? false : yesOrNo(line);
}

/**
 * What a line typed for an `ask` question may mean, in order: its JSON, or (when it is not
 * JSON, or is a JSON string) the text itself, then `{ decision: text }`.
 */
export function textAnswerCandidates(line: string): unknown[] {
	let parsed: unknown;
	try {
		parsed = JSON.parse(line);
	} catch {
		return [line, { decision: line }];
	}
	return typeof parsed === "string" ? [parsed, { decision: parsed }] : [parsed];
}

/** The first of `candidates` that `check` accepts; otherwise `check`'s last refusal. */
export function firstAccepted(candidates: unknown[], check: (candidate: unknown) => void) {
	let refusal: unknown;
	for (const candidate of candidates) {
		try {
			check(candidate);
			return candidate;
		} catch (error) {
			refusal = error;
		}
	}
	throw refusal;
}

/** How much of a subject a question shows as its preview. */
export const PREVIEW_CHARS = 2000;
