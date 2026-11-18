import { Writable } from "node:stream";

/** Whether a person is at the other end of `stdin` (a terminal), so a question can be asked here. */
export function isInteractive(stdin: unknown) {
	return Boolean((stdin as NodeJS.ReadStream | undefined)?.isTTY);
}

/** How long a question asked on the terminal waits for its answer (0: no limit). */
export function approvalInputTimeoutMs(env: Record<string, string | undefined> | undefined) {
	const value = Number(env?.LOBSTER_APPROVAL_INPUT_TIMEOUT_MS);
	if (!Number.isFinite(value) || value <= 0) return 0;
	return Math.floor(value);
}

/** A list as the stream of items a command returns. */
export async function* streamOf<T>(items: Iterable<T>): AsyncGenerator<T> {
	for (const item of items) yield item;
}

/** How much of a subject a question shows as its preview. */
export const PREVIEW_CHARS = 2000;

/** A stream that drops what is written to it: output nobody reads (the SDK's, a host's). */
export function discardStream(): Writable {
	return new Writable({
		write(_chunk, _encoding, callback) {
			callback();
		},
	});
}
