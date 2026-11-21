// Small stream helpers: a list as a command's stream of items, and a stream nobody reads.
import { Writable } from "node:stream";

/** A list as the stream of items a command returns. */
export async function* streamOf<T>(items: Iterable<T>): AsyncGenerator<T> {
	for (const item of items) yield item;
}

/** A stream that drops what is written to it: output nobody reads (the SDK's, a host's). */
export function discardStream(): Writable {
	return new Writable({
		write(_chunk, _encoding, callback) {
			callback();
		},
	});
}
