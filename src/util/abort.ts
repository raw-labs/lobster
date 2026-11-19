// Cancellation for code that waits on things that may not watch an abort signal themselves.
/**
 * The error a cancelled call rejects with: an AbortError (by name and code) that keeps the
 * host's message and carries its reason as `cause`. A host may abort with any reason it likes;
 * marking the rejection lets whoever awaits the call (an SDK host, a script) tell cancellation
 * from an ordinary failure by the error alone. The engine itself checks its signal.
 */
function cancellationError(signal: AbortSignal): unknown {
	const reason: any = signal.reason;
	if (reason === undefined || reason === null) {
		return new DOMException("The operation was aborted.", "AbortError");
	}
	if (reason?.name === "AbortError" || reason?.code === "ABORT_ERR") return reason;
	const message =
		typeof reason?.message === "string" && reason.message ? reason.message : String(reason);
	const error: any = new Error(message, { cause: reason });
	error.name = "AbortError";
	error.code = "ABORT_ERR";
	return error;
}

/** Rethrow `err` when it already reads as cancellation, else the run's reason. */
export function asCancellation(err: any, signal: AbortSignal): unknown {
	if (err?.name === "AbortError" || err?.code === "ABORT_ERR") return err;
	return cancellationError(signal);
}

export function throwIfCancelled(signal?: AbortSignal): void {
	if (signal?.aborted) throw cancellationError(signal);
}

/**
 * Reject as soon as the run is cancelled instead of waiting for `promise`.
 * HTTP adapters are cancelled at the socket, but an injected `ctx.llmAdapters`
 * adapter may ignore `ctx.signal` entirely; without this, one of those keeps a
 * timed-out step waiting for as long as it likes.
 */
export function abortable<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
	if (!signal) return promise;
	return new Promise<T>((resolve, reject) => {
		const onAbort = () => reject(cancellationError(signal));
		// Observe `promise` before anything can settle the wrapper, including the
		// already-aborted case below. An adapter can cancel the run from inside its own
		// `invoke` and reject afterwards; rejecting here without watching that promise
		// leaves the rejection unhandled, which ends the process under Node's default
		// handling -- long after this step was cancelled cleanly.
		promise.then(resolve, reject).finally(() => signal.removeEventListener("abort", onAbort));
		if (signal.aborted) {
			onAbort();
			return;
		}
		signal.addEventListener("abort", onAbort, { once: true });
	});
}
