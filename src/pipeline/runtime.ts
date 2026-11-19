import { PersistenceError } from "../persistence/backend.js";
import type { ModelCall } from "../llm/cost_tracker.js";
import {
	InputRequestSuspension,
	RequestInputResumeError,
	assertRequestInputResumeConsumed,
	createInputTracker,
	createStageRequestInput,
	type CommandInputResume,
} from "./input_request.js";
import { recorderFromEnv } from "../persistence/context.js";
import { createJsonRenderer } from "./render_json.js";
import { checkStages } from "./parser.js";
import { streamOf } from "../io/streams.js";

export async function runPipeline({
	pipeline,
	registry,
	stdin,
	stdout,
	stderr,
	env,
	mode = "human",
	renderOutput = true,
	input,
	cwd = undefined,
	llmAdapters = undefined,
	recordModelCall = undefined,
	signal = undefined,
	forceTerminationSignal = undefined,
	haltAfterStageOnAbort = false,
	dryRun = false,
	requestInputResume = undefined,
	requestInputEnabled = true,
	onExecutionStart = undefined,
	onNonRetryableSideEffect = undefined,
	heartbeat = undefined,
	assertInvocationCurrent = undefined,
	onUnsafeCommandDispatch = undefined,
}: {
	pipeline: any[];
	registry: any;
	stdin: any;
	stdout: any;
	stderr: any;
	env: any;
	mode?: string;
	renderOutput?: boolean;
	input?: any;
	cwd?: string | undefined;
	llmAdapters?: Record<string, any> | undefined;
	/** Counts a model call a command made in the run's cost. Absent outside a run. */
	recordModelCall?: ((call: ModelCall) => void) | undefined;
	signal?: AbortSignal | undefined;
	forceTerminationSignal?: AbortSignal | undefined;
	haltAfterStageOnAbort?: boolean;
	dryRun?: boolean;
	requestInputResume?: CommandInputResume | undefined;
	requestInputEnabled?: boolean;
	onExecutionStart?: (() => void | Promise<void>) | undefined;
	onNonRetryableSideEffect?: (() => void) | undefined;
	/** Commands call ctx.heartbeat() to show progress (see heartbeat_timeout_ms). */
	heartbeat?: (() => void) | undefined;
	assertInvocationCurrent?: (() => void) | undefined;
	onUnsafeCommandDispatch?: (() => void) | undefined;
}) {
	// Every stage is checked before the first runs (or is planned).
	const commands = checkStages(pipeline, registry);
	if (dryRun) {
		return dryRunPipeline({ pipeline, stderr });
	}

	await recorderFromEnv(env)?.definition(pipeline);
	let stream = input ?? [];
	let rendered = false;
	let halted = false;
	let haltedAt: { index: number; stage: (typeof pipeline)[number]; inPlace?: boolean } | null =
		null;
	let pipelineOutputStarted = false;
	let executionStarted = false;
	let executionStart: Promise<void> | undefined;
	const markExecutionStarted = async () => {
		if (executionStart) return executionStart;
		executionStart = (async () => {
			await onExecutionStart?.();
			executionStarted = true;
		})();
		return executionStart;
	};

	const baseCtx = {
		stdin,
		stdout,
		stderr,
		env,
		registry,
		mode,
		renderOutput,
		cwd,
		llmAdapters,
		recordModelCall,
		signal,
		forceTerminationSignal,
		onNonRetryableSideEffect,
		assertInvocationCurrent,
		heartbeat: heartbeat ?? (() => {}),
	};

	const unfinishedStages = new Set<() => Promise<void>>();
	try {
		const result = await executeStages();
		await closeStages();
		return result;
	} catch (error) {
		try {
			await closeStages();
		} catch (cleanupError) {
			// Keep unknown persistence outcomes intact. Otherwise a history failure
			// must prevent an ordinary runtime error from triggering an unsafe retry.
			if (!(error instanceof PersistenceError)) throw cleanupError;
		}
		throw error;
	}

	async function executeStages() {
		for (let idx = 0; idx < pipeline.length; idx++) {
			if (haltAfterStageOnAbort) signal?.throwIfAborted();
			const stage = pipeline[idx];
			const command = commands[idx];
			const history = recorderFromEnv(env);
			const attemptId = await history?.start(
				`pipeline/${history.pipelineOffset + idx}`,
				stage.name,
				history.pipelineOffset + idx,
			);
			let historyError: unknown;
			let historyWaiting = false;
			const inputTracker = createInputTracker(stream);
			const stageResume = idx === 0 ? requestInputResume : undefined;
			let commandActive = true;
			let inactiveReason: string | undefined;
			let commandOutputStarted = false;
			let stageFinished = false;
			const closeStage = () => finishStage({ assertResume: false, suppressCloseErrors: true });
			if (history?.historyEnabled) unfinishedStages.add(closeStage);
			async function finishStage({ assertResume = true, suppressCloseErrors = false } = {}) {
				if (stageFinished) return;
				stageFinished = true;
				unfinishedStages.delete(closeStage);
				commandActive = false;
				inputTracker.disableReplay();
				try {
					await inputTracker.close({ suppressErrors: suppressCloseErrors });
					if (assertResume) {
						assertRequestInputResumeConsumed(stageResume);
					}
				} catch (error) {
					historyError = error;
					throw error;
				} finally {
					if (attemptId && history) {
						await history.finish(
							attemptId,
							historyWaiting
								? "waiting"
								: historyError
									? "failed"
									: assertResume
										? "completed"
										: "closed",
							historyError,
						);
					}
				}
			}
			const stageStdout = trackWritableOutput(stdout, () => {
				pipelineOutputStarted = true;
			});
			const ctx = {
				...baseCtx,
				// The name used to invoke it, which may be an alias.
				commandName: stage.name,
				stdout: stageStdout,
				render: createJsonRenderer(stageStdout),
			};
			const stageCtx = {
				...ctx,
				requestInput: requestInputEnabled
					? createStageRequestInput({
							ctx,
							stageIndex: idx,
							mode,
							inputTracker,
							isCommandActive: () => commandActive,
							getInactiveReason: () => inactiveReason,
							isOutputStarted: () => pipelineOutputStarted || commandOutputStarted,
							resume: stageResume,
							onResumedInput:
								command.meta?.resumeSafeAfterInput === true
									? undefined
									: async () => {
											await markExecutionStarted();
											assertInvocationCurrent?.();
											onUnsafeCommandDispatch?.();
										},
						})
					: createUnsupportedRequestInput(),
			};

			let result;
			try {
				if (haltAfterStageOnAbort) {
					signal?.throwIfAborted();
				}
				if (command.meta?.resumeSafeBeforeInput !== true) {
					await markExecutionStarted();
				}
				assertInvocationCurrent?.();
				if (command.meta?.resumeSafeBeforeInput !== true) onUnsafeCommandDispatch?.();
				result = await command.run({
					input: inputTracker.iterable,
					args: stage.args,
					ctx: stageCtx,
				});
			} catch (err) {
				historyError = err;
				historyWaiting = err instanceof InputRequestSuspension;
				await finishStage({ assertResume: false, suppressCloseErrors: true });
				if (haltForInputRequest(err)) break;
				assertNoUnconsumedResumeAfterError(stageResume, err);
				throw err;
			}

			if (result?.rendered) {
				rendered = true;
			}

			const terminalOutput = Boolean(result?.halt);
			historyWaiting = terminalOutput;
			let stageHalted = Boolean(terminalOutput || (haltAfterStageOnAbort && signal?.aborted));
			const output = result?.output;
			if (Array.isArray(output)) {
				stream = output;
				await finishStage();
			} else if (output && idx < pipeline.length - 1 && !terminalOutput) {
				commandActive = false;
				inactiveReason = "requestInput cannot suspend from lazy output before downstream stages";
				assertRequestInputResumeConsumed(stageResume);
				const trackedOutput = trackCommandOutput(
					output,
					() => {
						commandOutputStarted = true;
					},
					() => assertRequestInputResumeConsumed(stageResume),
					(err) => {
						historyError = err;
						assertNoUnconsumedResumeAfterError(stageResume, err);
					},
					finishStage,
				);
				stream = haltAfterStageOnAbort
					? throwIfAbortedAfterDrain(trackedOutput, signal)
					: trackedOutput;
			} else {
				if (output) {
					const trackedOutput = trackCommandOutput(
						output,
						() => {
							commandOutputStarted = true;
						},
						() => assertRequestInputResumeConsumed(stageResume),
						(err) => {
							historyError = err;
							assertNoUnconsumedResumeAfterError(stageResume, err);
						},
						finishStage,
					);
					// Terminal output is drained after the stage loop as well. It needs the
					// same abort-aware read as a handoff, otherwise a stalled final iterator
					// can prevent the tool cancellation from settling forever.
					stream = haltAfterStageOnAbort
						? throwIfAbortedAfterDrain(trackedOutput, signal)
						: trackedOutput;
				} else {
					stream = [];
					await finishStage();
				}
			}

			stageHalted ||= Boolean(haltAfterStageOnAbort && signal?.aborted);
			if (stageHalted) {
				halted = true;
				haltedAt = { index: idx, stage };
				break;
			}
		}

		const items = [];
		try {
			for await (const item of stream) items.push(item);
		} catch (err) {
			if (haltForInputRequest(err)) {
				items.length = 0;
				for await (const item of stream) items.push(item);
			} else {
				throw err;
			}
		}
		if (haltAfterStageOnAbort) signal?.throwIfAborted();
		assertRequestInputResumeConsumed(requestInputResume);

		return { items, rendered, halted, haltedAt, executionStarted };
	}

	async function closeStages() {
		// Close history for stages whose output was never requested. Calling next()
		// here would execute unused work, so only release existing inputs/finalizers.
		let cleanupError: unknown;
		for (const close of [...unfinishedStages].reverse()) {
			try {
				await close();
			} catch (error) {
				cleanupError ??= error;
			}
		}
		if (cleanupError) throw cleanupError;
	}

	function haltForInputRequest(err: unknown) {
		if (!(err instanceof InputRequestSuspension)) return false;
		const stageIndex = err.stageIndex;
		halted = true;
		haltedAt = {
			index: stageIndex,
			stage: pipeline[stageIndex],
			inPlace: true,
		};
		stream = streamOf([err.request]);
		return true;
	}
}

function dryRunPipeline({ pipeline, stderr }: { pipeline: any[]; stderr: any }) {
	const lines: string[] = [];
	lines.push(`[DRY RUN] Pipeline (${pipeline.length} stage${pipeline.length !== 1 ? "s" : ""}):`);

	for (let idx = 0; idx < pipeline.length; idx++) {
		const stage = pipeline[idx];
		const formattedArgs = stage.args ? formatStageArgs(stage.args) : "";
		const argsStr = formattedArgs ? `  args: ${formattedArgs}` : "";
		lines.push(`  ${idx + 1}. ${stage.name}${argsStr}`);
	}

	lines.push("");
	const plan = lines.join("\n");
	stderr.write(plan);
	// Return rendered:true so the CLI does not print an empty JSON array to stdout.
	return {
		items: [],
		plan,
		rendered: true,
		halted: false,
		haltedAt: null,
		executionStarted: false,
	};
}

function formatStageArgs(args: Record<string, unknown>) {
	const parts: string[] = [];
	for (const [key, value] of Object.entries(args)) {
		if (key === "_") {
			const positional = Array.isArray(value) ? value : [value];
			for (const v of positional) {
				if (v !== undefined && v !== null) parts.push(String(v));
			}
		} else {
			parts.push(`${key}=${typeof value === "string" ? value : JSON.stringify(value)}`);
		}
	}
	return parts.join(", ");
}

function throwIfAbortedAfterDrain(input: AsyncIterable<unknown>, signal?: AbortSignal) {
	return (async function* () {
		const iterator = input[Symbol.asyncIterator]();
		let completed = false;
		try {
			while (true) {
				const next = await nextWithAbort(iterator, signal);
				if (next.done) {
					completed = true;
					break;
				}
				signal?.throwIfAborted();
				yield next.value;
			}
			signal?.throwIfAborted();
		} finally {
			if (!completed) await closeAfterAbortedRead(input, iterator, signal);
		}
	})();
}

type CancellableLazyOutput = {
	abort?: (reason?: unknown) => void | Promise<void>;
};

async function closeAfterAbortedRead(
	input: AsyncIterable<unknown>,
	iterator: AsyncIterator<unknown>,
	signal?: AbortSignal,
) {
	const cancellable = iterator as AsyncIterator<unknown> & CancellableLazyOutput;
	const inputCancellable = input as AsyncIterable<unknown> & CancellableLazyOutput;
	const abort = cancellable.abort ?? inputCancellable.abort;
	try {
		// A source that owns a pending timer, socket, or process can expose this
		// small cancellation hook. It must release the pending next() operation.
		if (signal?.aborted && abort) await abort(signal.reason);
		if (typeof iterator.return !== "function") return;
		const close = iterator.return();
		if (signal?.aborted && !abort) {
			// Legacy iterators cannot interrupt an in-flight next(). Keep the
			// existing prompt cancellation behavior, while resource-owning sources
			// opt into the abort hook above so their cleanup is awaited.
			void Promise.resolve(close).catch(() => {});
			return;
		}
		await close;
	} catch (err) {
		if (!signal?.aborted) throw err;
	}
}

async function nextWithAbort(iterator: AsyncIterator<unknown>, signal?: AbortSignal) {
	if (!signal) return iterator.next();
	signal.throwIfAborted();

	let onAbort!: () => void;
	const aborted = new Promise<never>((_resolve, reject) => {
		onAbort = () => {
			try {
				signal.throwIfAborted();
			} catch (err) {
				reject(err);
			}
		};
		signal.addEventListener("abort", onAbort, { once: true });
	});
	if (signal.aborted) onAbort();

	try {
		return await Promise.race([iterator.next(), aborted]);
	} finally {
		signal.removeEventListener("abort", onAbort);
	}
}

function trackCommandOutput(
	output: AsyncIterable<unknown> | Iterable<unknown>,
	markOutput: () => void,
	assertResumeConsumed: () => void,
	assertNoUnconsumedResumeAfterError: (err: unknown) => void,
	finishStage: (options?: {
		assertResume?: boolean;
		suppressCloseErrors?: boolean;
	}) => Promise<void>,
) {
	const source = output as AsyncIterable<unknown> & CancellableLazyOutput & AsyncIterator<unknown>;
	let sourceIterator: AsyncIterator<unknown> | undefined;
	const tracked = (async function* () {
		let completed = false;
		try {
			if (typeof source[Symbol.asyncIterator] === "function") {
				sourceIterator = source[Symbol.asyncIterator]();
				const iteratorInput: AsyncIterable<unknown> = {
					[Symbol.asyncIterator]: () => sourceIterator!,
				};
				for await (const item of iteratorInput) {
					assertResumeConsumed();
					markOutput();
					yield item;
				}
			} else {
				for (const item of output as Iterable<unknown>) {
					assertResumeConsumed();
					markOutput();
					yield item;
				}
			}
			completed = true;
		} catch (err) {
			assertNoUnconsumedResumeAfterError(err);
			await finishStage({ assertResume: false, suppressCloseErrors: true });
			throw err;
		} finally {
			await finishStage({ assertResume: completed });
		}
	})();
	// Iterator-owned abort hooks are only discoverable after iterator acquisition.
	// A getter lets the abort-aware wrapper find that hook once a read is pending,
	// without performing acquisition outside the generator's guarded cleanup path.
	Object.defineProperty(tracked, "abort", {
		configurable: true,
		get() {
			const cancellationOwner =
				sourceIterator && typeof (sourceIterator as CancellableLazyOutput).abort === "function"
					? (sourceIterator as CancellableLazyOutput)
					: source;
			const abort = cancellationOwner.abort;
			return typeof abort === "function"
				? (reason?: unknown) => abort.call(cancellationOwner, reason)
				: undefined;
		},
	});
	return tracked;
}

function assertNoUnconsumedResumeAfterError(resume: CommandInputResume | undefined, err: unknown) {
	if (err instanceof RequestInputResumeError) return;
	assertRequestInputResumeConsumed(resume);
}

function trackWritableOutput(stdout: any, markOutput: () => void) {
	return new Proxy(stdout, {
		get(target, prop, receiver) {
			const value = Reflect.get(target, prop, receiver);
			if (prop === "write" || prop === "end") {
				return (...args: unknown[]) => {
					markOutput();
					return value.apply(target, args);
				};
			}
			return typeof value === "function" ? value.bind(target) : value;
		},
	});
}

function createUnsupportedRequestInput() {
	const requestInput = async function requestInput() {
		throw new Error("requestInput is not supported in this pipeline context");
	};
	requestInput.getSuspendedState = function getSuspendedState() {
		return undefined;
	};
	return requestInput;
}
