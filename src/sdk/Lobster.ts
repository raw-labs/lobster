import { randomUUID } from "node:crypto";
import { validateCommandInputState, type CommandInputState } from "../input_request.js";
import {
	publishTrackedCheckpoint,
	type ContinuationPredecessor,
} from "../persistence/continuations.js";
import { runtimeErrorDetails } from "../persistence/backend.js";
import { recorderFromEnv, sessionFromEnv, bindResumeGuard } from "../persistence/context.js";
import { withExecution, withStoreContext } from "../persistence/execution.js";
import type { StoreSession } from "../persistence/session.js";
import {
	consumeResumeState,
	restoreConsumedResumeState,
	deleteUnconsumedResumeState,
	deleteStateJson,
	readStateJsonWithLock,
	writeStateJson,
} from "../state/store.js";
import { resolveWait, callerId } from "../persistence/waits.js";
import { encodeToken, decodeToken } from "../token.js";
import { compileCached } from "../validation.js";
import { runPipelineInternal } from "./runtime.js";
import { stateEnv } from "./state_env.js";

type SdkResumePayload = {
	protocolVersion: 1;
	v: 1;
	stageIndex?: number;
	resumeAtIndex: number;
	items?: unknown[];
	prompt?: string;
	inputSchema?: unknown;
	inputSubject?: unknown;
	resumeMode?: "next_stage" | "same_stage";
	stateKey?: string;
};

type SdkPauseResult = {
	status?: string;
	requiresApproval?: { resumeToken: string } | null;
	requiresInput?: { resumeToken: string } | null;
};

type SdkTrackedResumePayload = {
	protocolVersion: 1;
	v: 2;
	kind: "sdk-tracked";
	stateKey: string;
};

type SdkCommandInputResumeState = {
	resumeAtIndex: number;
	items: unknown[];
	inputSchema: unknown;
	inputSubject?: unknown;
	commandInput: CommandInputState;
};

export type LobsterOptions = {
	storeSession?: StoreSession;
	history?: boolean;
	trackRuns?: boolean;
	runMetadata?: { callerId?: string; hostRunId?: string; hostTaskId?: string };
	env?: NodeJS.ProcessEnv;
	stateDir?: string;
	signal?: AbortSignal;
};

export class Lobster {
	#stages = [];
	#options: LobsterOptions;
	#onExecutionStart?: () => Promise<void>;
	#meta = null;
	#commandInputCheckpoint?: { key: string; state: SdkCommandInputResumeState };

	constructor(options: LobsterOptions = {}) {
		this.#options = {
			storeSession: options.storeSession,
			history: options.history,
			trackRuns: options.trackRuns,
			runMetadata: options.runMetadata,
			env: options.env ?? process.env,
			stateDir: options.stateDir,
			signal: options.signal,
		};
	}

	pipe(stage) {
		if (typeof stage !== "function" && typeof stage?.run !== "function") {
			throw new Error("Stage must be a function or have a run() method");
		}
		this.#stages.push(stage);
		return this;
	}

	meta(meta) {
		this.#meta = meta;
		return this;
	}

	getMeta() {
		return this.#meta;
	}

	async run(initialInput = []) {
		return withExecution(
			{ ...this.#options, env: stateEnv(this.#options) },
			"sdk",
			undefined,
			async (ctx) => {
				const instance = new Lobster({
					...this.#options,
					env: ctx.env,
					storeSession: sessionFromEnv(ctx.env),
				});
				instance.#stages = this.#stages;
				const result = await instance.#runStages(instance.#stages, initialInput);
				return instance.#trackPause(result);
			},
		).catch((err) => {
			const error = runtimeErrorDetails(err);
			return {
				ok: false,
				status: "error",
				runId: error.runId,
				output: [],
				requiresApproval: null,
				requiresInput: null,
				error,
			};
		});
	}

	async #runStages(
		stages,
		initialInput,
		requestInputResume = undefined,
		stageOffset = 0,
		fallbackIndex = -1,
	) {
		const ctx = {
			env: stateEnv(this.#options),
			_onExecutionStart: this.#onExecutionStart,
			stateDir: this.#options.stateDir,
			mode: "sdk",
			signal: this.#options.signal,
		};

		const recorder = recorderFromEnv(ctx.env);
		if (recorder) {
			recorder.pipelineOffset = stageOffset;
		}
		try {
			const result = await runPipelineInternal({
				stages,
				ctx,
				input: initialInput,
				requestInputResume,
			});

			if (
				result.halted &&
				result.items.length === 1 &&
				result.items[0]?.type === "approval_request"
			) {
				const approval = result.items[0];
				const resumeToken = encodeToken({
					protocolVersion: 1,
					v: 1,
					stageIndex: stageOffset + (result.haltedAt?.index ?? fallbackIndex),
					resumeAtIndex: stageOffset + (result.haltedAt?.index ?? fallbackIndex) + 1,
					items: approval.items,
					prompt: approval.prompt,
				});

				return {
					ok: true,
					status: "needs_approval",
					output: [],
					requiresApproval: {
						prompt: approval.prompt,
						items: approval.items,
						resumeToken,
					},
					requiresInput: null,
				};
			}

			if (result.halted && result.items.length === 1 && result.items[0]?.type === "input_request") {
				const input = result.items[0];
				const resumeMode = input.commandInput ? "same_stage" : "next_stage";
				const resumeAtIndex =
					resumeMode === "same_stage"
						? stageOffset + (result.haltedAt?.index ?? fallbackIndex)
						: stageOffset + (result.haltedAt?.index ?? fallbackIndex) + 1;
				let stateKey: string | undefined;
				if (resumeMode === "same_stage") {
					const state: SdkCommandInputResumeState = {
						resumeAtIndex,
						items: input.items ?? [],
						inputSchema: input.responseSchema,
						...(input.subject !== undefined ? { inputSubject: input.subject } : {}),
						commandInput: input.commandInput,
					};
					if (recorder) {
						stateKey = `sdk_resume_${randomUUID()}`;
						this.#commandInputCheckpoint = { key: stateKey, state };
					} else {
						stateKey = await saveSdkCommandInputResumeState(this.#options, state);
					}
				}
				const resumeToken = encodeToken({
					protocolVersion: 1,
					v: 1,
					stageIndex: stageOffset + (result.haltedAt?.index ?? fallbackIndex),
					resumeAtIndex,
					resumeMode,
					...(resumeMode === "same_stage"
						? { stateKey }
						: { items: [], inputSchema: input.responseSchema }),
					inputSubject: input.subject,
				});

				return {
					ok: true,
					status: "needs_input",
					output: [],
					requiresApproval: null,
					requiresInput: {
						prompt: input.prompt,
						responseSchema: input.responseSchema,
						defaults: input.defaults,
						subject: input.subject,
						resumeToken,
					},
				};
			}

			return {
				ok: true,
				status: "ok",
				output: result.items,
				requiresApproval: null,
				requiresInput: null,
			};
		} catch (err) {
			return {
				ok: false,
				status: "error",
				output: [],
				requiresApproval: null,
				requiresInput: null,
				error: runtimeErrorDetails(err),
			};
		}
	}

	async resume(
		token: string,
		options: { approved?: boolean; response?: unknown; cancel?: boolean } = {},
	) {
		const decoded = decodeToken(token) as SdkResumePayload | SdkTrackedResumePayload;
		if (decoded?.v === 2 && decoded.kind === "sdk-tracked") {
			return this.#resumeTracked(decoded.stateKey, options);
		}
		const { approved, response, cancel } = options;
		const intentCount =
			Number(typeof approved === "boolean") +
			Number(response !== undefined) +
			Number(cancel === true);
		if (intentCount > 1) {
			throw new Error("resume accepts only one of approved, response, or cancel");
		}
		if (intentCount === 0) {
			throw new Error("resume requires approved, response, or cancel");
		}

		const payload = decodeSdkResumePayload(token);

		let sdkCommandInputState: SdkCommandInputResumeState | undefined;
		if (payload.resumeMode === "same_stage") {
			sdkCommandInputState = await loadSdkCommandInputResumeState(this.#options, payload.stateKey!);
			if (
				!this.#onExecutionStart &&
				"type" in sdkCommandInputState &&
				sdkCommandInputState.type === "lobster.sdk-tracked.v1"
			) {
				return this.#resumeTracked(payload.stateKey!, options);
			}
		}

		if (cancel === true) {
			if (payload.resumeMode === "same_stage" && !this.#onExecutionStart) {
				await deleteStateJson({ env: stateEnv(this.#options), key: payload.stateKey! });
			}
			return {
				ok: true,
				status: "cancelled",
				output: [],
				requiresApproval: null,
				requiresInput: null,
			};
		}

		const expectsInput = payload.inputSchema !== undefined || payload.resumeMode === "same_stage";
		if (expectsInput) {
			if (approved !== undefined) {
				throw new Error("resume token expects an input response, not approved");
			}
			if (response === undefined) {
				throw new Error("resume token expects response");
			}
		} else {
			if (response !== undefined) {
				throw new Error("resume token expects approved=true|false, not response");
			}
			if (typeof approved !== "boolean") {
				throw new Error("resume token expects approved=true|false");
			}
			if (approved === false) {
				return {
					ok: true,
					status: "cancelled",
					output: [],
					requiresApproval: null,
					requiresInput: null,
				};
			}
		}

		const resumeIndex = sdkCommandInputState?.resumeAtIndex ?? payload.resumeAtIndex ?? 0;
		let resumeItems = sdkCommandInputState?.items ?? payload.items ?? [];
		let requestInputResume;
		if (response !== undefined) {
			const schema = sdkCommandInputState?.inputSchema ?? payload.inputSchema;
			if (schema === undefined) {
				throw new Error("resume token does not support input responses");
			}
			let validator;
			try {
				validator = compileCached(schema as any);
			} catch {
				throw new Error("resume token input schema is invalid");
			}
			const ok = validator(response);
			if (!ok) {
				const first = validator.errors?.[0];
				throw new Error(
					`response does not match schema at ${first?.instancePath || "/"}: ${first?.message || "invalid"}`,
				);
			}
			if (payload.resumeMode === "same_stage") {
				resumeItems = sdkCommandInputState!.items;
				requestInputResume = {
					state: sdkCommandInputState!.commandInput,
					response,
					onConsumed: async () => {
						if (!this.#onExecutionStart) {
							await deleteStateJson({ env: stateEnv(this.#options), key: payload.stateKey! });
						}
					},
				};
			} else {
				resumeItems = [response];
			}
		}

		const remainingStages = this.#stages.slice(resumeIndex);
		return this.#runStages(remainingStages, resumeItems, requestInputResume, resumeIndex, 0);
	}

	async #trackPause<T extends SdkPauseResult>(
		result: T,
		predecessor?: ContinuationPredecessor,
	): Promise<T> {
		const env = stateEnv(this.#options);
		if (!recorderFromEnv(env) || !["needs_approval", "needs_input"].includes(result.status)) {
			return result;
		}
		const request = result.requiresApproval ?? result.requiresInput;
		const payload = decodeSdkResumePayload(request.resumeToken);
		const stateKey =
			payload.resumeMode === "same_stage" ? payload.stateKey : `sdk_resume_${randomUUID()}`;
		const commandState =
			payload.resumeMode === "same_stage"
				? (this.#commandInputCheckpoint?.state ??
					(await readStateJsonWithLock({ env, key: stateKey })))
				: {};
		const { resumeToken: _token, ...question } = request;
		await publishTrackedCheckpoint({
			env,
			key: stateKey,
			predecessor,
			signal: this.#options.signal,
			state: {
				...commandState,
				type: "lobster.sdk-tracked.v1",
				payload,
				wait: { ...question, kind: result.requiresInput ? "input" : "approval" },
			},
		});
		request.resumeToken = encodeToken({ protocolVersion: 1, v: 2, kind: "sdk-tracked", stateKey });
		return result;
	}

	async resumeWait(options: {
		waitId: string;
		expectedRevision: number;
		approved?: boolean;
		response?: unknown;
		cancel?: boolean;
	}) {
		return withStoreContext(
			{ ...this.#options, env: stateEnv(this.#options), trackRuns: true },
			async (ctx) => {
				const store = sessionFromEnv(ctx.env);
				if (!store) throw new Error("Wait resume requires a persistence session");
				const wait = await resolveWait(
					store,
					options.waitId,
					options.expectedRevision,
					callerId(this.#options.runMetadata),
				);
				if (!wait.key.startsWith("sdk_resume_"))
					throw new Error("This wait is not an SDK checkpoint");
				const instance = new Lobster({
					...this.#options,
					storeSession: store,
					env: bindResumeGuard(ctx.env, {
						key: wait.key,
						expectedRevision: options.expectedRevision,
						admitted: false,
					}),
				});
				instance.#stages = this.#stages;
				return instance.#resumeTracked(wait.key, options);
			},
		);
	}

	async #resumeTracked(
		key: string,
		decision: { approved?: boolean; response?: unknown; cancel?: boolean },
	) {
		if (typeof key !== "string" || !/^sdk_resume_[a-f0-9-]+$/.test(key)) {
			throw new Error("Invalid SDK tracked token");
		}
		return withExecution(
			{ ...this.#options, env: stateEnv(this.#options), _decision: decision },
			"sdk",
			key,
			async (ctx) => {
				const state = await readStateJsonWithLock({ env: ctx.env, key, signal: ctx.signal });
				if (state?.type !== "lobster.sdk-tracked.v1") {
					throw new Error("SDK resume state not found");
				}
				const instance = new Lobster({
					...this.#options,
					env: ctx.env,
					storeSession: sessionFromEnv(ctx.env),
				});
				instance.#stages = this.#stages;
				let claimed = false;
				let claimId: string | undefined;
				instance.#onExecutionStart = async () => {
					if (claimed) {
						return;
					}
					const claim = await consumeResumeState({
						env: ctx.env,
						key,
						expectedState: state,
						signal: ctx.signal,
					});
					if (!claim.consumed) {
						throw new Error("SDK resume state not found");
					}
					if (claim.signalAbortedAfterCommit) {
						await restoreConsumedResumeState({
							env: ctx.env,
							key,
							expectedState: state,
							claimId: claim.claimId,
						});
						ctx.signal?.throwIfAborted();
					}
					claimId = claim.claimId;
					claimed = true;
				};
				const result = await instance.resume(encodeToken(state.payload), decision);
				const paused = ["needs_approval", "needs_input"].includes(result.status);
				if (
					result.ok &&
					!paused &&
					!claimed &&
					(await readStateJsonWithLock({ env: ctx.env, key })) !== null
				) {
					const deletion = await deleteUnconsumedResumeState({
						env: ctx.env,
						key,
						signal: ctx.signal,
					});
					if (deletion !== "deleted") {
						throw new Error("SDK resume state not found");
					}
				}
				const tracked = await instance.#trackPause(result, { key, state, claimId });
				if (claimed && !paused) {
					await deleteStateJson({ env: ctx.env, key });
				}
				return tracked;
			},
		);
	}

	clone() {
		const cloned = new Lobster(this.#options);
		cloned.#stages = [...this.#stages];
		cloned.#meta = this.#meta ? { ...this.#meta } : null;
		return cloned;
	}
}

function decodeSdkResumePayload(token: string): SdkResumePayload {
	const payload = decodeToken(token);
	if (!payload || typeof payload !== "object") {
		throw new Error("Invalid token");
	}
	const data = payload as Record<string, unknown>;
	if (data.protocolVersion !== 1 || data.v !== 1) {
		throw new Error("Invalid token");
	}
	if (
		typeof data.resumeAtIndex !== "number" ||
		!Number.isInteger(data.resumeAtIndex) ||
		data.resumeAtIndex < 0
	) {
		throw new Error("Invalid token");
	}
	if (data.items !== undefined && !Array.isArray(data.items)) {
		throw new Error("Invalid token");
	}
	if (
		data.resumeMode !== undefined &&
		(typeof data.resumeMode !== "string" || !["next_stage", "same_stage"].includes(data.resumeMode))
	) {
		throw new Error("Invalid token");
	}
	if (data.resumeMode === "same_stage") {
		if (typeof data.stateKey !== "string" || data.stateKey.length === 0) {
			throw new Error("Invalid token");
		}
	} else if (data.stateKey !== undefined) {
		throw new Error("Invalid token");
	}
	if (data.commandInput !== undefined) throw new Error("Invalid token");
	return data as unknown as SdkResumePayload;
}

async function saveSdkCommandInputResumeState(
	options: LobsterOptions,
	state: SdkCommandInputResumeState,
) {
	const stateKey = `sdk_resume_${randomUUID()}`;
	await writeStateJson({ env: stateEnv(options), key: stateKey, value: state });
	return stateKey;
}

async function loadSdkCommandInputResumeState(
	options: LobsterOptions,
	stateKey: string,
): Promise<SdkCommandInputResumeState> {
	const stored = await readStateJsonWithLock({
		env: stateEnv(options),
		key: stateKey,
		signal: options?.signal,
	});
	if (!stored || typeof stored !== "object") throw new Error("SDK resume state not found");
	const data = stored as Partial<SdkCommandInputResumeState>;
	if (
		typeof data.resumeAtIndex !== "number" ||
		!Number.isInteger(data.resumeAtIndex) ||
		data.resumeAtIndex < 0
	) {
		throw new Error("Invalid SDK resume state");
	}
	if (!Array.isArray(data.items)) throw new Error("Invalid SDK resume state");
	if (data.inputSchema === undefined) throw new Error("Invalid SDK resume state");
	data.commandInput = validateCommandInputState(data.commandInput);
	return data as SdkCommandInputResumeState;
}
