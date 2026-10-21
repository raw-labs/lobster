import { validateCommandInputState } from "../input_request.js";
import { compileCached } from "../validation.js";
import { fingerprint, isRetiredCheckpoint, type LogicalArchive } from "./model.js";

export type LegacyCheckpointPage = {
	checkpoints: { checkpointId: string; kind: "pipeline" | "workflow" | "sdk" }[];
	nextCursor: string | null;
};

/** Wrap a stored SDK input frame without changing the legacy token's state key. */
export function legacyCheckpointText(key: string, text: string): string {
	if (!key.startsWith("sdk_resume_")) return text;
	const state = JSON.parse(text);
	if (state.type === "lobster.sdk-tracked.v1") return text;
	const metadata = state.commandInput?.pending?.metadata;
	return (
		JSON.stringify({
			...state,
			type: "lobster.sdk-tracked.v1",
			payload: {
				protocolVersion: 1,
				v: 1,
				resumeMode: "same_stage",
				resumeAtIndex: state.resumeAtIndex,
				stateKey: key,
			},
			...(typeof metadata?.prompt === "string" && metadata.responseSchema !== undefined
				? {
						wait: {
							kind: "input",
							prompt: metadata.prompt,
							responseSchema: metadata.responseSchema,
							...(metadata.defaults !== undefined ? { defaults: metadata.defaults } : {}),
							...(metadata.subject !== undefined ? { subject: metadata.subject } : {}),
						},
					}
				: {}),
		}) + "\n"
	);
}

const object = (item: unknown): item is Record<string, unknown> =>
	item !== null && typeof item === "object" && !Array.isArray(item);
const position = (item: unknown): item is number => Number.isSafeInteger(item) && Number(item) >= 0;

function validPipelineFrame(value: unknown): boolean {
	return (
		object(value) &&
		Array.isArray(value.pipeline) &&
		Array.isArray(value.items) &&
		position(value.resumeAtIndex) &&
		value.resumeAtIndex <= value.pipeline.length &&
		value.pipeline.every(
			(stage) =>
				object(stage) &&
				typeof stage.name === "string" &&
				stage.name.length > 0 &&
				object(stage.args),
		)
	);
}

export function validateCheckpoint(key: string, text: string): void {
	const value = JSON.parse(text);
	if (value?.type === "lobster.consumed-resume-state.v1") {
		if (!isRetiredCheckpoint(text)) throw new Error(`Invalid consumed continuation: ${key}`);
		return;
	}
	if (!object(value)) throw new Error(`Invalid continuation: ${key}`);
	if (key.startsWith("pipeline_resume_")) {
		if (
			!validPipelineFrame(value) ||
			(value.resumeMode !== undefined &&
				(typeof value.resumeMode !== "string" ||
					!["same_stage", "next_stage"].includes(value.resumeMode))) ||
			(value.haltType === "input_request" && value.inputSchema === undefined) ||
			(value.resumeMode === "same_stage" && !object(value.commandInput))
		) {
			throw new Error(`Invalid pipeline continuation: ${key}`);
		}
	} else if (key.startsWith("sdk_resume_")) {
		const tracked = value.type === "lobster.sdk-tracked.v1";
		const frame = tracked ? value.payload : value;
		if (
			!object(frame) ||
			!position(frame.resumeAtIndex) ||
			(tracked && (frame.v !== 1 || frame.protocolVersion !== 1)) ||
			(frame.resumeMode === "same_stage" &&
				(frame.stateKey !== key ||
					!object(value.commandInput) ||
					!Array.isArray(value.items) ||
					value.inputSchema === undefined)) ||
			(!tracked &&
				(!object(value.commandInput) ||
					!Array.isArray(value.items) ||
					value.inputSchema === undefined))
		) {
			throw new Error(`Invalid SDK continuation: ${key}`);
		}
	} else if (/^workflow[-_]resume_/.test(key)) {
		if (
			typeof value.filePath !== "string" ||
			!value.filePath ||
			!object(value.steps) ||
			!object(value.args) ||
			!position(value.resumeAtIndex) ||
			(value.inputKind === "pipeline_command" &&
				(!validPipelineFrame(value.pipelineInput) ||
					!object(value.pipelineInput) ||
					!object(value.pipelineInput.commandInput))) ||
			(value.inputStepId !== undefined &&
				(typeof value.inputStepId !== "string" || value.inputSchema === undefined))
		) {
			throw new Error(`Invalid workflow continuation: ${key}`);
		}
	} else {
		throw new Error(`Invalid checkpoint key: ${key}`);
	}
	try {
		const payload = object(value.payload) ? value.payload : undefined;
		const pipelineInput = object(value.pipelineInput) ? value.pipelineInput : undefined;
		const schema = value.inputSchema !== undefined ? value.inputSchema : payload?.inputSchema;
		if (schema !== undefined) {
			if (typeof schema !== "boolean" && !object(schema)) throw new Error("Invalid input schema");
			compileCached(schema);
		}
		const command =
			value.commandInput !== undefined ? value.commandInput : pipelineInput?.commandInput;
		if (command !== undefined) {
			const frame = validateCommandInputState(command);
			if (
				schema !== undefined &&
				fingerprint(schema) !== fingerprint(frame.pending.metadata.responseSchema)
			) {
				throw new Error("Command input schema mismatch");
			}
		}
	} catch (error) {
		throw new Error(`Invalid saved input continuation: ${key}`, { cause: error });
	}
}

export function validateContinuations(data: LogicalArchive): void {
	const entries = [
		...Object.entries(data.states),
		...data.runs.flatMap((run) => Object.entries(run.states)),
	];
	const states = new Map(entries);
	const resolve = (key: string): string | null | undefined => {
		if (states.has(key)) return states.get(key);
		const alternate = key.startsWith("workflow_resume_")
			? key.replace("workflow_resume_", "workflow-resume_")
			: key.replace("workflow-resume_", "workflow_resume_");
		return states.get(alternate);
	};
	for (const [key, text] of entries) {
		if (text === null) continue;
		if (/^(pipeline_resume_|workflow[-_]resume_|sdk_resume_)/.test(key)) {
			validateCheckpoint(key, text);
			const value = JSON.parse(text);
			if (
				key.startsWith("workflow_resume_") &&
				!isRetiredCheckpoint(text) &&
				!isRetiredCheckpoint(
					states.get(key.replace("workflow_resume_", "workflow-resume_")) ?? null,
				)
			) {
				throw new Error(`Conflicting workflow checkpoint aliases: ${key}`);
			}
			if (value.supersededResumeStateKeys !== undefined) {
				if (
					!Array.isArray(value.supersededResumeStateKeys) ||
					value.supersededResumeStateKeys.some(
						(previous) =>
							typeof previous !== "string" ||
							!/^(pipeline_resume_|workflow[-_]resume_)[a-z0-9._-]+$/.test(previous) ||
							previous === key ||
							(resolve(previous) != null && !isRetiredCheckpoint(resolve(previous))),
					)
				) {
					throw new Error(`Invalid superseded checkpoint reference: ${key}`);
				}
			}
		} else if (/^approval_[a-f0-9]+$/.test(key)) {
			const value = JSON.parse(text);
			if (
				!value ||
				typeof value.stateKey !== "string" ||
				!/^(pipeline_resume_|workflow[-_]resume_)[a-z0-9._-]+$/.test(value.stateKey)
			) {
				throw new Error(`Invalid approval checkpoint reference: ${key}`);
			}
			// A missing target is an already-expired optional short ID. Preserve it as
			// expired; never create a checkpoint to satisfy the reference.
		}
	}
}
