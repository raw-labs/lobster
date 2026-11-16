import { validateCommandInputState } from "../input_request.js";
import { compileCached } from "../validation.js";
import { fingerprint, isRetiredCheckpoint } from "./model.js";

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
		if (value.machineVersion !== undefined) {
			if (
				value.machineVersion !== 1 ||
				typeof value.runId !== "string" ||
				typeof value.activation !== "string" ||
				!object(value.wait) ||
				!["approval", "input"].includes(String(value.wait.kind))
			)
				throw new Error("Invalid workflow machine continuation: " + key);
			return;
		}

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
