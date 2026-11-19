// An `input:` step, or a question a pipeline asked: a structured answer from a person.
import type { WorkflowStep, WorkflowStepResult } from "../../workflows/types.js";
import { resolveInputValue } from "../../workflows/values.js";
import { activation, type WaitRequest } from "../checkpoint.js";
import type { Machine } from "../machine.js";
import type { Scope } from "../scope.js";
import { firstAccepted, textAnswerCandidates } from "../../io/terminal.js";
import { validateInputResponse } from "../asking.js";
import { askAtTerminal, failOnlyTheQuestion, resolveWait, waitDeadline, waitOf } from "../waits.js";

type InputWait = Extract<WaitRequest, { kind: "input" }>;

/**
 * Ask for an answer to the step's question, or to `asked.request`, a pipeline's. An answer is
 * typed as JSON, unless the question `acceptsText` (`ask`'s): then as `ask` reads it.
 */
export async function inputStep(
	machine: Machine,
	step: WorkflowStep,
	key: string,
	scope: Scope,
	asked?: { request: InputWait; acceptsText: boolean },
) {
	const frame = activation(machine.state, key);
	if (frame.phase === "completed") return frame.result!;
	const wait = waitOf(
		frame,
		"input",
		step.id,
		() =>
			asked?.request ?? {
				kind: "input",
				prompt: step.input!.prompt,
				responseSchema: step.input!.responseSchema,
				defaults: step.input!.defaults,
				subject: resolveInputSubject({ step, ...scope }),
				...waitDeadline(step.input, `Step ${step.id} input`),
			},
	);
	const unanswered = { id: step.id, subject: wait.subject };
	return failOnlyTheQuestion(machine, key, frame, scope, async () => {
		const read = await askAtTerminal(
			machine,
			key,
			frame,
			scope,
			wait.prompt + (asked?.acceptsText ? "\n> " : "\nEnter JSON response: "),
			unanswered,
		);
		if ("timedOut" in read) return read.timedOut;
		const candidates = asked?.acceptsText
			? textAnswerCandidates(read.answer)
			: [parseResponseJson(read.answer)];
		const response = firstAccepted(candidates, (candidate) =>
			validateInputResponse({
				schema: wait.responseSchema,
				response: candidate,
				stepId: step.id,
				standalone: scope.definition.standalone,
			}),
		);
		// Through resolveWait: a question published earlier (in tool mode) is consumed with it.
		return resolveWait(machine, frame, { ...unanswered, response });
	});
}

/** What an input step shows with its question: its `stdin:`, or the last step's value. */
function resolveInputSubject(params: {
	step: WorkflowStep;
	args: Record<string, unknown>;
	results: Record<string, WorkflowStepResult>;
	lastStepId?: string;
}) {
	if (params.step.stdin !== undefined) {
		return resolveInputValue(params.step.stdin, params.args, params.results);
	}
	if (!params.lastStepId) return null;
	const previous = params.results[params.lastStepId];
	if (!previous) return null;
	if (previous.json !== undefined) return previous.json;
	if (previous.response !== undefined) return previous.response;
	if (previous.stdout !== undefined) return previous.stdout;
	return null;
}

function parseResponseJson(raw: string): unknown {
	try {
		return JSON.parse(raw);
	} catch {
		throw new Error("Input response must be valid JSON");
	}
}
