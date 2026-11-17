import { activation } from "../state.js";
import type { Machine, Scope } from "../machine.js";
import type { WorkflowStep } from "../../workflows/types.js";
import type { WaitRequest } from "../../persistence/waits.js";
import {
	resolveInputSubject,
	validateInputResponse,
	isInteractive,
	parseResponseJson,
	questionDeadline,
	readAnswer,
} from "../gates.js";

export async function input(
	machine: Machine,
	step: WorkflowStep,
	key: string,
	scope: Scope,
	request?: WaitRequest,
) {
	const frame = activation(machine.state, key);
	frame.stepId = step.id;
	if (frame.phase === "completed") return frame.result!;
	if (!frame.wait) {
		const subject = resolveInputSubject({
			step,
			args: scope.args,
			results: scope.results,
			lastStepId: scope.lastStepId ?? null,
		});
		frame.wait = request ?? {
			kind: "input",
			prompt: step.input!.prompt,
			responseSchema: step.input!.responseSchema,
			defaults: step.input!.defaults,
			subject,
			...questionDeadline(step.input, `Step ${step.id} input`),
		};
	}
	const wait = frame.wait;
	if (wait.kind !== "input") throw new Error("Invalid input activation");
	if (machine.expired(frame))
		return machine.timeOut(key, frame, { id: step.id, subject: wait.subject });
	if (scope.ctx.mode === "tool" || !isInteractive(scope.ctx.stdin)) {
		await machine.suspend(key, frame);
	}
	scope.ctx.stdout.write(wait.prompt + "\nEnter JSON response: ");
	const read = await readAnswer(scope, wait.deadline);
	if ("expired" in read) return machine.timeOut(key, frame, { id: step.id, subject: wait.subject });
	const response = parseResponseJson(String(read.answer ?? "").trim());
	validateInputResponse({ schema: wait.responseSchema, response, stepId: step.id });
	// Through resolveWait: a question published earlier (in tool mode) is consumed with it.
	return machine.resolveWait(key, frame, { id: step.id, subject: wait.subject, response });
}
