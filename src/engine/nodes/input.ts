import { activation } from "../state.js";
import type { Machine, Scope } from "../machine.js";
import type { WorkflowStep } from "../../workflows/types.js";
import type { WaitRequest } from "../../persistence/waits.js";
import {
	resolveInputSubject,
	validateInputResponse,
	isInteractive,
	parseResponseJson,
	parseApprovalTimeoutMs,
} from "../gates.js";
import { readLineFromStream } from "../../read_line.js";

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
			lastStepId: Object.keys(scope.results).at(-1) ?? null,
		});
		frame.wait = request ?? {
			kind: "input",
			prompt: step.input!.prompt,
			responseSchema: step.input!.responseSchema,
			defaults: step.input!.defaults,
			subject,
		};
	}
	const wait = frame.wait;
	if (wait.kind !== "input") throw new Error("Invalid input activation");
	if (scope.ctx.mode === "tool" || !isInteractive(scope.ctx.stdin)) {
		await machine.suspend(key, frame);
	}
	scope.ctx.stdout.write(wait.prompt + "\nEnter JSON response: ");
	const raw = await readLineFromStream(scope.ctx.stdin, {
		timeoutMs: parseApprovalTimeoutMs(scope.ctx.env),
		signal: scope.ctx.signal,
	});
	const response = parseResponseJson(String(raw ?? "").trim());
	validateInputResponse({ schema: wait.responseSchema, response, stepId: step.id });
	frame.result = { id: step.id, subject: wait.subject, response };
	frame.phase = "completed";
	await machine.save();
	return frame.result;
}
