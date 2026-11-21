import test from "node:test";
import assert from "node:assert/strict";
import {
	earliestWaitDue,
	isPending,
	waitsForEvent,
	type Activation,
	type ExecutionState,
} from "../src/engine/checkpoint.js";

function execution(activations: Record<string, Activation>): ExecutionState {
	return {
		version: 1,
		revision: 1,
		definition: { filePath: "", hash: "", workflow: { steps: [] }, children: {} },
		args: {},
		activations,
		status: "waiting",
	};
}

test("a withdrawn wait (no key) is not pending: no wake time, no event", () => {
	const wait = (key?: string): Activation => ({
		phase: "waiting",
		wait: {
			request: { kind: "event", event: "deploy", since: 0, deadline: 1000 },
			stepId: "w",
			...(key ? { key } : {}),
		},
	});
	const withdrawn = execution({ "root/w/attempt-1": wait() });
	assert.equal(earliestWaitDue(withdrawn), undefined);
	assert.equal(waitsForEvent(withdrawn, "deploy", {}), false);
	assert.equal(isPending(withdrawn.activations["root/w/attempt-1"]), false);

	const published = execution({ "root/w/attempt-2": wait("workflow_resume_x") });
	assert.equal(earliestWaitDue(published), 1000);
	assert.equal(waitsForEvent(published, "deploy", {}), true);
	assert.equal(isPending(published.activations["root/w/attempt-2"]), true);
});
