import { openStoreSession, resumeToolRequest } from "../../src/core/index.js";

const [directory, waitId, revision, phase] = process.argv.slice(2);
const store = await openStoreSession({ directory, trackRuns: true });
const transition = store.backend.transition.bind(store.backend);
store.backend.transition = async (request, signal) => {
	if (request.action.kind !== "handoff") return transition(request, signal);
	if (phase === "after") await transition(request, signal);
	process.send({ operationId: request.operationId, successor: request.action.successor.key });
	// Keep the process alive until the parent kills it at the publication boundary.
	await new Promise(() => setInterval(() => {}, 1000));
	throw new Error("Unreachable");
};
await resumeToolRequest({
	waitId,
	expectedRevision: Number(revision),
	response: 1,
	ctx: { storeSession: store },
});
throw new Error("Fixture failed to reach the handoff");
