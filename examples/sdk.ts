// The SDK: a pipeline of your own JavaScript stages and Lobster's, paused for approval and
// resumed with the person's answer.
//
//   pnpm build && node examples/sdk.ts
//
// Runs are saved in LOBSTER_STATE_DIR (or the default state directory), like any other run.
import { createInterface } from "node:readline/promises";
import { Lobster, approve } from "@clawdbot/lobster";

type Email = { to: string; subject: string };

// A paused run is resumed by an instance with the same stages, so build it in one place.
function pipeline() {
	return (
		new Lobster()
			// approve() stops here and asks; with preview the request shows the items.
			.pipe(approve({ prompt: "Send these emails?", preview: true }))
			// Your own stage: it gets the items the stage before passed on.
			.pipe(async (emails: Email[]) =>
				emails.map((email) => `sent "${email.subject}" to ${email.to}`),
			)
	);
}

const emails: Email[] = [
	{ to: "ada@example.com", subject: "Your invoice" },
	{ to: "grace@example.com", subject: "Your invoice" },
];

const paused = await pipeline().run(emails);
if (paused.status !== "needs_approval")
	throw new Error(`Expected a question, got ${paused.status}`);

// The question, and a token to answer it with: from this process, or later from another one
// (the run is saved), or with `lobster resume <token> --approve yes|no`.
const { prompt, preview, resumeToken } = paused.requiresApproval;
console.log(preview);
const terminal = createInterface({ input: process.stdin, output: process.stdout });
const answer = await terminal.question(`${prompt} [y/N] `);
terminal.close();

const done = await pipeline().resume(resumeToken, { approved: /^y(es)?$/i.test(answer.trim()) });
console.log(done.status, done.output);
