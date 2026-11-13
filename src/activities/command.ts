import { runAbortableProcess } from "../abortable_process.js";
export async function runShellCommand({
	command,
	stdin,
	env,
	cwd,
	signal,
	forceTerminationSignal,
	assertInvocationCurrent,
	onEffectDispatch,
	killSignal,
}: {
	command: string;
	stdin: string | null;
	env: Record<string, string | undefined>;
	cwd?: string;
	signal?: AbortSignal;
	forceTerminationSignal?: AbortSignal;
	assertInvocationCurrent?: () => void;
	onEffectDispatch?: () => void;
	killSignal?: NodeJS.Signals | (() => NodeJS.Signals | undefined);
}) {
	signal?.throwIfAborted();
	const { stdout, stderr, code } = await runAbortableProcess({
		shellCommand: command,
		env,
		cwd,
		stdin,
		signal,
		assertInvocationCurrent,
		onEffectDispatch,
		forceTerminationSignal,
		killSignal,
		notFoundMessage: "workflow shell not found; check LOBSTER_SHELL or ComSpec",
	});
	if (code === 0) return { stdout, stderr };
	throw new Error(
		`workflow command failed (${code}): ${stderr.trim() || stdout.trim() || command}`,
	);
}
