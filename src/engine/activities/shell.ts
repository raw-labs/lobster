// Running a `run:` step's shell command.
import { runAbortableProcess } from "../../process/abortable_process.js";

/** Run `command` through the system shell; a non-zero exit fails with its output. */
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
	cwd: string;
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
		// Spawning reports a missing working directory as a missing shell: name both.
		notFoundMessage: `workflow shell or working directory (${cwd}) not found; check LOBSTER_SHELL or ComSpec, or the step's cwd`,
	});
	if (code === 0) return { stdout, stderr };
	throw new Error(
		`workflow command failed (${code}): ${stderr.trim() || stdout.trim() || command}`,
	);
}
