import type { PersistenceOptions } from "../persistence/context.js";
import type { LlmSpendLedger } from "../core/llm_accounting.js";
export type RunContext = PersistenceOptions & {
	stdin: NodeJS.ReadableStream;
	stdout: NodeJS.WritableStream;
	stderr: NodeJS.WritableStream;
	env: Record<string, string | undefined>;
	mode: "human" | "tool" | "sdk";
	cwd?: string;
	signal?: AbortSignal;
	forceTerminationSignal?: AbortSignal;
	assertInvocationCurrent?: () => void;
	registry?: {
		get: (name: string) => any;
	};
	llmAdapters?: Record<string, any>;
	dryRun?: boolean;
	_llmSpendLedger?: LlmSpendLedger;
	_onResumeStateResolved?: (stateKey: string) => void;
	_onExecutionStarted?: () => void | Promise<void>;
	_onUnsafeDispatch?: () => void;
	_onNonRetryableSideEffect?: () => void;
};
