/** Exactly one user decision. Runtime validation still protects JS/tool callers. */
export type ResumeDecision =
	| { approved: boolean; response?: undefined; cancel?: undefined }
	| { response: unknown; approved?: undefined; cancel?: undefined }
	| { cancel: true; approved?: undefined; response?: undefined };

export type WaitResumeOptions = ResumeDecision & {
	waitId: string;
	expectedRevision: number;
};

export type ResumeTarget =
	| { token: string; approvalId?: undefined; waitId?: undefined; expectedRevision?: undefined }
	| { approvalId: string; token?: undefined; waitId?: undefined; expectedRevision?: undefined }
	| { waitId: string; expectedRevision: number; token?: undefined; approvalId?: undefined };

/** An answer as callers pass it, checked to make exactly one decision (see decisionCount). */
export type Decision = { approved?: boolean; response?: unknown; cancel?: boolean };

/** How many decisions `options` makes (approve or reject, a response, a cancel): one is valid. */
export function decisionCount(options: {
	approved?: unknown;
	response?: unknown;
	cancel?: unknown;
}) {
	return (
		Number(typeof options.approved === "boolean") +
		Number(options.response !== undefined) +
		Number(options.cancel === true)
	);
}
