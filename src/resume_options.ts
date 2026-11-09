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
