import type { ToolEnvelope } from "../core/tool_runtime.js";

export type SdkApprovalRequest = NonNullable<ToolEnvelope["requiresApproval"]> & {
	resumeToken: string;
};
export type SdkInputRequest = NonNullable<ToolEnvelope["requiresInput"]> & { resumeToken: string };

/** SDK outcomes retain their existing wire shape (including status: "error"). */
export type SdkResult<T = any> = {
	runId?: string;
	output: T[];
} & (
	| {
			ok: true;
			status: "ok" | "cancelled";
			requiresApproval: null;
			requiresInput: null;
			error?: undefined;
	  }
	| {
			ok: true;
			status: "needs_approval";
			requiresApproval: SdkApprovalRequest;
			requiresInput: null;
			error?: undefined;
	  }
	| {
			ok: true;
			status: "needs_input";
			requiresApproval: null;
			requiresInput: SdkInputRequest;
			error?: undefined;
	  }
	| {
			ok: false;
			status: "error";
			requiresApproval: null;
			requiresInput: null;
			error: NonNullable<ToolEnvelope["error"]>;
	  }
);
