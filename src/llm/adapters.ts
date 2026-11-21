// Where `llm.invoke` sends a request: the provider chosen from flags, environment and host
// (`ctx.llmAdapters`), and the built-in OpenClaw, Pi and HTTP adapters.
import { httpResponseLimitFromEnv, readResponseTextCapped } from "../io/read_response_text.js";
import type { LlmResponseEnvelope } from "./protocol.js";
import type { CommandConfig } from "../commands/stdlib/llm_invoke.js";

type BuiltInProvider = "openclaw" | "pi" | "http";

export type SupportedProvider = BuiltInProvider | string;

type Adapter = {
	provider: SupportedProvider;
	source: string;
	invoke: (params: {
		env: any;
		args: any;
		payload: Record<string, any>;
		signal?: AbortSignal;
		assertInvocationCurrent?: () => void;
	}) => Promise<LlmResponseEnvelope>;
};

/** A host's adapter (`ctx.llmAdapters`): its invoke function, or one with a source name. */
type DirectInvoke = (params: {
	env: any;
	args: any;
	payload: Record<string, any>;
	ctx: any;
	signal?: AbortSignal;
	assertInvocationCurrent?: () => void;
}) => Promise<LlmResponseEnvelope>;
type DirectAdapter = DirectInvoke | { source?: string; invoke: DirectInvoke };

/** The built-in HTTP adapters: the variables each reads its URL and token from. */
const HTTP_ADAPTERS = {
	pi: { url: "LOBSTER_PI_LLM_ADAPTER_URL", token: "LOBSTER_PI_LLM_ADAPTER_TOKEN" },
	http: { url: "LOBSTER_LLM_ADAPTER_URL", token: "LOBSTER_LLM_ADAPTER_TOKEN" },
} as const;

export function resolveProvider(
	args: any,
	env: any,
	defaultProvider?: SupportedProvider | null,
	ctx?: any,
): SupportedProvider {
	const explicit = String(args.provider ?? env.LOBSTER_LLM_PROVIDER ?? "")
		.trim()
		.toLowerCase();
	if (explicit) {
		if (explicit === "openclaw" || explicit === "pi" || explicit === "http") {
			return explicit;
		}
		if (getDirectAdapter(ctx, explicit)) {
			return explicit;
		}
		throw new Error(`Unsupported llm provider: ${explicit}`);
	}
	if (defaultProvider) return defaultProvider;
	const directAdapters =
		ctx?.llmAdapters && typeof ctx.llmAdapters === "object"
			? Object.keys(ctx.llmAdapters).filter((key) => getDirectAdapter(ctx, key))
			: [];
	if (directAdapters.length === 1) return directAdapters[0];
	if (String(env.LOBSTER_PI_LLM_ADAPTER_URL ?? "").trim()) return "pi";
	if (String(env.OPENCLAW_URL ?? env.CLAWD_URL ?? "").trim()) return "openclaw";
	if (String(env.LOBSTER_LLM_ADAPTER_URL ?? "").trim()) return "http";
	throw new Error(
		"llm.invoke could not resolve a provider. Set --provider or LOBSTER_LLM_PROVIDER",
	);
}

export function resolveAdapter({
	provider,
	env,
	args,
	config,
	ctx,
}: {
	provider: SupportedProvider;
	env: any;
	args: any;
	config: CommandConfig;
	ctx: any;
}): Adapter {
	const direct = getDirectAdapter(ctx, provider);
	if (direct) {
		const invoke = typeof direct === "function" ? direct : direct.invoke;
		return {
			provider,
			source: typeof direct === "function" ? provider : (direct.source ?? provider),
			async invoke({ payload, signal, assertInvocationCurrent }) {
				return invoke({ env, args, payload, ctx, signal, assertInvocationCurrent });
			},
		};
	}

	const maxResponseBytes = httpResponseLimitFromEnv(env);
	if (provider === "openclaw") {
		const openclawUrl = String(env.OPENCLAW_URL ?? env.CLAWD_URL ?? "").trim();
		if (!openclawUrl) {
			throw new Error(`${config.name} requires OPENCLAW_URL (or CLAWD_URL) for provider=openclaw`);
		}
		const endpoint = new URL("/tools/invoke", openclawUrl);
		const token = String(args.token ?? env.OPENCLAW_TOKEN ?? env.CLAWD_TOKEN ?? "").trim();
		return {
			provider,
			source: config.sourceForProvider(provider),
			async invoke({ payload, signal, assertInvocationCurrent }) {
				return invokeOpenClawAdapter({
					endpoint,
					token,
					payload,
					signal,
					maxResponseBytes,
					assertInvocationCurrent,
				});
			},
		};
	}

	const variables = HTTP_ADAPTERS[provider === "pi" ? "pi" : "http"];
	const adapterUrl = String(env[variables.url] ?? "").trim();
	if (!adapterUrl) {
		throw new Error(`${config.name} requires ${variables.url} for provider=${provider}`);
	}
	const endpoint = buildAdapterEndpoint(adapterUrl);
	const token = String(args.token ?? env[variables.token] ?? "").trim();
	return {
		provider,
		source: config.sourceForProvider(provider),
		async invoke({ payload, signal, assertInvocationCurrent }) {
			return invokeHttpAdapter({
				endpoint,
				maxResponseBytes,
				token,
				payload,
				signal,
				assertInvocationCurrent,
			});
		},
	};
}

function getDirectAdapter(ctx: any, provider: string): DirectAdapter | null {
	const adapters = ctx?.llmAdapters;
	if (!adapters || typeof adapters !== "object") return null;
	const adapter = adapters[provider];
	if (typeof adapter === "function") return adapter as DirectAdapter;
	if (adapter && typeof adapter === "object" && typeof adapter.invoke === "function") {
		return adapter as DirectAdapter;
	}
	return null;
}

function buildAdapterEndpoint(rawUrl: string) {
	const endpoint = new URL(rawUrl);
	if (endpoint.pathname === "/" || endpoint.pathname === "") {
		endpoint.pathname = "/invoke";
	}
	return endpoint;
}

async function invokeOpenClawAdapter({
	endpoint,
	token,
	payload,
	signal,
	maxResponseBytes,
	assertInvocationCurrent,
}: {
	endpoint: URL;
	token: string;
	payload: any;
	signal?: AbortSignal;
	maxResponseBytes?: number;
	assertInvocationCurrent?: () => void;
}) {
	// Keep validation local: gateway schema errors discard the JSON needed for retries.
	const prompt = [
		payload.prompt,
		...(payload.outputSchema
			? ["Return JSON matching this JSON Schema: " + JSON.stringify(payload.outputSchema)]
			: []),
		...(payload.retryContext
			? ["Correct the previous validation failure: " + JSON.stringify(payload.retryContext)]
			: []),
	].join("\n\n");
	assertInvocationCurrent?.();
	const res = await fetch(endpoint, {
		method: "POST",
		signal,
		headers: {
			"content-type": "application/json",
			...(token ? { authorization: `Bearer ${token}` } : null),
		},
		body: JSON.stringify({
			tool: "llm-task",
			action: "invoke",
			args: {
				prompt,
				input: payload.artifacts,
				model: payload.model,
				temperature: payload.temperature,
				maxTokens: payload.maxOutputTokens,
			},
		}),
	});

	const text = await readResponseTextCapped(res, maxResponseBytes);
	if (!res.ok) {
		throw new Error(`${res.status} ${res.statusText}: ${text.slice(0, 400)}`);
	}

	let parsed: any;
	try {
		parsed = text ? JSON.parse(text) : null;
	} catch {
		throw new Error("Response was not JSON");
	}

	if (parsed?.ok !== true) {
		throw new Error(
			"openclaw adapter error: " + (parsed?.error?.message ?? "Invalid gateway response"),
		);
	}
	const result = parsed.result;
	const details = result?.details;
	if (
		result?.isError ||
		!details ||
		typeof details !== "object" ||
		!Object.hasOwn(details, "json")
	) {
		throw new Error("openclaw adapter received invalid llm-task result");
	}
	return {
		ok: true,
		result: {
			prompt: payload.prompt,
			...(typeof details.model === "string" ? { model: details.model } : {}),
			output: { format: "json", text: JSON.stringify(details.json), data: details.json },
		},
	} satisfies LlmResponseEnvelope;
}

async function invokeHttpAdapter({
	endpoint,
	token,
	payload,
	signal,
	maxResponseBytes,
	assertInvocationCurrent,
}: {
	endpoint: URL;
	token: string;
	payload: any;
	signal?: AbortSignal;
	maxResponseBytes?: number;
	assertInvocationCurrent?: () => void;
}) {
	assertInvocationCurrent?.();
	const res = await fetch(endpoint, {
		method: "POST",
		signal,
		headers: {
			"content-type": "application/json",
			...(token ? { authorization: `Bearer ${token}` } : null),
		},
		body: JSON.stringify(payload),
	});

	const text = await readResponseTextCapped(res, maxResponseBytes);
	if (!res.ok) {
		throw new Error(`${res.status} ${res.statusText}: ${text.slice(0, 400)}`);
	}

	let parsed: any;
	try {
		parsed = text ? JSON.parse(text) : null;
	} catch {
		throw new Error("Response was not JSON");
	}

	if (parsed && typeof parsed === "object" && !Array.isArray(parsed) && "ok" in parsed) {
		return parsed as LlmResponseEnvelope;
	}
	return { ok: true, result: parsed } as LlmResponseEnvelope;
}
