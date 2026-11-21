// Reading `llm.invoke` flags and the environment variables that stand in for them.
export function resolveModel(args: any, env: any) {
	return resolveEnvString(args.model, ["LOBSTER_LLM_MODEL", "LLM_TASK_MODEL"], env, "");
}

export function resolveEnvString(raw: any, envKeys: string[], env: any, fallback: string) {
	if (raw !== undefined && raw !== null && String(raw).trim()) return String(raw).trim();
	const fromEnv = getFirstEnv(env, envKeys);
	if (fromEnv && String(fromEnv).trim()) return String(fromEnv).trim();
	return fallback;
}

export function getFirstEnv(env: any, keys: string[]) {
	for (const key of keys) {
		if (env?.[key] !== undefined && env?.[key] !== null && String(env[key]).trim()) {
			return env[key];
		}
	}
	return undefined;
}

export function extractPrompt(args: any) {
	if (args.prompt) return String(args.prompt);
	if (Array.isArray(args._) && args._.length) {
		return args._.join(" ");
	}
	return "";
}

export function parseJsonArray(raw: any, label: string) {
	if (!raw) return [];
	try {
		const parsed = JSON.parse(String(raw));
		if (!Array.isArray(parsed)) throw new Error("must be array");
		return parsed;
	} catch {
		throw new Error(`${label} must be a JSON array`);
	}
}

export function parseJsonObject(raw: any, label: string) {
	if (!raw) return null;
	try {
		const parsed = JSON.parse(String(raw));
		if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
			throw new Error("must be an object");
		}
		return parsed;
	} catch {
		throw new Error(`${label} must be a JSON object`);
	}
}

export function parseOptionalNumber(value: any) {
	if (value === undefined || value === null) return null;
	const num = Number(value);
	return Number.isFinite(num) ? num : null;
}

export function flag(value: any) {
	if (value === undefined || value === null) return false;
	if (typeof value === "boolean") return value;
	if (typeof value === "string") {
		const normalized = value.trim().toLowerCase();
		if (["false", "0", "no"].includes(normalized)) return false;
		if (["true", "1", "yes"].includes(normalized)) return true;
	}
	return Boolean(value);
}
