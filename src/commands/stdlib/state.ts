import { defineCommand } from "../define.js";
import { carryLlmProvenance } from "../../llm/accounting.js";
import {
	defaultStateDir,
	keyToPath,
	writeStateJson,
	readStateTextWithLock,
} from "../../state/store.js";
import { streamOf } from "../../io/streams.js";

// What this process last wrote to each state key. A value read straight back is the same value
// rebuilt from its own JSON, and the marks a command attached in-process are not in that JSON:
// without this, `llm.invoke | state.set k | state.get k` turns a replay that cost nothing into an
// item indistinguishable from one that was paid for. The remembered text has to match the stored
// text byte for byte, so nothing written by anything else can pick up marks it was never given.
const lastWritten = new Map<string, { text: string; value: unknown }>();
const MAX_REMEMBERED_WRITES = 64;

function rememberWrite(filePath: string, text: string, value: unknown) {
	lastWritten.set(filePath, { text, value });
	for (const oldest of lastWritten.keys()) {
		if (lastWritten.size <= MAX_REMEMBERED_WRITES) break;
		lastWritten.delete(oldest);
	}
}

async function readRememberedState({
	env,
	key,
	signal,
}: {
	env: Record<string, string | undefined>;
	key: string;
	signal?: AbortSignal;
}) {
	const text = await readStateTextWithLock({ env, key, signal });
	if (text === null) return null;
	const value = JSON.parse(text);
	const written = lastWritten.get(keyToPath(defaultStateDir(env), key));
	if (written?.text === text) carryLlmProvenance(written.value, value);
	return value;
}

export const stateGetCommand = defineCommand({
	name: "state.get",
	meta: {
		description: "Read a JSON value from Lobster state",
		argsSchema: {
			type: "object",
			properties: {
				_: { type: "array", items: { type: "string" }, description: "Key" },
			},
			required: ["_"],
		},
		sideEffects: ["reads_state"],
		repeatable: true,
	},
	help() {
		return `state.get — read a JSON value from Lobster state\n\nUsage:\n  state.get <key>\n\nEnv:\n  LOBSTER_STATE_DIR overrides storage directory\n`;
	},
	async run({ args, ctx }) {
		const key = args._[0];
		if (!key) throw new Error("state.get requires a key");

		const value = await readRememberedState({ env: ctx.env, key, signal: ctx.signal });

		return { output: streamOf([value]) };
	},
});

export const stateSetCommand = defineCommand({
	name: "state.set",
	meta: {
		description: "Write a JSON value to Lobster state",
		argsSchema: {
			type: "object",
			properties: {
				_: { type: "array", items: { type: "string" }, description: "Key" },
			},
			required: ["_"],
		},
		sideEffects: ["writes_state"],
		repeatable: true,
	},
	help() {
		return `state.set — write a JSON value to Lobster state\n\nUsage:\n  <value> | state.set <key>\n\nNotes:\n  - Consumes the entire input stream; stores a single JSON value.\n`;
	},
	async run({ input, args, ctx }) {
		const key = args._[0];
		if (!key) throw new Error("state.set requires a key");

		const items = [];
		for await (const item of input) items.push(item);

		const value = items.length === 1 ? items[0] : items;

		const { text } = await writeStateJson({
			env: ctx.env,
			key,
			value,
			signal: ctx.signal,
			assertInvocationCurrent: ctx.assertInvocationCurrent,
		});
		const filePath = keyToPath(defaultStateDir(ctx.env), key);
		rememberWrite(filePath, text, value);

		return { output: streamOf([value]) };
	},
});
