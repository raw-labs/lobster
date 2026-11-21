import test from "node:test";
import assert from "node:assert/strict";

import {
	formatSchemaError,
	InvalidSchemaError,
	prepareSchema,
	schemaValidationMessage,
	withSchemaCompiler,
	type HostSchemaCompiler,
} from "../src/util/validation.js";

const validateValue = (schema: unknown, value: unknown, options?: { allErrors?: boolean }) =>
	prepareSchema(schema).validate(value, options);

const errorsOf = (schema: unknown, value: unknown, allErrors = true) =>
	validateValue(schema, value, { allErrors }).errors.map(formatSchemaError);

test("prepareSchema reuses validators for structurally equivalent schemas", () => {
	const first = prepareSchema({
		type: "object",
		properties: { value: { type: "string" } },
		required: ["value"],
	});
	const second = prepareSchema({
		required: ["value"],
		properties: { value: { type: "string" } },
		type: "object",
	});
	assert.equal(second, first);
	assert.deepEqual(first.validate({ value: "ok" }), { ok: true, errors: [] });
});

test("errors are reported as <path> <message>, first only unless all are asked for", () => {
	const schema = {
		type: "object",
		properties: { n: { type: "integer" }, label: { type: "string", minLength: 2 } },
		required: ["n", "label"],
		additionalProperties: false,
	};
	const value = { n: "x", label: "", extra: 1 };
	assert.deepEqual(errorsOf(schema, value), [
		"/extra is not allowed",
		"/n must be integer",
		"/label must not have fewer than 2 characters",
	]);
	assert.equal(errorsOf(schema, value, false).length, 1);
	assert.equal(
		schemaValidationMessage(
			"ask response",
			validateValue(schema, { n: 1, label: "ok", x: 1 }).errors,
		),
		"ask response failed schema validation at /x: is not allowed",
	);
	assert.deepEqual(errorsOf({ type: "integer" }, "x"), ["/ must be integer"]);
	assert.deepEqual(
		errorsOf({ type: "array", items: [{ type: "string" }], additionalItems: false }, ["a", 1]),
		["/1 is not allowed"],
	);
});

test("schemas are read as draft-07, as Ajv read them", () => {
	const valid = (schema: unknown, value: unknown) => validateValue(schema, value).ok;
	// format is not checked.
	assert.equal(valid({ type: "string", format: "email" }, "not an email"), true);
	assert.equal(valid({ type: "string", format: "made-up" }, "x"), true);
	// Keywords from later drafts, and unknown ones, are ignored.
	assert.equal(valid({ type: "object", dependentRequired: { a: ["b"] } }, { a: 1 }), true);
	assert.equal(valid({ type: "array", prefixItems: [{ type: "string" }] }, [1]), true);
	assert.equal(
		valid({ properties: { a: {} }, unevaluatedProperties: false }, { a: 1, b: 2 }),
		true,
	);
	assert.equal(valid({ type: "string", "x-ui": { widget: "text" } }, "x"), true);
	// Draft-07 keywords apply, including next to $ref and in tuples.
	assert.equal(valid({ type: "object", dependencies: { a: ["b"] } }, { a: 1 }), false);
	assert.equal(
		valid({ definitions: { s: { type: "string" } }, $ref: "#/definitions/s", minLength: 3 }, "ab"),
		false,
	);
	assert.equal(valid({ items: [{ type: "string" }, { type: "number" }] }, ["a", "b"]), false);
	assert.equal(valid({ definitions: { any: true }, $ref: "#/definitions/any" }, 1), true);
	assert.equal(valid({ definitions: { none: false }, $ref: "#/definitions/none" }, 1), false);
	// nullable (OpenAPI) allows null, as Ajv's nullable keyword did.
	assert.equal(valid({ type: "string", nullable: true }, null), true);
	assert.equal(valid({ type: "string", nullable: true }, 1), false);
	// A format inside a property named "format" is a property, not the keyword.
	assert.equal(
		valid({ type: "object", properties: { format: { type: "string" } } }, { format: 1 }),
		false,
	);
	// The caller's schema is not changed.
	const schema = { type: "string", format: "email", nullable: true };
	validateValue(schema, "x");
	assert.deepEqual(schema, { type: "string", format: "email", nullable: true });
});

test("schemas that cannot be used are refused when prepared", () => {
	const refused = (schema: unknown, pattern: RegExp) =>
		assert.throws(
			() => prepareSchema(schema),
			(error: unknown) => {
				assert.ok(error instanceof InvalidSchemaError, String(error));
				assert.match((error as Error).message, pattern);
				return true;
			},
		);
	refused({ type: "nope" }, /^schema is invalid: data\/type /);
	refused({ type: "string", minLength: "x" }, /^schema is invalid: data\/minLength /);
	refused({ required: "a" }, /^schema is invalid: data\/required /);
	refused({ type: "string", pattern: "(" }, /pattern/);
	refused({ $ref: "#/definitions/missing" }, /can't resolve reference #\/definitions\/missing/);
	refused({ $ref: "https://example.test/other" }, /can't resolve reference/);
	refused({ $schema: "https://json-schema.org/draft/2020-12/schema" }, /no schema with key or ref/);
	refused("string", /must be an object or boolean/);
	assert.equal(
		prepareSchema({ $schema: "http://json-schema.org/draft-07/schema#" }).validate(1).ok,
		true,
	);
});

test("schemas that share an $id stay separate", () => {
	const id = "https://example.test/shared";
	const strings = prepareSchema({ $id: id, type: "string" });
	const numbers = prepareSchema({ $id: id, type: "number" });
	assert.equal(strings.validate("a").ok, true);
	assert.equal(strings.validate(1).ok, false);
	assert.equal(numbers.validate(1).ok, true);
	assert.equal(numbers.validate("a").ok, false);
	// A reference to an $id inside the same schema resolves.
	const nested = prepareSchema({
		definitions: { s: { $id: "https://example.test/s", type: "string" } },
		$ref: "https://example.test/s",
	});
	assert.equal(nested.validate("a").ok, true);
	assert.equal(nested.validate(1).ok, false);
});

test("withSchemaCompiler prepares schemas with the host's compiler for its callback", async () => {
	const calls: {
		schema: unknown;
		options: { scope: object; allErrors: boolean; addUsedSchema: boolean };
	}[] = [];
	const compiler: HostSchemaCompiler = (schema, options) => {
		calls.push({ schema, options });
		const validate = Object.assign(
			(value: unknown) => {
				validate.errors =
					value === "host-ok" ? null : [{ instancePath: "/host", message: "says no" }];
				return value === "host-ok";
			},
			{ errors: null as { instancePath: string; message: string }[] | null },
		);
		return validate;
	};
	const schema = { type: "string", title: "host test" };
	const inside = await withSchemaCompiler(compiler, async () => {
		await new Promise((resolve) => setImmediate(resolve));
		return [
			validateValue(schema, "host-ok"),
			validateValue(schema, "plain"),
			validateValue(schema, 1, { allErrors: true }),
		];
	});
	assert.deepEqual(
		inside.map((r) => r.ok),
		[true, false, false],
	);
	assert.deepEqual(inside[1].errors, [{ path: "/host", message: "says no" }]);
	assert.deepEqual(
		calls.map((c) => [c.options.allErrors, c.options.addUsedSchema]),
		[
			[false, false],
			[true, false],
		],
		"one compile per error mode, never registering the schema",
	);
	assert.ok(calls.every((c) => c.options.scope === calls[0].options.scope && c.schema === schema));
	// Outside the callback Lobster's own engine is used again.
	assert.equal(validateValue(schema, "plain").ok, true);
	assert.equal(calls.length, 2);
	// A host compiler that refuses a schema reports it as invalid.
	assert.throws(
		() =>
			withSchemaCompiler(
				() => {
					throw new Error("host refuses");
				},
				() => prepareSchema({ title: "refused by host" }),
			),
		(error: unknown) =>
			error instanceof InvalidSchemaError && /host refuses/.test((error as Error).message),
	);
});

test("a host's compiler checks a resumed answer through runToolRequest and resumeToolRequest", async (t) => {
	const {
		runToolRequest,
		resumeToolRequest,
		withSchemaCompiler: hostHook,
	} = await import("../src/core/index.js");
	const { mkdtemp, rm } = await import("node:fs/promises");
	const { tmpdir } = await import("node:os");
	const path = await import("node:path");
	const dir = await mkdtemp(path.join(tmpdir(), "lobster-host-compiler-"));
	t.after(() => rm(dir, { recursive: true, force: true }));
	const env = { ...process.env, LOBSTER_STATE_DIR: dir };
	const compiled: unknown[] = [];
	// Shaped like OpenClaw's compileLobsterSchema: it refuses 13, which the schema allows.
	const compiler: HostSchemaCompiler = (schema) => {
		compiled.push(schema);
		const validate = Object.assign(
			(value: unknown) => {
				const ok = typeof value === "number" && value !== 13;
				validate.errors = ok ? null : [{ instancePath: "", message: "is unlucky" }];
				return ok;
			},
			{ errors: null as { instancePath: string; message: string }[] | null },
		);
		return validate;
	};
	const paused = await hostHook(compiler, () =>
		runToolRequest({
			pipeline: `ask --emit --prompt Pick --schema '{"type":"integer"}'`,
			ctx: { env },
		}),
	);
	assert.equal(paused.status, "needs_input", JSON.stringify(paused));
	const token = paused.requiresInput!.resumeToken;
	const refused = await hostHook(compiler, () =>
		resumeToolRequest({ token, response: 13, ctx: { env } }),
	);
	assert.equal(refused.ok, false);
	assert.match(refused.error!.message, /failed schema validation at \/: is unlucky/);
	assert.ok(compiled.some((schema) => JSON.stringify(schema) === '{"type":"integer"}'));
	// Without the host's compiler, Lobster's own engine accepts 13.
	const accepted = await resumeToolRequest({ token, response: 13, ctx: { env } });
	assert.equal(accepted.ok, true, JSON.stringify(accepted));
});
