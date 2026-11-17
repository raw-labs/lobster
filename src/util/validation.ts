/**
 * JSON Schema validation for every schema Lobster checks: `ask`, input requests,
 * workflow input steps and `llm.invoke`. This is the only module that knows the
 * engine.
 *
 * The engine is TypeBox's interpreting checker (`typebox/schema`: `CheckSchema` and
 * `Errors`). It walks the schema for each value and never generates code, so it runs
 * where code generation from strings is disabled (OpenClaw's single-file build). Its
 * `Compile` and `Build`, which do generate code, are never used here.
 *
 * Schemas follow draft-07 as Lobster's previous engine (Ajv, `strict: false`) read
 * them:
 * - `format` is not checked;
 * - keywords from later drafts (`prefixItems`, `dependentRequired`,
 *   `unevaluatedProperties`, ...) and unknown keywords are ignored;
 * - `nullable: true` also allows null;
 * - an invalid schema, an unresolvable `$ref` or a `$schema` other than draft-07 is
 *   refused when the validator is prepared.
 * Each prepared schema stands alone: schemas never see each other's `$id`, so
 * different schemas may repeat one.
 */
import { AsyncLocalStorage } from "node:async_hooks";
import { CheckContext, CheckSchema, Errors, Meta, NextStack, Resolve, Stack } from "typebox/schema";

import { stableStringify } from "../state/serialization.js";

/** One way a value does not match its schema. `path` is a JSON pointer, "/" for the value itself. */
export type SchemaError = { path: string; message: string };

export type ValidationResult = { ok: true; errors: [] } | { ok: false; errors: SchemaError[] };

export type SchemaValidator = {
	/** Checks `value`. Reports only the first error unless `allErrors` is set. */
	validate(value: unknown, options?: { allErrors?: boolean }): ValidationResult;
};

/** A schema that cannot be used: invalid, unresolvable, or of another draft. */
export class InvalidSchemaError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "InvalidSchemaError";
	}
}

/** "<path> <message>", the form every caller reports, and what `llm.invoke` shows the model. */
export function formatSchemaError(error: SchemaError): string {
	return `${error.path} ${error.message}`.trim();
}

/** "<subject> failed schema validation at <path>: <message>" for the first error. */
export function schemaValidationMessage(subject: string, errors: SchemaError[]): string {
	const first = errors[0];
	if (!first) return `${subject} failed schema validation`;
	return `${subject} failed schema validation at ${first.path}: ${first.message}`;
}

/**
 * Prepares a validator for `schema`, throwing `InvalidSchemaError` when the schema
 * cannot be used.
 *
 * Preparing checks the schema against the draft-07 metaschema and resolves its
 * references, so validators are kept, by schema content, for reuse. The previous
 * engine had to keep every schema it compiled (it retained them internally); this
 * one keeps nothing, so the cache is bounded instead.
 */
export function prepareSchema(schema: unknown): SchemaValidator {
	const compiler = hostCompiler.getStore();
	const key = stableStringify(schema);
	const cache = compiler ? hostCache(compiler) : ownCache;
	let validator = cache.get(key);
	if (!validator) {
		validator = compiler ? fromHostCompiler(compiler, schema) : prepareOwn(schema);
		cache.set(key, validator);
	}
	return validator;
}

/** Prepares `schema` and validates `value` against it. */
export function validateValue(
	schema: unknown,
	value: unknown,
	options?: { allErrors?: boolean },
): ValidationResult {
	return prepareSchema(schema).validate(value, options);
}

// ------------------------------------------------------------------
// Host compilers
// ------------------------------------------------------------------

/**
 * The compiler contract of `withSchemaCompiler`, as OpenClaw's Lobster plugin calls
 * it: `.errors` is set by each call, null after a value that matches.
 */
export type HostSchemaCompiler = (
	schema: any,
	options: { scope: object; allErrors: boolean; addUsedSchema: boolean },
) => {
	(value: unknown): boolean;
	errors?: { instancePath?: string; message?: string }[] | null;
};

const hostCompiler = new AsyncLocalStorage<HostSchemaCompiler>();
/** The one scope Lobster names to host compilers. Nothing is registered in it. */
const hostScope = Object.freeze({ owner: "lobster" });

/**
 * Runs `callback` with `compiler` preparing every schema Lobster checks during it,
 * including in async work it starts. Other callers are not affected.
 *
 * Lobster's own engine needs no code generation, so a host does not need this; it
 * remains so that a host that relies on its own validator keeps that validator.
 * Schemas are compiled with `addUsedSchema: false`: each stands alone, as with
 * Lobster's own engine.
 */
export function withSchemaCompiler<T>(compiler: HostSchemaCompiler, callback: () => T): T {
	return hostCompiler.run(compiler, callback);
}

const hostCaches = new WeakMap<HostSchemaCompiler, BoundedCache<SchemaValidator>>();

function hostCache(compiler: HostSchemaCompiler) {
	let cache = hostCaches.get(compiler);
	if (!cache) {
		cache = new BoundedCache(CACHE_SIZE);
		hostCaches.set(compiler, cache);
	}
	return cache;
}

function fromHostCompiler(compile: HostSchemaCompiler, schema: unknown): SchemaValidator {
	const prepare = (allErrors: boolean) => {
		try {
			return compile(schema, { scope: hostScope, allErrors, addUsedSchema: false });
		} catch (error) {
			throw new InvalidSchemaError((error as Error)?.message ?? String(error));
		}
	};
	const first = prepare(false);
	let all: ReturnType<HostSchemaCompiler> | undefined;
	return {
		validate(value, options) {
			const check = options?.allErrors ? (all ??= prepare(true)) : first;
			if (check(value)) return { ok: true, errors: [] };
			const errors = (check.errors ?? []).map((error) => ({
				path: error.instancePath || "/",
				message: error.message ?? "does not match the schema",
			}));
			return {
				ok: false,
				errors: errors.length ? errors : [{ path: "/", message: "does not match the schema" }],
			};
		},
	};
}

// ------------------------------------------------------------------
// Lobster's own engine
// ------------------------------------------------------------------

const CACHE_SIZE = 256;
const DRAFT_7 = "http://json-schema.org/draft-07/schema#";

/** Most recently used entries, up to `size`. */
class BoundedCache<T> {
	#entries = new Map<string, T>();
	constructor(readonly size: number) {}
	get(key: string) {
		const value = this.#entries.get(key);
		if (value !== undefined) {
			this.#entries.delete(key);
			this.#entries.set(key, value);
		}
		return value;
	}
	set(key: string, value: T) {
		this.#entries.delete(key);
		this.#entries.set(key, value);
		if (this.#entries.size > this.size) {
			this.#entries.delete(this.#entries.keys().next().value!);
		}
	}
}

const ownCache = new BoundedCache<SchemaValidator>(CACHE_SIZE);

function prepareOwn(schema: unknown): SchemaValidator {
	if (typeof schema === "boolean") return engineValidator(schema);
	if (!isObject(schema)) throw new InvalidSchemaError("schema must be an object or boolean");
	const declared = schema.$schema;
	if (declared !== undefined && declared !== DRAFT_7 && declared !== DRAFT_7.slice(0, -1)) {
		throw new InvalidSchemaError(`no schema with key or ref "${String(declared)}"`);
	}
	const invalid = firstEngineError(metaschema, schema);
	if (invalid)
		throw new InvalidSchemaError(
			`schema is invalid: data${invalid.instancePath} ${invalid.message}`,
		);
	const prepared = asDraft7(schema) as Record<string, unknown>;
	assertReferencesResolve(prepared);
	return engineValidator(prepared);
}

function engineValidator(schema: boolean | Record<string, unknown>): SchemaValidator {
	const stack = Stack({}, schema);
	return {
		validate(value, options) {
			// A fresh context per value: it carries state only for that check.
			if (CheckSchema(stack, new CheckContext(), schema, value)) return { ok: true, errors: [] };
			const errors = engineErrors(schema, value);
			return { ok: false, errors: options?.allErrors ? errors : errors.slice(0, 1) };
		},
	};
}

function firstEngineError(schema: Record<string, unknown>, value: unknown) {
	if (CheckSchema(Stack({}, schema), new CheckContext(), schema, value)) return undefined;
	return Errors({}, schema, value)[1][0];
}

/**
 * The engine's errors as Lobster reports them. A property or item that a `false`
 * subschema refuses (`additionalProperties: false`, `additionalItems: false`) is
 * reported at its own path as not allowed, in place of the engine's
 * "schema is false" plus a separate summary that does not name it.
 */
function engineErrors(schema: boolean | Record<string, unknown>, value: unknown): SchemaError[] {
	const errors = Errors({}, schema, value)[1];
	// Objects whose refused properties were each named: their summary adds nothing.
	const named = new Set<string>();
	const result: SchemaError[] = [];
	for (const error of errors) {
		const refused = /\/additional(Properties|Items)$/.exec(error.schemaPath);
		if (error.keyword === "boolean" && refused) {
			const owner = error.instancePath.slice(0, error.instancePath.lastIndexOf("/"));
			named.add(`${error.schemaPath.slice(0, refused.index)} ${owner}`);
			result.push({ path: error.instancePath || "/", message: "is not allowed" });
			continue;
		}
		if (
			error.keyword === "additionalProperties" &&
			named.has(`${error.schemaPath} ${error.instancePath}`)
		) {
			continue;
		}
		result.push({ path: error.instancePath || "/", message: error.message });
	}
	return result.length ? result : [{ path: "/", message: "does not match the schema" }];
}

/** Keywords from drafts after 07 that the previous engine ignored as unknown. */
const LATER_DRAFT_KEYWORDS = [
	"$anchor",
	"$dynamicAnchor",
	"$dynamicRef",
	"$recursiveAnchor",
	"$recursiveRef",
	"$vocabulary",
	"dependentRequired",
	"dependentSchemas",
	"maxContains",
	"minContains",
	"prefixItems",
	"unevaluatedItems",
	"unevaluatedProperties",
];
/** Keywords whose value is one subschema. */
const SUBSCHEMA = [
	"additionalItems",
	"additionalProperties",
	"contains",
	"else",
	"if",
	"not",
	"propertyNames",
	"then",
];
/** Keywords whose value is a list of subschemas. */
const SUBSCHEMA_LIST = ["allOf", "anyOf", "oneOf"];
/** Keywords whose value maps names to subschemas. */
const SUBSCHEMA_MAP = ["$defs", "definitions", "patternProperties", "properties"];

/**
 * A copy of `schema` that the engine checks the way draft-07 (as Ajv read it) does.
 * The caller's schema is never changed.
 */
function asDraft7(schema: unknown, inDefinitions = false): unknown {
	if (typeof schema === "boolean") {
		// The engine resolves a `$ref` only to an object schema.
		return inDefinitions ? (schema ? {} : { not: {} }) : schema;
	}
	if (!isObject(schema)) return schema;
	const result: Record<string, unknown> = { ...schema };
	delete result.format;
	// The engine compiles patterns while checking values: refuse a bad one now.
	if (typeof result.pattern === "string") assertPattern(result.pattern);
	if (isObject(result.patternProperties))
		Object.keys(result.patternProperties).forEach(assertPattern);
	for (const keyword of LATER_DRAFT_KEYWORDS) delete result[keyword];
	if (result.nullable === true && result.type !== undefined) {
		const types = Array.isArray(result.type) ? result.type : [result.type];
		if (!types.includes("null")) result.type = [...types, "null"];
	}
	delete result.nullable;
	for (const keyword of SUBSCHEMA) {
		if (keyword in result) result[keyword] = asDraft7(result[keyword]);
	}
	for (const keyword of SUBSCHEMA_LIST) {
		if (Array.isArray(result[keyword]))
			result[keyword] = (result[keyword] as unknown[]).map((s) => asDraft7(s));
	}
	if ("items" in result) {
		result.items = Array.isArray(result.items)
			? result.items.map((s) => asDraft7(s))
			: asDraft7(result.items);
	}
	for (const keyword of SUBSCHEMA_MAP) {
		const map = result[keyword];
		if (!isObject(map)) continue;
		const definitions = keyword === "$defs" || keyword === "definitions";
		result[keyword] = Object.fromEntries(
			Object.entries(map).map(([name, s]) => [name, asDraft7(s, definitions)]),
		);
	}
	if (isObject(result.dependencies)) {
		result.dependencies = Object.fromEntries(
			Object.entries(result.dependencies).map(([name, d]) => [
				name,
				Array.isArray(d) ? d : asDraft7(d),
			]),
		);
	}
	return result;
}

/** Every `$ref` must point inside the schema: nothing else is registered. */
function assertReferencesResolve(schema: Record<string, unknown>) {
	const visit = (node: unknown, parent: ReturnType<typeof Stack>) => {
		if (!isObject(node)) return;
		const stack = NextStack(parent, node);
		if (typeof node.$ref === "string" && Resolve.Ref(stack, node as any).schema === undefined) {
			throw new InvalidSchemaError(`can't resolve reference ${node.$ref}`);
		}
		for (const keyword of [...SUBSCHEMA, "items"]) {
			const value = node[keyword];
			for (const child of Array.isArray(value) ? value : [value]) visit(child, stack);
		}
		for (const keyword of SUBSCHEMA_LIST) {
			if (Array.isArray(node[keyword]))
				for (const child of node[keyword] as unknown[]) visit(child, stack);
		}
		for (const keyword of [...SUBSCHEMA_MAP, "dependencies"]) {
			const map = node[keyword];
			if (isObject(map)) for (const child of Object.values(map)) visit(child, stack);
		}
	};
	visit(schema, Stack({}, schema));
}

function assertPattern(pattern: string) {
	try {
		new RegExp(pattern, "u");
	} catch (error) {
		throw new InvalidSchemaError(
			`schema is invalid: pattern ${JSON.stringify(pattern)}: ${(error as Error).message}`,
		);
	}
}

function isObject(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** The draft-07 metaschema, checked without `format` like every other schema. */
const metaschema = asDraft7(Meta[DRAFT_7]) as Record<string, unknown>;
