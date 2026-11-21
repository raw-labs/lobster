// The condition evaluator before the AST rewrite, kept as the reference the rewrite is
// compared against (test/condition_ast.test.ts). Do not use it elsewhere.
import { isDeepStrictEqual } from "node:util";
import type { WorkflowStepResult } from "../../src/workflows/types.js";
import { getStepRefValue } from "../../src/workflows/expressions.js";

export function evaluateConditionReference(
	condition: unknown,
	results: Record<string, WorkflowStepResult>,
) {
	if (condition === undefined || condition === null) return true;
	if (typeof condition === "boolean") return condition;
	if (typeof condition !== "string") throw new Error("Unsupported condition type");

	const trimmed = condition.trim();
	if (trimmed === "true") return true;
	if (trimmed === "false") return false;
	return evaluateConditionExpression(trimmed, results);
}

type ConditionToken =
	| {
			type: "lparen" | "rparen" | "and" | "or" | "eq" | "neq" | "lt" | "lte" | "gt" | "gte" | "not";
	  }
	| { type: "step_ref"; value: { id: string; path: string } }
	| { type: "string" | "number" | "boolean" | "null" | "identifier"; value: unknown };

function evaluateConditionExpression(
	expression: string,
	results: Record<string, WorkflowStepResult>,
	syntaxOnly = false,
) {
	const tokens = tokenizeCondition(expression);
	if (tokens.length === 0) {
		throw new Error(`Unsupported condition: ${expression}`);
	}
	let index = 0;
	// Operands that cannot change the result are parsed but not checked, so a guard
	// such as `$x.json.score != null && $x.json.score > 0.8` works.
	let skipping = syntaxOnly ? 1 : 0;
	const skipped = (parse: () => unknown) => {
		skipping += 1;
		try {
			parse();
		} finally {
			skipping -= 1;
		}
	};

	function parseOr(): unknown {
		let left = parseAnd();
		while (match("or")) {
			if (left) skipped(parseAnd);
			else left = Boolean(parseAnd());
			left = Boolean(left);
		}
		return left;
	}

	function parseAnd(): unknown {
		let left = parseEquality();
		while (match("and")) {
			if (!left) skipped(parseEquality);
			else left = Boolean(parseEquality());
			left = Boolean(left);
		}
		return left;
	}

	function order(
		left: unknown,
		right: unknown,
		op: string,
		cmp: (a: number, b: number) => boolean,
	) {
		if (isStrictlyNumeric(left) && isStrictlyNumeric(right))
			return cmp(Number(left), Number(right));
		if (skipping) return false;
		throw new Error(
			`Condition "${expression}" cannot compare ${describeOperand(left)} ${op} ${describeOperand(right)}: only numbers can be ordered`,
		);
	}

	function parseEquality(): unknown {
		const leftIsNull = nullLiteralAt(index);
		const left = parseUnary(false);
		if (match("eq")) {
			const againstNull = leftIsNull || nullLiteralAt(index);
			return compareConditionValues(left, parseUnary(true), againstNull);
		}
		if (match("neq")) {
			const againstNull = leftIsNull || nullLiteralAt(index);
			return !compareConditionValues(left, parseUnary(true), againstNull);
		}
		if (match("lt")) {
			return order(left, parseUnary(true), "<", (a, b) => a < b);
		}
		if (match("lte")) {
			return order(left, parseUnary(true), "<=", (a, b) => a <= b);
		}
		if (match("gt")) {
			return order(left, parseUnary(true), ">", (a, b) => a > b);
		}
		if (match("gte")) {
			return order(left, parseUnary(true), ">=", (a, b) => a >= b);
		}
		return left;
	}

	function parseUnary(allowBareIdentifier: boolean): unknown {
		if (match("not")) {
			return !parseUnary(allowBareIdentifier);
		}
		return parsePrimary(allowBareIdentifier);
	}

	function parsePrimary(allowBareIdentifier: boolean): unknown {
		const token = tokens[index];
		if (!token) {
			throw new Error(`Unsupported condition: ${expression}`);
		}
		index += 1;

		if (token.type === "lparen") {
			const value = parseOr();
			expect("rparen");
			return value;
		}
		if (token.type === "step_ref") {
			if (syntaxOnly) return undefined;
			// A side that cannot change the result is not checked, references included.
			return getStepRefValue(token.value, results, skipping === 0);
		}
		if (
			token.type === "string" ||
			token.type === "number" ||
			token.type === "boolean" ||
			token.type === "null"
		) {
			return token.value;
		}
		if (token.type === "identifier" && allowBareIdentifier) {
			return token.value;
		}
		throw new Error(`Unsupported condition: ${expression}`);
	}

	/** Whether the operand at `at` is the literal `null`, possibly in parentheses. */
	function nullLiteralAt(at: number) {
		let depth = 0;
		while (tokens[at + depth]?.type === "lparen") depth++;
		if (tokens[at + depth]?.type !== "null") return false;
		for (let i = 1; i <= depth; i++) if (tokens[at + depth + i]?.type !== "rparen") return false;
		return true;
	}

	function match(type: ConditionToken["type"]) {
		if (tokens[index]?.type !== type) return false;
		index += 1;
		return true;
	}

	function expect(type: ConditionToken["type"]) {
		if (!match(type)) {
			throw new Error(`Unsupported condition: ${expression}`);
		}
	}

	const value = parseOr();
	if (index !== tokens.length) {
		throw new Error(`Unsupported condition: ${expression}`);
	}
	return Boolean(value);
}

function compareConditionValues(left: unknown, right: unknown, againstNull = false) {
	// Only against the literal `null` does a missing field count as null, so `$x.json.v != null`
	// guards either; comparing two fields never matches on "no value" (absent, or one absent and
	// one null), which would let `$a.json.token == $b.json.token` pass without a token.
	if (
		againstNull &&
		(left === undefined || left === null) &&
		(right === undefined || right === null)
	)
		return true;
	if (
		Array.isArray(left) ||
		Array.isArray(right) ||
		isPlainConditionObject(left) ||
		isPlainConditionObject(right)
	) {
		return isDeepStrictEqual(left, right);
	}
	return Object.is(left, right);
}

function isStrictlyNumeric(value: unknown): boolean {
	if (typeof value === "number") return !Number.isNaN(value);
	if (typeof value === "string") return value.trim() !== "" && !Number.isNaN(Number(value));
	return false;
}

function describeOperand(value: unknown): string {
	if (value === undefined) return "a missing value";
	if (typeof value === "string") return JSON.stringify(value);
	if (value === null || typeof value === "boolean") return String(value);
	return Array.isArray(value) ? "a list" : typeof value === "object" ? "an object" : String(value);
}

function isPlainConditionObject(value: unknown): value is Record<string, unknown> {
	return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function tokenizeCondition(expression: string): ConditionToken[] {
	const tokens: ConditionToken[] = [];
	let index = 0;

	while (index < expression.length) {
		const ch = expression[index];
		if (/\s/.test(ch)) {
			index += 1;
			continue;
		}
		if (ch === "(") {
			tokens.push({ type: "lparen" });
			index += 1;
			continue;
		}
		if (ch === ")") {
			tokens.push({ type: "rparen" });
			index += 1;
			continue;
		}
		if (expression.startsWith("&&", index)) {
			tokens.push({ type: "and" });
			index += 2;
			continue;
		}
		if (expression.startsWith("||", index)) {
			tokens.push({ type: "or" });
			index += 2;
			continue;
		}
		if (expression.startsWith("==", index)) {
			tokens.push({ type: "eq" });
			index += 2;
			continue;
		}
		if (expression.startsWith("!=", index)) {
			tokens.push({ type: "neq" });
			index += 2;
			continue;
		}
		if (expression.startsWith("<=", index)) {
			tokens.push({ type: "lte" });
			index += 2;
			continue;
		}
		if (expression.startsWith(">=", index)) {
			tokens.push({ type: "gte" });
			index += 2;
			continue;
		}
		if (ch === "<") {
			tokens.push({ type: "lt" });
			index += 1;
			continue;
		}
		if (ch === ">") {
			tokens.push({ type: "gt" });
			index += 1;
			continue;
		}
		if (ch === "!") {
			tokens.push({ type: "not" });
			index += 1;
			continue;
		}
		if (ch === "$") {
			const matched = matchConditionStepRef(expression, index);
			if (!matched) {
				throw new Error(`Unsupported condition: ${expression}`);
			}
			tokens.push({ type: "step_ref", value: matched.ref });
			index = matched.nextIndex;
			continue;
		}
		if (ch === '"' || ch === "'") {
			const parsed = parseQuotedConditionString(expression, index, ch);
			tokens.push({ type: "string", value: parsed.value });
			index = parsed.nextIndex;
			continue;
		}
		const numberMatch = expression.slice(index).match(/^-?\d+(?:\.\d+)?/);
		if (numberMatch) {
			tokens.push({ type: "number", value: Number(numberMatch[0]) });
			index += numberMatch[0].length;
			continue;
		}
		const identMatch = expression.slice(index).match(/^[A-Za-z_][A-Za-z0-9_-]*/);
		if (identMatch) {
			const raw = identMatch[0];
			if (raw === "true") {
				tokens.push({ type: "boolean", value: true });
			} else if (raw === "false") {
				tokens.push({ type: "boolean", value: false });
			} else if (raw === "null") {
				tokens.push({ type: "null", value: null });
			} else {
				tokens.push({ type: "identifier", value: raw });
			}
			index += raw.length;
			continue;
		}
		throw new Error(`Unsupported condition: ${expression}`);
	}

	return tokens;
}

function matchConditionStepRef(expression: string, startIndex: number) {
	const match = expression
		.slice(startIndex)
		.match(/^\$([A-Za-z0-9_-]+)\.([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*)/);
	if (!match) return null;
	return {
		ref: { id: match[1], path: match[2] },
		nextIndex: startIndex + match[0].length,
	};
}

function parseQuotedConditionString(expression: string, startIndex: number, quoteChar: '"' | "'") {
	let value = "";
	let index = startIndex + 1;
	while (index < expression.length) {
		const ch = expression[index];
		if (ch === "\\") {
			const next = expression[index + 1];
			if (next === undefined) break;
			value += next;
			index += 2;
			continue;
		}
		if (ch === quoteChar) {
			return { value, nextIndex: index + 1 };
		}
		value += ch;
		index += 1;
	}
	throw new Error(`Unsupported condition: ${expression}`);
}

export function checkConditionSyntaxReference(condition: unknown) {
	if (typeof condition !== "string") return;
	const trimmed = condition.trim();
	if (trimmed === "true" || trimmed === "false") return;
	evaluateConditionExpression(trimmed, {}, true);
}
