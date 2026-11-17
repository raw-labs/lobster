/**
 * The condition language of `when:` and `condition:`.
 *
 * A condition is parsed once into a syntax tree, then evaluated against step results.
 * Parsing alone checks syntax: when a workflow loads, and in dry-run for conditions on
 * output a plan does not have.
 *
 *   or      := and ("||" and)*
 *   and     := compare ("&&" compare)*
 *   compare := unary [("==" | "!=" | "<" | "<=" | ">" | ">=") unary']
 *   unary   := "!" unary | primary
 *   primary := "(" or ")" | $step.path | "string" | 'string' | number | true | false | null
 *
 * `unary'` on the right of a comparison may also be a bare word, read as a string:
 * `$review.response.decision == approve`. A comparison does not chain.
 */
import { isDeepStrictEqual } from "node:util";
import { getStepRefValue } from "./expressions.js";
import type { WorkflowStepResult } from "./types.js";

export type ComparisonOperator = "==" | "!=" | "<" | "<=" | ">" | ">=";

export type ConditionNode =
	| { kind: "literal"; value: string | number | boolean | null }
	| { kind: "ref"; id: string; path: string }
	| { kind: "not"; operand: ConditionNode }
	| { kind: "and" | "or"; left: ConditionNode; right: ConditionNode }
	| { kind: "compare"; operator: ComparisonOperator; left: ConditionNode; right: ConditionNode };

export type Condition = { source: string; root: ConditionNode };

/** Whether a step runs: absent means yes, a boolean is itself, a string is a condition. */
export function evaluateCondition(
	condition: unknown,
	results: Record<string, WorkflowStepResult>,
): boolean {
	if (condition === undefined || condition === null) return true;
	if (typeof condition === "boolean") return condition;
	if (typeof condition !== "string") throw new Error("Unsupported condition type");
	const parsed = parseCondition(condition);
	return Boolean(evaluate(parsed.root, parsed.source, results));
}

/** Check that a condition parses, without step results. */
export function checkConditionSyntax(condition: unknown) {
	if (typeof condition === "string") parseCondition(condition);
}

/** The step fields a condition reads, in order of appearance; none for a non-string. */
export function conditionRefs(condition: unknown): { id: string; path: string }[] {
	if (typeof condition !== "string") return [];
	const refs: { id: string; path: string }[] = [];
	const visit = (node: ConditionNode) => {
		if (node.kind === "ref") refs.push({ id: node.id, path: node.path });
		else if (node.kind === "not") visit(node.operand);
		else if (node.kind !== "literal") {
			visit(node.left);
			visit(node.right);
		}
	};
	visit(parseCondition(condition).root);
	return refs;
}

// ------------------------------------------------------------------
// Parsing
// ------------------------------------------------------------------

const parsed = new Map<string, Condition>();
const PARSED_LIMIT = 1000;

/** Parses `text`, throwing "Unsupported condition: <text> (<reason>)" if it is not valid. */
export function parseCondition(text: string): Condition {
	const source = text.trim();
	let condition = parsed.get(source);
	if (!condition) {
		condition = { source, root: new Parser(source).parse() };
		if (parsed.size >= PARSED_LIMIT) parsed.delete(parsed.keys().next().value!);
		parsed.set(source, condition);
	}
	return condition;
}

type Token =
	| { type: "(" | ")" | "&&" | "||" | "!" | ComparisonOperator; at: number }
	| { type: "ref"; at: number; id: string; path: string }
	| { type: "literal"; at: number; value: string | number | boolean | null }
	| { type: "word"; at: number; value: string };

const COMPARISONS: ComparisonOperator[] = ["==", "!=", "<=", ">=", "<", ">"];
const SYMBOLS = ["&&", "||", ...COMPARISONS, "!", "(", ")"] as const;

class Parser {
	#tokens: Token[];
	#index = 0;

	constructor(readonly source: string) {
		this.#tokens = tokenize(source, (reason) => this.#fail(reason));
	}

	parse(): ConditionNode {
		if (this.#tokens.length === 0) this.#fail("it is empty");
		const root = this.#or();
		const extra = this.#tokens[this.#index];
		if (extra) {
			this.#fail(
				COMPARISONS.includes(extra.type as ComparisonOperator) && this.#index > 0
					? `comparisons do not chain: group them with && or parentheses`
					: `unexpected ${describe(extra)} at character ${extra.at + 1}`,
			);
		}
		return root;
	}

	#or(): ConditionNode {
		let left = this.#and();
		while (this.#take("||")) left = { kind: "or", left, right: this.#and() };
		return left;
	}

	#and(): ConditionNode {
		let left = this.#compare();
		while (this.#take("&&")) left = { kind: "and", left, right: this.#compare() };
		return left;
	}

	#compare(): ConditionNode {
		const left = this.#unary(false);
		const operator = this.#tokens[this.#index]?.type;
		if (!COMPARISONS.includes(operator as ComparisonOperator)) return left;
		this.#index += 1;
		return {
			kind: "compare",
			operator: operator as ComparisonOperator,
			left,
			right: this.#unary(true),
		};
	}

	#unary(wordAllowed: boolean): ConditionNode {
		if (this.#take("!")) return { kind: "not", operand: this.#unary(wordAllowed) };
		return this.#primary(wordAllowed);
	}

	#primary(wordAllowed: boolean): ConditionNode {
		const token = this.#tokens[this.#index];
		if (!token) this.#fail("it ends where a value is expected");
		this.#index += 1;
		switch (token.type) {
			case "(": {
				const inner = this.#or();
				if (!this.#take(")")) {
					const next = this.#tokens[this.#index];
					this.#fail(
						next
							? `expected ) at character ${next.at + 1}, found ${describe(next)}`
							: "a ( is not closed",
					);
				}
				return inner;
			}
			case "ref":
				return { kind: "ref", id: token.id, path: token.path };
			case "literal":
				return { kind: "literal", value: token.value };
			case "word":
				if (wordAllowed) return { kind: "literal", value: token.value };
				this.#fail(
					`${token.value} is not a value: quote it, or compare a step field with it ($step.json.field == ${token.value})`,
				);
			default:
				this.#fail(`unexpected ${describe(token)} at character ${token.at + 1}`);
		}
	}

	#take(type: Token["type"]) {
		if (this.#tokens[this.#index]?.type !== type) return false;
		this.#index += 1;
		return true;
	}

	#fail(reason: string): never {
		throw new Error(`Unsupported condition: ${this.source} (${reason})`);
	}
}

function describe(token: Token) {
	if (token.type === "ref") return `$${token.id}.${token.path}`;
	if (token.type === "literal") return JSON.stringify(token.value);
	if (token.type === "word") return token.value;
	return `"${token.type}"`;
}

const STEP_REF = /^\$([A-Za-z0-9_-]+)\.([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*)/;
const NUMBER = /^-?\d+(?:\.\d+)?/;
const WORD = /^[A-Za-z_][A-Za-z0-9_-]*/;

function tokenize(source: string, fail: (reason: string) => never): Token[] {
	const tokens: Token[] = [];
	let at = 0;
	while (at < source.length) {
		const rest = source.slice(at);
		if (/^\s/.test(rest)) {
			at += 1;
			continue;
		}
		const symbol = SYMBOLS.find((s) => rest.startsWith(s));
		if (symbol) {
			tokens.push({ type: symbol, at });
			at += symbol.length;
			continue;
		}
		if (rest[0] === "$") {
			const ref = STEP_REF.exec(rest);
			if (!ref) fail(`a step reference at character ${at + 1} must look like $step.field`);
			tokens.push({ type: "ref", at, id: ref[1], path: ref[2] });
			at += ref[0].length;
			continue;
		}
		if (rest[0] === '"' || rest[0] === "'") {
			const quote = rest[0];
			let value = "";
			let i = 1;
			for (; i < rest.length && rest[i] !== quote; i++) {
				if (rest[i] === "\\") {
					if (i + 1 >= rest.length) break;
					i += 1;
				}
				value += rest[i];
			}
			if (rest[i] !== quote) fail(`a string at character ${at + 1} is not closed`);
			tokens.push({ type: "literal", at, value });
			at += i + 1;
			continue;
		}
		const number = NUMBER.exec(rest);
		if (number) {
			tokens.push({ type: "literal", at, value: Number(number[0]) });
			at += number[0].length;
			continue;
		}
		const word = WORD.exec(rest);
		if (word) {
			const raw = word[0];
			if (raw === "true" || raw === "false")
				tokens.push({ type: "literal", at, value: raw === "true" });
			else if (raw === "null") tokens.push({ type: "literal", at, value: null });
			else tokens.push({ type: "word", at, value: raw });
			at += raw.length;
			continue;
		}
		fail(`unexpected ${JSON.stringify(rest[0])} at character ${at + 1}`);
	}
	return tokens;
}

// ------------------------------------------------------------------
// Evaluation
// ------------------------------------------------------------------

/**
 * The value of `node`. `&&` and `||` evaluate their right side only when it can change
 * the result, so a guard such as `$x.json.score != null && $x.json.score > 0.8` keeps
 * the comparison from failing on a missing score.
 */
function evaluate(
	node: ConditionNode,
	source: string,
	results: Record<string, WorkflowStepResult>,
): unknown {
	switch (node.kind) {
		case "literal":
			return node.value;
		case "ref":
			return getStepRefValue(node, results, true);
		case "not":
			return !evaluate(node.operand, source, results);
		case "and":
			return (
				Boolean(evaluate(node.left, source, results)) &&
				Boolean(evaluate(node.right, source, results))
			);
		case "or":
			return (
				Boolean(evaluate(node.left, source, results)) ||
				Boolean(evaluate(node.right, source, results))
			);
		case "compare": {
			const left = evaluate(node.left, source, results);
			const right = evaluate(node.right, source, results);
			if (node.operator === "==" || node.operator === "!=") {
				const equal = equals(left, right, isNull(node.left) || isNull(node.right));
				return node.operator === "==" ? equal : !equal;
			}
			if (!isStrictlyNumeric(left) || !isStrictlyNumeric(right)) {
				throw new Error(
					`Condition "${source}" cannot compare ${describeOperand(left)} ${node.operator} ${describeOperand(right)}: only numbers can be ordered`,
				);
			}
			const [a, b] = [Number(left), Number(right)];
			if (node.operator === "<") return a < b;
			if (node.operator === "<=") return a <= b;
			if (node.operator === ">") return a > b;
			return a >= b;
		}
	}
}

/** The literal `null`, parentheses included: the AST has none. */
function isNull(node: ConditionNode) {
	return node.kind === "literal" && node.value === null;
}

function equals(left: unknown, right: unknown, againstNull: boolean) {
	// Only against the literal `null` does a missing field count as null, so `$x.json.v != null`
	// guards either; comparing two fields never matches on "no value" (absent, or one absent and
	// one null), which would let `$a.json.token == $b.json.token` pass without a token.
	if (againstNull && left == null && right == null) return true;
	if (isStructured(left) || isStructured(right)) return isDeepStrictEqual(left, right);
	return Object.is(left, right);
}

function isStructured(value: unknown) {
	return typeof value === "object" && value !== null;
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
