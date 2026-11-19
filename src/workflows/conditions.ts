/**
 * The condition language of `when:`, `condition:` and `switch` cases.
 *
 * A condition is parsed once into a syntax tree, then evaluated against step results.
 * Parsing checks syntax; `checkCondition` also checks what can be known before a run
 * (step references, literal operands), and runs when a workflow loads.
 *
 *   or      := and ("||" and)*
 *   and     := compare ("&&" compare)*
 *   compare := unary [operator unary']
 *   operator:= "==" | "!=" | "<" | "<=" | ">" | ">=" | "in" | "contains" | "matches"
 *   unary   := "!" unary | primary
 *   primary := "(" or ")" | "[" [primary ("," primary)*] "]" | $step.path
 *            | "string" | 'string' | number | true | false | null
 *
 * On the right of an operator (and inside a list there) a bare word is a string:
 * `$review.response.decision == approve`, `$c.json.label in [refund, spam]`.
 * A comparison does not chain. `matches` takes a regular expression as a quoted string.
 */
import { isDeepStrictEqual } from "node:util";
import { getStepRefValue, STEP_REF } from "./expressions.js";
import type { WorkflowStepResult } from "./types.js";

export type ComparisonOperator =
	| "=="
	| "!="
	| "<"
	| "<="
	| ">"
	| ">="
	| "in"
	| "contains"
	| "matches";

/** `at` is where the node starts in the condition text, from 0. */
export type ConditionNode =
	| { kind: "literal"; at: number; value: string | number | boolean | null }
	| { kind: "list"; at: number; items: ConditionNode[] }
	| { kind: "ref"; at: number; id: string; path: string }
	| { kind: "not"; at: number; operand: ConditionNode }
	| { kind: "and" | "or"; at: number; left: ConditionNode; right: ConditionNode }
	| {
			kind: "compare";
			at: number;
			operator: ComparisonOperator;
			left: ConditionNode;
			right: ConditionNode;
	  };

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

/**
 * Check a condition before any step runs: its syntax, that every step it reads is one of
 * `known` (the steps that run before it), and that no literal operand makes it fail on
 * every run (`$a.json.n > "high"`, `$a.json.n in 5`, `5 matches "x"`).
 */
export function checkCondition(condition: unknown, known?: ReadonlySet<string>) {
	if (typeof condition !== "string") return;
	const { source, root } = parseCondition(condition);
	const fail = (reason: string, at: number): never => {
		throw new Error(`Condition "${source}" ${reason} (at character ${at + 1})`);
	};
	walk(root, (node) => {
		if (node.kind === "ref" && known && !known.has(node.id))
			fail(`reads $${node.id}.${node.path}, but no step "${node.id}" runs before it`, node.at);
		if (node.kind !== "compare") return;
		const { operator, left, right } = node;
		if (["<", "<=", ">", ">="].includes(operator)) {
			for (const side of [left, right])
				if (isLiteralKind(side) && !isStrictlyNumeric(literalValue(side)))
					fail(`cannot order ${describeNode(side)}: only numbers can be ordered`, side.at);
		}
		const container = operator === "in" ? right : operator === "contains" ? left : undefined;
		if (container && isLiteralKind(container)) {
			const value = literalValue(container);
			if (!Array.isArray(value) && typeof value !== "string")
				fail(
					`cannot look inside ${describeNode(container)}: only lists and strings can be`,
					container.at,
				);
		}
		if (operator === "matches" && isLiteralKind(left) && typeof literalValue(left) !== "string")
			fail(`cannot match ${describeNode(left)}: only strings can be matched`, left.at);
	});
}

/** The step fields a condition reads, in order of appearance; none for a non-string. */
export function conditionRefs(condition: unknown): { id: string; path: string }[] {
	if (typeof condition !== "string") return [];
	const refs: { id: string; path: string }[] = [];
	walk(parseCondition(condition).root, (node) => {
		if (node.kind === "ref") refs.push({ id: node.id, path: node.path });
	});
	return refs;
}

function walk(node: ConditionNode, visit: (node: ConditionNode) => void) {
	visit(node);
	if (node.kind === "not") walk(node.operand, visit);
	else if (node.kind === "list") node.items.forEach((item) => walk(item, visit));
	else if (node.kind !== "literal" && node.kind !== "ref") {
		walk(node.left, visit);
		walk(node.right, visit);
	}
}

/** A literal or a list of literals: its value is known before the run. */
function isLiteralKind(node: ConditionNode): boolean {
	return node.kind === "literal" || (node.kind === "list" && node.items.every(isLiteralKind));
}

function literalValue(node: ConditionNode): unknown {
	return node.kind === "list" ? node.items.map(literalValue) : (node as { value: unknown }).value;
}

function describeNode(node: ConditionNode) {
	return describeOperand(literalValue(node));
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

type Symbol = "(" | ")" | "[" | "]" | "," | "&&" | "||" | "!" | ComparisonOperator;
type Token =
	| { type: Symbol; at: number }
	| { type: "ref"; at: number; id: string; path: string }
	| { type: "literal"; at: number; value: string | number | boolean | null }
	| { type: "word"; at: number; value: string };

/** The depth of a tree, without recursion (a long chain must not overflow the stack here). */
function treeDepth(root: ConditionNode) {
	let deepest = 0;
	const stack: Array<[ConditionNode, number]> = [[root, 1]];
	while (stack.length) {
		const [node, depth] = stack.pop()!;
		deepest = Math.max(deepest, depth);
		if (node.kind === "not") stack.push([node.operand, depth + 1]);
		else if (node.kind === "list") for (const item of node.items) stack.push([item, depth + 1]);
		else if (node.kind === "and" || node.kind === "or" || node.kind === "compare")
			stack.push([node.left, depth + 1], [node.right, depth + 1]);
	}
	return deepest;
}

const COMPARISONS: ComparisonOperator[] = ["==", "!=", "<=", ">=", "<", ">"];
const WORD_OPERATORS: ComparisonOperator[] = ["in", "contains", "matches"];
const SYMBOLS: Symbol[] = ["&&", "||", ...COMPARISONS, "!", "(", ")", "[", "]", ","];

/**
 * How deeply a condition may nest (parentheses, lists, `!`), and how deep its tree may be
 * (a chain of `&&` grows it by one per term). Far beyond any real condition; it keeps a
 * pathological one from overflowing the stack when it is parsed, checked or evaluated.
 */
const MAX_NESTING = 64;
const MAX_TREE_DEPTH = 256;

class Parser {
	#tokens: Token[];
	#index = 0;
	#nesting = 0;

	constructor(readonly source: string) {
		this.#tokens = tokenize(source, (reason) => this.#fail(reason));
	}

	parse(): ConditionNode {
		if (this.#tokens.length === 0) this.#fail("it is empty");
		const root = this.#or();
		if (treeDepth(root) > MAX_TREE_DEPTH)
			this.#fail(`it is too long or nested too deeply (more than ${MAX_TREE_DEPTH} levels)`);
		const extra = this.#tokens[this.#index];
		if (extra) {
			this.#fail(
				this.#operatorAt(this.#index)
					? `comparisons do not chain: group them with && or parentheses (at character ${extra.at + 1})`
					: `unexpected ${describe(extra)} at character ${extra.at + 1}`,
			);
		}
		return root;
	}

	#or(): ConditionNode {
		let left = this.#and();
		while (this.#take("||")) left = { kind: "or", at: left.at, left, right: this.#and() };
		return left;
	}

	#and(): ConditionNode {
		let left = this.#compare();
		while (this.#take("&&")) left = { kind: "and", at: left.at, left, right: this.#compare() };
		return left;
	}

	#compare(): ConditionNode {
		const left = this.#unary(false);
		const operator = this.#operatorAt(this.#index);
		if (!operator) return left;
		this.#index += 1;
		const right = this.#unary(true);
		if (operator === "matches") {
			if (right.kind !== "literal" || typeof right.value !== "string")
				this.#fail(
					`matches takes a regular expression in quotes, as in matches "^re-" (at character ${right.at + 1})`,
				);
			try {
				pattern(right.value);
			} catch (error) {
				this.#fail(
					`${(error as Error).message.replace(/^Invalid regular expression: /, "invalid regular expression ")} (at character ${right.at + 1})`,
				);
			}
		}
		return { kind: "compare", at: left.at, operator, left, right };
	}

	/** The operator at `index`: a symbol, or `in`/`contains`/`matches` after a value. */
	#operatorAt(index: number): ComparisonOperator | undefined {
		const token = this.#tokens[index];
		if (!token) return undefined;
		if (COMPARISONS.includes(token.type as ComparisonOperator))
			return token.type as ComparisonOperator;
		if (token.type === "word" && WORD_OPERATORS.includes(token.value as ComparisonOperator))
			return token.value as ComparisonOperator;
		return undefined;
	}

	#unary(wordAllowed: boolean): ConditionNode {
		const at = this.#tokens[this.#index]?.at;
		if (this.#take("!")) {
			this.#enter(at!);
			const operand = this.#unary(wordAllowed);
			this.#nesting--;
			return { kind: "not", at: at!, operand };
		}
		return this.#primary(wordAllowed);
	}

	/** One level deeper; refused past MAX_NESTING. */
	#enter(at: number) {
		if (++this.#nesting > MAX_NESTING)
			this.#fail(
				`it is nested too deeply (more than ${MAX_NESTING} levels at character ${at + 1})`,
			);
	}

	#primary(wordAllowed: boolean): ConditionNode {
		const token = this.#tokens[this.#index];
		if (!token)
			this.#fail(`it ends at character ${this.source.length + 1}, where a value is expected`);
		this.#index += 1;
		switch (token.type) {
			case "(": {
				this.#enter(token.at);
				const inner = this.#or();
				this.#close(")", token);
				this.#nesting--;
				return inner;
			}
			case "[": {
				this.#enter(token.at);
				const items: ConditionNode[] = [];
				if (!this.#take("]")) {
					do items.push(this.#primary(wordAllowed));
					while (this.#take(","));
					this.#close("]", token);
				}
				this.#nesting--;
				return { kind: "list", at: token.at, items };
			}
			case "ref":
				return { kind: "ref", at: token.at, id: token.id, path: token.path };
			case "literal":
				return { kind: "literal", at: token.at, value: token.value };
			case "word":
				if (wordAllowed) return { kind: "literal", at: token.at, value: token.value };
				this.#fail(
					`${token.value} at character ${token.at + 1} is not a value: quote it, or compare a step field with it ($step.json.field == ${token.value})`,
				);
			default:
				this.#fail(`unexpected ${describe(token)} at character ${token.at + 1}`);
		}
	}

	#close(type: ")" | "]", open: Token) {
		if (this.#take(type)) return;
		const next = this.#tokens[this.#index];
		this.#fail(
			next
				? `expected ${type} at character ${next.at + 1}, found ${describe(next)}`
				: `the ${open.type} at character ${open.at + 1} is not closed`,
		);
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

const STEP_REF_AHEAD = new RegExp(`^${STEP_REF.source}`);
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
			const ref = STEP_REF_AHEAD.exec(rest);
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
				// Only a quote or a backslash is escaped. Any other backslash is kept as written,
				// so a pattern like "^\d+$" or "a\.b" means what a regular expression means.
				if (rest[i] === "\\" && (rest[i + 1] === quote || rest[i + 1] === "\\")) i += 1;
				else if (rest[i] === "\\" && i + 1 >= rest.length) break;
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

const patterns = new Map<string, RegExp>();

/** The compiled `matches` pattern; patterns come from workflow files only, never from data. */
function pattern(source: string): RegExp {
	let compiled = patterns.get(source);
	if (!compiled) {
		compiled = new RegExp(source, "u");
		if (patterns.size >= PARSED_LIMIT) patterns.delete(patterns.keys().next().value!);
		patterns.set(source, compiled);
	}
	return compiled;
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
		case "list":
			return node.items.map((item) => evaluate(item, source, results));
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
		case "compare":
			return compare(node, source, results);
	}
}

function compare(
	node: Extract<ConditionNode, { kind: "compare" }>,
	source: string,
	results: Record<string, WorkflowStepResult>,
) {
	const left = evaluate(node.left, source, results);
	const right = evaluate(node.right, source, results);
	const refuse = (reason: string): never => {
		throw new Error(`Condition "${source}" ${reason}`);
	};
	switch (node.operator) {
		case "==":
		case "!=": {
			const equal = equals(left, right, isNull(node.left) || isNull(node.right));
			return node.operator === "==" ? equal : !equal;
		}
		case "in":
		case "contains": {
			const [item, container] = node.operator === "in" ? [left, right] : [right, left];
			if (Array.isArray(container)) return container.some((entry) => equals(item, entry, false));
			if (typeof container === "string" && typeof item === "string")
				return container.includes(item);
			return refuse(
				typeof container === "string"
					? `cannot look for ${describeOperand(item)} in a string: only a string can be found in one`
					: `cannot look inside ${describeOperand(container)}: only lists and strings can be`,
			);
		}
		case "matches":
			if (typeof left !== "string")
				refuse(`cannot match ${describeOperand(left)}: only strings can be matched`);
			return pattern(right as string).test(left as string);
		default: {
			if (!isStrictlyNumeric(left) || !isStrictlyNumeric(right))
				refuse(
					`cannot compare ${describeOperand(left)} ${node.operator} ${describeOperand(right)}: only numbers can be ordered`,
				);
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
