import test from "node:test";
import assert from "node:assert/strict";

import {
	checkCondition,
	checkConditionSyntax,
	conditionRefs,
	evaluateCondition,
	parseCondition,
} from "../src/workflows/conditions.js";
import {
	checkConditionSyntaxReference,
	evaluateConditionReference,
} from "./fixtures/condition_reference.js";

/** The tree without positions, which the tests below check separately. */
const shape = (text: string) =>
	JSON.parse(
		JSON.stringify(parseCondition(text).root, (key, value) => (key === "at" ? undefined : value)),
	);

test("conditions parse into a tree with the usual precedence", () => {
	const ref = (id: string, path: string) => ({ kind: "ref", id, path });
	assert.deepEqual(shape("$a.ok || $b.ok && !$c.ok"), {
		kind: "or",
		left: ref("a", "ok"),
		right: { kind: "and", left: ref("b", "ok"), right: { kind: "not", operand: ref("c", "ok") } },
	});
	// ! binds tighter than a comparison; a bare word on the right is a string.
	assert.deepEqual(shape("!$a.approved == done"), {
		kind: "compare",
		operator: "==",
		left: { kind: "not", operand: ref("a", "approved") },
		right: { kind: "literal", value: "done" },
	});
	// Parentheses group and leave no node behind.
	assert.deepEqual(shape(" ((null)) != $a.json.v "), {
		kind: "compare",
		operator: "!=",
		left: { kind: "literal", value: null },
		right: ref("a", "json.v"),
	});
	assert.equal(parseCondition("  true ").source, "true");
});

test("invalid conditions say what is wrong", () => {
	const rejects = (condition: string, reason: RegExp) =>
		assert.throws(
			() => checkConditionSyntax(condition),
			(error: Error) => {
				assert.match(error.message, new RegExp(`^Unsupported condition: `));
				assert.match(error.message, reason);
				return true;
			},
		);
	rejects("", /it is empty/);
	rejects("approve", /approve at character 1 is not a value: quote it/);
	rejects("$a.json.v == 1 == 2", /comparisons do not chain: .* \(at character 16\)/);
	rejects("($a.ok", /the \( at character 1 is not closed/);
	rejects("$a.ok)", /unexpected "\)" at character 6/);
	rejects("$a.json.ok = 1", /unexpected "=" at character 12/);
	rejects("$a == 1", /must look like \$step\.field/);
	rejects("$a.ok == 'x", /a string at character 10 is not closed/);
	rejects("$a.ok &&", /it ends at character 9, where a value is expected/);
});

test("dry-run reads step references from the tree, not from quoted text", () => {
	assert.deepEqual(conditionRefs(`$a.json.v == "$b.json.w" || !$c.approved`), [
		{ id: "a", path: "json.v" },
		{ id: "c", path: "approved" },
	]);
	assert.deepEqual(conditionRefs(true), []);
});

// ------------------------------------------------------------------
// The rewrite decides every condition as the evaluator it replaced did.
// ------------------------------------------------------------------

function random(seed: number) {
	return () => {
		seed = (seed + 0x6d2b79f5) | 0;
		let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
}

const ATOMS = [
	"$a.json.v",
	"$a.json.n",
	"$a.json.missing",
	"$a.json.list.0",
	"$b.approved",
	"$b.skipped",
	"$b.json.v",
	"$c.json.obj",
	"$zz.json.x",
	"null",
	"true",
	"false",
	"0",
	"1",
	"-1",
	"2.5",
	'"1"',
	'"x"',
	"'y'",
	'""',
	"ok",
	"approve",
];
const COMPARISONS = ["==", "!=", "<", "<=", ">", ">="];
const VALUES = [null, undefined, 0, 1, -1, 2.5, "1", "x", "", true, false, [1], { k: 1 }];

function expression(next: () => number, depth = 0): string {
	const pick = <T>(list: T[]) => list[Math.floor(next() * list.length)];
	const roll = next();
	if (depth > 3 || roll < 0.3) return pick(ATOMS);
	if (roll < 0.5)
		return `${expression(next, depth + 1)} ${pick(COMPARISONS)} ${expression(next, depth + 1)}`;
	if (roll < 0.65) return `!${expression(next, depth + 1)}`;
	if (roll < 0.8) return `(${expression(next, depth + 1)})`;
	return `${expression(next, depth + 1)} ${pick(["&&", "||"])} ${expression(next, depth + 1)}`;
}

/** Breaks an expression in a small way, so invalid ones are compared too. */
function mutate(text: string, next: () => number) {
	const at = Math.floor(next() * (text.length + 1));
	const edits = ["(", ")", "!", "&&", "=", "$", "'", " ", "", "==", "x"];
	const edit = edits[Math.floor(next() * edits.length)];
	return next() < 0.5
		? text.slice(0, at) + edit + text.slice(at)
		: text.slice(0, at) + text.slice(at + 1);
}

function results(next: () => number) {
	const value = () => VALUES[Math.floor(next() * VALUES.length)];
	const step = (id: string, extra: object = {}) => ({
		id,
		json: { v: value(), n: value(), list: [value()] },
		...extra,
	});
	return {
		a: step("a"),
		b: step("b", { approved: value(), skipped: value() }),
		c: { id: "c", json: { obj: next() < 0.5 ? { k: 1 } : value() } },
	} as any;
}

function outcome(text: string, run: () => unknown) {
	try {
		return { value: run() };
	} catch (error) {
		// The reason after "Unsupported condition: <text>" is new; the rest must match.
		const message = (error as Error).message;
		const prefix = `Unsupported condition: ${text.trim()} (`;
		return {
			error: message.startsWith(prefix) && message.endsWith(")") ? prefix.slice(0, -2) : message,
		};
	}
}

test("the tree evaluator decides every condition as the evaluator it replaced", () => {
	const next = random(20261117);
	let decided = 0;
	let refused = 0;
	let syntaxErrors = 0;
	for (let i = 0; i < 20_000; i++) {
		let text = expression(next);
		if (next() < 0.3) text = mutate(text, next);
		const syntax = outcome(text, () => checkConditionSyntaxReference(text));
		assert.deepEqual(
			outcome(text, () => checkConditionSyntax(text)),
			syntax,
			`syntax of: ${text}`,
		);
		// A condition that does not parse is refused when the workflow loads, before anything
		// evaluates it. (The old evaluator could hit a value error before reaching the syntax
		// error; the tree is parsed whole first.)
		if ("error" in syntax) {
			syntaxErrors++;
			continue;
		}
		const state = results(next);
		const expected = outcome(text, () => evaluateConditionReference(text, state));
		const actual = outcome(text, () => evaluateCondition(text, state));
		assert.deepEqual(actual, expected, `condition: ${text}\nresults: ${JSON.stringify(state)}`);
		if ("value" in expected) decided++;
		else refused++;
	}
	// Both kinds of outcome were exercised, not only one.
	assert.ok(
		decided > 2000 && refused > 2000 && syntaxErrors > 2000,
		`decided ${decided}, refused ${refused}, syntax errors ${syntaxErrors}`,
	);
});

test("in, contains and matches test lists, strings and patterns", () => {
	const results = {
		c: { id: "c", json: { label: "refund", tags: ["urgent", "vip"], note: "call me back", n: 3 } },
	} as any;
	const holds = (condition: string) => evaluateCondition(condition, results);
	assert.equal(holds("$c.json.label in [refund, spam]"), true);
	assert.equal(holds('$c.json.label in ["spam", "junk"]'), false);
	assert.equal(holds("$c.json.n in [1, 2, 3]"), true);
	assert.equal(holds('$c.json.n in ["3"]'), false, "membership does not convert types");
	assert.equal(holds("$c.json.tags contains urgent"), true);
	assert.equal(holds('"vip" in $c.json.tags'), true);
	assert.equal(holds('$c.json.note contains "back"'), true);
	assert.equal(holds('"call" in $c.json.note'), true);
	assert.equal(holds('$c.json.label matches "^ref"'), true);
	assert.equal(holds('$c.json.label matches "^spam$" || $c.json.n in [3]'), true);
	assert.equal(holds("!($c.json.label in [refund])"), false);
	assert.equal(holds("$c.json.tags == [urgent, vip]"), true, "lists compare by value");
	assert.equal(
		holds("$c.json.tags contains $c.json.missing"),
		false,
		"a missing item is not in a list",
	);
	// A guard keeps a missing field from failing the test.
	assert.equal(holds("$c.json.missing != null && $c.json.missing in [1]"), false);
	for (const [condition, error] of [
		["1 in $c.json.missing", /cannot look inside a missing value: only lists and strings can be/],
		["1 in $c.json.n", /cannot look inside 3: only lists and strings can be/],
		["$c.json.tags in $c.json.note", /cannot look for a list in a string/],
		['$c.json.n matches "3"', /cannot match 3: only strings can be matched/],
		['$c.json.missing matches "x"', /cannot match a missing value/],
	] as const)
		assert.throws(() => holds(condition), error, condition);
});

test("matches takes a valid pattern in quotes", () => {
	assert.throws(
		() => checkConditionSyntax("$c.json.v matches $c.json.pattern"),
		/matches takes a regular expression in quotes, as in matches "\^re-" \(at character 19\)/,
	);
	assert.throws(
		() => checkConditionSyntax('$c.json.v matches "("'),
		/invalid regular expression .* \(at character 19\)/,
	);
	assert.throws(
		() => checkConditionSyntax("$c.json.v in [1, 2"),
		/the \[ at character 14 is not closed/,
	);
	// A bare word on the right is a value, even when it spells an operator.
	assert.equal(
		evaluateCondition("$c.json.v == in", { c: { id: "c", json: { v: "in" } } } as any),
		true,
	);
});

test("checkCondition refuses what would fail every run, before any step runs", () => {
	const known = new Set(["a"]);
	const refuses = (condition: string, error: RegExp) =>
		assert.throws(() => checkCondition(condition, known), error, condition);
	refuses(
		"$b.json.v == 1",
		/reads \$b\.json\.v, but no step "b" runs before it \(at character 1\)/,
	);
	refuses("$a.ok && !$later.ok", /no step "later" runs before it \(at character 11\)/);
	refuses(
		'$a.json.n > "high"',
		/cannot order "high": only numbers can be ordered \(at character 13\)/,
	);
	refuses("true >= $a.json.n", /cannot order true/);
	refuses(
		"$a.json.v in 5",
		/cannot look inside 5: only lists and strings can be \(at character 14\)/,
	);
	refuses('5 matches "x"', /cannot match 5: only strings can be matched \(at character 1\)/);
	// What may hold on some run is accepted.
	for (const condition of [
		'$a.json.n > "3"',
		"$a.json.tags contains 3",
		'"x" in $a.json.list',
		"$a.json.v in [1, 2]",
		'"abc" contains "b"',
	])
		assert.doesNotThrow(() => checkCondition(condition, known), condition);
	// Without known steps, references are not checked.
	assert.doesNotThrow(() => checkCondition("$z.ok"));
});
