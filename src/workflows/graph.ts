import { resolveArgsTemplate, stepRefIds } from "./expressions.js";
import type { WorkflowFile, WorkflowStep } from "./types.js";
import { getStepExecution, isApprovalStep, isInputStep, stepIds, stepKind } from "./steps.js";

export type WorkflowGraphFormat = "mermaid" | "dot" | "ascii";

type GraphNode = {
	id: string;
	type: string;
	label: string;
	shape: "box" | "diamond";
};

type GraphEdge = {
	from: string;
	to: string;
	label?: string;
};

type RenderGraphParams = {
	workflow: WorkflowFile;
	format: WorkflowGraphFormat;
	args?: Record<string, unknown>;
};

function stepDetails(step: WorkflowStep, args: Record<string, unknown>) {
	if (step.switch) {
		const cases = step.switch.cases.length;
		return `switch (${cases} case${cases === 1 ? "" : "s"}${step.switch.else ? " + else" : ""})`;
	}
	if (step.repeat) {
		return `repeat until ${String(step.repeat.until).trim()} (at most ${step.repeat.max})`;
	}
	if (step.parallel) {
		return `parallel (${step.parallel.wait ?? "all"})`;
	}
	if (typeof step.for_each === "string") {
		const atOnce = (step.concurrency ?? 1) > 1 ? ` (${step.concurrency} at once)` : "";
		return `for_each: ${resolveArgsTemplate(step.for_each, args)}${atOnce}`;
	}
	const execution = getStepExecution(step);
	if (execution.kind === "workflow" || execution.kind === "pipeline")
		return `${execution.kind}: ${resolveArgsTemplate(execution.value, args)}`;
	if (execution.kind === "shell") return `run: ${resolveArgsTemplate(execution.value, args)}`;
	if (step.sleep !== undefined) return `sleep: ${step.sleep}`;
	if (step.wait) return `wait for event: ${step.wait.event}`;
	if (isApprovalStep(step.approval)) return "approval gate";
	if (isInputStep(step.input)) return "input request";
	return "";
}

function extractStepRefs(value: unknown): string[] {
	if (typeof value === "string") return stepRefIds(value);
	if (Array.isArray(value)) {
		const refs = new Set<string>();
		for (const item of value) {
			for (const ref of extractStepRefs(item)) refs.add(ref);
		}
		return [...refs];
	}
	if (value && typeof value === "object") {
		const refs = new Set<string>();
		for (const v of Object.values(value as Record<string, unknown>)) {
			for (const ref of extractStepRefs(v)) refs.add(ref);
		}
		return [...refs];
	}
	return [];
}

type Read = { value: unknown; label: string };

/**
 * What the parts of `step` drawn inside its node read (its branches' stdin, a repeat's steps
 * and `until`, a loop's body, nested to any depth), and the names defined in there (step ids,
 * loop variables), which are local to the node. A switch's cases are nodes of their own.
 */
function insideReads(step: WorkflowStep) {
	const reads: Read[] = [];
	const names = new Set<string>();
	const condition = (value: unknown, label: string) => {
		if (typeof value === "string" && value.trim())
			reads.push({ value, label: `${label}: ${value.trim()}` });
	};
	const walkParts = (s: WorkflowStep, nested: boolean) => {
		for (const branch of s.parallel?.branches ?? []) {
			names.add(branch.id);
			reads.push({ value: branch.stdin, label: "stdin" });
		}
		if (s.repeat) condition(String(s.repeat.until), "until");
		if (typeof s.for_each === "string") {
			names.add(s.item_var ?? "item");
			names.add(s.index_var ?? "index");
		}
		for (const inner of s.steps ?? []) walkStep(inner);
		if (!nested) return;
		for (const entry of s.switch?.cases ?? []) {
			condition(entry.when, "when");
			entry.steps.forEach(walkStep);
		}
		(s.switch?.else ?? []).forEach(walkStep);
	};
	const walkStep = (s: WorkflowStep) => {
		names.add(s.id);
		reads.push({ value: s.stdin, label: "stdin" }, { value: s.for_each, label: "for_each" });
		condition(s.when ?? s.condition, "when");
		walkParts(s, true);
	};
	walkParts(step, false);
	return { reads, names };
}

function truncate(value: string, max = 80) {
	if (value.length <= max) return value;
	return `${value.slice(0, max - 1)}…`;
}

function collectGraph(workflow: WorkflowFile, args: Record<string, unknown>) {
	const nodes: GraphNode[] = [];
	const edges: GraphEdge[] = [];
	// The node that draws each readable step id: a repeat's steps and a parallel step's
	// branches are drawn as that step; a switch's case steps as nodes of their own.
	const nodeOf = new Map<string, string>();
	const collectIds = (steps: WorkflowStep[]) => {
		for (const step of steps) {
			for (const id of stepIds([step])) nodeOf.set(id, step.id);
			for (const entry of step.switch?.cases ?? []) collectIds(entry.steps);
			collectIds(step.switch?.else ?? []);
		}
	};
	collectIds(workflow.steps);

	const seenEdgeKeys = new Set<string>();
	const addEdge = (edge: GraphEdge) => {
		const key = `${edge.from}|${edge.to}|${edge.label ?? ""}`;
		if (seenEdgeKeys.has(key)) return;
		seenEdgeKeys.add(key);
		edges.push(edge);
	};
	/** Edges from the steps `value` reads to `to` (none from a node to itself, or `local` names). */
	const refEdges = (value: unknown, to: string, label: string, local?: Set<string>) => {
		for (const ref of extractStepRefs(value)) {
			if (local?.has(ref)) continue;
			const from = nodeOf.get(ref);
			if (from && from !== to) addEdge({ from, to, label });
		}
	};
	const conditionEdges = (condition: unknown, to: string, label: string) => {
		if (typeof condition === "string" && condition.trim()) refEdges(condition, to, label);
	};

	/**
	 * Adds `steps` in order, each entered by `incoming` (edges still waiting for a target),
	 * and returns the edges that leave the last of them. A switch enters each case's first
	 * step, and every case leaves to whatever follows the switch.
	 */
	const visit = (steps: WorkflowStep[], incoming: Omit<GraphEdge, "to">[]) => {
		for (const step of steps) {
			const type = stepKind(step);
			const details = stepDetails(step, args);
			const label = details ? `${step.id}\\n${truncate(details)}` : step.id;
			nodes.push({
				id: step.id,
				type,
				label,
				shape: isApprovalStep(step.approval) || step.switch ? "diamond" : "box",
			});
			for (const edge of incoming) addEdge({ ...edge, to: step.id });

			refEdges(step.stdin, step.id, "stdin");
			refEdges(step.for_each, step.id, "for_each");
			const inside = insideReads(step);
			for (const read of inside.reads)
				refEdges(read.value, step.id, truncate(read.label, 70), inside.names);
			const condition = step.when ?? step.condition;
			if (typeof condition === "string")
				conditionEdges(condition, step.id, truncate(`when: ${condition.trim()}`, 70));

			if (!step.switch) {
				incoming = [{ from: step.id, label: "next" }];
				continue;
			}
			const leaving: Omit<GraphEdge, "to">[] = [];
			step.switch.cases.forEach((entry, index) => {
				const caseLabel = truncate(`case ${index + 1}: when ${String(entry.when).trim()}`, 70);
				conditionEdges(entry.when, step.id, caseLabel);
				leaving.push(...visit(entry.steps, [{ from: step.id, label: caseLabel }]));
			});
			if (step.switch.else)
				leaving.push(...visit(step.switch.else, [{ from: step.id, label: "else" }]));
			incoming = leaving;
		}
		return incoming;
	};
	visit(workflow.steps, []);

	return { nodes, edges };
}

function sanitizeMermaidId(id: string) {
	return id.replace(/[^A-Za-z0-9_]/g, "_");
}

function escapeMermaidLabel(value: string) {
	const entities = {
		"&": "&amp;",
		'"': "&quot;",
		"<": "&lt;",
		">": "&gt;",
		"|": "&#124;",
	} as const;
	return value.replace(/[&"<>|]/g, (character) => entities[character as keyof typeof entities]);
}

function renderMermaid(nodes: GraphNode[], edges: GraphEdge[]) {
	const idMap = new Map<string, string>();
	const used = new Set<string>();
	for (const node of nodes) {
		// Prefixed, so no step id reads as a Mermaid keyword (`end`, `style`) or starts with a digit.
		const base = `s_${sanitizeMermaidId(node.id)}`;
		let key = base;
		for (let i = 2; used.has(key); i += 1) key = `${base}_${i}`;
		used.add(key);
		idMap.set(node.id, key);
	}

	const lines = ["flowchart TD"];
	for (const node of nodes) {
		const key = idMap.get(node.id)!;
		const label = escapeMermaidLabel(node.label);
		if (node.shape === "diamond") {
			lines.push(`  ${key}{"${label}"}`);
		} else {
			lines.push(`  ${key}["${label}"]`);
		}
	}
	if (nodes.length) lines.push("");

	for (const edge of edges) {
		const from = idMap.get(edge.from);
		const to = idMap.get(edge.to);
		if (!from || !to) continue;
		if (edge.label) {
			lines.push(`  ${from} -->|${escapeMermaidLabel(edge.label)}| ${to}`);
		} else {
			lines.push(`  ${from} --> ${to}`);
		}
	}
	return lines.join("\n");
}

function escapeDot(value: string) {
	return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

function renderDot(nodes: GraphNode[], edges: GraphEdge[]) {
	const lines = ["digraph workflow {", "  rankdir=TB;"];
	for (const node of nodes) {
		const shape = node.shape === "diamond" ? "diamond" : "box";
		lines.push(`  "${escapeDot(node.id)}" [shape=${shape},label="${escapeDot(node.label)}"];`);
	}
	if (nodes.length) lines.push("");
	for (const edge of edges) {
		if (edge.label) {
			lines.push(
				`  "${escapeDot(edge.from)}" -> "${escapeDot(edge.to)}" [label="${escapeDot(edge.label)}"];`,
			);
		} else {
			lines.push(`  "${escapeDot(edge.from)}" -> "${escapeDot(edge.to)}";`);
		}
	}
	lines.push("}");
	return lines.join("\n");
}

function renderAscii(nodes: GraphNode[], edges: GraphEdge[]) {
	const lines = ["Workflow Graph", "", "Nodes:"];
	for (const node of nodes) {
		lines.push(
			`- ${node.id} [${node.type}] ${node.label.includes("\\n") ? `(${node.label.split("\\n")[1]})` : ""}`.trim(),
		);
	}
	lines.push("", "Edges:");
	for (const edge of edges) {
		lines.push(`- ${edge.from} -> ${edge.to}${edge.label ? ` (${edge.label})` : ""}`);
	}
	if (edges.length === 0) lines.push("- (none)");
	return lines.join("\n");
}

export function renderWorkflowGraph({ workflow, format, args = {} }: RenderGraphParams) {
	const { nodes, edges } = collectGraph(workflow, args);
	if (format === "dot") return renderDot(nodes, edges);
	if (format === "ascii") return renderAscii(nodes, edges);
	return renderMermaid(nodes, edges);
}
