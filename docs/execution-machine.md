# Restartable workflow execution

## Scope

Replace the workflow-file executor with a saved-state interpreter. Keep YAML, commands, expressions, accounting and the existing storage adapters. Do not add task planning, general external-event/timer features, a Gateway service, or legacy state migration in this change. Standalone pipelines and SDK function stages retain their existing execution contracts; they are not secretly restartable workflows.

## Source layout

- `definition/`: resolve arguments and snapshot YAML/static children; hash the saved definition tree.
- `engine/state.ts`: versioned execution data and activation identities.
- `engine/store.ts`: serialized checkpoint writes, ownership leases and fencing.
- `engine/machine.ts`: shared attempt/retry/accounting lifecycle and run advancement.
- `engine/nodes/`: sequence, condition, activity, loop, parallel, child workflow, approval and input handlers.
- `activities/`: subprocess and pipeline execution; no separate retry or persistence engine.
- `workflows/file.ts`: public start/resume/advance facade. The previous executor is removed.
- `persistence/`: atomic transitions, storage receipts, history and backend implementations.

This is a saved-state interpreter, not a bytecode VM or arbitrary JavaScript replay engine. Existing YAML nesting restrictions remain unchanged.

## Model

A run owns a versioned definition snapshot and activation records. An activation is one occurrence of a node (including iteration and explicit retry identities). Sequence cursors, loop inputs and progress, parallel branch outcomes, activity inputs/results, gates, retry deadlines and cost accounting are saved independently of optional diagnostic history. Control handlers share a single activity lifecycle. Completed activations return their saved result; interrupted activities either explicitly permit repetition or stop with an unknown outcome. A missing result is not proof that an effect did not happen.

A command/pipeline is one activity boundary. Pipelines must be fully drained before completion. Recovery inside a pipeline remains limited to its existing input continuation. Child workflows use the same engine, state and ownership, not another public invocation.

## Transactions and ownership

Persist inputs/attempt before dispatch. Persist the result and completed activation together. Control cursor advancement can then be repeated safely. Save a wait and its public continuation in one transition. Consume the wait and save the decision in one transition. Execution ownership uses an expiring lease and increasing epoch; every checkpoint is fenced by epoch and checkpoint version. Losing ownership aborts dispatch. Fencing storage cannot fence an arbitrary external service. The default lease is 30 seconds; file-store orphan locks may require the existing 30-second abandoned-owner grace. A new owner closes the old owner's diagnostic attempts without declaring their external effects successful; unresolved effects remain in the execution checkpoint.

Explicit configured retry creates a new attempt; recovery of a process does not reset attempts. Save the selected retry date before waiting. Whole-control-node retries create a new subtree generation, so deliberate repetition differs from restart recovery. Parallel branches save independently through a serialized checkpoint writer. A race winner is saved before abandoning losing branches; unfinished losing effects remain visible.

## Definitions and compatibility

Snapshot definitions and static child references before execution. Dynamic child references are pinned when resolved, before entering the child. Relative paths retain their original source directory. Executable scripts, tools and environments are not snapshotted. Reject unsupported execution versions. Old workflow checkpoints require a separate one-off migration; do not retain the old executor as a fallback.

## Host boundary

Start/advance/resume execute until finished or suspended. The host invokes unfinished runs after restart. The engine does not assume a conversation exists, start a background service or promise that agent launch can be deduplicated by session name. The Gateway adapter's external-run reconciliation is a separate integration.

## Verification

`test/execution_machine.test.ts` kills a real child process and starts a fresh process against both file and SQLite stores. Cases cover result committed before cursor advancement, explicitly repeatable and uncertain activities, loop iteration progress, parallel branch progress, child-definition snapshots and retry count/deadline recovery. Counters prove which effects executed. The same file checks competing owners, stale checkpoint writes, interruption after question/decision/result commits, token versus short-ID competition, and host retirement before decision, dispatch or successor publication.

Existing suites cover conditions, retries, timeouts, cancellation, composition, accounting, pipelines, input protocols, storage and packaging. Old workflow tests tied to physical legacy checkpoint renames are replaced by execution-transition checks; standalone pipeline/SDK handoff tests remain.

## Limits

Each checkpoint currently replaces the execution snapshot, including saved outputs. Both backends implement the same correctness contract; this change does not claim a high-throughput scheduler or incremental large-run storage. History is separate and optional. A blocked activity needs outcome inspection; there is no general adapter reconciliation/result-injection API yet. Code, external services and host environments remain outside the saved definition. Recovery of an OpenClaw agent launch is not inferred from a session name.

Old workflow state conversion, Gateway background dispatch and new timed/event waits are deferred, not hidden compatibility modes.
