# Restartable workflow execution

## Scope

Replace the workflow-file executor with a saved-state interpreter. Keep YAML, commands, expressions and cost tracking; persist runs in one SQLite store, and import state saved by older versions once with `lobster upgrade`. Add durable sleeps, external-event waits and deadlines as pauses (see Time, events and liveness). Do not add task planning or a Gateway service in this change. Standalone pipelines and SDK pipelines run on the same interpreter (see Pipelines and the SDK), so there is one pause, resume and recovery mechanism.

## Source layout

- `workflows/`: loading, validating and freezing workflow files (`definition.ts`, `args.ts`), the step kinds (`steps.ts`) and the retry policy (`retry.ts`). Below the engine.
- `engine/checkpoint.ts`: the saved execution: its activations (one occurrence of a node, typed per kind of node), what waits wait for, and the queries persistence uses (wake time, unresolved activities).
- `engine/machine.ts`: visiting a step: its condition, attempts, retries, `on_error`, timeouts, cost and the approval after it. `engine/step_clock.ts` times one attempt (timeout and heartbeat).
- `engine/nodes/`: one handler per kind of step (activity, input, sleep, wait, for_each, repeat, switch, parallel, child workflow), mapped from step kinds in `nodes/index.ts`, which also says how the machine treats each (activity, control or gate); plus the pieces the machine calls directly: sequence, the step's condition, and the approval after a step.
- `engine/due.ts`: advancing every run whose time has come, and the runs an event wakes. `engine/legacy_pauses.ts`: pauses saved by older versions, rebuilt as waiting runs.
- `engine/activities/`: running a shell command or a pipeline.
- `engine/waits.ts`: publishing, answering, timing out, resolving and withdrawing waits, and asking at a terminal (or pausing when nobody can answer). `engine/asking.ts`: reading an answer at a terminal, and checking an answer and who gave it. `engine/outcome.ts`: what a paused run reports (token, approval ID, request).
- `engine/records.ts`: values kept beside the checkpoint (loop items and outputs, large step outputs). `engine/store.ts`: serialized checkpoint writes, ownership leases and fencing.
- `engine/scope.ts`, `engine/context.ts`, `engine/results.ts`, `engine/errors.ts`: what a node runs in, the run's context, step results, and the engine's errors.
- `engine/runner.ts`: the run API for one run: start, resume, advance, abandon. `engine/upgrade.ts`: `lobster upgrade`, importing a state directory written by older versions.
- `persistence/`: atomic transitions, storage receipts, history and the SQLite store. It stores checkpoints whole and reads them through `engine/checkpoint.ts`.
- `commands/`, `packs/`: the command contract (`defineCommand`), the pack registry, `stdlib` and the integration packs; `repeatable: false` commands are never re-run on crash recovery.

This is a saved-state interpreter, not a bytecode VM or arbitrary JavaScript replay engine. Existing YAML nesting restrictions remain unchanged.

## Model

A run owns a versioned definition snapshot and activation records. An activation is one occurrence of a node (including iteration and explicit retry identities). Sequence cursors, loop inputs and progress, parallel branch outcomes, activity inputs/results, gates, retry deadlines and spend are saved independently of optional diagnostic history. Control handlers share a single activity lifecycle. Completed activations return their saved result; interrupted activities either explicitly permit repetition or stop with an unknown outcome. A missing result is not proof that an effect did not happen.

A command/pipeline is one activity boundary. Pipelines must be fully drained before completion. Recovery inside a pipeline remains limited to its existing input continuation. Child workflows use the same engine, state and ownership, not another public invocation.

## Transactions and ownership

Persist inputs/attempt before dispatch. Persist the result and completed activation together. Control cursor advancement can then be repeated safely. Save a wait and its public continuation in one transition. Consume the wait and save the decision in one transition. Execution ownership uses an expiring lease and increasing epoch; every checkpoint is fenced by epoch and checkpoint version. Losing ownership aborts dispatch. Fencing storage cannot fence an arbitrary external service. The default lease is 30 seconds; orphaned state-key locks may require the existing 30-second abandoned-owner grace. A new owner closes the old owner's diagnostic attempts without declaring their external effects successful; unresolved effects remain in the execution checkpoint.

Explicit configured retry creates a new attempt; recovery of a process does not reset attempts. Save the selected retry date before waiting. Whole-control-node retries create a new subtree generation, so deliberate repetition differs from restart recovery. Parallel branches save independently through a serialized checkpoint writer. A race winner is saved before abandoning losing branches; unfinished losing effects remain visible.

## Definitions and compatibility

Snapshot definitions and static child references before execution. Dynamic child references are pinned when resolved, before entering the child. Relative paths retain their original source directory. Executable scripts, tools and environments are not snapshotted. Reject unsupported execution versions. Old workflow checkpoints require a separate one-off migration; do not retain the old executor as a fallback.

## Pipelines and the SDK

A standalone pipeline (`lobster '...'`, `runToolRequest({ pipeline })`) is a one-step definition whose step is the pipeline (`standalone: true`); its output is the pipeline's items as they are, and in human mode renderers write to the terminal as it runs. An SDK pipeline is the same, with the stages named `sdk.stage.0 | sdk.stage.1 | ...` and supplied by the instance as a registry; its input items are saved with the definition, never read as templates. Because closures cannot be saved, an SDK run is resumed or advanced only by an instance whose stages produce the same pipeline text, and the CLI and `runs advance --due` leave SDK runs alone.

A pipeline pauses inside its step. `approve` and `ask --emit` halt after their stage, and the run continues with the next stage, fed the approved items or `[response]`. A command calling `ctx.requestInput` is resumed in the same stage with its saved frame. Either way the paused activity saves the stages' text, position, items and command frame, and a gate activation publishes the question, so answering, deadlines, approval IDs and wait discovery work as for workflow steps. A rejection cancels the run.

`lobster upgrade` converts pipeline pauses saved by older versions into such waiting runs under the same key, so old tokens keep working. Paused SDK steps cannot be converted without their stages: they become runs on the first resume by their host. A stateless token from an older SDK is marked used, for good, before its run is created, so it is honoured once even after the run is deleted.

## Failed runs

A failed run keeps its activation records. `runs advance` (core `advanceWorkflowRun`, SDK `Lobster.advance`) retries it: the deepest failed activations and everything below them are cleared, enclosing activations that failed because of them are reopened, and the run continues. Completed activations keep their results, and the retried step starts with its full retry budget. A failed step that reported dispatch of a command that may not be repeated is refused. An activity stopped before any command started (a history write failed, Ctrl-C while preparing) returns to `ready`, so the step simply starts again; if that save fails too, it stays `started` and is treated as interrupted.

## Host boundary

Start/advance/resume/signal execute until finished or suspended. The host, or a scheduler calling `lobster runs advance --due`, advances unfinished runs. The engine does not assume a conversation exists, start a background service or promise that agent launch can be deduplicated by session name. The Gateway adapter's external-run reconciliation is a separate integration.

## Time, events and liveness

Lobster stays run-on-demand: a process runs until the workflow finishes or must wait, saves, and exits. It never runs a loop of its own. Something outside — the host (OpenClaw's gateway), cron/launchd/systemd, or a person — decides when to look again. The design therefore has one primitive and one entry point.

**Wake time.** Every run record carries `wakeAt`, derived from its execution on every transition: for a `running` run, the lease expiry (a live owner keeps pushing it forward; a crashed owner leaves it in the past); for a `waiting` run, the earliest timer or wait deadline; otherwise none. SQLite indexes it.

**Advancing due runs.** `lobster runs advance --due` (core `advanceDueRuns`) claims every run whose `wakeAt` has passed and advances it, then exits. Claiming uses the existing lease, so concurrent callers skip each other's runs. The same rules as `runs advance <runId>` apply: completed work is never redone, a step that may have acted is re-run only when it is repeatable and marked `recovery: repeat`, and everything else becomes `blocked` for a person. Blocked runs are never due. Timers fire at or after their time, never early; how late depends on how often the caller looks.

**Sleep.** A `sleep:` step publishes a timer wait with a fixed wake time in the same save that reaches it, exactly as an approval publishes its question. If the remaining time is short (`LOBSTER_INLINE_SLEEP_MS`, default 60 s) the process waits in place and consumes the wait itself; otherwise the run returns `status: "waiting"` with `waitingFor: { kind: "timer", until }` and exits. Re-entering the step before its time pauses again; after it, the step completes. A crash or Ctrl-C during an in-place sleep leaves an ordinary waiting run.

**Deadlines on questions.** An approval or input request may carry `timeout` and `on_timeout`. The deadline is fixed when the question is first asked and becomes the wait's due time. When a due run re-enters the question after the deadline, it consumes the wait: `on_timeout: cancel` (default) cancels the run like a rejection; `continue` completes the step with `timedOut: true` and no approval or response. An answer and a timeout race through the same wait revision, so exactly one wins.

**External events.** `lobster signal <name> [--data JSON] [--id ID]` (core `signalEvent`) stores the event in a durable inbox (`--id` makes repeated delivery idempotent) and advances the waiting runs whose event wait matches. A `wait:` step waits for an event by name, optionally matching fields of its data, with an optional `timeout`/`on_timeout`. It matches the oldest event received since the run was created that the run has not consumed, so an event that arrives before the step publishes its wait is not lost. Each run consumes an event at most once; the event id is part of the step's result. Events are pruned with finished runs, but never while an unfinished run that started before them could still wait for them. A `match` reference that resolves to nothing fails the step, and an event received after the wait's deadline does not count. After publishing its wait the step looks at the inbox once more, and `signal` retries a run that is briefly busy, so an event signalled while the run was saving its wait is not missed.

**Heartbeats.** A step with `heartbeat_timeout_ms` must show progress at least that often: a shell command (or `exec`) touches the file named by `LOBSTER_HEARTBEAT_FILE`, a pipeline command calls `ctx.heartbeat()`. Missing it aborts the step like `timeout_ms` (the process tree is killed), after which retry, `on_error` and the repeatable rules apply. The run-level lease, renewed by the executor, already covers a dead process; heartbeats cover a live process whose step has hung. A deliberate wait that does not pause the run (a person answering at the terminal, a batch pause, a sleep waited in place, a retry delay) holds the clocks of the steps around it, timeout and heartbeat alike (`deliberateWait` in `engine/step_clock.ts`): like a pause, it is not their work. The wait saves the run first, inside the hold, so neither that save nor the save of a stop (Ctrl-C) during the wait records those deadlines: a crash or a stop during the wait gives those steps a fresh budget, as a pause does. Inside branches that run at once (`parallel`, `for_each` with `concurrency`) a wait holds nothing above them: while one branch waits, the others are still the step's work.

Timers, deadlines and event waits are gates, like approvals and input requests: they are not allowed inside `parallel` branches, child workflows, a `for_each` with `concurrency`, or a loop or switch with `retry`.

## Verification

`test/execution_machine.test.ts` kills a real child process and starts a fresh process against the SQLite store. Cases cover result committed before cursor advancement, explicitly repeatable and uncertain activities, loop iteration progress, parallel branch progress, child-definition snapshots and retry count/deadline recovery. Counters prove which effects executed. The same file checks competing owners, stale checkpoint writes, interruption after question/decision/result commits, token versus short-ID competition, and host retirement before decision, dispatch or successor publication.

Existing suites cover conditions, retries, timeouts, cancellation, composition, cost, pipelines, input protocols, storage and packaging. Old workflow and pipeline tests tied to the previous checkpoint protocol (claims, consumed markers, handoffs, tombstones) are replaced by execution-transition checks; `failed_run_retry.test.ts` covers retries, and `upgrade.test.ts` resumes pipeline pauses written by the previous version.

## Limits

Each checkpoint replaces the execution snapshot; a loop's items and outputs, and step outputs over 1 KB, are records written once beside it. This change does not claim a high-throughput scheduler. History is separate and optional. A blocked activity needs outcome inspection; there is no general adapter reconciliation/result-injection API yet. Code, external services and host environments remain outside the saved definition. Recovery of an OpenClaw agent launch is not inferred from a session name.

Gateway background dispatch is deferred. Lobster provides `runs advance --due`; scheduling the call (host loop, cron, systemd timer) is the caller's choice.
