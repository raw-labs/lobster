# 🦞 Lobster

![Lobster banner](docs/assets/readme-banner.jpg)

An OpenClaw-native workflow shell: typed (JSON-first) pipelines and approval gates.

## Example

A named workflow fetches a pull request with `gh`, compares it with the saved snapshot, and returns a notification only when it changes:

```bash
node bin/lobster.js run --mode tool "workflows.run --name github.pr.monitor.notify --args-json '{\"repo\":\"openclaw/lobster\",\"pr\":167}'"
```

For repeatable automation, put shell commands, native pipeline steps, and explicit approval/input gates in a workflow file. OpenClaw presents the gate to the user and resumes with their decision.

## Goals

- Typed pipelines (objects/arrays), not text pipes.
- Local-first execution.
- No new auth surface: Lobster must not own OAuth/tokens.
- Composable macros that OpenClaw (or any agent) can invoke in one step to save tokens.

## Quick start

Requires Node.js 22 or newer and the pnpm version pinned in `package.json`. From this folder:

- `pnpm install --frozen-lockfile`
- `pnpm build`
- `pnpm test`
- `pnpm lint`
- `node ./bin/lobster.js --help`
- `node ./bin/lobster.js doctor`
- `node ./bin/lobster.js "exec --json --shell 'echo [1,2,3]' | where '0>=0' | json"`

### Notes

- `pnpm test` runs `tsc` and then executes tests against `dist/`.
- `bin/lobster.js` runs the compiled entrypoint in `dist/`; build after changing source files.
Invalid workflow file paths are reported as input errors (exit code 2), whether passed with `--file` or as a single positional path (no whitespace; contains a path separator or ends in `.lobster`, `.yaml`, `.yml`, or `.json`). In `--mode tool`, these failures use the same JSON error envelope as other parsing errors.

## Persistence and run history

File storage remains the default. Existing state keys, approval IDs, and saved
continuations keep their readers. Retained history is opt-in on either backend:

```sh
LOBSTER_HISTORY=1 lobster run --mode tool --file workflow.lobster
lobster history list
lobster history show <runId>                    # first 50 events
lobster history show <runId> <afterSequence>    # next page
```

For a **fresh state directory**, select SQLite explicitly:

```sh
LOBSTER_STATE_DIR=/path/to/new/state LOBSTER_BACKEND=sqlite LOBSTER_HISTORY=1 \
  lobster run --mode tool --file workflow.lobster
```

SQLite requires Node 22.13+ on the Node 22 line, 23.4+ on Node 23, or Node 24+,
**and a bundled SQLite version containing the WAL-reset fix**: 3.51.3 or later,
or the 3.50.7/3.44.6 backports and subsequent patches on those branches. Node's
version alone does not establish that its SQLite is patched. Check with
`node -p process.versions.sqlite`; upgrade Node if SQLite selection rejects it.
The same qualification applies to migration. See the [SQLite fix](https://sqlite.org/wal.html#walreset).
The file backend retains Node >=22 support and does not load `node:sqlite`.
A worker owns the SQLite connection, prepared statements, and short transactions;
embedded hosts receive promises. The database uses WAL, a 5-second busy timeout,
and FULL durability. A session admits at most 256 outstanding worker requests
(configurable through `maxPending`); overflow fails before dispatch.

A tracked execution keeps one run ID through approvals and structured-input
pauses. History records step occurrences, attempts, retries, loop item ordinals,
parallel branches, and child-workflow paths. Lazy stages finish when their
iterators finish, fail, or close early; recording does not drain an unused stream.
Workflow files retain their current reload-on-resume behavior. Definition hashes
record what was supplied in each execution segment; they do not freeze execution.

Default output summaries contain types and counts rather than raw outputs, error
messages, prompts, arguments, or environment variables. Summary payloads are
limited to 4 KiB. Inspection returns at most 100 events per page (50 by default),
with related attempts and waits, and excludes executable continuation payloads
and operation receipts. A logical export includes full authoritative state, so
store exports and backups privately. Continuations retain their existing full
payloads and existing output limits; history capture adds no execution truncation.

New tracked file continuations live with their events in one atomically replaced
per-run record under `.history`. SQLite commits continuation transitions and their
events together in indexed tables. The file backend rewrites its aggregate when
history changes; SQLite avoids that cost. Legacy JSON values keep their layout
until migration or association with a tracked continuation. New tracked tokens
require this runtime; compatibility means reading existing tokens, not making
older binaries understand new storage. History enables run tracking. To keep live
run status and pending reviews without detailed history, use `LOBSTER_RUNS=1` or
`trackRuns: true`. Each run retains its original history setting when resumed.

### Run and pending review API

Enable durable runs on either backend with `trackRuns: true`; add `history: true`
when you also want step attempts and event history. A pending question is saved
with its original prompt, schema, defaults, and full review subject. Discovery
reads that snapshot, rather than rebuilding it from chat or a workflow file.

```ts
import {
  openStoreSession, listRuns, getRun, getWait, resumeToolRequest,
} from '@clawdbot/lobster/core';

const store = await openStoreSession({
  directory: '/path/to/state', backend: 'sqlite', trackRuns: true,
});
const ctx = {
  storeSession: store,
  runMetadata: { callerId: 'host:stable-logical-session' },
};
try {
  const page = await listRuns({ status: 'waiting', limit: 20, ctx });
  const run = page.runs[0] && await getRun({ runId: page.runs[0].id, ctx });
  const pending = run?.pendingWaits[0];
  const wait = pending && await getWait({ waitId: pending.id, ctx });
  // Present wait.request to the user. After an explicit decision:
  // await resumeToolRequest({
  //   waitId: wait.id, expectedRevision: wait.revision, response: answer, ctx,
  // });
} finally {
  await store.close();
}
```

The three query functions are also exported from the SDK entry point. Run lists
are paged (`after`, `limit`, `nextCursor`) and may be filtered by status. `getRun`
returns status, metadata, the history setting, and pending wait IDs; it does not
expose raw checkpoint payloads. `getWait` returns the complete saved question and
its wait revision, without a resume token. Both getters return `null` for missing or caller-invisible records. `getWait`
also returns `null` for consumed or retired waits; completed runs remain
inspectable through `getRun`.

`expectedRevision` refers to the **wait**, not the run's history revision. It is
checked again in the same transaction as checkpoint consumption or cancellation.

When a tracked run reaches another pause, Lobster retires the previous checkpoint
and publishes the next one, its history event, and an operation receipt in one
commit. Competing answers cannot create two pending successors. A crash before
that commit preserves the previous checkpoint; a crash after it preserves the
next one. Cancellation after publication does not undo the committed handoff.
Lost handoff replies are reconciled against the receipt before reporting success;
an unreadable receipt leaves the outcome explicitly unknown. Steps that already
claimed a checkpoint before an unsafe operation remain subject to the execution
recovery limits below. Untracked legacy JSON checkpoints keep their existing
publication behavior.

Invalid answers remain correctable; changed, consumed, or foreign waits return
`wait_conflict`. The old token and approval-ID paths continue to use the same
single-consumption mechanism. Host admission for those legacy bearer paths is
unchanged.

`callerId` is an opaque association supplied through trusted host context at run
creation. Scoped discovery and wait-ID resume require an exact match. It is a
filter, not a user-authentication system: hosts must authorize that context and
keep it stable across the resets they intend to support. Unscoped local callers
can inspect all runs in their store. Lobster owns saved execution facts; the host
owns identity, conversation routing, and presentation.

SDK function stages still require an instance supplying the original stages:
`lobster.resumeWait({ waitId, expectedRevision, response })`. They are not
serialized or resurrected from storage. Workflow-file and pipeline waits resume
through `resumeToolRequest`. Existing untracked checkpoints retain token-based
resume; they do not retroactively acquire run history or a known caller. Imported
older tracked waits may have `request: null` when the original question was not
saved; discovery does not invent it by rereading a changed workflow file.

CLI equivalents use the same API:

```sh
LOBSTER_RUNS=1 lobster run --mode tool --file review.lobster
lobster runs list --status waiting
lobster runs show <runId>
lobster waits show <waitId>
lobster resume --wait <waitId> --expected-revision <revision> --response-json '{...}'
# Or use --approve yes|no or --cancel for the appropriate decision.
```

Full wait inspection is an explicit read of private review material; callers must
apply their own display limits. If a durable workflow question cannot fit the
bounded tool envelope, the call reports a rendering error with its run ID and the
saved question remains discoverable. Pending waits are protected from terminal
history pruning. These APIs rediscover saved pauses; they do not automatically
replay steps interrupted by a process crash.

### Migrating existing state

A storage directory has one selected backend. SQLite selection refuses to create
an empty authoritative database over existing file state. Coordinate all writers,
including older binaries, then migrate explicitly:

```sh
LOBSTER_STATE_DIR=/path/to/state lobster storage migrate sqlite --offline
```

`--offline` declares that the caller has stopped all writers. Migration rejects
live persistence sessions and outstanding state locks, writes a private logical
backup, imports into a staging store, verifies it, closes it, and atomically
activates backend selection. Retrying an interrupted conversion before activation
keeps the source authoritative. After activation, stale files are never a fallback.
The `.backup-*.archive` files contain full private state. Migration never scans or
modifies OpenClaw's databases.

The same command can migrate back to `files`, preserving the logical state and
history. This is a backend round trip, not a binary downgrade. Source JSON must be
valid; malformed authoritative input aborts conversion. Legacy runs have absent
history until a stored continuation is associated with a new, explicitly partial
record. Self-contained legacy SDK tokens remain usable with supplied SDK stages
and cannot be discovered by scanning a directory. An owning host can build
coordinated upgrade-time conversion on this API; automatic conversion is not enabled.

### Adopting legacy pauses

Backend migration preserves old checkpoints; it does not assign owners or invent
run history. To make existing pauses discoverable before answering them, use
`listLegacyCheckpoints` and `adoptLegacyCheckpoint` from either the core or SDK entry
point. Both file and SQLite stores support the same maintenance operations.

```ts
import { listLegacyCheckpoints, adoptLegacyCheckpoint } from '@clawdbot/lobster/core';

const inventory = await listLegacyCheckpoints({ ctx: { storeSession: store }, limit: 20 });
const checkpoint = inventory.checkpoints[0];
if (checkpoint) {
  // The host establishes the owner mapping; checkpointId is opaque.
  const run = await adoptLegacyCheckpoint({
    checkpointId: checkpoint.checkpointId,
    ctx: { storeSession: store, runMetadata: { callerId: 'host:verified-owner' } },
  });
  // Inspect run.pendingWaits and answer through the normal APIs.
}
```

Inventory is bounded to 100 entries per page (20 by default), using `after` and
`nextCursor`. It lists unassociated stored checkpoints, excludes consumed tokens,
and requires unscoped host access because those records have no known caller.
Adoption requires a nonempty trusted caller ID, executes no workflow steps, and
creates one partial run with its pending wait in an atomic commit. Repeating it
for the same owner preserves run identity; competing owners cannot both adopt a
checkpoint. An already-associated, unowned partial run can also be bound using
its saved checkpoint ID. Adoption cannot reassign an existing owner.

These are host maintenance APIs, not permission checks or model-facing tools.
The host must establish who owns a checkpoint before adopting it; possession of
an old token alone does not establish that identity. CLI callers can make the
same explicit association:

```sh
lobster storage legacy list
lobster storage legacy list <after>
lobster storage legacy adopt <checkpointId> --caller host:verified-owner
```

Existing tokens and short approval IDs remain usable after adoption. Stored SDK
command-input tokens also keep working with an instance supplying the original
stages. Self-contained legacy SDK tokens remain outside inventory and retain
their previous behavior. No historical events or missing review material are
reconstructed: `getWait().request` can be `null` for an old workflow checkpoint
that did not save the question. Workflow source is never reread to invent it.

Migration validates legacy and tracked checkpoint shapes and references before
preparing the destination, and rejects ambiguous workflow-key aliases. Already
expired optional approval IDs and references to deleted predecessor markers
remain expired; migration never creates executable state to satisfy them.

```sh
lobster storage export                         # versioned logical archive
lobster history prune 2026-01-01T00:00:00Z      # dry run
lobster history prune 2026-01-01T00:00:00Z --apply
```

Retention removes only terminal runs older than the cutoff with no live
continuation, pending wait, started attempt, or unacknowledged operation receipt.
Valid consumed-capability markers may be removed with their terminal run; their
tokens remain unusable because neither backend falls back to an old checkpoint.
Committed transitions have stable operation IDs and receipts. A lost reply is an
unknown outcome until reconciled; it does not authorize replaying an external
side effect. Receipts are acknowledged only after the executor observes commit,
and remain protected otherwise. Started attempts are history, not automatic
restart checkpoints. A run's `running` status is its last recorded state, not a
liveness guarantee after process death. Crash recovery and external-service
idempotency remain separate executor work.

History persistence errors bypass workflow retry and `on_error` continuation.
Tool envelopes report `persistence_error` or `persistence_outcome_unknown`, with
`error.runId` and `error.operationId`. Embedded callers can query
`store.reconcile(runId, operationId)` and acknowledge an observed receipt. Neither
reconciliation nor acknowledgement resumes an external action.

Session writes return an explicit committed result for ordinary state or tracked
run state. Tracked results include the run ID, operation ID, and revision;
`confirmation` distinguishes a direct acknowledgement from a reconciled receipt.
Revision-checked transitions return `committed` or `conflict`. Unknown outcomes
remain errors until reconciliation establishes commit.

### Embedded and SDK usage

CLI/core/SDK roots open and close their selected session automatically. An embedded
host can reuse a session and own its lifecycle:

```ts
import { openStoreSession, runToolRequest } from '@clawdbot/lobster/core';

const store = await openStoreSession({
  directory: '/path/to/state', backend: 'sqlite', history: true,
});
try {
  const result = await runToolRequest({
    filePath: '/path/to/workflow.lobster',
    ctx: { storeSession: store, runMetadata: { hostRunId: 'trigger-123' } },
  });
  const page = await store.listRuns({ limit: 20 });
  const history = await store.inspect(page.runs[0].id, { limit: 50 });
} finally {
  await store.close();
}
```

`close()` rejects new requests and waits for admitted operations before closing
the backend and releasing the session's migration fence.

Session injection takes precedence over environment configuration. Correlation
metadata accepts bounded `hostRunId`, `hostTaskId`, and `callerId` strings; these are
records, not credentials or continuing authorization. Listing uses lexical run
IDs as cursors, so new IDs before an existing cursor appear on a fresh scan.
Event pagination uses committed run-local sequence numbers and reads current
attempt/wait status for the returned references.

`new Lobster({ storeSession: store })` or `new Lobster({ history: true, stateDir })`
tracks SDK runs with additive stored tokens. Resume still requires an instance
with the original JavaScript stages; closures are not serialized. Untracked
legacy SDK tokens retain their decoder and behavior. Applications must obtain a
user decision before resuming any approval, as before.

## Process output limits

Captured process stdout and stderr remain unlimited by default. Set
`LOBSTER_MAX_OUTPUT_BYTES` to a positive integer to opt into a byte limit **per
stream, per process**. Unset or empty means unlimited; zero, negative, fractional,
and non-numeric values are rejected before a process starts. This applies to
CLI `exec`, workflow shell steps, SDK `exec`/shell calls, GitHub recipes, and Gog
commands. The existing `openclaw.agent` 10 MiB limit remains in place; an opted-in
limit can lower it.

```bash
LOBSTER_MAX_OUTPUT_BYTES=1048576 lobster run --mode tool 'exec my-command'
```

Use workflow or step `env` to scope the limit:

```yaml
steps:
  - id: bounded
    env:
      LOBSTER_MAX_OUTPUT_BYTES: "1048576"
    run: my-command
```

SDK callers set the same variable in `new Lobster({ env: { ...process.env,
LOBSTER_MAX_OUTPUT_BYTES: "1048576" } })`. Values exactly at the limit succeed;
overflow terminates the process tree and returns an error without partial
output. POSIX capped calls own a process group so background producers cannot
keep pipes open after overflow. Calls without an AbortSignal relay terminal
interrupts to that group and retain the host's existing signal handlers or
conventional interrupt exit status.

## Commands

- `exec`: run OS commands
- `exec --json <command...>`: parse subprocess stdout as JSON before the next stage
- `exec --stdin raw|json|jsonl`: feed pipeline input into subprocess stdin
- Explicit quoted empty arguments are retained by both pipeline `exec` and SDK `exec`. Invocation shims preserve argument punctuation without shell expansion.
- `where`, `pick`, `head`: data shaping
- `head --n N` stops reading upstream as soon as N items have been yielded; `--n 0` does not read upstream input.
- `pick` and `map` preserve field names such as `__proto__` as ordinary own data fields.
- `json`, `table`: renderers
- `approve`: approval gate (TTY prompt or `--emit` for OpenClaw integration)

In top-level `--mode tool` pipelines (including resumes and the core tool API), `json` and `table` preserve typed items in the single JSON envelope instead of writing terminal output. Human-mode rendering and rendered output captured within workflow-file steps remain unchanged.

## SDK cancellation

Pass an `AbortSignal` to `new Lobster({ signal })` to cancel SDK `exec()` stages,
including `exec(command, { shell: true })`. The signal is retained by `clone()`
and forwarded when a pipeline resumes. Aborting terminates the running child
process tree before `run()` or `resume()` returns an error result containing the
abort reason. A signal that is already aborted prevents the command from starting.

```js
import { Lobster, exec } from '@clawdbot/lobster';

const controller = new AbortController();
const pending = new Lobster({ signal: controller.signal })
  .pipe(exec('node long-task.js', { json: false }))
  .run();
// When the host needs to stop the command:
controller.abort(new Error('Host cancelled the command'));
const result = await pending;
```

Custom stages receive the signal as `ctx.signal` and must cooperate with it to
cancel their own work.

## Host invocation authority

Hosts using the core `runToolRequest()` and `resumeToolRequest()` APIs can pass
`ctx.assertInvocationCurrent`, a synchronous callback that throws when the host
has retired an invocation. Lobster rechecks it before built-in process, network,
and state effects, including after asynchronous resume preparation. Keep passing
an `AbortSignal` to cancel work that has already started.

A retired resume restores its caller-owned checkpoint when no unsafe dispatch
has started. Once an unsafe pipeline command has been entered, the approval stays
consumed even if a later authority check prevents the effect. This conservative
boundary prevents replaying work that may already have happened. Custom commands
and injected adapters must check the callback themselves after awaited preparation
and before their own effects. The SDK does not expose this host-authority contract;
CLI behavior is unchanged when no callback is supplied.

## GitHub recipe cancellation

The SDK GitHub recipes accept an `AbortSignal` in both
`prMonitor({ repo, pr, signal })` and `prMonitorNotify({ repo, pr, signal })`.
Aborting stops the running GitHub CLI child before the recipe returns an error
result containing the abort reason. A cancelled fetch does not update the saved
PR snapshot or emit a notification.

For a manually composed pipeline, pass the signal to
`new Lobster({ signal }).pipe(ghPrView({ repo, pr }))`. Custom stages receive
`ctx.signal` and must cooperate with it to cancel their own work.

## Workflow files

Lobster workflow files are meant to read like small scripts:

- `run:` or `command:` for deterministic shell/CLI steps
- `pipeline:` for native Lobster stages like `llm.invoke`
- `approval:` for hard workflow gates between steps
- `stdin: $step.stdout` or `stdin: $step.json` to pass data forward

```
lobster run path/to/workflow.lobster
lobster run --file path/to/workflow.lobster --args-json '{"tag":"family"}'
```

Example file:

```yaml
name: jacket-advice
args:
  location:
    default: Phoenix
steps:
  - id: fetch
    run: weather --json ${location}

  - id: confirm
    approval: Want jacket advice from the LLM?
    stdin: $fetch.json

  - id: advice
    pipeline: >
      llm.invoke --prompt "Given this weather data, should I wear a jacket?
      Be concise and return JSON."
    stdin: $fetch.json
    when: $confirm.approved
```

Notes:

- `run:` and `command:` are equivalent; `run:` is the preferred spelling for new files.
- `pipeline:` shares the same args/env/results model as shell steps, so later steps can still reference `$step.stdout` or `$step.json`.
- If you need a human checkpoint before an LLM call, use a dedicated `approval:` step in the workflow file rather than `approve` inside the nested pipeline.
- `cwd`, `env`, `stdin`, `when`, and `condition` work for both shell and pipeline steps.
- Use `retry`, `timeout_ms`, and `on_error` per step to control transient-failure behavior and recovery.
- On `for_each`, these policies cover the entire loop: each attempt gets one timeout budget (including batch pauses), and retry restarts at the first item. Previously completed children may run again, so only enable loop retries for work that is safe to repeat. A `cost_limit` stop or an attempted OpenClaw dispatch prevents retries; paid LLM usage is checked even when a later pipeline command fails. `on_error: continue` records `$loop.error` and proceeds to the next workflow step; `skip_rest` skips the remaining workflow steps.
- Approval steps can optionally enforce identity constraints:
  - `approval.required_approver` (or `requiredApprover`) requires an exact approver id.
  - `approval.require_different_approver` (or `requireDifferentApprover`) requires approver id to differ from initiator.
  - `approval.initiated_by` (or `initiatedBy`) sets the initiator id for comparison.
  - `LOBSTER_APPROVAL_INITIATED_BY` can provide a default initiator id at run time.
  - `LOBSTER_APPROVAL_APPROVED_BY` is used at resume/approval time for identity checks.

### Retries after OpenClaw dispatch

`openclaw.invoke` (including `clawd.invoke`) and `openclaw.agent` suppress automatic workflow-step retries once dispatch is attempted. This applies to timeouts, HTTP or process failures, and errors in later commands of the same pipeline step, even when `retry.max` is greater than one. A failed local request cannot establish whether a remote action already completed. Read-only and dry-run requests use the same conservative rule: Lobster does not know the remote tool’s retry guarantees, and the [OpenClaw gateway currently ignores `dryRun`](https://docs.openclaw.ai/gateway/tools-invoke-http-api#request-body). The flag does not prevent tool execution. Check the remote outcome before manually running the step again. `llm.invoke` retains its existing retry policy.

### Command-level input requests

Pipeline commands can call `ctx.requestInput({ prompt, responseSchema, defaults, subject, suspendedState })` to pause in tool mode, workflows, or the SDK and resume the same command after a structured response. CLI/tool resume tokens store only a state key; the persisted state validates the suspended request metadata before returning the submitted response to the command. SDK same-command resumes store the command frame in the configured SDK state directory.

Commands are re-run on resume, so they must be idempotent until `requestInput` returns. Array-backed command input is snapshotted with bounds for replay; lazy stream input is not buffered and requires a compact JSON `suspendedState` supplied by the command. On resume, call `ctx.requestInput.getSuspendedState()` before reading lazy input to restore that command-owned continuation state.

## Visualizing workflows

Use `lobster graph` to inspect workflow structure before execution.

```bash
lobster graph --help
lobster graph --file path/to/workflow.lobster
lobster graph --file path/to/workflow.lobster --format mermaid
lobster graph --file path/to/workflow.lobster --format dot
lobster graph --file path/to/workflow.lobster --format ascii
lobster graph --file path/to/workflow.lobster --args-json '{"location":"Seattle"}'
```

What gets visualized:

- each workflow step as a node (`run`, `pipeline`, `approval`, etc.)
- data-flow edges from `stdin: $step.stdout` / `$step.json` references
- conditional dependencies from `when:` / `condition:` expressions
- approval gates as diamond-shaped nodes in `mermaid` and `dot` output

Format notes:

- `mermaid` (default): emits `flowchart TD` text for GitHub/Markdown rendering
- `dot`: emits Graphviz DOT syntax
- `ascii`: emits a terminal-friendly node/edge list

## Calling LLMs from workflows

Use `llm.invoke` from a native `pipeline:` step for model-backed work:

```bash
llm.invoke --prompt 'Summarize this diff'
llm.invoke --provider openclaw --prompt 'Summarize this diff'
llm.invoke --provider pi --prompt 'Summarize this diff'
```

Provider resolution order:

- `--provider`
- `LOBSTER_LLM_PROVIDER`
- auto-detect from environment

Built-in providers today:

- `openclaw` via `OPENCLAW_URL` / `OPENCLAW_TOKEN`
- `pi` via `LOBSTER_PI_LLM_ADAPTER_URL` (typically supplied by the Pi extension)
- `http` via `LOBSTER_LLM_ADAPTER_URL`

The OpenClaw adapter calls the gateway's `llm-task` tool: artifacts become `input`, and `--max-output-tokens` becomes `maxTokens`. It reads JSON from the tool's `details.json` result. `--output-schema` is included in the model prompt and validated locally so `--max-validation-retries` still controls retries, including validation feedback. Gateway failures are surfaced without a validation retry. The tool currently does not return token usage, so this adapter cannot estimate spend or enforce token-based cost limits for these calls.

A host embedding Lobster can supply its own adapters through `ctx.llmAdapters`. Step `timeout_ms` and workflow cancellation reach an adapter as `ctx.signal`: Lobster stops waiting as soon as that signal aborts, so the step fails or retries on time either way, but it cannot cancel work an adapter has already started. An injected adapter should observe `ctx.signal` and abort its own request — otherwise a timed-out step can leave a model call running, and billed, in the background.

Workflow `_meta.cost` and `cost_limit` use a static pricing table plus optional overrides from `LOBSTER_LLM_PRICING_JSON`, for example `{"my-model":{"input":1.0,"output":2.0}}` in USD per million tokens. Unknown or missing model IDs still record token counts with zero estimated cost, but Lobster warns on stderr so stale or missing pricing does not fail silently.

A cached or replayed answer is not billed again: a model call is counted once, in the run that made it, however many later steps re-emit its answer. This holds while the answer stays inside Lobster: through pipelines, renderers, projections, run state, `workflow:` steps and a resume. It does not survive a stage that hands the items to an external process and reads them back — `exec --stdin json --json ...` — because what comes back is whatever that process printed, and Lobster cannot tell a faithful copy of a replay from a fresh claim to have made the call. Such a step is billed as a call, which is what earlier versions did everywhere. A `workflow:` step counts what its sub-workflow spent, and a replay the sub-workflow returned is not billed a second time by the run that composed it. Spend also survives a pause — a workflow that stops at an approval or `input` gate keeps what it has recorded, so `_meta.cost` covers the whole run after a resume and `cost_limit` applies to the whole run rather than to the steps after the last gate.

`llm_task.invoke` remains available as a backward-compatible alias for the OpenClaw provider.

### Calling configured OpenClaw agents

Use `openclaw.agent` when a workflow needs a configured OpenClaw agent rather than a direct model call:

```bash
openclaw.agent --agent ops --prompt 'Summarize these logs'
openclaw.agent --agent ops --session-key incident-42 --model openai/gpt-5.4 --prompt 'Continue the investigation'
```

The command delegates agent identity, model defaults and overrides, sessions, authentication, and execution to the installed `openclaw agent` CLI. It accepts `--agent`, `--session-key`, `--session-id`, `--model`, `--thinking`, `--timeout`, and `--local`, and returns OpenClaw's structured `--json` response. Pipeline input is appended to the prompt as labeled JSONL.

### `pipeline:` vs `run:` for LLM calls

- Use `pipeline:` for `openclaw.agent`, `llm.invoke`, and `llm_task.invoke` (they are Lobster pipeline stages, not shell executables).
- Use `run:` only for real binaries in your shell (for example `openclaw.invoke`).

Example (`stdin` from a prior step is passed to the LLM as artifacts):

```yaml
steps:
  - id: make_words
    run: echo "One two three four five six"

  - id: count_words
    pipeline: llm_task.invoke --prompt "How many words have been pasted below?"
    stdin: $make_words.stdout
```

## Calling OpenClaw tools from workflows

Shell `run:` steps execute in your system shell, so OpenClaw tool calls there must be real executables.

If you install Lobster via npm/pnpm, it installs a small shim executable named:

- `openclaw.invoke` (preferred)
- `clawd.invoke` (alias)

These shims forward to the Lobster pipeline command of the same name.

### Example: invoke llm-task

Prereqs:

- `OPENCLAW_URL` points at a running OpenClaw gateway
- optionally `OPENCLAW_TOKEN` if auth is enabled

```bash
export OPENCLAW_URL=http://127.0.0.1:18789
# export OPENCLAW_TOKEN=...
```

In a workflow:

```yaml
name: hello-world
steps:
  - id: greeting
    run: >
      openclaw.invoke --tool llm-task --action json --args-json '{"prompt":"Hello"}'
```

### Passing data between steps (no temp files)

Use `stdin: $stepId.stdout` to pipe output from one step into the next.

## Args and shell-safety

`${arg}` substitution is a raw string replace into the shell command text.

For anything that may contain quotes, `$`, backticks, or newlines, prefer env vars:

- every resolved workflow arg is exposed as `LOBSTER_ARG_<NAME>` (uppercased, non-alnum → `_`)
- the full args object is also available as `LOBSTER_ARGS_JSON`

Example:

```yaml
args:
  text:
    default: ""
steps:
  - id: safe
    command: |
      jq -n --arg text "$LOBSTER_ARG_TEXT" '{"result": $text}'
```

Values in an `env:` block are not shell-expanded, so `TEXT: "$LOBSTER_ARG_TEXT"`
sets `TEXT` to that literal string. To pass an arg under another name, use
`TEXT: "${text}"`; env values never go through the shell, so this stays safe.

## HTTP response limits

HTTP response bodies remain unlimited by default. Set
`LOBSTER_MAX_HTTP_RESPONSE_BYTES` to a positive integer to opt into a per-response
byte limit for `openclaw.invoke`/`clawd.invoke` and the built-in OpenClaw, HTTP,
and Pi `llm.invoke` adapters. Unset or empty means unlimited; invalid or non-positive
values fail before dispatch. Host-injected adapters own their transport limits.

```bash
LOBSTER_MAX_HTTP_RESPONSE_BYTES=1048576 lobster run --mode tool 'openclaw.invoke --tool example --action read'
```

Workflow and step `env` can set the same variable. SDK integrations pass it in
their environment (`new Lobster({ env: { ...process.env,
LOBSTER_MAX_HTTP_RESPONSE_BYTES: "1048576" } })` or the tool-runtime context).
Responses exactly at the limit succeed. Oversized declared or streamed bodies
are cancelled and rejected before JSON parsing; no partial response is returned.
The limit counts bytes read from the response stream, including decompressed
content. A declared Content-Length over the limit is also rejected immediately.
Tool dispatch remains non-retryable after an overflow, as with other
post-dispatch errors; `llm.invoke` retains its existing retry policy.

## Development

The source is one TypeScript package. `src/core` contains the embeddable tool API, cost tracking, and LLM accounting; `src/sdk` provides pipeline composition; `src/commands` holds the command registry and standard library. Workflow loading, expressions, dry-run rendering, and execution live under `src/workflows`. `src/state` owns atomic file persistence, locks, and resume capabilities. GitHub SDK recipes and built-in workflows share transport and snapshot helpers.

Run `pnpm test`, `pnpm typecheck`, and `pnpm lint` before submitting changes. Tests compile into `dist/test` and use Node's test runner; platform-specific process tests run only where their OS primitives exist. Dependencies observe the two-day release-age policy in `pnpm-workspace.yaml`.

To validate release packaging from the current `main` without publishing, run `gh workflow run lobster-npm-release.yml --ref main -f tag=v$(node -p "require('./package.json').version") -F dry_run=true`. This builds the selected commit and skips publishing, promotion, and uploading a promotable artifact. The existing `preflight_only` mode still builds the requested release tag for later promotion.
