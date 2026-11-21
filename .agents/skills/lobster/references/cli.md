# The lobster CLI

Every command prints one JSON envelope on stdout and accepts `--mode tool`:

```json
{ "protocolVersion": 1, "ok": true, ...result }
{ "protocolVersion": 1, "ok": false, "error": { "type": "...", "message": "..." }, "runId": "..." }
```

Exit codes: 0 success; 2 a mistake in the command line (`parse_error`); 1 anything else.
stderr is for people (retry notices, warnings, the upgrade reminder). `lobster help <command>`
or `<command> --help` describes a command; `lobster help <pipeline command>` one of the
commands a pipeline runs.

In the Lobster repository, run it as `node bin/lobster.js` after `pnpm build`.

## Contents

- [run](#run) · [resume](#resume) · [signal](#signal)
- [runs list / show / advance / abandon](#runs) · [waits show](#waits-show)
- [history and storage](#history-and-storage) · [upgrade, doctor, graph, version](#upgrade-doctor-graph-version)
- [Error types](#error-types) · [Environment](#environment)

## run

```sh
lobster run --mode tool path/to/flow.lobster [--args-json '{"k":"v"}']
lobster run --mode tool --file path/to/flow.lobster [--args-json '{...}']
lobster run --mode tool 'exec --json --shell "echo [1,2]" | head --n 1'   # a pipeline
lobster run --mode tool --dry-run path/to/flow.lobster                    # plan only
```

- A lone word that names a file (contains `/` or ends in `.lobster`/`.yaml`/`.yml`/`.json`)
  is a workflow file; anything else is a pipeline. `lobster '<pipeline>'` works too.
- `--args-json` takes a JSON object, for a workflow file only.
- `--dry-run` loads and checks everything (keys, conditions, pipeline commands and flags, child
  workflows) and returns `plan` (text) without running or saving anything.
- **Always pass `--mode tool`.** Without it, `run` prints only the output and asks approvals
  and questions at the terminal.

Envelope fields: `status`, `output`, `requiresApproval`, `requiresInput`, `waitingFor`,
`runId`, `plan` (dry run).

| `status` | Also set |
|---|---|
| `ok` | `output`: the last step's output |
| `needs_approval` | `requiresApproval`: `{ prompt, items, preview?, resumeToken, approvalId, initiatedBy?, requiredApprover?, onReject? }` |
| `needs_input` | `requiresInput`: `{ prompt, responseSchema, defaults?, subject?, resumeToken }` |
| `waiting` | `waitingFor`: `{ kind: "timer", stepId, until }` or `{ kind: "event", stepId, event, until? }` |
| `cancelled` | the run was rejected, or hit a deadline with `on_timeout: cancel` |

A failure is `ok: false` with `error` and, when a run was created, `runId`.

## resume

```sh
lobster resume <ref> --approve yes        # or no
lobster resume <ref> --response-json '{"decision":"approve"}'
lobster resume <ref> --cancel
lobster resume <waitId> --approve yes --expected-revision 3
```

`<ref>` is the `resumeToken`, the short `approvalId`, or a wait ID (from `runs show` /
`waits show`). Exactly one decision. The answer must fit the question's schema. Prints the
run's envelope, as `run` does (the run continues until it finishes or stops again).

- A token is used once; answering again fails ("already answered or withdrawn", naming the
  run and its status). To retry a run that then failed, use `runs advance <runId>`.
- `--expected-revision` (with a wait ID) refuses the answer with `wait_conflict` if the
  question changed since that revision.
- Resume only with the person's decision. Never approve on their behalf.

## signal

```sh
lobster signal order.update --data '{"order":"o-1","status":"paid"}' [--id <delivery id>]
```

Stores the event, then advances the runs whose `wait:` step it matches (`match` fields
contained in `--data`). A run that reaches its wait later still finds it.
Envelope: `event: { id, name, inserted }`, `advanced: [{ runId, status }]`. The same `--id`
again is a retry (`inserted: false`); reused with other data or another event name, refused.

## runs

```sh
lobster runs list [--status running|waiting|blocked|completed|failed|cancelled] [--limit 20] [--after <runId>]
lobster runs show <runId>
lobster runs advance <runId>
lobster runs advance --due [--limit 100] [--concurrency 4] [--dry-run]
lobster runs abandon <runId>
```

- `list` → `runs: [{ id, kind, status, createdAt, updatedAt, ... }]`, `nextCursor` (pass to
  `--after`). Ordered by run ID, not time: sort by `createdAt` to find the newest.
- `show` → `run`: status, `execution` (status, unresolved activities, wake time),
  `pendingWaits: [{ id, kind, revision, ... }]`. It does not include step outputs.
- `advance <runId>` → the run's envelope. Continues a run that stopped (a crash, Ctrl-C, a
  lease that lapsed) or **retries a failed run from the step that failed** (finished steps
  keep their results). A step whose command may have acted is not run again unless it has
  `recovery: repeat`; such a run stays `blocked`.
- `advance --due` → `due: [runId]`, `advanced: [{ runId, status, error? }]`: every run whose
  sleep or deadline has passed, or whose process died. Call it from cron / a scheduler
  (`* * * * * lobster runs advance --due`). If a run failed or blocked, the envelope is not
  `ok`, the exit code is 1 and each such run is named on stderr.
- `abandon <runId>` → `runId`, `status`, `abandoned`: cancels the run for good and withdraws
  its questions. Use it for a `blocked` run you have checked by hand.

Run statuses: `running`, `waiting` (a question, a sleep or an event), `blocked` (a command's
outcome is unknown; check it, then advance or abandon), `completed`, `failed`, `cancelled`.

## waits show

```sh
lobster waits show <waitId>
```

→ `wait: { id, runId, revision, kind, request, ... }`: a pending question in full (prompt,
schema, subject).

## history and storage

```sh
LOBSTER_HISTORY=1 lobster run --mode tool flow.lobster   # record attempts for history
lobster history list [<after runId>]                     # → runs, nextCursor
lobster history show <runId> [afterSequence]             # → history: events, attempts (50 per page)
lobster history prune 2026-01-31 [--apply]               # → dryRun, runs (removed only with --apply)
lobster storage export                                   # → archive: the whole store
```

History records which steps ran and their status and timing, not their outputs. Finished runs
are deleted after 30 days (`LOBSTER_RUN_RETENTION_DAYS`, `0` keeps them).

## upgrade, doctor, graph, version

```sh
lobster upgrade [--dry-run]     # → upgrade (report), summary: import state an older version saved
lobster doctor                  # → output: version, a check that commands run
lobster graph flow.lobster [--format mermaid|dot|ascii] [--args-json '{...}']   # prints the graph (text)
lobster version                 # prints the version (text)
```

Resuming something an older version saved fails with `upgrade_required` until
`lobster upgrade` has run once.

## Error types

| `error.type` | Meaning | Exit |
|---|---|---|
| `parse_error` | Bad command line, flag, value, ID, args JSON, or a pipeline that does not parse | 2 |
| `runtime_error` | A step failed, a workflow file is invalid, a run was not found, ... | 1 |
| `wait_conflict` | The question changed or was answered meanwhile (`--expected-revision`, concurrent answers) | 1 |
| `upgrade_required` | State saved by an older version: run `lobster upgrade` | 1 |
| `activity_outcome_unknown` | A command was interrupted; its outcome is unknown (run is `blocked`) | 1 |
| `execution_conflict` | Another process holds the run right now | 1 |
| `persistence_error`, `persistence_outcome_unknown` | The store failed; `error.operationId` identifies the write | 1 |

## Environment

| Variable | Effect |
|---|---|
| `LOBSTER_STATE_DIR` | Where runs are saved (default `~/.lobster/state`) |
| `LOBSTER_HISTORY=1` | Record attempts for `history show` |
| `LOBSTER_INLINE_SLEEP_MS` | Sleeps up to this long wait in place (default 60000); longer ones pause the run |
| `LOBSTER_CACHE_DIR` | `llm.invoke`'s answer cache (default `.lobster-cache` in the current directory) |
| `LOBSTER_LLM_PROVIDER`, `LOBSTER_LLM_ADAPTER_URL`, `OPENCLAW_URL`, ... | Model provider (see `pipeline-commands.md`) |
| `LOBSTER_LLM_PRICING_JSON` | Prices for models not in the built-in table, for `cost_limit` |
| `LOBSTER_APPROVAL_INITIATED_BY`, `LOBSTER_APPROVAL_REQUIRED_APPROVER`, `LOBSTER_APPROVAL_REQUIRE_DIFFERENT_APPROVER`, `LOBSTER_APPROVAL_APPROVED_BY` | Approval identity checks |
| `LOBSTER_RUN_RETENTION_DAYS` | Days finished runs are kept (default 30) |
| `LOBSTER_DEBUG=1` | Print stack traces for unexpected errors |
