# Workflow file syntax

A workflow file is YAML (or JSON) ending in `.lobster`, `.yaml`, `.yml` or `.json`. Unknown
keys are refused when the file loads, with the nearest known key suggested.

## Contents

- [Top level](#top-level)
- [Steps: common keys](#steps-common-keys)
- [Values, references and templates](#values-references-and-templates)
- [Step kinds](#step-kinds): run, pipeline, approval, input, sleep, wait, switch, for_each,
  repeat, parallel, workflow
- [Conditions](#conditions)
- [Failure handling](#failure-handling): retry, timeout_ms, heartbeat_timeout_ms, on_error,
  recovery
- [Cost limits](#cost-limits)
- [Where questions and waits are allowed](#where-questions-and-waits-are-allowed)

## Top level

```yaml
name: my-workflow              # optional
description: What it does      # optional
args:                          # optional; with it, only these args are accepted
  repo:
    description: owner/name
  limit:
    default: 20                # an arg without default must be given
env:                           # optional, for every step (values not shell-expanded)
  GH_PAGER: ""
cwd: ./work                    # optional, relative to the run's directory
cost_limit: { max_usd: 0.50, action: stop }   # optional; action: stop | warn
steps:                         # required: the steps, in order
  - id: list
    run: gh pr list --repo "$LOBSTER_ARG_REPO" --limit "$LOBSTER_ARG_LIMIT" --json number
```

Args come from `--args-json '{"repo":"a/b"}'`. A workflow that declares `args:` refuses
unknown names and missing required ones before any step runs.

## Steps: common keys

Every step has a unique `id` (keep to letters, digits, `_` and `-`, so `$id.field`
references read unambiguously) and one kind (below). Any step may
also take:

| Key | Meaning |
|---|---|
| `when` (or `condition`) | Run only if the condition holds; otherwise the step is `skipped` |
| `env` | Extra environment variables (values may reference args and steps) |
| `cwd` | Working directory (relative to the run's directory) |
| `stdin` | Input for the step: `$id.json`, `$id.stdout`, `$id.json.field`, or a literal |
| `retry`, `timeout_ms`, `heartbeat_timeout_ms`, `on_error`, `recovery` | See [Failure handling](#failure-handling) |

A step's result, readable by later steps as `$id.<field>`:

| Field | Set by |
|---|---|
| `stdout` | Commands and pipelines: the raw output |
| `json` | Output parsed as JSON; for a pipeline, its single item (or the list of items) |
| `approved` | Approvals: `true` / `false` |
| `response` | `input:` steps (the answer); `wait:` steps (the event's id, name, time) |
| `timedOut` | Approval, input or wait that hit its timeout with `on_timeout: continue` |
| `skipped` | `true` when `when:` was false, or a branch/case did not run |
| `error`, `errorMessage` | A step that failed under `on_error: continue` / `skip_rest` |
| `case` | `switch`: the case that ran (from 1) or `"else"` |
| `rounds` | `repeat`: how many rounds ran |

The workflow's own output is the output of the **last step that ran**.

## Values, references and templates

- `$id.json`, `$id.stdout`, `$id.json.a.b`, `$id.approved`: a step's result. They work in
  `stdin`, `env`, `when`, `for_each`, `workflow_args`, switch cases, `repeat.until`, and
  inside `run:` and `pipeline:` text (an exact `$id.path` is replaced by the value; in a
  longer string it is replaced by its text). Shell variables without a dot (`$HOME`) are left
  alone.
- `${arg}`: an arg, pasted into the text as is. Unsafe for free text in shell commands:
  read `$LOBSTER_ARG_<NAME>` (uppercased; non-alphanumerics become `_`) or
  `$LOBSTER_ARGS_JSON` instead. In `env:` values, `${arg}` is safe (no shell).
- Inside a `for_each`, `$item.json` and `$index.json` (or the names in `item_var` /
  `index_var`); `$item` alone is not replaced.
- Args are text when substituted: a list arg becomes `a,b` in `${urls}` and
  `$LOBSTER_ARG_URLS`. For a list, read `$LOBSTER_ARGS_JSON` in a step that prints it as JSON,
  and use that step's `json`.
- The run's directory is the one `lobster run` was started in. Commands run there (unless
  `cwd:` says otherwise), relative paths resolve there, and it stays the same when the run is
  resumed or advanced from elsewhere.
- Every command also gets `LOBSTER_RUN_ID`, and `LOBSTER_ACTIVITY_ID` /
  `LOBSTER_ACTIVITY_ATTEMPT` (unique per dispatch; use them as idempotency keys).

## Step kinds

### run (or command): a shell command

```yaml
- id: list
  run: gh pr list --repo "$LOBSTER_ARG_REPO" --json number,title
```

Runs with the system shell. Non-zero exit fails the step.

### pipeline: Lobster commands

```yaml
- id: summary
  pipeline: >
    llm.invoke --prompt "Summarize these pull requests" | map --unwrap output | map --unwrap text
  stdin: $list.json
```

Stages joined by `|` pass JSON values. Every command and flag is checked when the file loads.
See `pipeline-commands.md`. A pipeline that asks (`approve`, `ask`) pauses the run there.

### approval: a person decides

```yaml
- id: confirm
  approval: Deploy to production?          # or: approval: true, or the object form
  stdin: $build.json                        # what the person is shown

- id: deploy
  run: ./deploy.sh
  approval:                                 # on a command step: asked after the command ran
    prompt: Keep this deploy?
    timeout: 1d
    on_timeout: cancel                      # cancel (default) | continue
    on_reject: continue                     # "no" → approved: false (default: cancels the run)
    required_approver: alice                # or require_different_approver: true
```

Object keys: `prompt`, `items`, `preview`, `timeout`, `on_timeout`, `on_reject`,
`initiated_by`, `required_approver`, `require_different_approver` (camelCase spellings
accepted too, one per setting). A "no" cancels the run unless `on_reject: continue`.
Approval steps report `$id.approved`.

### input: a person answers

```yaml
- id: review
  stdin: $draft.json                         # shown with the question
  input:
    prompt: Approve, or say what to change
    responseSchema:
      type: object
      properties:
        decision: { enum: [approve, revise] }
        notes: { type: string }
      required: [decision]
    defaults: { decision: approve }          # optional
    timeout: 2d                              # optional, with on_timeout: cancel | continue
```

The answer must fit the schema (JSON Schema draft-07). It is `$review.response`.

### sleep: wait for time

```yaml
- id: cool_off
  sleep: 2h          # 500ms, 30s, 5m, 2h, 3d, or a number of milliseconds
```

Up to `LOBSTER_INLINE_SLEEP_MS` (60 s by default) it waits in place; longer, the run returns
`waiting` (`waitingFor.kind: timer`) and `lobster runs advance --due` continues it.

### wait: an outside event

```yaml
- id: payment
  wait:
    event: order.update
    match: { order: "${order}" }    # the event's data must contain these fields
    timeout: 3d
    on_timeout: continue            # default: cancel the run
```

Returns `waiting` (`waitingFor.kind: event`) until `lobster signal order.update --data
'{"order":"o-1","status":"paid"}'`. `$payment.json` is the event's data. An event delivered
after the run started but before it reached the wait still counts.

### switch: several branches

```yaml
- id: route
  switch:
    cases:
      - when: $triage.json.label == refund
        steps: [ { id: refund, run: ./refund.sh } ]
      - when: $triage.json.label in [spam, junk]
        steps: [ { id: archive, run: ./archive.sh } ]
    else: []                       # without else and with no case matching, the step fails
```

The first case whose `when` holds runs. `$route.case` is its number or `"else"`; `$route.json`
is the output of its last step. Steps of other cases count as skipped.

### for_each: per item

```yaml
- id: each
  for_each: $files.json            # a list
  concurrency: 4                   # optional, up to 1000; default 1
  steps:
    - id: process
      run: ./process.sh "$FILE"
      env: { FILE: $item.json }
```

`$each.json` is a list with one entry per item, in order: `{ item, index, <stepId>: output }`.
A step that was skipped, or failed under `on_error: continue`, has no key in its entry.
`item_var` / `index_var` rename `$item` / `$index`. With `concurrency: 1`, `batch_size` and
`pause_ms` pace the items. If an item fails, no new item starts and the loop fails; a retry
(`runs advance`) runs only the items that did not finish.

### repeat: rounds until a condition

```yaml
- id: ci
  repeat: { until: $check.json.state != running, max: 30, on_max: fail }
  steps:
    - id: check
      run: >-
        gh run view "$RUN" --json status --jq '{state: .status}'
    - id: pause
      sleep: 1m
      when: $check.json.state == running
```

`until` is checked after each round; `max` is required (`on_max: fail | continue`). After the
loop, the last round's steps are readable and `$ci.rounds` says how many ran.

### parallel: at once

```yaml
- id: gather
  parallel:
    wait: all                      # all (default) | any: the first to finish wins
    timeout_ms: 60000
    branches:
      - id: weather
        run: ./weather.sh
      - id: calendar
        pipeline: exec --json --shell './calendar.sh'
```

Branches take `id`, `run`/`command`/`pipeline`, `env`, `cwd`, `stdin`, `recovery`. Each
branch's result is readable by its id (`$weather.json`); `$gather.json` is `{ weather, calendar }`.
With `wait: any`, the losers are stopped and count as skipped. A failing branch fails the step.

### workflow: another workflow file

```yaml
- id: notes
  workflow: format-notes.lobster   # relative to this file; may be "$pick.stdout"
  workflow_args:
    title: Release 2.0
    items: $changes.json           # arrives in the child as JSON text
```

The child runs inside this run (its definition is frozen when the run starts, or when the
step is entered for a name built from a step's output). Its output is this step's output. Its
own `cost_limit` applies to its own calls. A child that calls itself is refused.

## Conditions

Used by `when`, switch `cases[].when` and `repeat.until`.

- Values: `$id.field`, quoted strings, numbers, `true`, `false`, `null`, lists `[a, b]`. On the
  right of an operator a bare word is a string: `$r.json.decision == approve`.
- Operators: `==`, `!=`, `<`, `<=`, `>`, `>=`, `in`, `contains`, `matches "regex"`, `!`, `&&`,
  `||`, parentheses.
- `<` `<=` `>` `>=` need numbers (numeric strings count). A missing value fails the step; guard
  it: `$s.json.score != null && $s.json.score > 0.8` (`&&` / `||` short-circuit).
- Compared with the literal `null` (also inside a list), a missing field counts as null. Two
  fields never match on "no value".
- `in` / `contains` test list membership, or a substring when both are strings.
- Checked when the file loads: syntax, that referenced steps run earlier, and literals that
  could never work.

## Failure handling

```yaml
- id: fetch
  run: curl -fsS https://example.com/data.json
  retry: { max: 3, backoff: exponential, delay_ms: 500, max_delay_ms: 10000, jitter: true }
  timeout_ms: 30000              # per attempt; covers work, not pauses
  heartbeat_timeout_ms: 60000    # stop a command that shows no progress for this long
  on_error: continue             # stop (default) | continue | skip_rest
  recovery: repeat               # after a crash mid-command: repeat | manual (default)
```

- `retry.max` counts attempts in all (default 1, no retry). `backoff`: `fixed` (default) or
  `exponential`.
- `on_error: continue` completes the step with `error: true` and `errorMessage`;
  `skip_rest` also skips the remaining steps.
- A heartbeat is touching the file in `$LOBSTER_HEARTBEAT_FILE` (shell) or calling
  `ctx.heartbeat()` (pipeline command).
- `recovery`: if the process dies while a command runs, its outcome is unknown. With
  `manual` (default) the run becomes `blocked` until someone checks and runs
  `lobster runs advance` (repeats nothing unsafe) or `lobster runs abandon`. With `repeat`, the
  command runs again; use it only for commands that are safe to repeat.
- Commands that may not be repeated (`openclaw.invoke`, `openclaw.agent`, `gog.gmail.send`)
  are never retried automatically once dispatched, whatever `retry` or `recovery` say.
- On `for_each` / `repeat` / `switch`, `retry` re-runs the whole step from its start.

## Cost limits

`cost_limit: { max_usd, action }` at the top of a workflow counts the model calls
`llm.invoke` makes in the run (cached or replayed answers are not calls). `action: stop`
ends the run once spend passes the limit; `warn` writes to stderr and goes on. Calls are
priced from a table of known models; set `LOBSTER_LLM_PRICING_JSON`
(`{"model":{"input":USD_per_M_tokens,"output":...}}`) for others, or they count as zero. A
child workflow's limit applies to its own calls.

## Where questions and waits are allowed

Approvals, `input:`, `sleep:` and `wait:` pause the run, so they are refused where several
things run at once or would be asked again:

- inside `parallel:` branches;
- inside a `for_each` with `concurrency > 1` (a sequential loop, even with `batch_size` /
  `pause_ms`, is fine);
- inside a child workflow (`workflow:` step), including pipelines in it that call
  `approve` / `ask`;
- inside a `for_each`, `repeat` or `switch` that has `retry`.

They are fine at the top level and inside switch cases and sequential loops.
