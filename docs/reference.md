# Lobster reference

The full reference for Lobster. Start with the [README](../README.md) for what Lobster is and a first workflow.

- **Workflows**
  - [Workflow files](#workflow-files)
    - [Conditions](#conditions)
    - [Switch](#switch)
    - [Loops](#loops)
    - [Recipes](#recipes)
    - [Retries after external dispatch](#retries-after-external-dispatch)
    - [Command-level input requests](#command-level-input-requests)
  - [Waiting for time and events](#waiting-for-time-and-events)
    - [Heartbeats](#heartbeats)
  - [Restarting workflow-file runs](#restarting-workflow-file-runs)
  - [Visualizing workflows](#visualizing-workflows)
- **Runs and storage**
  - [Persistence and run history](#persistence-and-run-history)
    - [Run and pending review API](#run-and-pending-review-api)
    - [Upgrading from an older version](#upgrading-from-an-older-version)
    - [Embedded and SDK usage](#embedded-and-sdk-usage)
- **Commands and pipelines**
  - [Commands](#commands)
    - [Command packs](#command-packs)
  - [Process output limits](#process-output-limits)
  - [Question limits](#question-limits)
  - [HTTP response limits](#http-response-limits)
  - [Args and shell-safety](#args-and-shell-safety)
- **LLMs and OpenClaw**
  - [Calling LLMs from workflows](#calling-llms-from-workflows)
  - [Calling OpenClaw tools from workflows](#calling-openclaw-tools-from-workflows)
- **Hosts and the SDK**
  - [Host invocation authority](#host-invocation-authority)
  - [SDK cancellation](#sdk-cancellation)
  - [GitHub recipe cancellation](#github-recipe-cancellation)
- **Development**
  - [Building and testing](#building-and-testing)
  - [Source layout and checks](#source-layout-and-checks)

# Workflows

## Workflow files

Lobster workflow files are meant to read like small scripts:

- `run:` or `command:` for deterministic shell/CLI steps
- `pipeline:` for native Lobster stages like `llm.invoke`
- `approval:` for hard workflow gates between steps
- `when:` and `switch:` for branching (see [Conditions](#conditions) and [Switch](#switch))
- `stdin: $step.stdout` or `stdin: $step.json` to pass data forward

```
lobster run path/to/workflow.lobster
lobster run --file path/to/workflow.lobster --args-json '{"tag":"family"}'
```

`--args-json` must be a JSON object. Anything else (an array, a string, `null`, an empty value) is an input error (exit code 2, `parse_error` in tool mode) and no step runs. A workflow that declares `args:` takes only those names: an arg it does not declare, or a declared arg with no `default` that is not given, is refused before any step runs (for a child workflow too, with its `workflow_args`). A workflow without `args:` takes any args.

Invalid workflow file paths are reported as input errors (exit code 2), whether passed with `--file` or as a single positional path (no whitespace; contains a path separator or ends in `.lobster`, `.yaml`, `.yml`, or `.json`). In `--mode tool`, these failures use the same JSON error envelope as other parsing errors.

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
    approval: { prompt: Want jacket advice from the LLM?, on_reject: continue }
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
- Unknown keys are rejected when the file loads, with a suggestion for likely typos, so a misspelled `when:` cannot make a step run unconditionally.
- `<`, `<=`, `>` and `>=` compare numbers (numeric strings such as `"0.9"` count). Comparing anything else, including a missing value or `null`, fails the step with an error rather than evaluating to false. Guard optional values with `&&` or `||`, which skip the right side when the left decides the result; compared with the literal `null` (also in parentheses), a missing field counts as null; two fields never match on "no value": `$score.json.value != null && $score.json.value > 0.8`.
- Use `retry`, `timeout_ms`, and `on_error` per step to control transient-failure behavior and recovery.
- On `for_each` and `repeat`, these policies cover the entire loop: each attempt gets one timeout budget (`pause_ms` pauses between batches do not count against it), and retry restarts at the first item or round. Previously completed children may run again, so only enable loop retries for work that is safe to repeat. A `cost_limit` stop or an attempted OpenClaw dispatch prevents retries; model calls are counted even when a later pipeline command fails. `on_error: continue` records `$loop.error` and proceeds to the next workflow step; `skip_rest` skips the remaining workflow steps.
- Approval steps can optionally enforce identity constraints:
  - `approval.required_approver` (or `requiredApprover`) requires an exact approver id.
  - `approval.require_different_approver` (or `requireDifferentApprover`) requires approver id to differ from initiator.
  - `approval.initiated_by` (or `initiatedBy`) sets the initiator id for comparison.
  - `LOBSTER_APPROVAL_INITIATED_BY`, `LOBSTER_APPROVAL_REQUIRED_APPROVER` and `LOBSTER_APPROVAL_REQUIRE_DIFFERENT_APPROVER` give the host's defaults at run time, for step approvals and for an `approve` inside a pipeline alike; a step's own `approval:` settings take precedence over them.
  - `LOBSTER_APPROVAL_APPROVED_BY` is used at resume/approval time for identity checks.
  - A step whose output asks for approval itself (`{ prompt, requiredApprover, ... }`) only fills in what the step's settings and these variables leave unset.
- A rejected approval cancels the run, whether the "no" is typed at the terminal or sent with `resume --approve no`. At the terminal, an approval takes `y`/`yes` or `n`/`no` (an empty line is no); any other answer is refused, as `resume --approve` refuses it, and asked again when the run is advanced. In human mode, `lobster run` reports a cancelled run on stderr and exits 1. With `approval: { prompt: ..., on_reject: continue }`, a "no" completes the step with `approved: false` and the run goes on, so later steps can check `when: $step.approved`. Such a rejection is a decision like an approval: the identity rules above apply to it.

### Conditions

`when:` (or its older spelling `condition:`; a step takes one, not both) and switch cases take a condition:

- **Values:** step fields (`$classify.json.label`, `$confirm.approved`, `$route.case`), strings in quotes, numbers, `true`, `false`, `null`, and lists (`[refund, spam]`). On the right of an operator, a bare word is a string: `$review.response.decision == approve`.
- **Operators:** `==`, `!=`, `<`, `<=`, `>`, `>=`, `in`, `contains`, `matches`, then `!`, `&&`, `||` and parentheses. A comparison does not chain.
- **`in` / `contains`:** `$c.json.label in [refund, spam]` and `$c.json.tags contains urgent` test list membership (by value, without converting types); with two strings they test for a substring. Anything else fails the step.
- **`matches`:** `$c.json.label matches "^urgent-"` tests a regular expression, which must be a quoted string in the file. Only a string can be matched.
- **Checked when the file loads:** syntax (errors give the character position), that every step a condition reads runs before it, and literal operands that would fail on every run (`$n.json.count > "high"`, `5 matches "x"`).

### Switch

A `switch` step runs the steps of its first case whose `when` holds, or its `else`:

```yaml
steps:
  - id: classify
    pipeline: llm.invoke --prompt "Classify this email" --output-schema '{"type":"object"}'

  - id: route
    switch:
      cases:
        - when: $classify.json.label == refund && $classify.json.score >= 0.8
          steps:
            - id: confirm
              approval: { prompt: Issue the refund?, on_reject: continue }
            - id: refund
              run: billing refund --order ${order}
              when: $confirm.approved
        - when: $classify.json.label in [spam, junk]
          steps:
            - id: drop
              run: mail archive ${message}
      else:
        - id: triage
          run: tickets create --queue support

  - id: notify
    run: notify --summary
    stdin: $route.json
```

- The choice is made once from saved results and kept, so a resumed or recovered run continues the same case.
- A case's steps are full steps: approvals, inputs, waits, child workflows, nested switches.
- `$route.json` is the output of the last step that ran in the chosen case ("whichever branch ran"), and `$route.case` is the case's number from 1, or `"else"`.
- Steps of the cases that did not run count as skipped (`$drop.skipped`), so later steps can read any of them. A case cannot read the steps of another case.
- With no matching case and no `else`, the step fails; `else: []` means "nothing".
- A switch takes `when`, `timeout_ms`, `on_error` and `retry` like any step; a retry runs the chosen case again, so only retry work that is safe to repeat.

### Loops

`for_each` runs its steps once per item of a list; `repeat` runs them in rounds until a condition holds:

```yaml
steps:
  - id: drafts
    pipeline: llm.invoke --prompt "Draft a reply to each email" --output-schema '{"type":"array"}'

  - id: each
    for_each: $drafts.json          # item_var (default item), index_var (default index)
    steps:
      - id: confirm
        approval: { prompt: Send this reply?, on_reject: continue }
        run: cat
        stdin: $item.json
      - id: send
        pipeline: gog.gmail.send
        stdin: $item.json
        when: $confirm.approved

  - id: ci
    repeat: { until: $check.json.state != running, max: 60 }
    steps:
      - id: check
        run: ci status --json
      - id: pause
        sleep: 1m
        when: $check.json.state == running
```

- The steps inside a loop are steps like any other: approvals, input, sleeps, waits, switches, child workflows and loops. A question inside a loop is asked once per item or round, and the run pauses until it is answered.
- **`for_each`:** inside, `$item` and `$index` are the current item and its position. `$each.json` is a list with one entry per item: the item, its index and the output of each step. The steps' own ids are visible only inside the loop. Inside the loop, `$item` and `$index` (or the names `item_var` and `index_var` give) mean the loop's own variables, even when an earlier step or an outer loop's variable has the same name; no step inside the loop may take one of those names, or the name of a step before the loop.
- **`concurrency: N`** on a `for_each` runs up to N items at once (default 1; at most 1000). `$each.json` keeps item order whichever finishes first. If an item fails, no new item starts, the items already running finish, and then the loop fails; their outputs are kept, so a retry (`lobster runs advance`) runs only the items that did not finish. A loop that runs items at once cannot contain approvals, input, sleeps or waits (several items would wait at once), and cannot use `pause_ms`/`batch_size`, which pace items one at a time. Loops nest: `concurrency: 4` inside `concurrency: 4` can run 16 items at once.
- **`repeat`:** `until` is checked after each round, so the steps run at least once. `max` is required: after that many rounds the step fails, or ends with `on_max: continue`. A round sees the results of the round before it (`$check.json` in round 2 is round 1's until `check` runs again). Only the last round's results are kept, so a long loop does not grow the saved run. Afterwards later steps see the last round's steps, and `$ci.rounds` says how many rounds ran; `$ci.json` is the output of the last step that ran.
- A loop or a `switch` with `retry` cannot contain approvals, input, sleeps or waits, nor a pipeline that asks: a retry runs it again from its start, so it would ask again. This is refused when the file loads, or, for a pipeline, when it asks.
- A step's `timeout_ms` covers its work, not pauses: when a run pauses inside a loop, a switch or any step that encloses the pause, those steps get a fresh budget when the run continues. Waits that do not pause the run do not count either: a question answered at the terminal, a `pause_ms` between batches, a short `sleep` waited in place, a retry delay. (Inside a `parallel` step or a `for_each` with `concurrency`, a retry delay in one branch still counts: the other branches are working.)

### Recipes

Patterns for things other engines build in, written with the steps above.

**Decide with a classifier, and ask a person when it is unsure.** The output schema keeps the model to the options, the answer is saved (a resume does not call the model again), and a low confidence goes to a person. Both paths end in a step whose `json` has `label`, so `$label.json.label` is the decision either way:

```yaml
steps:
  - id: email
    run: mail show --json ${email_id}
  - id: triage
    pipeline: >
      llm.invoke --prompt "Classify this email"
      --output-schema '{"type":"object","properties":{"label":{"enum":["refund","spam","other"]},"confidence":{"type":"number"}},"required":["label","confidence"]}'
    stdin: $email.json
  - id: label
    switch:
      cases:
        - when: $triage.json.confidence < 0.8
          steps:
            - id: ask
              input:
                prompt: Which kind of email is this?
                responseSchema:
                  type: object
                  properties: { label: { enum: [refund, spam, other] } }
                  required: [label]
            - id: answered
              run: cat
              stdin: $ask.response
      else:
        - id: model
          run: cat
          stdin: $triage.json
  - id: route
    switch:
      cases:
        - when: $label.json.label == refund
          steps:
            - id: refund
              run: billing refund --email ${email_id}
        - when: $label.json.label == spam
          steps:
            - id: drop
              run: mail archive ${email_id}
      else:
        - id: ticket
          run: tickets create --queue support
```

**Remind an approver who does not answer.** Each round asks; if a day passes, it sends a reminder and asks again. The loop ends on an answer, and fails after five days without one:

```yaml
steps:
  - id: chase
    repeat: { until: $ok.approved || !$ok.timedOut, max: 5 }
    steps:
      - id: ok
        approval: { prompt: Approve the refund?, timeout: 1d, on_timeout: continue }
      - id: remind
        run: notify-approver --about refund
        when: $ok.timedOut
  - id: refund
    run: billing refund --order ${order}
    when: $ok.approved
```

**Wait for whichever outcome comes first.** A `wait:` takes one event name, so let the sender use one name with a field that says what happened, and branch on it; the timeout is the third outcome:

```yaml
steps:
  - id: outcome
    wait: { event: order.update, match: { order: "${order}" }, timeout: 3d, on_timeout: continue }
  - id: route
    switch:
      cases:
        - when: $outcome.timedOut
          steps:
            - id: chase
              run: notify-sales --order ${order}
        - when: $outcome.json.status == paid
          steps:
            - id: ship
              run: shipping create --order ${order}
        - when: $outcome.json.status == cancelled
          steps:
            - id: release
              run: inventory release --order ${order}
      else:
        - id: unknown
          run: tickets create --queue orders
```

`lobster signal order.update --data '{"order":"o-1","status":"paid"}'` delivers one.

**Undo earlier steps when a later one fails.** Let the risky step fail without stopping the run, undo what came before it, then fail the run so it does not go on and shows why:

```yaml
steps:
  - id: reserve
    run: inventory reserve --order ${order}
  - id: charge
    run: billing charge --order ${order}
    on_error: continue
  - id: undo
    switch:
      cases:
        - when: $charge.error == true
          steps:
            - id: release
              run: inventory release --order ${order}
            - id: stop
              run: echo "charge failed; reservation released" >&2; exit 1
      else: []
  - id: ship
    run: shipping create --order ${order}
```

Undo steps are ordinary steps: give them `retry`, and make them safe to run twice, since a crash can repeat one.

### Retries after external dispatch

Commands declared `repeatable: false` — `openclaw.invoke` (including `clawd.invoke`),
`openclaw.agent` and `gog.gmail.send` — are never repeated automatically. After a crash, a step
whose pipeline contains one is reported as `blocked` instead of being re-run, even with
`recovery: repeat`, because nothing records whether it had already acted.

Within a run, these commands suppress automatic workflow-step retries once dispatch is attempted. This applies to timeouts, HTTP or process failures, and errors in later commands of the same pipeline step, even when `retry.max` is greater than one. A failed local request cannot establish whether a remote action already completed. Read-only and dry-run requests use the same conservative rule: Lobster does not know the remote tool’s retry guarantees, and the [OpenClaw gateway currently ignores `dryRun`](https://docs.openclaw.ai/gateway/tools-invoke-http-api#request-body). The flag does not prevent tool execution. Check the remote outcome before manually running the step again. `llm.invoke` retains its existing retry policy.

### Command-level input requests

Pipeline commands can call `ctx.requestInput({ prompt, responseSchema, defaults, subject, suspendedState })` to pause in tool mode, workflows, or the SDK and resume the same command after a structured response. Resume tokens store only a state key; the run saves the suspended command's frame and validates the suspended request metadata before returning the submitted response to the command. This is the same for the CLI, tool mode and the SDK.

Every schema Lobster checks (`ask --schema`, `responseSchema`, workflow `input.responseSchema`, `llm.invoke --output-schema`) is JSON Schema draft-07. `format` is not checked, keywords from later drafts are ignored, `nullable: true` also allows null, and each schema stands alone, so separate schemas may share an `$id`. Lobster validates without generating code, so it also runs where code generation from strings is disabled. A host with its own validator can run requests inside `withSchemaCompiler(compiler, callback)` from `@clawdbot/lobster/core` to use it instead.

Commands are re-run on resume, so they must be idempotent until `requestInput` returns. Array-backed command input is snapshotted with bounds for replay; lazy stream input is not buffered and requires a compact JSON `suspendedState` supplied by the command. On resume, call `ctx.requestInput.getSuspendedState()` before reading lazy input to restore that command-owned continuation state.

## Waiting for time and events

Lobster runs on demand: a run works until it finishes or has to wait, saves, and
exits. It never runs a loop of its own. Waiting for time or for something outside
is a pause, like an approval, and something outside decides when to look again.

```yaml
steps:
  - id: send
    pipeline: gog.gmail.send
  - id: cool_off
    sleep: 3d                 # 500ms, 30s, 5m, 2h, 3d, or a number of ms
  - id: reply
    wait:                     # resolved by `lobster signal email.reply`
      event: email.reply
      match: { thread: $send.json.thread }
      timeout: 2d
      on_timeout: continue    # default: cancel the run
  - id: chase
    run: send-reminder
    when: $reply.timedOut
  - id: approve
    approval:
      prompt: Close the ticket?
      timeout: 24h            # questions can expire too
```

- **`sleep:`** fixes its wake time when first reached. Up to
  `LOBSTER_INLINE_SLEEP_MS` (default 60 s) the process waits in place; a longer
  sleep returns `status: "waiting"` with `waitingFor: { kind: "timer", until }`
  and exits. Ctrl-C or a crash during a sleep leaves an ordinary sleeping run.
- **`wait:`** completes with the oldest event of that name received since the run
  started that it has not consumed, whose data contains every field of `match`
  (references such as `$send.json.thread` are resolved first). `$reply.json` is
  the event's data and `$reply.response` its id, name and time. Until then the run
  returns `status: "waiting"` with `waitingFor: { kind: "event", event }`.
- **`timeout`/`on_timeout`** on `wait:`, `approval:` (object form) and `input:`
  fix a deadline when the step is first reached. After it, `cancel` (the default)
  cancels the run like a rejection, and `continue` completes the step with
  `timedOut: true` and no approval or response. An answer after the deadline is
  refused. With `continue` on an approval, the following steps run without
  approval unless they check it (`when: $approve.approved`); use it only for an
  escalation path.
- Events are scoped like runs: an event signalled by a caller with a `callerId`
  reaches only that caller's runs, and its delivery `--id` cannot collide with
  another caller's. An unscoped caller (the CLI, a trusted host) reaches any run.
- Sleeps, waits and deadlines are gates: like approvals they are allowed inside
  `switch` cases and loops, but not inside `parallel` branches, child workflows, a
  `for_each` with `concurrency`, or a loop or `switch` with `retry`.

Something outside continues these runs:

```sh
lobster runs advance --due [--limit 100] [--concurrency 4] [--dry-run]   # every run whose time has come
lobster signal email.reply --data '{"thread":"t-42"}' [--id <delivery id>]
```

`runs advance --due` advances runs whose sleep or deadline has passed, and runs
whose process died (their lease lapsed): completed work is kept, a step that may
have acted re-runs only when it is repeatable and `recovery: repeat`, everything
else becomes `blocked` for a person, and blocked runs are never due. Concurrent
callers skip runs another process holds. Timers fire at or after their time,
never early; how late depends on how often this is called. `lobster signal`
stores the event (a repeated `--id` is ignored), then advances the runs waiting
for it; a run that reaches its `wait:` later still finds it. Embedded hosts use
`advanceDueRuns({ ctx, concurrency })` and `signalEvent({ name, data, id, ctx })` from
`@clawdbot/lobster/core`.

One call advances up to 4 due runs at once (`--concurrency N` to change it); each run's
own steps still run in order, and each run keeps its own lease, so a slow step in one run
delays only that run. The runs' commands do overlap: two runs of the same workflow may
call the same API, or write the same file, at the same time. `--concurrency 1` advances
them one after another. In the load tests, waking 1,000 runs whose next step takes 50 ms
went from 15 runs/s at 1 to 59/s at 4 and 185/s at 16.

Who calls `runs advance --due` is the caller's choice: a long-running host such as
the OpenClaw gateway every minute or so, or the operating system:

```sh
# cron: every minute
* * * * * LOBSTER_STATE_DIR=$HOME/.lobster/state lobster runs advance --due >/dev/null
```

On macOS a launchd agent with `StartInterval` 60, on Linux a systemd timer with
`OnUnitActiveSec=60`, does the same.

### Heartbeats

A step can require signs of progress, so a hung but still running command is
stopped instead of waiting for `timeout_ms`:

```yaml
- id: export
  run: export-everything
  heartbeat_timeout_ms: 60000
```

A shell command (or `exec`) shows progress by touching the file named in
`LOBSTER_HEARTBEAT_FILE`; a pipeline command calls `ctx.heartbeat()`. Silence for
longer than the limit stops the step like a timeout (its process tree is killed),
after which `retry`, `on_error` and the repeatable rules apply. A dead process is
already covered by the run's lease and `runs advance --due`. Deliberate waits
inside the step (a question at the terminal, a `pause_ms` between batches, a short
`sleep`, a retry delay) are not silence: waiting is not hanging.

## Restarting workflow-file runs

A workflow run saves its definition, resolved activity inputs, complete results,
control position and spend. After a process restart, completed activities
are not executed again. Continue an unfinished run explicitly:

```sh
lobster runs list
lobster runs show <runId>
lobster runs advance <runId>
```

Embedded hosts use `advanceWorkflowRun({ runId, ctx })` from
`@clawdbot/lobster/core`. An active execution lease prevents a competing
advance; after process death, wait for lease expiry before recovery, or let
`lobster runs advance --due` pick the run up once its lease has lapsed (see
[Waiting for time and events](#waiting-for-time-and-events)).

An interrupted command with no saved result has an unknown outcome by default.
Only commands safe to repeat should opt in:

```yaml
steps:
  - id: fetch
    run: fetch-metrics --json
    recovery: repeat
```

`recovery: manual` is the default. It stops automatic continuation at an
uncertain activity. There is no general external-action reconciliation API yet.

A run stopped while a command was running (Ctrl-C, SIGTERM, a host abort, or an
advance that meets such a command) has status `blocked` (`lobster runs list
--status blocked`). Check what the command did, then either continue it with
`lobster runs advance <runId>` (steps with `recovery: repeat` run again) or stop
it for good:

```sh
lobster runs abandon <runId>
```

Abandoning records the started commands as abandoned (nothing runs again),
withdraws pending questions and approval IDs, and cancels the run, so `history
prune` can remove it. Embedded hosts use `abandonWorkflowRun({ runId, ctx })`.
A step whose pipeline contains a command that may not be repeated
(`openclaw.invoke`, `openclaw.agent`, `gog.gmail.send`) is never re-run after a
crash, even with `recovery: repeat`. Arbitrary shell commands must be classified by the author;
Lobster cannot infer their effects. `LOBSTER_RUN_ID` and `LOBSTER_ACTIVITY_ID`
provide stable logical identities to adapters; `LOBSTER_ACTIVITY_ATTEMPT`
identifies the saved attempt.

Explicit retries still respond to reported failures. They preserve their attempt
count and selected backoff deadline across restarts. Whole-control-node retries
deliberately repeat their subtree; recovery within an attempt skips completed
children. Retry delays and `for_each` batch pauses wait inside the current
invocation; use a `sleep:` step for a durable pause.

Shell commands and pipelines are whole activity boundaries, not arbitrary
instruction-level checkpoints. A partially executed pipeline may have effects;
its existing structured-input protocol is the only supported internal
continuation. SDK JavaScript closures are not made restartable by this engine.

See [execution-machine.md](execution-machine.md) for the implementation
boundary and source layout.

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

- each workflow step as a node (`run`, `pipeline`, `approval`, etc.), in order, joined by `next` edges
- data-flow edges from `stdin: $step.stdout` / `$step.json` references and from a `for_each` list
- conditional dependencies from `when:` / `condition:` expressions
- a `switch` and its cases: each case's steps are nodes of their own, entered by an edge labelled with the case's condition
- a `parallel`, `repeat` or `for_each` step as one node: a later step that reads one of its branches or inner steps gets an edge from that node, and what its insides read (a branch's `stdin`, the steps' `stdin` and conditions, a repeat's `until`) is drawn as edges into it; names local to it (its own steps, loop variables) draw none
- approval gates and switches as diamond-shaped nodes in `mermaid` and `dot` output

References inside `run` commands and pipeline text are not drawn.

Format notes:

- `mermaid` (default): emits `flowchart TD` text for GitHub/Markdown rendering
- `dot`: emits Graphviz DOT syntax
- `ascii`: emits a terminal-friendly node/edge list

# Runs and storage

## Persistence and run history

Lobster keeps its state in one SQLite database, `lobster.sqlite`, in the state
directory (`~/.lobster/state` by default, or `LOBSTER_STATE_DIR`). Every run saves its
progress: workflow files, standalone pipelines (`lobster '...'`, `runToolRequest({ pipeline })`)
and SDK pipelines all run on the same engine, so they pause, resume and recover the same
way. Detailed diagnostic history remains opt-in:

```sh
LOBSTER_HISTORY=1 lobster run --mode tool --file workflow.lobster
lobster history list
lobster history show <runId>                    # first 50 events
lobster history show <runId> <afterSequence>    # next page
```

Lobster needs the same runtime as OpenClaw: **Node >=24.16.0 <25 or >=26.1.0**
(earlier `node:sqlite` builds truncate text at embedded NUL characters).
**The SQLite library must contain the WAL-reset fix**: 3.51.3 or later,
or the 3.50.7/3.44.6 backports and subsequent patches on those branches. The worker
checks `sqlite_version()` on its actual connection before enabling WAL or writing
schema. Runtime version metadata can differ from a dynamically selected library.
Upgrade the runtime or selected SQLite library if qualification rejects it.
See the [SQLite fix](https://sqlite.org/wal.html#walreset).
A worker owns the SQLite connection, prepared statements, and short transactions;
embedded hosts receive promises. The database uses WAL, a 5-second busy timeout,
and FULL durability. Writes are short transactions, so processes rarely wait on each
other; but a process that is frozen (a debugger, `SIGSTOP`, a suspended laptop) while
inside one keeps the write lock, and other processes then fail with "database is locked"
after 5 seconds until it goes on. Nothing is lost: the next attempt, or the frozen
process once it resumes, carries on. A session admits at most 256 outstanding worker requests
(configurable through `maxPending`); overflow fails before dispatch.

A tracked execution keeps one run ID through approvals and structured-input
pauses. History records step occurrences, attempts, retries, loop item ordinals,
parallel branches, and child-workflow paths. Lazy stages finish when their
iterators finish, fail, or close early; recording does not drain an unused stream.
Workflow files and static child definitions are snapshotted before execution.
Continuation uses the snapshot, not a reread of the source file. Dynamic child
references are snapshotted when first entered. Scripts, command implementations
and the host environment are not frozen.

Default output summaries contain types and counts rather than raw outputs, error
messages, prompts, arguments, or environment variables. Summary payloads are
limited to 4 KiB. Inspection returns at most 100 events per page (50 by default),
with related attempts and waits, and excludes executable continuation payloads
and operation receipts. A logical export includes full authoritative state, so
store exports and backups privately. Continuations retain their existing full
payloads and existing output limits; history capture adds no execution truncation.

SQLite commits continuation transitions and their events together in indexed
tables. Each execution checkpoint replaces the previous one, stored with its run
metadata. Large values are kept beside it as records, written once in the same
transaction as the save that first needs them: a `for_each` loop's items and each
item's output, and any step output over 1 KB (the checkpoint refers to it). A save
therefore costs the same however long a loop is or however much a run has produced; a
loop of 8,000 items runs in a few seconds. A run that completes or is cancelled drops
its records; a failed run keeps them for a retry until retention deletes it, and
`storage export` includes them. New tracked tokens require this runtime; older
binaries cannot understand the new execution state. State saved by older
versions as loose JSON files needs `lobster upgrade` once (see
[Upgrading from an older version](#upgrading-from-an-older-version)). Every run is
recorded; `history: true` (`LOBSTER_HISTORY=1`) only adds diagnostic events, and each
run keeps its original history setting when resumed.

### Run and pending review API

Every run is recorded: workflow files, standalone pipelines and SDK pipelines alike;
add `history: true` for diagnostic events. A pending question is saved
with its original prompt, schema, defaults, and full review subject. Discovery
reads that snapshot, rather than rebuilding it from chat or a workflow file.

```ts
import {
  openStoreSession, listRuns, getRun, getWait, resumeToolRequest,
} from '@clawdbot/lobster/core';

const store = await openStoreSession({ directory: '/path/to/state' });
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

Workflow-file answers consume the question and save the accepted decision in one
transaction. The decision remains accepted if later execution is interrupted.
A successor question is published with its execution checkpoint in a separate
transaction; a crash between those commits leaves recoverable progress, not a
reusable old approval. Publication and consumption use ownership fencing and
revision checks. Lost checkpoint replies are reconciled against their receipts;
an unreadable receipt leaves the outcome explicitly unknown.

A pipeline is a run of one step whose command is the pipeline, so all of this applies
to pipelines and SDK pipelines too.

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

SDK function stages are closures, so a run saves only their position
(`sdk.stage.0 | sdk.stage.1 | ...`) and an instance with the same stages resumes it:
`lobster.resume(token, decision)` or `lobster.resumeWait({ waitId, expectedRevision, response })`.
An instance with a different number of stages is refused and the run is left as it was.
`lobster.advance(runId)` continues an SDK run that stopped without a question (its
process stopped, or it failed). `lobster runs advance` refuses SDK runs and
`runs advance --due` skips them: only their host has the stages. Like every token, an
SDK token resumes once; a token handed out by an older version is honoured once, then
refused. Workflow-file and pipeline waits resume through `resumeToolRequest`.

The SDK's ready-made stages run the built-in commands: `exec(command, options)` is `exec`
(JSON output by default; `json: false` yields lines; `shell`, `cwd`, `stdin`),
`approve({ prompt, preview, limit })` is `approve --emit`, `stateGet`/`stateSet` are
`state.get`/`state.set`, and `diffLast(key, { changesOnly })` is `diff.last`. They behave
exactly as those commands do in a pipeline, including how a resumed run treats them.
Legacy records do not retroactively acquire history or a known caller. Imported
older tracked waits may have `request: null` when the original question was not
saved; discovery does not invent it by rereading a changed workflow file.

CLI equivalents use the same API:

```sh
lobster run --mode tool --file review.lobster
lobster runs list --status waiting
lobster runs show <runId>
lobster waits show <waitId>
lobster resume <waitId> --response-json '{...}'
# Or use --approve yes|no or --cancel for the appropriate decision.
# <waitId> may also be the run's resume token or short approval ID. With a wait ID,
# --expected-revision <revision> refuses the answer if the question changed meanwhile.
```

Full wait inspection is an explicit read of private review material; callers must
apply their own display limits. If a durable workflow question cannot fit the
bounded tool envelope, the call reports a rendering error with its run ID and the
saved question remains discoverable. Pending waits are protected from terminal
history pruning. Runs can also continue through `advanceWorkflowRun` or
`lobster runs advance`, subject to interrupted-activity recovery policy. A failed run
is retried from the step that failed: steps that completed keep their results, and a
step that reported dispatch of a command that may not be repeated is not run again.
A failed run or resume reports the run's `runId` in its envelope (in human mode, `lobster run` prints `Run <runId>: ...`); once a resume is
accepted its token is spent, so retry with `lobster runs advance <runId>`. Failed runs keep their step records for this until retention deletes them.
No background dispatcher is included.

### Upgrading from an older version

Older versions kept state as loose JSON files in the state directory. After
installing this version, stop older Lobster processes and run once:

```sh
lobster upgrade             # or: lobster upgrade --dry-run
```

It moves everything into `lobster.sqlite` under the same keys, so resume tokens and
short approval IDs handed out before keep working:

- workflows paused at an approval step, an input step, or a command asking for
  input, and pipelines paused at `approve`, `ask` or a command asking for input,
  become waiting runs; steps and stages that already ran are not run again;
- paused SDK steps, approval IDs and `state.set` values are imported as they are. A
  paused SDK step becomes a run the first time its host resumes it, since only the
  host has its stages.

The original files are moved to `.pre-upgrade/<time>/` in the state directory. A
paused workflow whose workflow file is missing, or no longer has the paused step at
the same position, is moved to `.upgrade-skipped/` and listed in the report, as is
a value whose key already holds something different. Running it again does nothing.
It refuses while an older Lobster process holds a state lock.

Until then, commands that run or continue workflows (`run`, a bare pipeline, `signal`,
`runs advance`) ask on a terminal whether to import now; `resume`, tool mode and a run
without a terminal print a one-line reminder on stderr instead. Inspection commands
(`runs list`/`show`/`abandon`, `waits`, `history`, `storage`, `doctor`) do not check.
Resuming something not yet imported fails with error type `upgrade_required`. Hosts embedding Lobster can call
`upgradeStore({ directory })` from the core or SDK entry point instead.

```sh
lobster storage export                         # versioned logical archive
lobster history prune 2026-01-01T00:00:00Z      # dry run
lobster history prune 2026-01-01T00:00:00Z --apply
```

Finished runs (completed, failed or cancelled) are deleted automatically 30 days
after they last changed. The check runs at most once a day, when a command opens
the store for writing. Set `LOBSTER_RUN_RETENTION_DAYS` to keep them longer or
shorter (`0` keeps them forever); hosts can pass `retentionDays` to
`openStoreSession`. Waiting and blocked runs are never deleted automatically.

Retention removes only terminal runs older than the cutoff with no live
continuation, pending wait, started attempt, or unacknowledged operation receipt.
Valid consumed-capability markers may be removed with their terminal run; their
tokens remain unusable because nothing falls back to an old checkpoint.
Committed transitions have stable operation IDs and receipts. A lost reply is an
unknown outcome until reconciled; it does not authorize replaying an external
side effect. Receipts are acknowledged only after the executor observes commit,
and remain protected otherwise. Diagnostic attempts are not restart checkpoints;
workflow-file execution uses its separate saved execution records. A run's
`running` status is its last recorded state, not a liveness guarantee.
`getRun().execution` reports execution status, lease activity, saved retry dates
and unresolved activity paths. Storage receipts do not supply external-service
idempotency.

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
  directory: '/path/to/state', history: true,
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
the database connection.

The public `StoreSession` handle exposes lifecycle and inspection only:
`directory`, `history`, `readOnly`, `listRuns`, `getRun`, `getWait`, `inspect`, `retain`,
`reconcile`, `acknowledge`, and `close`. Use the execution APIs for
resume, and SDK state primitives for ordinary state. Backend access, raw
checkpoints, and executor transitions are internal, not package API.

Store discovery is also available as `store.getRun(runId, { callerId })` and
`store.getWait(waitId, { callerId })`; these use the same validation and caller
scope as the top-level functions. Omit the scope only for unscoped host access.
Existing top-level discovery functions remain supported. `inspect`, retention,
and receipt methods are unscoped host-maintenance APIs.

Both entry points export named persistence, migration, metadata, history and
resume types. Package builds include TypeScript declarations. `ResumeDecision`
allows exactly one of `{ approved: boolean }`, `{ response: unknown }`, or
`{ cancel: true }`. Core `ResumeToolOptions` also requires exactly one target:
`{ token }`, `{ approvalId }`, or `{ waitId, expectedRevision }`. SDK
`resumeWait` uses `WaitResumeOptions`; SDK token resume uses `ResumeDecision`.
Malformed JavaScript/tool requests still receive runtime validation.

SDK `run`, `resume` and `resumeWait` return `Promise<SdkResult>`, preserving the
existing SDK statuses (including `"error"`), output and saved request fields.
Core execution continues to return `ToolEnvelope`; these are not interchangeable
wire formats. SDK closures still require the original stages on resume.

Session injection takes precedence over environment configuration. Correlation
metadata accepts bounded `hostRunId`, `hostTaskId`, and `callerId` strings; these are
records, not credentials or continuing authorization. Listing uses lexical run
IDs as cursors, so new IDs before an existing cursor appear on a fresh scan.
Event pagination uses committed run-local sequence numbers and reads current
attempt/wait status for the returned references.

An SDK run is recorded like any other (in `stateDir`, or a given `storeSession`). Resume
still requires an instance with the original JavaScript stages; closures are not
serialized. A token from an older SDK is converted into a run on its first resume and
honoured once. Applications must obtain a user decision before resuming any approval, as
before.

# Commands and pipelines

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

### Command packs

Commands come in packs. The default registry enables all of them:

| Pack | Commands |
|---|---|
| `stdlib` | `where`, `pick`, `map`, `sort`, `head`, `dedupe`, `groupBy`, `template`, `diff.last`, `json`, `table`, `exec`, `state.get`, `state.set`, `approve`, `ask`, `llm.invoke`, `commands.list`, `workflows.list`, `workflows.run` |
| `openclaw` | `openclaw.invoke` (alias `clawd.invoke`), `openclaw.agent`, `llm_task.invoke` |
| `google` | `gog.gmail.search`, `gog.gmail.send`, `email.triage` |
| `github` | named workflows `github.pr.monitor` and `github.pr.monitor.notify`; SDK recipes in `@clawdbot/lobster/recipes/github` |

Every command declares a contract with `defineCommand`: a description, a schema of its flags,
labels for what it touches, and whether it is `repeatable`. Lobster enforces it:

- A flag the command does not declare fails the stage before it runs, with a suggestion for
  likely typos (`head --m 2` → `did you mean --n?`).
- A flag declared as a boolean never takes the next word as its value; only an explicit
  `true` or `false` does (`--dry-run false` is false).
- A command declared `repeatable: false` (`openclaw.invoke`, `openclaw.agent`,
  `gog.gmail.send`) is never repeated automatically; see
  [Retries after external dispatch](#retries-after-external-dispatch).

Older names keep working without special cases. `clawd.invoke` is an alias: the same command,
which learns the name it was invoked by from `ctx.commandName`. `llm_task.invoke` is a variant
of `llm.invoke` fixed to the OpenClaw adapter, kept separate because its cache and saved-run
namespaces are its own. `commands.list` reports `aliasOf` and `repeatable` for each name.

Hosts embedding Lobster can choose packs or add their own through the core entry point:

```ts
import { builtinPacks, createRegistry, defineCommand, runToolRequest } from "@clawdbot/lobster/core";

const notify = defineCommand({
	name: "acme.notify",
	meta: {
		description: "Post a message to the Acme channel",
		argsSchema: { type: "object", properties: { channel: { type: "string" } } },
		sideEffects: ["sends_message"],
		repeatable: false,
	},
	help: () => "acme.notify --channel <name>\n",
	async run({ input, args, ctx }) {
		ctx.onNonRetryableSideEffect?.(); // report dispatch right before acting
		// ...
		return { output: (async function* () {})() };
	},
});
const registry = createRegistry({
	packs: [...builtinPacks, { name: "acme", description: "Acme", commands: [notify] }],
});
await runToolRequest({ pipeline: "acme.notify --channel ops", ctx: { registry } });
```

The flags in `argsSchema` are checked before any stage runs: a flag the command does not declare, a `type: "number"` flag whose value is not a number, a value outside a flag's `enum`, and a flag given together with another spelling of it (a property with `aliasOf: "<flag>"`) are refused.

A name or alias defined by two packs is an error rather than an override.

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

## Question limits

- `LOBSTER_APPROVAL_INPUT_TIMEOUT_MS`: how long a question asked at a terminal (an approval, an `input:` step, `ask`, `requestInput`) waits for its answer. Unset, zero or not a positive number means no limit. A read that times out fails only the question: `lobster runs advance` asks it again without running the step before it again.
- `LOBSTER_MAX_TOOL_ENVELOPE_BYTES`: the largest `needs_input` tool envelope a host accepts (default 512000 bytes; values under 1024 are ignored). A question's subject that does not fit is cut to a preview, then left out; a question that still does not fit is an error.

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

# LLMs and OpenClaw

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

Each model call `llm.invoke` makes is counted when its answer arrives, under the step that made it: the `workflow:` step for a call inside a child workflow, `loop.step` inside a `for_each`. A cached or replayed answer is not a call and is never counted, however many later steps pass it on. Every call counts, including a rejected answer that `--max-validation-retries` asks again for, a call in a `wait: any` branch that lost the race (if its answer came before the branch was stopped), and a call in a step that then failed (so `cost_limit` stops a retry after a paid attempt). Only `llm.invoke` calls count: usage another command prints in its output is not counted. Spend survives a pause, so `_meta.cost` covers the whole run after a resume and `cost_limit` applies to the whole run rather than to the steps after the last gate; a child workflow's limit applies to what it spent.

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

# Hosts and the SDK

## Host invocation authority

Hosts using the core `runToolRequest()` and `resumeToolRequest()` APIs can pass
`ctx.assertInvocationCurrent`, a synchronous callback that throws when the host
has retired an invocation. Lobster rechecks it before built-in process, network,
and state effects, including after asynchronous resume preparation. Keep passing
an `AbortSignal` to cancel work that has already started.

For every run, workflow file or pipeline, a committed answer remains accepted; later
host retirement cannot undo it.
Authority is checked again before activity dispatch and question publication.
Recovery uses saved execution rather than asking for the same decision again.
Custom commands and injected adapters must check the callback after asynchronous
preparation and before their own effects. The SDK does not expose this
host-authority contract.

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

## GitHub recipe cancellation

The SDK GitHub recipes accept an `AbortSignal` in both
`prMonitor({ repo, pr, signal })` and `prMonitorNotify({ repo, pr, signal })`.
Aborting stops the running GitHub CLI child before the recipe returns an error
result containing the abort reason. A cancelled fetch does not update the saved
PR snapshot or emit a notification.

For a manually composed pipeline, pass the signal to
`new Lobster({ signal }).pipe(ghPrView({ repo, pr }))`. Custom stages receive
`ctx.signal` and must cooperate with it to cancel their own work.

# Development

## Building and testing

Requires Node.js >=24.16.0 <25 or >=26.1.0 and the pnpm version pinned in `package.json`. From this folder:

- `pnpm install --frozen-lockfile`
- `pnpm build`
- `pnpm test`
- `pnpm lint`
- `node ./bin/lobster.js --help`
- `node ./bin/lobster.js doctor`
- `node ./bin/lobster.js "exec --json --shell 'echo [1,2,3]' | head --n 2 | json"` (prints `[1, 2]`)

### Notes

- `pnpm test` runs `tsc` and then executes tests against `dist/`.
- `bin/lobster.js` runs the compiled entrypoint in `dist/`; build after changing source files.

## Source layout and checks

The source is one TypeScript package; only `src/cli.ts` sits at the top of `src`.

| Folder | Holds |
|---|---|
| `src/core` | The embeddable tool API (public entry point), tool runtime and run queries |
| `src/sdk` | Pipeline composition for the SDK entry point |
| `src/commands` | The command contract (`defineCommand`), the pack registry and the `stdlib` pack |
| `src/packs` | The `openclaw`, `google` and `github` packs |
| `src/pipeline` | Pipeline parsing and stage checks, the stage runtime, the one-shot runner (`runPipeline`), input requests and pipeline resume state |
| `src/workflows` | Workflow loading, expressions, definition snapshots, dry runs, graphs and the retry policy |
| `src/engine` | Saved execution, node handlers, activities, `lobster upgrade`, and the run API (`runner.ts`: start, resume, advance, abandon; `due.ts`: due runs and events) |
| `src/persistence` | The SQLite run store, sessions, history and waits |
| `src/state` | Stored values (`state.get`/`state.set`), short approval IDs, state keys, file locks and atomic writes |
| `src/resume` | Resume arguments, options and tokens |
| `src/llm` | `llm.invoke`'s protocol, adapters, answer cache and flags; cost tracking |
| `src/process`, `src/io`, `src/util` | Child processes, stream and HTTP reads, small shared helpers |

Run `pnpm test`, `pnpm typecheck`, and `pnpm lint` before submitting changes.

`pnpm test:campaign` runs the testing campaign in `test/campaign`, which is too slow for every push and runs nightly in CI (`.github/workflows/campaign.yml`):

- **Randomized:** random workflows from every node type, run once undisturbed and once interrupted at random checkpoints (Ctrl-C, a crash before a save, a failed write), with answers replayed and timers advanced twice at once. Both runs must end the same way, and nothing that may not be repeated may run twice.
- **Chaos:** worker processes killed with SIGKILL at random checkpoints, frozen with SIGSTOP past their lease, given a clock that is wrong, and a database locked by another process.
- **Fuzzing:** random and broken conditions, workflow files, pipelines and command lines must be refused with a message, never a crash.
- **Load and soak:** many runs woken by `runs advance --due` at several concurrencies, long loops, and (nightly) a steady stream of runs for ten minutes.

`LOBSTER_CAMPAIGN_SCALE=nightly` runs many cases instead of a few. Every failure names its seed; `LOBSTER_CAMPAIGN_SEED=<seed> LOBSTER_CAMPAIGN_CASES=1` with the test's name pattern reruns that case exactly. Tests compile into `dist/test` and use Node's test runner; platform-specific process tests run only where their OS primitives exist. Dependencies observe the two-day release-age policy in `pnpm-workspace.yaml`.

To validate release packaging from the current `main` without publishing, run `gh workflow run lobster-npm-release.yml --ref main -f tag=v$(node -p "require('./package.json').version") -F dry_run=true`. This builds the selected commit and skips publishing, promotion, and uploading a promotable artifact. The existing `preflight_only` mode still builds the requested release tag for later promotion.
