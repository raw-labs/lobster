# Patterns and pitfalls

## Contents

- [Designing a workflow](#designing-a-workflow)
- Recipes: [ask a person before acting](#ask-a-person-before-acting) ·
  [decide with a model, ask a person when unsure](#decide-with-a-model-ask-a-person-when-unsure) ·
  [review loop](#review-loop) · [poll until done](#poll-until-done) ·
  [wait for an outside event](#wait-for-an-outside-event) · [batch](#batch) ·
  [fan out](#fan-out) · [undo on failure](#undo-on-failure) ·
  [notify only on change](#notify-only-on-change) · [reuse a workflow](#reuse-a-workflow)
- [Running on a schedule](#running-on-a-schedule)
- [Pitfalls](#pitfalls)

## Designing a workflow

1. **List the steps and what each needs from earlier ones.** Each step passes data on through
   `stdin: $prev.json`, `env: { X: $prev.json.field }`, or references in its command text.
2. **Mark what is irreversible** (sending, paying, deploying, deleting) and put an
   `approval:` before it, with what the person needs to decide in its `stdin:`.
3. **Mark where you wait** (a person, a time, an outside event) and use `approval:` / `input:`
   / `sleep:` / `wait:`. The run is saved there; nothing has to stay running.
4. **Decide what may run twice.** After a crash, a command whose outcome is unknown blocks the
   run unless the step says `recovery: repeat`; say it only for commands that are safe to
   repeat (reads, idempotent writes keyed by `$LOBSTER_ACTIVITY_ID`).
5. **Decide what a failure means** for each step: stop the run (default), `retry` it, or
   `on_error: continue` and handle `$id.error` later.
6. **Make the last step produce the result** the caller needs; it is the run's output.
7. **Dry-run it** (`lobster run --mode tool --dry-run flow.lobster --args-json '{...}'`) and
   fix what it reports, then run it.

## Ask a person before acting

```yaml
name: deploy
args:
  version: { default: 1.0.0 }
steps:
  - id: build
    run: make release VERSION="$LOBSTER_ARG_VERSION" && cat dist/manifest.json
  - id: approve
    approval:
      prompt: Deploy this build to production?
      on_reject: continue
      timeout: 1d                   # no answer in a day: cancel
    stdin: $build.json
  - id: deploy
    run: ./deploy.sh "$LOBSTER_ARG_VERSION"
    when: $approve.approved
  - id: declined
    run: echo "not deployed"
    when: "!$approve.approved"
```

Running it returns `needs_approval` with `requiresApproval.resumeToken`. Show the person
`requiresApproval.prompt` and `items`, then `lobster resume <token> --approve yes|no` with
their answer.

## Decide with a model, ask a person when unsure

Both paths end in a step whose output has `label`, so `$label.json.label` is the decision
either way.

```yaml
name: triage
args:
  ticket: { default: "The export button does nothing on Safari" }
steps:
  - id: ticket
    run: printf '%s' "$LOBSTER_ARG_TICKET"
  - id: model
    pipeline: >
      llm.invoke --prompt "Label this ticket bug, feature or question, with a confidence from 0 to 1"
      --output-schema '{"type":"object","properties":{"label":{"enum":["bug","feature","question"]},"confidence":{"type":"number"}},"required":["label","confidence"]}'
      | map --unwrap output | map --unwrap data
    stdin: $ticket.stdout
  - id: label
    switch:
      cases:
        - when: $model.json.confidence < 0.8
          steps:
            - id: ask
              stdin: $ticket.stdout          # the person sees the ticket
              input:
                prompt: The model is unsure. Which label?
                responseSchema:
                  type: object
                  properties: { label: { enum: [bug, feature, question] } }
                  required: [label]
            - id: answered
              run: cat
              stdin: $ask.response
      else:
        - id: confident
          run: cat
          stdin: $model.json
  - id: file
    run: echo "filed as $LABEL"
    env: { LABEL: $label.json.label }
```

## Review loop

A draft goes to a person until they approve it. Questions are fine inside a sequential
`repeat` (one without `retry`).

```yaml
name: review-loop
steps:
  - id: drafting
    repeat: { until: $review.response.decision == approve, max: 5 }
    steps:
      - id: draft
        pipeline: >
          llm.invoke --prompt "Write a short release note. Apply any feedback given."
          | map --unwrap output | map --unwrap text
      - id: review
        stdin: $draft.json
        input:
          prompt: Approve the note, or say what to change
          responseSchema:
            type: object
            properties:
              decision: { enum: [approve, revise] }
              feedback: { type: string }
            required: [decision]
  - id: publish
    run: cat
    stdin: $draft.json
```

To feed the feedback back to the model, pass it as input: `stdin: $review.response` on the
`draft` step (the first round has none yet, so guard or default it).

## Poll until done

```yaml
- id: ci
  repeat: { until: $check.json.state != running, max: 60 }
  steps:
    - id: check
      run: >-
        gh run view "$RUN_ID" --json status
        --jq '{state: (if .status == "completed" then "done" else "running" end)}'
      env: { RUN_ID: $start.json.id }
    - id: pause
      sleep: 1m
      when: $check.json.state == running
```

A sleep over `LOBSTER_INLINE_SLEEP_MS` (60 s) pauses the run (`status: waiting`); something
must call `lobster runs advance --due` (cron, a host loop) to continue it.

## Wait for an outside event

One event name, with a field saying what happened; the timeout is the third outcome.

```yaml
name: order
args:
  order: { default: o-1 }
steps:
  - id: outcome
    wait: { event: order.update, match: { order: "${order}" }, timeout: 3d, on_timeout: continue }
  - id: route
    switch:
      cases:
        - when: $outcome.timedOut
          steps:
            - id: chase
              run: echo "no news; chasing"
        - when: $outcome.json.status == paid
          steps:
            - id: ship
              run: echo "shipping"
      else:
        - id: other
          run: echo "unexpected update"
```

`lobster signal order.update --data '{"order":"o-1","status":"paid"}'` delivers one.

## Batch

```yaml
- id: files
  run: ls inbox/*.csv | jq -R . | jq -s .
- id: each
  for_each: $files.json
  concurrency: 4
  steps:
    - id: load
      run: ./load.sh "$FILE"
      env: { FILE: $item.json }
      retry: { max: 3, backoff: exponential, delay_ms: 1000 }
      on_error: continue            # one bad file does not stop the others
- id: report
  run: jq '[.[] | select(.load == null) | .item]'   # files whose load did not succeed
  stdin: $each.json
```

No approvals, questions, sleeps or waits inside a loop with `concurrency > 1`.

## Fan out

```yaml
- id: gather
  parallel:
    wait: all
    timeout_ms: 120000
    branches:
      - id: issues
        run: gh issue list --json number,title
      - id: prs
        run: gh pr list --json number,title
- id: combine
  run: >-
    jq '{issues: .issues, prs: .prs}'
  stdin: $gather.json
```

`wait: any` takes the first branch to finish (a race between mirrors, say).

## Undo on failure

Let the risky step fail without stopping the run, undo what came before, then fail the run so
it shows why:

```yaml
- id: reserve
  run: ./inventory.sh reserve "$LOBSTER_ARG_ORDER"
- id: charge
  run: ./billing.sh charge "$LOBSTER_ARG_ORDER"
  on_error: continue
- id: undo
  switch:
    cases:
      - when: $charge.error == true
        steps:
          - id: release
            run: ./inventory.sh release "$LOBSTER_ARG_ORDER"
          - id: stop
            run: echo "charge failed; reservation released" >&2; exit 1
    else: []
- id: ship
  run: ./shipping.sh create "$LOBSTER_ARG_ORDER"
```

## Notify only on change

```yaml
- id: changed
  pipeline: exec --json --shell 'gh pr list --json number,title' | diff.last --key open-prs
- id: notify
  run: ./notify.sh
  stdin: $changed.json.after
  when: $changed.json.changed
```

## Reuse a workflow

```yaml
- id: notes
  workflow: format-notes.lobster         # relative to this file
  workflow_args:
    title: Release 2.0
    items: $changes.json                 # the child receives the JSON text
```

In the child, read args from `$LOBSTER_ARGS_JSON` and parse a list that may arrive as text.
A child cannot ask questions or wait (approvals, inputs, sleeps, waits): keep those in the
parent.

## Running on a schedule

Lobster has no scheduler of its own. A daily check, from cron:

```sh
# Start the workflow every morning; keep its envelope (it may stop at a question).
0 8 * * * cd /srv/checks && LOBSTER_STATE_DIR=/srv/lobster lobster run --mode tool uptime.lobster --args-json '{"urls":["https://a.example"]}' >> uptime.log
# Continue sleeps, fire timeouts and recover crashed runs.
* * * * * LOBSTER_STATE_DIR=/srv/lobster lobster runs advance --due >> advance.log
```

If the run can stop for a person, the job that started it must tell them: send the
`requiresApproval.prompt` and `approvalId` somewhere they will see it (or run Lobster under a
host such as OpenClaw, which does). They answer with `lobster resume <approvalId> --approve
yes|no`. A model answer is cached by prompt and input; use `llm.invoke --refresh` (or put the
date in the input) so each day gets a fresh one.

## Pitfalls

- **Reading `llm.invoke` directly.** Its output is a record (`output.data`, `usage`, ...).
  Unwrap: `| map --unwrap output | map --unwrap data` (or `text` without a schema).
- **Lobster commands in `run:`.** `run: llm.invoke ...` fails: these commands only exist in
  `pipeline:`. A pipeline can call programs with `exec`.
- **`${arg}` in shell text.** It is pasted in as is; quotes or `$` in the value break the
  command or worse. Use `"$LOBSTER_ARG_NAME"` (quoted) in `run:`.
- **Expecting every step's output.** The run's output is its last step's. Pass what you need
  to the last step.
- **Comparing missing values.** `$s.json.score > 0.8` fails when `score` is missing; guard with
  `$s.json.score != null && ...`. `== null` and `in [null, ...]` treat a missing field as null.
- **Prompts with args.** `approval.prompt` and `input.prompt` are plain text; put the details
  in the step's `stdin:`.
- **Questions in the wrong place.** Approvals, inputs, sleeps and waits are refused inside
  `parallel:`, `for_each` with `concurrency > 1`, child workflows, and loops or switches with
  `retry`.
- **Approving for the person.** Never `lobster resume ... --approve yes` without the person's
  decision.
- **Forgetting the scheduler.** Long sleeps, deadlines and crashed runs continue only when
  `lobster runs advance --due` runs (cron, or the host). Events continue on `lobster signal`.
- **Retrying non-idempotent work.** `retry` and `recovery: repeat` re-run commands; make them
  safe to run twice (use `$LOBSTER_ACTIVITY_ID` as an idempotency key) or leave them off.
- **YAML that is not what it looks like.** A plain value containing `: ` (as in
  `--jq '{a: .b}'`) is not valid YAML, and a comma inside a `{ ... }` flow mapping
  (`{ id: x, run: echo "a, b" }`) splits the value. Write such commands as a block:
  `run: >-` with the command on the next lines (folded into one line). A value starting with
  `!`, `{`, `[`, `*`, `&` or `@` needs quotes: `when: "!$approve.approved"`. The dry run
  reports the line and column.
- **List args in loops.** `for_each: ${urls}` gets text, not a list. Print the list from
  `$LOBSTER_ARGS_JSON` in a step and loop over `$step.json`; inside, use `$item.json`.
- **Finding "my" run.** `runs list` is ordered by ID, not time; keep the `runId` from the
  `run` envelope instead.
