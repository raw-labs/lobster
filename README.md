# 🦞 Lobster

![Lobster banner](docs/assets/readme-banner.jpg)

Lobster runs repeatable workflows for agents such as OpenClaw, with people in the loop.

A workflow is a file of steps. Lobster runs it on your machine, saves every step as it goes,
pauses when it needs a person, a time or an event, and picks up where it stopped. That
holds after a crash too. It needs no server: all state lives in one SQLite file.

This page is the short guide. The [reference](docs/reference.md) has everything else.

## Goals

- Typed pipelines (objects/arrays), not text pipes.
- Local-first execution.
- No new auth surface: Lobster must not own OAuth/tokens.
- Composable macros that OpenClaw (or any agent) can invoke in one step to save tokens.

## Install

Requires Node.js >=24.16.0 <25 or >=26.1.0 and the pnpm version pinned in `package.json`.

```sh
pnpm install --frozen-lockfile
pnpm build
node ./bin/lobster.js --help
```

## Five ideas

1. **Steps.** `run:` runs a shell command and `pipeline:` runs Lobster's typed commands
   (`llm.invoke`, `where`, `openclaw.invoke`…). A later step reads an earlier one as
   `$id.json`, `$id.stdout`, `$id.approved`.
2. **Gates.** `approval:` asks a yes/no and `input:` asks for structured data. The run
   pauses, and the agent or host shows the question and resumes with the answer.
3. **Flow.** `when:` runs a step only if a condition holds, and `switch:` picks one of
   several cases. `for_each:` loops over a list, `repeat:` loops until a condition holds,
   `parallel:` runs commands at once, and `workflow:` calls another workflow file.
4. **Waits.** `sleep: 3d` and `wait: { event: … }` pause a run until a time or an event.
   Nothing keeps running meanwhile. Something outside, such as cron or the OpenClaw
   gateway, calls `lobster runs advance --due` or `lobster signal`.
5. **Runs.** Every run is saved. Steps that finished are never run again. A command that
   may already have acted and isn't safe to repeat stops the run as `blocked` for a person
   to check, rather than running twice.

## A first workflow

```yaml
name: support-triage
args:
  email: { description: ID of the customer's email }
steps:
  - id: fetch
    run: mail show --json ${email}

  - id: classify
    pipeline: >
      llm.invoke --prompt "Classify this email as refund, spam or other, with a score"
      --output-schema '{"type":"object","properties":{"label":{"type":"string"},"score":{"type":"number"}}}'
      | map --unwrap output | map --unwrap data
    stdin: $fetch.json

  - id: route
    switch:
      cases:
        - when: $classify.json.label == refund && $classify.json.score >= 0.8
          steps:
            - id: confirm
              approval: { prompt: Issue the refund?, on_reject: continue }
            - id: refund
              run: billing refund --email ${email}
              when: $confirm.approved
        - when: $classify.json.label in [spam, junk]
          steps:
            - id: drop
              run: mail archive ${email}
      else:
        - id: ticket
          run: tickets create --queue support

  - id: follow_up
    sleep: 2d

  - id: survey
    run: send-survey --email ${email}
    when: $refund.json.ok == true
```

Run it, see where it stopped, and answer:

```sh
lobster run --file support-triage.lobster --args-json '{"email":"m-123"}'
lobster runs list --status waiting
lobster resume <approval ID or token> --approve yes
```

The run pauses at `confirm` until someone answers, then at `follow_up` for two days. A
scheduled `lobster runs advance --due` wakes it, and `survey` runs.

[examples/](examples/) has workflows that run as they are, one or two features each:
approvals, loops, parallel steps, waiting for events, calling other workflows, and the SDK.

`lobster run --dry-run <file>` prints the plan without running anything, and
`lobster graph <file>` draws it. A file with a typo in a key, a condition that reads a
step that doesn't run before it, or a malformed condition is refused before any step runs.

## Calling Lobster from an agent

Agents use tool mode, which prints one JSON envelope:

```sh
lobster run --mode tool --file workflow.lobster --args-json '{"tag":"family"}'
```

- **`ok`** is `false` on an error, and `error` says what went wrong.
- **`status`** is `ok`, `needs_approval`, `needs_input`, `waiting` or `cancelled`.
- **`requiresApproval` / `requiresInput`** hold the question and its resume token. Show
  them to the user, and resume only with the user's own answer:
  `lobster resume <token> --approve yes|no` or `--response-json '{…}'`.
- **`runId`** names the run, also on failures, so `lobster runs advance <runId>` can retry it.

A pipeline works the same way without a file: `lobster run --mode tool "exec --json --shell
'echo [1,2,3]' | head --n 2"`. Hosts can embed Lobster through
`@clawdbot/lobster/core` (`runToolRequest`, `resumeToolRequest`) or the SDK.

## Operating runs

```sh
lobster runs list [--status waiting|blocked|failed]
lobster runs show <runId>
lobster runs advance <runId>       # continue or retry a run
lobster runs advance --due         # wake every run whose time has come (cron, launchd, systemd)
                                   #   (4 at once; --concurrency N to change)
lobster signal <event> --data '{…}'
lobster runs abandon <runId>       # stop a run for good
```

Finished runs are deleted after 30 days (`LOBSTER_RUN_RETENTION_DAYS`). State saved by an
older version needs `lobster upgrade` once.

## Read more

- [Workflow files](docs/reference.md#workflow-files): every step key, [conditions](docs/reference.md#conditions), [switch](docs/reference.md#switch), [loops](docs/reference.md#loops), retries and errors.
- [Waiting for time and events](docs/reference.md#waiting-for-time-and-events): `sleep`, `wait`, deadlines and heartbeats.
- [Restarting runs](docs/reference.md#restarting-workflow-file-runs): crashes, `blocked` runs and `recovery: repeat`.
- [Persistence and run history](docs/reference.md#persistence-and-run-history): the store, the run API, upgrading and the SDK.
- [Commands](docs/reference.md#commands): command packs and adding your own.
- [Calling LLMs](docs/reference.md#calling-llms-from-workflows) and [OpenClaw tools](docs/reference.md#calling-openclaw-tools-from-workflows).
- [Development](docs/reference.md#development): building, testing and the source layout.
- [VISION.md](VISION.md): what Lobster is for, and what it leaves to the host.
