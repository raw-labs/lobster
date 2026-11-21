---
name: lobster
description: Build, run and debug Lobster workflows with the `lobster` CLI. Lobster runs multi-step automations as saved runs that can pause for a person (approvals, questions), wait for time or outside events, call LLMs, loop, branch and run steps in parallel, and resume later without redoing finished work. Use this skill whenever the user wants to write, fix or review a `.lobster` workflow file or a Lobster pipeline; run, resume, answer, signal, advance, abandon or inspect Lobster runs; turn a multi-step task (with approvals, polling, waiting for an event, an LLM decision, a human in the loop) into a Lobster workflow; or mentions lobster, resume tokens, approval IDs, `needs_approval`, `llm.invoke` or `runs advance`, even if they never say "skill".
---

# Lobster

Lobster is a workflow runtime for agents. A workflow file (YAML, `.lobster`) lists steps:
shell commands, Lobster pipelines (`llm.invoke`, `approve`, `where`, ...), approval gates,
questions, sleeps, event waits, loops, branches, parallel branches and calls to other
workflows. Every run is saved in a local SQLite store, so a run can stop at a question or a
wait, exit, and be continued later (by another process, hours later) without running its
finished steps again.

The CLI is made for agents: every command prints one JSON envelope on stdout. Read it;
don't scrape text.

## The loop

1. **Write** the workflow file (see "A first workflow" below, and
   `references/workflow-syntax.md` for every key).
2. **Check** it without running anything:
   `lobster run --mode tool --dry-run path/to/flow.lobster --args-json '{...}'`.
   This loads the file, checks every key, condition and pipeline flag, and returns the plan
   in the envelope's `plan`. Fix whatever it refuses before running for real.
3. **Run** it: `lobster run --mode tool path/to/flow.lobster --args-json '{...}'`.
4. **Act on `status`** in the envelope:

   | `status` | What happened | What to do |
   |---|---|---|
   | `ok` | Finished | Use `output` (the last step's output) |
   | `needs_approval` | Stopped at an approval | Show `requiresApproval.prompt` (and `items` / `preview`) to the person, then `lobster resume <resumeToken> --approve yes\|no` with **their** answer |
   | `needs_input` | Stopped at a question | Get the answer, then `lobster resume <resumeToken> --response-json '<json fitting requiresInput.responseSchema>'` |
   | `waiting` | Sleeping, or waiting for an event | `waitingFor.kind` is `timer` (continued by `lobster runs advance --due`) or `event` (continued by `lobster signal <event> --data '{...}'`) |
   | `cancelled` | A "no", or a deadline with `on_timeout: cancel` | Nothing to continue |
   | `ok: false` | A step failed, or bad input | Read `error.type` / `error.message`; if `runId` is set, fix the cause and `lobster runs advance <runId>` retries from the failed step |

5. **Inspect** when unsure: `lobster runs list --status waiting`, `lobster runs show <runId>`
   (status and pending questions), `lobster waits show <waitId>` (a question in full).

The plan shows what can run, not what will: a condition on output the plan does not have is
marked "depends on step output". A plain approval is planned as granted (a "no" cancels the
run); one with `on_reject` or `on_timeout: continue` leaves both paths open.

**Never answer an approval or a question on the person's behalf.** A `needs_approval` is a
hard stop: ask the human, and resume only with what they decided. An approval exists because
someone wanted a person to decide; approving it yourself defeats the workflow.

## The envelope

```json
{ "protocolVersion": 1, "ok": true, "status": "needs_approval", "output": [],
  "requiresApproval": { "prompt": "Deploy?", "items": [...], "resumeToken": "...", "approvalId": "ab12cd34" },
  "requiresInput": null, "runId": "..." }
{ "protocolVersion": 1, "ok": false, "error": { "type": "runtime_error", "message": "..." }, "runId": "..." }
```

- Exit code 2 means a mistake in the command line (`parse_error`: a bad flag, value or
  ID); 1 means something else failed. stderr is for people (retries, warnings); never parse it.
- Every command accepts `--mode tool`. Pass it to `lobster run` always: without it, `run`
  prints only the output, and asks questions at a terminal instead of returning them.
- The other commands answer in named fields: `runs list` → `runs`; `runs show` → `run`;
  `signal` → `event`, `advanced`. `references/cli.md` lists them all.

## A first workflow

```yaml
name: release
args:
  version:
    default: 1.0.0
steps:
  - id: build
    run: echo '{"artifact":"app.tar.gz","tests":"passed"}'   # any shell command
  - id: approve
    approval:
      prompt: Ship this build?
      on_reject: continue          # a "no" continues with approved: false (default: cancel the run)
    stdin: $build.json             # shown to the person with the question
  - id: ship
    run: echo "shipping $LOBSTER_ARG_VERSION"
    when: $approve.approved
```

- Steps run in order. A later step reads an earlier one as `$id.json` (its stdout parsed as
  JSON), `$id.stdout`, `$id.approved`, `$id.response`, `$id.skipped`, `$id.timedOut`...
- `run:` runs a shell command; `pipeline:` runs Lobster's own commands (`llm.invoke`,
  `approve`, `map`, `where`...). Lobster commands are not programs on the PATH, so they only
  work in `pipeline:`.
- Read args from `$LOBSTER_ARG_<NAME>` in commands. `${name}` is pasted into the command text
  as is, so it is unsafe for free text (quotes, `$`, spaces).

## Running on a schedule

Lobster does not start runs on its own. Start them from cron or the host
(`0 8 * * * lobster run --mode tool /path/flow.lobster --args-json '{...}'`), and also run
`lobster runs advance --due` every minute or so: it continues long sleeps, fires timeouts on
questions and waits, and recovers runs whose process died. When a scheduled run stops at
`needs_approval` or `needs_input`, whatever started it must pass the question on (a message
with the `approvalId`, say); `lobster runs list --status waiting` shows what is pending.
Relative paths in commands resolve in the directory `lobster run` was started in.

## Choosing a step

| Need | Use |
|---|---|
| Run a program or script | `run: <shell command>` |
| Call a model, reshape JSON, send email, call OpenClaw | `pipeline: <cmd> \| <cmd>` |
| A person must agree first | `approval:` (on its own step, or after a step's command) |
| A person must answer something | `input:` with a `responseSchema` |
| Branch | `when:` on a step; `switch:` for several cases |
| Do something per item | `for_each:` (`concurrency: N` for N at once) |
| Poll until done | `repeat: { until, max }` with a `sleep:` step inside |
| Do things at once | `parallel:` (`wait: all` or `wait: any`) |
| Wait for something outside | `wait: { event, match, timeout }`; deliver with `lobster signal` |
| Wait for time | `sleep: 2h` |
| Reuse another workflow | `workflow: other.lobster` with `workflow_args:` |

## Mistakes that bite

- **`llm.invoke` yields a record of the call, not the answer.** The answer is under
  `output.data` (with `--output-schema`) or `output.text`. To branch on it, unwrap it in the
  pipeline: `llm.invoke ... --output-schema '...' | map --unwrap output | map --unwrap data`,
  then `$step.json.label` works.
- **A workflow's output is only its last step's output.** Earlier steps' results are not in
  the envelope; make the last step produce what you need (or pass earlier values to it
  through `stdin:` / `env:`).
- **Questions can't go where several things run at once:** no approval, `input:`, `sleep:` or
  `wait:` inside `parallel:` branches, a `for_each` with `concurrency > 1`, child workflows,
  or a loop or `switch` with `retry`. The loader refuses them.
- **Comparisons are strict.** `<`, `>`, `<=`, `>=` need numbers; comparing a missing field
  fails the step. Guard: `$s.json.score != null && $s.json.score > 0.8`.
- **Prompts are plain text.** `${arg}` is not filled into `approval.prompt` or `input.prompt`;
  put the details in the step's `stdin:` (the question's subject) instead.
- **`workflow_args` values are text.** A list passed as `$step.json` arrives as JSON text in
  the child; parse it there (`JSON.parse(process.env.LOBSTER_ARGS_JSON)`).
- **List args don't reach `for_each` directly.** `${urls}` and `$LOBSTER_ARG_URLS` are text
  (`a,b`). Turn the arg into a step's JSON first, then loop over it:
  `run: node -e 'console.log(JSON.stringify(JSON.parse(process.env.LOBSTER_ARGS_JSON).urls))'`,
  then `for_each: $list.json`. Inside the loop use `$item.json` (`$item` alone is not replaced).
- **Model answers are cached** by prompt, model and input. A scheduled run with the same input
  gets the earlier answer; add `--refresh` when the answer must be fresh.
- **Unknown keys and flags are refused** when the file loads, with a suggestion. Trust the
  dry run's errors; they name the step and the key.

`references/patterns.md` has these with worked recipes.

## Reference files

Read the one you need; don't load them all.

- `references/workflow-syntax.md`: every workflow key, step kind, condition operator, result
  field, setting and limit. Read when writing or fixing a workflow file.
- `references/cli.md`: every CLI command, its flags, its envelope, error types and exit
  codes. Read when running, resuming or inspecting runs, or handling an error.
- `references/pipeline-commands.md`: the commands a `pipeline:` can run (`llm.invoke`,
  `exec`, `approve`, `ask`, `map`, `where`, `state.set`, `openclaw.invoke`, ...) and their
  flags. Read when writing a `pipeline:` step.
- `references/patterns.md`: recipes (classifier with a human fallback, approval before an
  action, polling, waiting for an event, batch with retries, fan-out, undo on failure, child
  workflows) and the pitfalls in detail. Read when designing a workflow.

In the Lobster repository, `examples/` has runnable workflows and `docs/reference.md` the
full documentation.
