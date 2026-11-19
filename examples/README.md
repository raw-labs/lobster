# Examples

Workflows you can run and copy. Their commands are stand-ins (`echo`, `node -e`), so each
one runs as it is; a comment marks where a real command (`gh`, `kubectl`, an API) would go.
`docs/reference.md` explains every key they use.

| File                                                             | Shows                                                                                                                     |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| [hello.lobster](hello.lobster)                                   | Commands, an arg with a default, passing output to the next step (`stdin`, `$step.json`), a `when:` condition             |
| [deploy-with-approval.lobster](deploy-with-approval.lobster)     | An `approval:` gate with a timeout, `on_reject: continue`, steps that check `$approve.approved`                           |
| [triage-with-classifier.lobster](triage-with-classifier.lobster) | `llm.invoke` with an output schema, `switch:`, an `input:` step that asks a person when the model is unsure, `cost_limit` |
| [batch-process.lobster](batch-process.lobster)                   | `for_each` with `concurrency`, `retry` and `timeout_ms` on a step, collecting results from `$each.json`                   |
| [wait-for-ci.lobster](wait-for-ci.lobster)                       | `repeat:` with `until` and `max`, `sleep:` between rounds, `$ci.rounds`                                                   |
| [fan-out.lobster](fan-out.lobster)                               | `parallel:` with `wait: all` and `wait: any`                                                                              |
| [wait-for-event.lobster](wait-for-event.lobster)                 | `wait:` for an event with `match`, a deadline with `on_timeout: continue`, `lobster signal`                               |
| [compose/release-notes.lobster](compose/release-notes.lobster)   | `workflow:` calling [compose/format-list.lobster](compose/format-list.lobster) with `workflow_args`                       |
| [sdk.ts](sdk.ts)                                                 | The SDK: your own JavaScript stages with `approve()`, `run`, and `resume` with a person's answer                          |

## Running them

From the repository, after `pnpm install && pnpm build` (or with `lobster` installed, from
anywhere):

```sh
node bin/lobster.js run examples/hello.lobster
node bin/lobster.js run --file examples/hello.lobster --args-json '{"name":"Ada"}'
node bin/lobster.js run --dry-run examples/batch-process.lobster   # the plan, nothing runs
```

A workflow that asks a person (an approval, an `input:` step) asks at the terminal. With
`--mode tool` it returns `needs_approval` or `needs_input` with a resume token instead, and
the run waits, saved, until someone answers:

```sh
node bin/lobster.js run --mode tool --file examples/deploy-with-approval.lobster
node bin/lobster.js resume <token> --approve yes        # or --approve no
```

A workflow that waits for an event or a long sleep returns `waiting` and continues when the
event arrives, or when something advances due runs (cron, a host):

```sh
node bin/lobster.js run --mode tool --file examples/wait-for-event.lobster
node bin/lobster.js signal order.update --data '{"order":"o-1","status":"paid"}'
node bin/lobster.js runs advance --due
```

`triage-with-classifier.lobster` calls a model: set `LOBSTER_LLM_PROVIDER` and its URL first
(see "Calling LLMs from workflows" in `docs/reference.md`). The SDK example runs with Node
directly: `node examples/sdk.ts`.

Runs are saved in `LOBSTER_STATE_DIR` (by default `~/.lobster/state`); `lobster runs list`
shows them.

## Pipelines without a file

A pipeline is a line of commands, run as one step:

```sh
node bin/lobster.js "exec --json --shell 'echo [1,2,3]' | head --n 2"
node bin/lobster.js "exec --json --shell 'echo [1,2,3]' | approve --prompt 'Use these?'"
node bin/lobster.js "exec --json --shell 'echo 42' | state.set counter"
node bin/lobster.js "state.get counter"
```

`lobster help` lists every command, and `lobster help <command>` explains one.
