# Pipeline commands

A `pipeline:` step (or `lobster run --mode tool '<pipeline>'`) runs Lobster's own commands,
joined by `|`. Each stage receives the previous stage's items (JSON values) and passes items
on; the step's `json` is the last stage's single item, or its list of items. The step's
`stdin:` is the first stage's input.

Every command and flag is checked before anything runs: an unknown flag, a flag given with its
alias, or a number flag without a number is a `parse_error`. A bare `--flag` before another
flag means `true`, so give string and number flags a value.

`lobster help <command>` shows a command's help; `lobster run --mode tool commands.list` lists
every command with its flags.

## Contents

- [llm.invoke](#llminvoke) · [exec](#exec) · [approve](#approve) · [ask](#ask)
- [Shaping data](#shaping-data): where, pick, map, sort, head, dedupe, groupBy, template, json, table
- [State](#state): state.set, state.get, diff.last
- [OpenClaw and email](#openclaw-and-email): openclaw.invoke, openclaw.agent, gog.gmail.search, gog.gmail.send, email.triage
- [Discovery](#discovery): commands.list, workflows.list, workflows.run

## llm.invoke

Calls a model. Flags: `--prompt`, `--model`, `--provider` (`openclaw`, `pi`, `http`, or a
host's adapter), `--output-schema '<JSON Schema>'`, `--max-validation-retries <n>`,
`--temperature`, `--max-output-tokens`, `--artifacts-json`, `--metadata-json`,
`--schema-version`, `--state-key`, `--refresh` (ignore the cache), `--disable-cache`,
`--token`.

- The stage's input items are sent to the model as artifacts (context for the prompt).
- It yields **one record per call**:
  `{ kind, model, status, output: { format, text, data }, usage, cacheKey, cached, ... }`.
  The answer is `output.data` (with `--output-schema`) or `output.text`. Take it out to use
  it: `llm.invoke ... | map --unwrap output | map --unwrap data` (or `... text`).
- With `--output-schema`, an answer that does not fit is asked again, up to
  `--max-validation-retries` more times; then the step fails.
- Answers are cached by prompt, model and input (`LOBSTER_CACHE_DIR`), and saved with the run,
  so a resumed or retried run does not call the model again. `--refresh` forces a new call.
- Provider: `--provider`, else `LOBSTER_LLM_PROVIDER`, else detected from the environment:
  `openclaw` uses `OPENCLAW_URL` (+ `OPENCLAW_TOKEN`); `pi` uses
  `LOBSTER_PI_LLM_ADAPTER_URL`; `http` uses `LOBSTER_LLM_ADAPTER_URL` (+
  `LOBSTER_LLM_ADAPTER_TOKEN`).
- Each live call counts toward the workflow's `cost_limit`; cached or replayed answers don't.
- `llm_task.invoke` is the same command fixed to the OpenClaw provider (older name).

```yaml
- id: classify
  pipeline: >
    llm.invoke --prompt "Label this ticket bug, feature or question, with a confidence 0-1"
    --output-schema '{"type":"object","properties":{"label":{"enum":["bug","feature","question"]},"confidence":{"type":"number"}},"required":["label","confidence"]}'
    | map --unwrap output | map --unwrap data
  stdin: $ticket.json
- id: is_bug
  run: echo yes
  when: $classify.json.label == bug && $classify.json.confidence >= 0.7
```

## exec

Runs a program from a pipeline. `exec <program> <args...>` runs it directly;
`exec --shell '<command line>'` runs it with the shell. `--json` parses stdout as one JSON
value (otherwise each line is an item); `--stdin raw|json|jsonl` writes the stage's input to
the program's stdin.

```yaml
- id: big_prs
  pipeline: exec --json --shell 'gh pr list --json number,title' | where number>100
```

## approve

Stops the pipeline for a person: `approve --prompt "Send these?" [--preview-from-stdin]
[--limit 5]`. In tool mode the run returns `needs_approval`; once approved, the items pass on
to the next stage; a "no" cancels the run. Use a workflow `approval:` step instead when the
decision is between steps; use `approve` when the items to approve are in the middle of a
pipeline. Not allowed where questions can't pause (parallel branches, child workflows, ...).

## ask

Stops for a person's answer: `ask --prompt "..." [--schema '<JSON Schema>']
[--subject-from-stdin]`. The run returns `needs_input`; the answer becomes the next stage's
item. Without `--schema` the answer must be
`{ "decision": "approve" | "reject" | "redraft", ... }`; give a schema for anything else.

## Shaping data

| Command | Does | Example |
|---|---|---|
| `where <path><op><value>` | Keep items matching (`==` `=` `!=` `<` `<=` `>` `>=`; dotted paths; `true`/`false`/`null`/numbers parsed) | `where state==open`, `where reviews.count>=2` |
| `pick a,b,c` | Keep only these top-level fields | `pick number,title,url` |
| `map --unwrap <field>` | Replace each item by one of its fields | `map --unwrap output` |
| `map --wrap <field>` | Wrap each item: `{ field: item }` | `map --wrap pr` |
| `map k=v k2={{path}}` | Add fields (templated from the item) | `map source=github id={{number}}` |
| `sort [--key k] [--desc]` | Stable sort (missing keys last) | `sort --key updatedAt --desc` |
| `head --n <n>` | First n items | `head --n 5` |
| `dedupe [--key k]` | Drop repeats, keep the first | `dedupe --key id` |
| `groupBy --key k` | `{ key, items, count }` per group | `groupBy --key author` |
| `template --text '...'` | Render each item to text (`{{field}}`, `{{field \| upper}}`, ...) | `template --text 'PR {{number}}: {{title}}'` |
| `json` / `table` | Render for a person (output is then text) | `... \| table` |

## State

Lobster keeps a small key-value store (in the same database as runs):

- `<value> | state.set <key>`: store the input (one item, or the list) as one JSON value.
- `state.get <key>`: yield it (`null` if unset).
- `<items> | diff.last --key <key>`: compare with the last items stored under that key and
  store the new ones; yields `{ changed, key, before, after }`. Good for "notify only when
  something changed".

## OpenClaw and email

These act outside Lobster and **may not be repeated**: Lobster never retries them
automatically after dispatch, and a crash during one leaves the run `blocked` for a person.

- `openclaw.invoke --tool <tool> --action <action> --args-json '{...}' [--each --item-key k]
  [--dry-run]`: call an OpenClaw tool (`OPENCLAW_URL`, `OPENCLAW_TOKEN`). `clawd.invoke` is the
  older name.
- `openclaw.agent --agent <name> --prompt "..." [--session-key k] [--model m]`: run an
  OpenClaw agent turn; input items are appended to the message.
- `gog.gmail.search --query '...' [--max 20]`: fetch Gmail messages (needs the `gog` CLI).
- `... | gog.gmail.send`: send drafts `{ to, subject, body }`. Put an approval before it.
- `email.triage [--llm --model m] [--emit drafts]`: categorize emails and draft replies.

## Discovery

- `commands.list`: every command with its description, flags (`argsSchema`), side effects and
  whether it may be repeated.
- `workflows.list` / `workflows.run --name <name> [--args-json '{...}']`: named workflows that
  ship with Lobster (for example `github.pr.monitor`).
