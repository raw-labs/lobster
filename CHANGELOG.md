# Changelog

All notable changes to Lobster will be documented in this file.

## Unreleased

- Saves no longer grow with a run: a `for_each` loop's items and each item's output, and any step output over 1 KB, are kept beside the checkpoint as records written once, in the same transaction as the save that first needs them. A loop's saves used to rewrite every item and every finished output, so time and bytes grew with the square of its length (3,200 items wrote 630 MB and took 49 s; 8,000 items now take under 6 s). Records go when a run completes or is cancelled, stay with a failed run for its retry, and are included in `storage export`.
- `lobster runs advance --due` advances up to 4 due runs at once instead of one after another, so a slow run no longer delays the others; `--concurrency N` (`advanceDueRuns({ concurrency })`) changes it, and `--concurrency 1` restores the old behaviour. Each run keeps its own lease.
- A stop (Ctrl-C, a host abort, a lost lease) between a step being marked started and its command starting no longer leaves the run blocked: the step goes back to ready, since nothing ran. Shell steps count as started only once their process is spawned.
- Conditions nested more than 64 levels deep, or with a tree more than 256 levels deep (a very long `&&` chain), are refused with a message instead of overflowing the stack.
- Workflow files: `steps:` on a step other than `for_each` or `repeat` is refused (it used to crash with a TypeError when the run started); a NUL character in a `workflow` or `cwd` path is refused; a file that is not valid YAML or JSON says so, with its path.
- The CLI reports an error as one line instead of a stack trace (`LOBSTER_DEBUG=1` shows it): exit code 2 for a command line it does not understand, 1 otherwise. `lobster runs show` and `waits show` of an unknown ID fail with "not found" instead of printing `null`.
- Add the testing campaign (`pnpm test:campaign`, nightly in CI): randomized workflows run with and without interruptions, process chaos (SIGKILL, SIGSTOP past the lease, wrong clocks, a locked database), fuzzing of conditions, workflow files, pipelines and the CLI, and load and soak tests.
- Document recipes for deciding with a classifier (and asking a person when it is unsure), reminding an approver, waiting for whichever outcome comes first, and undoing earlier steps when a later one fails.
- Loops can pause: approvals, input, sleeps, waits, switches and child workflows may be steps of a `for_each`. A question inside a loop is asked once per item. A loop with `retry` cannot contain them, since a retry would ask again; that is refused when the file loads.
- Add `repeat` steps: `repeat: { until: <condition>, max: N }` runs its steps in rounds until the condition holds after one (for polling: check, sleep, check again). `max` is required; reaching it fails the step, or ends it with `on_max: continue`. Later steps see the last round's results and `$step.rounds`.
- Add `on_reject: continue` to approvals: a "no" completes the step with `approved: false` instead of cancelling the run, so a loop can skip one item. Identity rules apply to such a rejection too.
- A step's `timeout_ms` no longer counts the time a run spends paused inside it (a sleep, a wait or a question in a loop or a switch): enclosing steps get a fresh budget when the run continues. Before, a pause longer than an enclosing step's timeout failed that step on resume, except for answered questions.
- Split the README into a one-page guide (what Lobster is, five ideas, a first workflow, tool mode, operating runs) and a full reference in `docs/reference.md`, which keeps every section of the old README, grouped by topic. The package now ships the reference.
- Add `switch` steps: the first case whose `when` holds runs its steps (any kind of step, approvals and nested switches included), or `else`. The choice is saved, `$switch.json` is the output of the case that ran, `$switch.case` says which, and steps of other cases count as skipped. A switch with no matching case and no `else` fails.
- Conditions gain `in`, `contains` (list membership or substring), `matches` (a quoted regular expression) and list literals such as `[refund, spam]`. Every syntax error gives a character position.
- Conditions are checked further when the file loads: a condition that reads a step that does not run before it, or a literal operand that fails on every run (`$n.json.count > "high"`), is refused before any step runs. Before, a reference to an unknown step failed only when the condition was evaluated.
- Parse `when:` and `condition:` into a syntax tree once, then evaluate the tree, instead of evaluating while parsing. Conditions decide exactly as before (checked against the previous evaluator on a million generated conditions). Syntax errors now say what is wrong, for example "Unsupported condition: $a.json.ok = 1 (unexpected "=" at character 12)", and dry-run no longer mistakes `$step.field` inside a quoted string for a reference.
- Validate JSON Schemas with TypeBox's checker instead of Ajv, so Lobster no longer generates code at runtime and runs with code generation from strings disabled. Schemas are still read as draft-07 with `format` unchecked. Error messages keep the "<path> <message>" shape with some wording changes (for example "must have required properties a, b", "/extra is not allowed"). `llm.invoke` no longer fails when two output schemas share an `$id`. `withSchemaCompiler` is exported from `@clawdbot/lobster/core` for hosts that bring their own validator.
- Keep all state in one SQLite database (`lobster.sqlite`) in the state directory, and save workflow-file progress so interrupted runs can be continued with `lobster runs advance`. Requires the same runtime as OpenClaw: Node >=24.16.0 <25 or >=26.1.0.
- Add `lobster upgrade`, a one-off import of state saved by older versions: paused workflows and pipelines become waiting runs, and approval IDs, `state.set` values and paused SDK steps are imported. Existing resume tokens and approval IDs keep working. Until it runs, resuming such state fails with `upgrade_required`.
- Run standalone pipelines (`lobster '...'`, `runToolRequest({ pipeline })`) and SDK pipelines on the same engine as workflow files. Every pipeline is a recorded run (kind `pipeline` or `sdk`) that pauses, resumes and recovers like a workflow, and each resume token or approval ID resumes once. The whole pipeline is checked (command names and flags) before its first stage runs. History shows the pipeline's own step above its stages.
- In human mode without a terminal, a pipeline that reaches `approve` or `ask` now pauses and prints how to answer it (status, request and resume token), as workflow files do, instead of printing the request as output with no way to resume.
- An answer, once accepted, stands: if the run stops or fails afterwards, the question is not asked again and the run continues with `lobster runs advance`; a failed resume reports the run in its envelope (`runId`). Pipelines used to put the question back in some of these cases.
- SDK runs are resumed only by an instance with the same stages; add `Lobster.advance(runId)` for SDK runs that stopped or failed. `lobster runs advance` refuses SDK runs and `--due` skips them. SDK tokens resume once; tokens from older versions are honoured once, then refused.
- Retry a failed run from the step that failed with `lobster runs advance <runId>` (core `advanceWorkflowRun`, SDK `Lobster.advance`). Completed steps keep their results; a failed step that reported dispatch of a command that may not be repeated is refused. Failed runs keep their step records until retention deletes them.
- An answer for an outdated wait revision fails with `wait_conflict` and leaves the run free to answer at once, instead of locking it until its lease expired. Resume errors for an unknown or used token now read "Resume state not found" for pipelines and workflows alike.
- A run started by a host without an abort signal stops its child processes on Ctrl-C, as the SDK and pipelines did before.
- Workflow loops save their progress after every iteration, so very large loops run slower than before.
- Report runs stopped while a command was running as `blocked`, and add `lobster runs abandon <runId>` to stop them for good.
- Delete finished runs (completed, failed or cancelled) 30 days after they last changed, at most once a day when the store is opened for writing; set `LOBSTER_RUN_RETENTION_DAYS` (or `retentionDays` when opening a store) to change this, `0` keeps them forever. Waiting and blocked runs are never deleted.
- Update Node.js types to 24.19.2 to match the new runtime floor.
- Reject unknown keys in workflow files instead of ignoring them, suggesting the intended key for likely typos. A misspelled `when:` previously made its step run unconditionally.
- Fail a condition that orders anything other than numbers (strings, booleans, `null`, missing values) instead of evaluating it to false. `&&` and `||` now skip their right side when the left side decides the result, and a missing field equals the literal `null`, so guards like `$x.json.v != null && $x.json.v > 1` work. Conditions are checked for syntax when the workflow loads, and dry-run only checks the syntax of conditions on step output it does not have.
- Reject an `approval:` that is not `true`/`false`, a non-blank prompt or an object, and an `approval.prompt`/`preview` that is not a string or `approval.items` that is not a list. Such a value used to drop the gate, so the step ran without asking anyone.
- Reject a `when:`/`condition:` that is not a boolean or an expression string when the workflow loads, instead of failing when the step is reached after earlier steps ran.
- Validate `timeout_ms`, `retry`, `on_error` and `when` of `for_each` sub-steps like those of any other step, and reject any `approval:` or `input:` key in a sub-step. A quoted `timeout_ms: "5000"` inside a loop made the step time out at once.
- Refuse an approval or input gate inside a sub-workflow before its command runs; the command used to run and only then fail the run.
- Dry-run no longer lists a step as skipped when its condition reads an earlier step's output; the step is planned with its condition marked as unknown at plan time.
- Group commands into packs (`stdlib`, `openclaw`, `google`, `github`) and give every command a declared contract (`defineCommand`): description, flag schema, side-effect labels and whether it is `repeatable`. Hosts can build registries from packs with `createRegistry` and `builtinPacks` from `@clawdbot/lobster/core`; a name defined twice is an error.
- Reject a flag a command does not declare, suggesting the intended flag for likely typos, instead of ignoring it.
- Read boolean flags from each command's schema: a boolean flag no longer takes the next word as its value, and `--flag false` now means false (it used to be the truthy string `"false"`).
- Never re-run a step on crash recovery, even with `recovery: repeat`, when its pipeline contains a command that may not be repeated (`openclaw.invoke`, `openclaw.agent`, `gog.gmail.send`). Only `openclaw.agent` was covered before, so a crash right after `gog.gmail.send` sent could send again. A step whose pipeline fails before `openclaw.agent` dispatches can now be retried like the others.
- Keep `clawd.invoke` as an alias of `openclaw.invoke` and `llm_task.invoke` as a configured variant of `llm.invoke`; `commands.list` reports `aliasOf` and `repeatable`.
- `lobster resume <ref>` takes a resume token, a short approval ID or a wait ID as one argument; with a wait ID, `--expected-revision` is optional. `--token`, `--id` and `--decision` keep working. `lobster --help` is grouped into starting and answering, managing runs, diagnostics and setup.
- Add durable waits without a server: a `sleep:` step, a `wait:` step for external events delivered with `lobster signal <event>` (core `signalEvent`), and `timeout`/`on_timeout` on approvals, input requests and event waits. Waiting runs return `status: "waiting"` with `waitingFor`.
- Add `lobster runs advance --due` (core `advanceDueRuns`): advance every run whose sleep or deadline has passed, or whose process died. Every run records when it next needs attention (`wakeAt`); a host, cron or systemd timer decides how often to call it.
- Add `heartbeat_timeout_ms`: a step that shows no progress (touching `LOBSTER_HEARTBEAT_FILE`, or `ctx.heartbeat()` in a pipeline command) for that long is stopped like a timeout.

## 2026.11.2 - 2026-11-15

**Highlights:** Show usage for built-in help topics and reject malformed workflow arguments cleanly.

- Show the usage for `lobster help run`, `help resume`, `help doctor`, `help version` and `help help`, the same text `lobster run --help` prints, instead of failing with `Unknown command: run` (exit code 2); thanks @KrasimirKralev.
- Reject a non-object or empty `--args-json` on `lobster run`, `lobster graph`, and `workflows.run` instead of running the workflow on its default args or failing with a raw `TypeError`; thanks @KrasimirKralev.
- Update Oxfmt to 0.78.0, Oxlint to 1.93.0, and pnpm to 12.18.0 across development and release tooling while retaining the two-day dependency cooldown and Node.js 22 runtime floor.

## 2026.11.1 - 2026-11-05

**Highlights:** Show graph-specific help for `lobster help graph` and patch URI parsing vulnerabilities in `fast-uri`.

- Show the graph help for `lobster help graph`, which `lobster --help` lists under Commands, instead of failing with `Unknown command: graph` (exit code 2); thanks @KrasimirKralev.
- Update the `fast-uri` override to 4.2.3 to fix URI scheme spoofing, scheme injection, host confusion, and path normalization vulnerabilities.
- Refresh Oxfmt to 0.75.0 and Oxlint to 1.90.0 while preserving the two-day dependency cooldown and Node.js 22 runtime floor.
- Remove the unused internal `getFilter` helper from the template filters; `applyFilters` keeps reading the filter table directly and template output is unchanged; thanks @KrasimirKralev.

## 2026.10.1 - 2026-10-20

**Highlights:** Honor host invocation authority before effects, prevent unsafe resume replay, and report invalid workflow paths consistently.

- Honor host invocation authority across approved resumes and built-in effect boundaries, restoring safe checkpoints after retirement while preventing replay after unsafe dispatch; thanks @Patrick-Erichsen (PR [#200](https://github.com/openclaw/lobster/pull/200)).
- Report missing, directory, or wrong-extension positional workflow paths such as `lobster run deploy.lobster` as input errors (exit code 2, `parse_error` in tool mode) instead of `Unknown command`, matching `--file` while quoted pipelines and command names still run as pipelines; thanks @KrasimirKralev (PR [#209](https://github.com/openclaw/lobster/pull/209)).
- Keep missing and null keys last for descending keyed sorts, so top-N pipelines select populated values; thanks @goutamadwant (PR [#207](https://github.com/openclaw/lobster/pull/207)) and @notlakshitha for the report (Issue [#206](https://github.com/openclaw/lobster/issues/206)).
- Fix the shell-safe workflow argument example to read the injected environment variable directly and clarify environment-value substitution; thanks @KrasimirKralev (PR [#198](https://github.com/openclaw/lobster/pull/198)).
- Update pnpm to 12.14.0 across development, CI hydration, and release tooling; add a release-workflow dry run that validates the selected ref without publishing or creating a promotable artifact.
- Refresh Oxfmt to 0.73.0, Oxlint to 1.88.0, pnpm to 12.11.2, and the fast-uri override to 4.2.2 across development, CI hydration, and release tooling while retaining the two-day dependency release-age policy and Node.js 22 runtime floor.
- Refresh Oxfmt to 0.72.0, Oxlint to 1.87.0, and pnpm to 12.10.1 across development, CI hydration, and release tooling while retaining the two-day dependency release-age policy and Node.js 22 runtime floor.
- Refresh pnpm to 12.9.1 across development, CI hydration, and release tooling, and update Node.js 22 types to 22.20.5 while retaining the two-day dependency release-age policy and Node.js 22 runtime floor.

## 2026.9.16 - 2026-10-01

**Highlights:** Tool pipelines and resumes return one valid JSON envelope, even when they use JSON or table renderers.

- Keep `json` and `table` output inside the single tool JSON envelope, preserving typed items through CLI/core pipeline runs and resumes without changing human rendering or workflow-step captures.

## 2026.9.15 - 2026-10-01

**Highlights:** Approval resumes share consistent ID resolution, with refreshed development and release tooling.

- Share approval-ID resolution between tool and CLI resume paths, preserving token formats and error envelopes; thanks @KrasimirKralev (PR [#192](https://github.com/openclaw/lobster/pull/192)).
- Update Oxfmt to 0.71.0, Oxlint to 1.86.0, and pnpm to 12.8.1 across development, CI hydration, and release tooling; retain the two-day dependency release-age policy and Node.js 22 runtime floor.
- Refresh Oxc formatting and lint tooling, and align development, CI hydration, and release tooling on pnpm 12.7.0 while retaining the two-day dependency release-age policy.

## 2026.9.14 - 2026-09-21

**Highlights:** OpenClaw LLM pipelines use the gateway's current tool protocol, and large tables render without crashing.

- Translate OpenClaw `llm-task` requests and tool results correctly in `llm.invoke` and `llm_task.invoke`, preserving local schema validation and retries; thanks @bggsolar (Issue [#185](https://github.com/openclaw/lobster/issues/185)).
- Render large tables without overflowing the JavaScript argument limit while calculating column widths.
- Stop `head` at its requested item limit without reading an extra upstream item, avoiding unwanted work, errors, or hangs after the requested output is complete.
- Preserve `__proto__` fields in `pick` projections and `map` assignments without changing output object prototypes or silently dropping data.
- Keep compiled tests out of npm packages prepared after validation, reducing download and install size while preserving runtime source maps.
- Cover empty, object-table, and mixed-value terminal rendering; thanks @KrasimirKralev (PR [#184](https://github.com/openclaw/lobster/pull/184)).

## 2026.9.13 - 2026-09-13

**Highlights:** Cost limits no longer stop workflows falsely when later steps reuse LLM results, and CLI pipelines preserve empty and quoted arguments.

- Count live LLM results only once when later workflow steps reuse them through JSON references, including rendered and projected outputs, preventing false cost-limit stops while preserving charges for new calls and untrusted JSON.
- Preserve explicit empty command arguments in CLI pipelines and SDK exec, and preserve apostrophes, quotes, backslashes, and literal pipeline characters through both invocation shims.
- Report invalid explicit workflow targets through the tool JSON error envelope (exit code 2), with concise human-mode errors instead of uncaught filesystem stacks.
- Show graph-specific options for `lobster graph --help` and `-h`.
- Test the supported Node.js 22 and 24 runtimes in CI, align Node types with the minimum supported major, make TypeScript 7 the explicit compiler, update pnpm to 12.4.1, and pin hydration/release actions while retaining the two-day dependency release-age policy.

## 2026.9.11 - 2026-09-11

**Highlights:** SDK shell commands share Lobster's process handling and report missing-shell configuration clearly.

- Route SDK shell execution through the shared shell-command boundary, preserving direct argument handling, cancellation, and output limits while improving missing-shell errors; thanks @vincentkoc (PR [#162](https://github.com/openclaw/lobster/pull/162)).
- Refresh Node.js types and Oxc tooling, and align development, CI, and release tooling on pnpm 12.4.0 while retaining the two-day dependency release-age policy.
- Restore Crabbox Linux validation startup by matching the root disk size to the current runner image's 400 GB minimum.
- Cover malformed saved pipeline states with ten focused rejection tests and clean up their temporary state directories; thanks @KrasimirKralev (PR [#164](https://github.com/openclaw/lobster/pull/164)).

## 2026.9.8 - 2026-09-07

**Highlights:** Workflow loops honor their execution policies, and paid LLM calls cannot hide from budget checks when a later pipeline command fails.

- Honor `timeout_ms`, `retry`, and `on_error` on `for_each` steps, including pipeline children and batch pauses; document whole-loop replay and show policies in dry runs. Thanks @SebTardif (PR [#160](https://github.com/openclaw/lobster/pull/160)).
- Stop retries and error recovery after a `cost_limit` stop, including paid LLM usage discarded by a failed pipeline, preventing additional over-budget calls. Thanks @SebTardif (PR [#160](https://github.com/openclaw/lobster/pull/160)).

## 2026.9.7 - 2026-09-07

**Highlights:** Direct `exec --json` commands now preserve their executable, alongside more reliable npm release promotion.

- Fix `exec --json <command...>` consuming the executable as the JSON flag's value, restoring direct commands and quoted shell commands while preserving value-taking options.
- Retry the npm `latest` dist-tag read-back with backoff after promoting a beta release, so registry propagation delay no longer fails a promotion that already succeeded.
- Update development, CI, and release tooling to pnpm 12.3.4 and pnpm/action-setup 6.1.0 for native pnpm 12 installation support, retaining the two-day dependency release-age policy.
- Accept dated changelog headings during npm release validation.

## 2026.9.5

**Highlights:** Run configured OpenClaw agents directly from workflows, stop cancelled work before further side effects, and track LLM spend without charging cached answers again.

- Add first-class `openclaw.agent` workflow turns with configured agent, session, model, thinking, and timeout selection delegated to OpenClaw. Thanks to [@Stoff81](https://github.com/Stoff81) (Issue [#117](https://github.com/openclaw/lobster/issues/117)).
- Prevent automatic workflow retries after dispatching `openclaw.invoke`, `clawd.invoke`, or `openclaw.agent`, avoiding duplicate remote actions after timeouts or failures, including requests carrying the gateway's currently ignored `dryRun` flag. Thanks to [@SebTardif](https://github.com/SebTardif) (PR [#153](https://github.com/openclaw/lobster/pull/153)).
- Stop cancelled CLI, tool-runtime, and SDK workflows before later commands or side effects; interrupt prompts and stalled streams, terminate child process trees, and prevent replay of approval/input resumes after an effect starts. Thanks to [@zhangguiping-xydt](https://github.com/zhangguiping-xydt) (PR [#119](https://github.com/openclaw/lobster/pull/119)).
- Honor SDK cancellation in `exec()` and shell commands, including constructor signals on run, clone, and resume. Thanks to [@SebTardif](https://github.com/SebTardif) (PR [#144](https://github.com/openclaw/lobster/pull/144)).
- Honor cancellation in `ghPrView()` and both exported GitHub monitor recipes, stopping the GitHub CLI before returning an error without saving a cancelled snapshot. Thanks to [@SebTardif](https://github.com/SebTardif) (PR [#145](https://github.com/openclaw/lobster/pull/145)).
- Cancel timed-out `gog.gmail.send` process trees and suppress automatic retries after dispatch because termination cannot prove Gmail did not accept the message. Thanks to [@SebTardif](https://github.com/SebTardif) (PR [#136](https://github.com/openclaw/lobster/pull/136)).
- Honor LLM step timeouts and cancellation across adapter calls, cached results, and persistence; stop waiting even when a host adapter ignores its abort signal. Thanks to [@Yigtwxx](https://github.com/Yigtwxx) (PR [#132](https://github.com/openclaw/lobster/pull/132)).
- Add opt-in process output limits with `LOBSTER_MAX_OUTPUT_BYTES`, preserving unlimited capture by default and terminating isolated process groups on overflow while forwarding terminal interrupts. Thanks @SebTardif (PR [#150](https://github.com/openclaw/lobster/pull/150)).
- Add opt-in HTTP response limits with `LOBSTER_MAX_HTTP_RESPONSE_BYTES` across tool and built-in LLM adapters, preserving unlimited responses by default and preventing replay of dispatched tools after overflow. Thanks @SebTardif (PR [#151](https://github.com/openclaw/lobster/pull/151)).
- Count each model call once across cached answers, pipeline projections, sub-workflows, and resumes; preserve accumulated spend across approval/input pauses so `cost_limit` covers the whole run. Thanks to [@Yigtwxx](https://github.com/Yigtwxx) (PR [#134](https://github.com/openclaw/lobster/pull/134)).
- Include temperature and maximum output tokens in LLM cache identity so changing generation settings does not return stale answers. Thanks to [@Yigtwxx](https://github.com/Yigtwxx) (PR [#130](https://github.com/openclaw/lobster/pull/130)).
- Enforce `--max-validation-retries` as the number of extra model calls after the first, avoiding an additional billed call on schema failures. Thanks to [@Yigtwxx](https://github.com/Yigtwxx) (PR [#128](https://github.com/openclaw/lobster/pull/128)).
- Serialize state updates and roll back cancelled cache/snapshot publications; preserve a replacement state lock when the filesystem reuses a stale lock's inode.
- Create cache and state directory chains correctly on Windows. Thanks to [@Yigtwxx](https://github.com/Yigtwxx) (PR [#126](https://github.com/openclaw/lobster/pull/126)).
- Handle subprocess stdin closing early without crashing workflow execution with `EPIPE`. Thanks to [@kesslerio](https://github.com/kesslerio) (PR [#122](https://github.com/openclaw/lobster/pull/122)).
- Keep direct executable arguments separate from explicit shell commands, preserving literal shell metacharacters in direct calls. Thanks to [@vincentkoc](https://github.com/vincentkoc) (PRs [#139](https://github.com/openclaw/lobster/pull/139), [#141](https://github.com/openclaw/lobster/pull/141)).
- Contain prototype-named workflow arguments and step IDs, preserve argument environment names, and escape Mermaid graph labels. Thanks to [@vincentkoc](https://github.com/vincentkoc) (PR [#140](https://github.com/openclaw/lobster/pull/140)).
- Normalize long state keys without expensive regular-expression backtracking. Thanks to [@vincentkoc](https://github.com/vincentkoc) (PR [#138](https://github.com/openclaw/lobster/pull/138)).
- Resolve invocation shims relative to the installed package, reject non-HTTP invocation URLs, and require an explicit token before sending gateway credentials to a remote endpoint. Thanks to [@vincentkoc](https://github.com/vincentkoc).
- Refresh TypeScript, Node.js types, and Oxc tooling; align development and CI on pnpm 12.3.1, retain the two-day dependency release-age policy, and update the `fast-uri` override to 4.1.4.
- Run Node 24 CI on pull requests and pushes to `main`, checking workflow syntax, frozen dependency installation, build, types, formatting, lint, and tests; strengthen parser/filter coverage and cancellation-test readiness.

## 2026.6.11

- Add command-level `ctx.requestInput(...)` for CLI/tool/SDK pipeline commands, with state-backed same-command resume, bounded command-input replay, and workflow `pipeline:` propagation (Issue [#101](https://github.com/openclaw/lobster/issues/101)).
- Warn when LLM usage records an unknown or missing model ID or invalid `LOBSTER_LLM_PRICING_JSON`, keeping zero-cost fallback behavior visible for `cost_limit` users. Thanks to [@KrasimirKralev](https://github.com/KrasimirKralev) (Issue [#107](https://github.com/openclaw/lobster/issues/107)).
- Require Node.js 22 or newer for the npm package, matching release CI.
- Write Lobster state files atomically while preserving restricted file modes, preventing truncated resume/session state after process termination. Thanks to [@KrasimirKralev](https://github.com/KrasimirKralev) (Issues [#108](https://github.com/openclaw/lobster/issues/108), [#109](https://github.com/openclaw/lobster/issues/109), PR [#110](https://github.com/openclaw/lobster/pull/110)).
- Harden LLM cache files, diff snapshots, and approval ID indexes against truncated JSON after process termination. Disposable cache/snapshot corruption now recovers as a miss, authoritative resume state still surfaces malformed JSON, and approval short-ID indexes are published atomically without overwriting existing mappings. Thanks to [@TurboTheTurtle](https://github.com/TurboTheTurtle) (Issues [#111](https://github.com/openclaw/lobster/issues/111), [#112](https://github.com/openclaw/lobster/issues/112), [#113](https://github.com/openclaw/lobster/issues/113), PR [#114](https://github.com/openclaw/lobster/pull/114)).
- Fix `timeout_ms` + `retry` so per-attempt timeouts retry as documented while external workflow cancellation still stops immediately. Thanks to [@KrasimirKralev](https://github.com/KrasimirKralev) (PR [#106](https://github.com/openclaw/lobster/pull/106)).

## 2026.5.22

- Memoize Ajv schema compilation for repeated validation paths to avoid retained SchemaEnv/closure growth in long-running processes. Thanks to [@KrasimirKralev](https://github.com/KrasimirKralev) (PR [#98](https://github.com/openclaw/lobster/pull/98)) and [@cmi525](https://github.com/cmi525) (Issue [#96](https://github.com/openclaw/lobster/issues/96)).
- Improve workflow resume compatibility for `stateKey` naming by accepting both `workflow_resume_` and `workflow-resume_` prefixes, including cleanup against the resolved on-disk key. Thanks to [@brownetw-ai](https://github.com/brownetw-ai) (PR [#4](https://github.com/openclaw/lobster/pull/4)).
- Add per-step workflow `retry` policies (`max`, `backoff`, `delay_ms`, `max_delay_ms`, `jitter`) with retry-aware stderr logs and dry-run visibility. Thanks to [@scottgl9](https://github.com/scottgl9) (PR [#84](https://github.com/openclaw/lobster/pull/84)).
- Add optional approval identity constraints for workflow gates (`approval.initiated_by`, `approval.required_approver`, `approval.require_different_approver`) with resume-time enforcement via `LOBSTER_APPROVAL_APPROVED_BY` and envelope metadata for integrations. Thanks to [@coolmanns](https://github.com/coolmanns) (Issue [#44](https://github.com/openclaw/lobster/issues/44)).
- Clarify `pipeline:` vs `run:` usage for `llm.invoke` / `llm_task.invoke` in workflow files, and add regression coverage to ensure `stdin: $step.stdout` is forwarded as LLM artifacts for `llm_task.invoke` pipeline steps. Thanks to [@RatkoJ](https://github.com/RatkoJ) (Issue [#41](https://github.com/openclaw/lobster/issues/41)).
- Add `lobster graph` workflow visualization with `mermaid` (default), `dot`, and `ascii` outputs, including step-type nodes, `stdin` data-flow edges, conditional dependency labels (`when`/`condition`), approval-gate diamond shapes, and `--args-json` label resolution support. Thanks to [@vignesh07](https://github.com/vignesh07) (Issue [#53](https://github.com/openclaw/lobster/issues/53)).
- Add workflow composition via `workflow:` + `workflow_args`, including recursive sub-workflow execution, cycle detection, and dry-run visibility for workflow steps. Sub-workflow approval/input halts are rejected with resume-state cleanup. Thanks to [@scottgl9](https://github.com/scottgl9) (PR [#73](https://github.com/openclaw/lobster/pull/73)).
- Add per-step `on_error` workflow policies (`stop|continue|skip_rest`) for partial-failure recovery, with structured step error fields (`error`, `errorMessage`) for condition-based branching. Thanks to [@scottgl9](https://github.com/scottgl9) (PR [#72](https://github.com/openclaw/lobster/pull/72)).
- Add per-step workflow `timeout_ms` handling, including timeout-triggered aborts, `SIGKILL` for timed shell steps, and dry-run annotations. Thanks to [@scottgl9](https://github.com/scottgl9) (PR [#74](https://github.com/openclaw/lobster/pull/74)).
- Add workflow condition comparison operators `<`, `<=`, `>`, and `>=` with strict numeric semantics (booleans/null do not coerce), including mixed boolean-expression support with `&&`/`||`. Thanks to [@scottgl9](https://github.com/scottgl9) (PR [#71](https://github.com/openclaw/lobster/pull/71)).
- Add workflow-level LLM cost tracking with `_meta.cost` summaries, per-step usage attribution, and optional `cost_limit` controls with `warn`/`stop` actions (plus custom pricing via `LOBSTER_LLM_PRICING_JSON`). Thanks to [@scottgl9](https://github.com/scottgl9) (PR [#70](https://github.com/openclaw/lobster/pull/70)).
- Add `parallel` workflow steps with branch fan-out, `wait: all|any`, block-level timeout support, and branch result references in downstream steps. Thanks to [@scottgl9](https://github.com/scottgl9) (PR [#69](https://github.com/openclaw/lobster/pull/69)).
- Add `for_each` workflow steps for per-item sub-step execution over arrays, including loop-scoped vars (`item_var`/`index_var`), optional `batch_size` + `pause_ms`, and collected iteration outputs for downstream steps. Thanks to [@scottgl9](https://github.com/scottgl9) (PR [#68](https://github.com/openclaw/lobster/pull/68)).
- Add pipe-based template filters (for example `upper`, `length`, `join`, `default`, `date`) for the `template` command with quote-aware filter parsing and chain evaluation. Thanks to [@scottgl9](https://github.com/scottgl9) (PR [#67](https://github.com/openclaw/lobster/pull/67)).

## 2026.4.6

- Add workflow file support for `.lobster`, YAML, and JSON, including workflow args/env, native pipeline steps, and shell-safe `LOBSTER_ARG_*` inputs.
- Add structured input pauses with `ask`, workflow `input`, `needs_input`, and `lobster resume --response-json '{...}'` for resumable human-in-the-loop flows.
- Add richer workflow condition expressions with `!`, `==`, `!=`, `&&`, `||`, and parentheses.
- Export the embeddable runtime via `@clawdbot/lobster/core` so Lobster can run in-process inside OpenClaw and other hosts.
- Add generic `llm.invoke` adapters, `openclaw.invoke --each`, and keep `clawd.invoke` as a supported alias.
- Add compact state-backed workflow/pipeline resume tokens, safer resume validation, and hardened approval ID handling.
- Improve dry-run and shell interoperability with `exec --stdin raw|json|jsonl`, `approve --preview-from-stdin --limit N`, and better template/shell-variable preservation.
- Improve Windows CLI/build compatibility and fix quoted-argument parser edge cases.

## 2026.1.21-1

- Published release (pre-changelog).

## 2026.1.21

- Initial published release (pre-changelog).
