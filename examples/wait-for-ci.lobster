# Poll until something finishes: check, sleep, and check again.
#
#   lobster run examples/wait-for-ci.lobster
#
# A short sleep waits in place. A longer one (over LOBSTER_INLINE_SLEEP_MS, 60 s by default)
# pauses the run instead, and `lobster runs advance --due`, from cron say, continues it.
name: wait-for-ci
steps:
  # Rounds run until `until` holds, at most `max` of them (then the step fails).
  - id: ci
    repeat: { until: $check.json.state != running, max: 10 }
    steps:
      # Stand-in for a real status check (gh run view --json status ...): it reports "running"
      # twice, then "success". It counts its calls in a file named after the run.
      - id: check
        run: >-
          node -e 'const fs = require("fs"), path = require("path"), os = require("os");
          const file = path.join(os.tmpdir(), "lobster-example-ci-" + process.env.LOBSTER_RUN_ID);
          const calls = (fs.existsSync(file) ? Number(fs.readFileSync(file, "utf8")) : 0) + 1;
          fs.writeFileSync(file, String(calls));
          console.log(JSON.stringify({ state: calls < 3 ? "running" : "success" }))'
      - id: pause
        sleep: 1s
        when: $check.json.state == running

  # After the loop, the last round's steps are readable, and $ci.rounds says how many ran.
  - id: result
    run: echo "CI finished ($STATE) after $ROUNDS checks."
    env: { STATE: $check.json.state, ROUNDS: $ci.rounds }
