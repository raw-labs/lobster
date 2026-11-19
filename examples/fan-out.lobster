# Do independent things at once: wait for all of them, or take whichever finishes first.
#
#   lobster run examples/fan-out.lobster
name: fan-out
steps:
  # wait: all (the default) waits for every branch. A failing branch stops the others.
  # Stand-ins for real lookups.
  - id: gather
    parallel:
      wait: all
      timeout_ms: 60000
      branches:
        - id: weather
          run: echo '{"temp":21}'
        - id: calendar
          run: echo '{"meetings":3}'

  # wait: any takes the first branch to finish and stops the rest; $fastest.json holds only
  # the winner's output, and the losers count as skipped.
  - id: fastest
    parallel:
      wait: any
      branches:
        - id: slow_mirror
          run: sleep 2; echo '{"from":"slow mirror"}'
        - id: fast_mirror
          run: echo '{"from":"fast mirror"}'

  # Each branch's output is readable by its own id.
  - id: summary
    run: echo "$TEMP degrees, $MEETINGS meetings; downloaded from the $FROM."
    env:
      TEMP: $weather.json.temp
      MEETINGS: $calendar.json.meetings
      FROM: $fast_mirror.json.from
