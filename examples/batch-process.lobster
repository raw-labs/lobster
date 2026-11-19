# Work through a list: a few items at a time, retrying a step that fails, and collecting the
# results.
#
#   lobster run examples/batch-process.lobster
name: batch-process
steps:
  # Stand-in for listing real work (files in an inbox, rows from an API ...).
  - id: files
    run: echo '["january.csv","february.csv","march.csv","april.csv"]'

  # Runs its steps once per item, up to two items at once. Inside, $item is the current item.
  - id: each
    for_each: $files.json
    concurrency: 2
    steps:
      # Stand-in for real work on the file. A failing attempt would be tried again, up to three
      # times, 200 ms apart (this one does not fail).
      - id: count
        run: >-
          node -e 'const f = process.env.FILE; console.log(JSON.stringify({ file: f, rows: f.length * 10 }))'
        env: { FILE: $item.json }
        retry: { max: 3, delay_ms: 200 }
        timeout_ms: 30000

  # $each.json has one entry per item, in the list's order: the item, its index and the output
  # of each step.
  - id: total
    run: >-
      node -e 'let s = ""; process.stdin.on("data", (d) => (s += d)).on("end", () => {
      const rows = JSON.parse(s).reduce((n, e) => n + e.count.rows, 0);
      console.log(JSON.stringify({ rows })) })'
    stdin: $each.json
