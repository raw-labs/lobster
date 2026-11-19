# A workflow called by release-notes.lobster; it runs on its own too:
#
#   lobster run --file examples/compose/format-list.lobster --args-json '{"title":"Todo","items":["a","b"]}'
name: format-list
args:
  title:
    default: Notes
  items:
    default: []
    description: A list, or its JSON text (what a parent's workflow_args pass).
steps:
  # LOBSTER_ARGS_JSON holds every arg as JSON.
  - id: format
    run: >-
      node -e 'const { title, items } = JSON.parse(process.env.LOBSTER_ARGS_JSON);
      const list = typeof items === "string" ? JSON.parse(items) : items;
      console.log(["# " + title, ...list.map((item) => "- " + item)].join("\n"))'
