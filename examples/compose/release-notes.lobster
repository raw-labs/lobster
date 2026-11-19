# Reuse a workflow: call another file as a step, passing it args.
#
#   lobster run examples/compose/release-notes.lobster
name: release-notes
steps:
  # Stand-in for the real list (git log, a milestone's issues ...).
  - id: changes
    run: echo '["Fix sign-in on Safari","Faster search","Export to CSV"]'

  # Runs format-list.lobster (found next to this file) with these args. Each value is text, so
  # the list arrives as its JSON. The child's output is this step's output, and the run saves
  # the child as it was, so a resumed run runs the same one.
  - id: notes
    workflow: format-list.lobster
    workflow_args:
      title: Release 2.0
      items: $changes.json

  - id: publish
    run: cat # stand-in for posting the notes
    stdin: $notes.stdout
