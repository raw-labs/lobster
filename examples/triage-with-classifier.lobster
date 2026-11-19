# Decide with a model, and ask a person when it is unsure.
#
# Needs a model: set LOBSTER_LLM_PROVIDER and its URL (see "Calling LLMs from workflows" in
# docs/reference.md). Then:
#
#   lobster run --mode tool --file examples/triage-with-classifier.lobster
#
# A confident answer is routed on its own. An unsure one returns status "needs_input"; answer
# it with:
#
#   lobster resume <token> --response-json '{"label":"refund"}'
name: triage-with-classifier
# The model calls of this run may cost at most 10 cents; past that the run stops.
cost_limit: { max_usd: 0.10, action: stop }
args:
  email:
    default: Hi, I was charged twice for order 1234. Can I get my money back?
steps:
  - id: email
    run: printf '%s' "$LOBSTER_ARG_EMAIL"

  # The output schema keeps the model to these labels. llm.invoke yields a record of the call;
  # the two maps take out its answer (output.data), so $triage.json is { label, confidence }.
  # The answer is saved with the run, so a resume does not call the model again.
  - id: triage
    pipeline: >
      llm.invoke --prompt "Classify this email as refund, spam or other, with your confidence from 0 to 1."
      --output-schema '{"type":"object","properties":{"label":{"enum":["refund","spam","other"]},"confidence":{"type":"number"}},"required":["label","confidence"]}'
      | map --unwrap output | map --unwrap data
    stdin: $email.stdout

  # Both paths end in a step whose output has a label, so $label.json.label is the decision
  # whichever way it was made.
  - id: label
    switch:
      cases:
        - when: $triage.json.confidence < 0.8
          steps:
            - id: ask
              input:
                prompt: The model is unsure. Which kind of email is this?
                responseSchema:
                  type: object
                  properties: { label: { enum: [refund, spam, other] } }
                  required: [label]
            - id: answered
              run: cat
              stdin: $ask.response
      else:
        - id: model
          run: cat
          stdin: $triage.json

  # Stand-ins for the real actions (a billing API, the mail client, a ticket system).
  - id: route
    switch:
      cases:
        - when: $label.json.label == refund
          steps:
            - id: refund
              run: echo "Refund opened."
        - when: $label.json.label == spam
          steps:
            - id: archive
              run: echo "Archived as spam."
      else:
        - id: ticket
          run: echo "Ticket created for the support queue."
