# A gate before acting: build, ask a person, and deploy only if they approve.
#
#   lobster run --file examples/deploy-with-approval.lobster --args-json '{"version":"1.4.2"}'
#
# At a terminal it asks there. In tool mode (--mode tool) it returns status "needs_approval"
# with a resume token, and the run waits until someone answers:
#
#   lobster resume <token> --approve yes
name: deploy-with-approval
args:
  version:
    default: 1.0.0
steps:
  # Stand-in for a real build (make release, docker build ...).
  - id: build
    run: echo '{"artifact":"app-${version}.tar.gz","tests":"passed"}'

  # The person sees the build's output with the prompt (the prompt itself is plain text: args
  # are not filled into it). With on_reject: continue a "no" does not cancel the run: the step
  # completes with approved: false and the steps below decide what to do.
  # required_approver or require_different_approver restrict who may answer.
  - id: approve
    approval:
      prompt: Deploy this build to production?
      on_reject: continue
      timeout: 1d
      on_timeout: cancel
    stdin: $build.json

  # Stand-in for the real deploy (kubectl rollout, a deploy API ...).
  - id: deploy
    run: echo "Deployed $LOBSTER_ARG_VERSION."
    when: $approve.approved

  - id: declined
    run: echo "Deploy of $LOBSTER_ARG_VERSION was not approved."
    when: "!$approve.approved"
