# Wait for something outside: an update about an order, with a deadline as a third outcome.
#
#   lobster run --mode tool --file examples/wait-for-event.lobster --args-json '{"order":"o-1"}'
#
# returns status "waiting". Deliver the event, and the run continues:
#
#   lobster signal order.update --data '{"order":"o-1","status":"paid"}'
name: wait-for-event
args:
  order:
    default: o-1
steps:
  # Completes with the first order.update event whose data has this order. After 3 days
  # without one it completes with timedOut instead (on_timeout: continue; the default cancels).
  - id: outcome
    wait:
      event: order.update
      match: { order: "${order}" }
      timeout: 3d
      on_timeout: continue

  # Stand-ins for the real actions.
  - id: route
    switch:
      cases:
        - when: $outcome.timedOut
          steps:
            - id: chase
              run: echo "No word on order $LOBSTER_ARG_ORDER; asking sales."
        - when: $outcome.json.status == paid
          steps:
            - id: ship
              run: echo "Shipping order $LOBSTER_ARG_ORDER."
        - when: $outcome.json.status == cancelled
          steps:
            - id: release
              run: echo "Releasing the stock held for order $LOBSTER_ARG_ORDER."
      else:
        - id: unknown
          run: echo "Unexpected update for order $LOBSTER_ARG_ORDER."
