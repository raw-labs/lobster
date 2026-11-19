# The basics: commands, an arg with a default, and one step's output feeding the next.
#
#   lobster run examples/hello.lobster
#   lobster run --file examples/hello.lobster --args-json '{"name":"Ada"}'
name: hello
args:
  name:
    default: world
    description: Who to greet.
steps:
  # A command's output is the step's stdout; when it is JSON, $greet.json is the parsed value.
  # Free text such as an arg is safest read from LOBSTER_ARG_<NAME> rather than pasted in.
  - id: greet
    run: >-
      node -e 'console.log(JSON.stringify({ greeting: "Hello, " + process.env.LOBSTER_ARG_NAME + "!" }))'

  # stdin passes another step's output to this one, or a field of it.
  - id: shout
    run: tr a-z A-Z
    stdin: $greet.json.greeting

  # A condition reads earlier results. This step is skipped for the default name.
  - id: welcome
    run: echo "Nice to meet you."
    when: $greet.json.greeting != "Hello, world!"
