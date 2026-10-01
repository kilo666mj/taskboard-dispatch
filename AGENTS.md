# AGENTS.md

Keep taskboard-dispatch small and mechanism-neutral. The `dispatch` package
owns the Taskboard run contract (advertise, claim, lease renewal, controls,
handoffs); backends own only launching, observing, and stopping workers.
Mechanism policy such as images, node selectors, sandboxes, and budgets stays
in each backend's configuration.

Talk to Taskboard only through its MCP tools: Taskboard accepts agent
credentials on `/mcp` and nowhere else. Never send prompts, logs, credentials,
or raw tool output to Taskboard; handoffs carry concise observed facts.

Never claim stale tasks. Stale work waits for a person to review and requeue
it, so a failing worker must not be retried automatically.

After Go changes, run:

```sh
gofmt -w .
go test -race ./...
go vet ./...
```

Schema assumptions about Taskboard tools are covered by unit tests against
fakes. When changing a tool call, also run a dispatcher against a local
Taskboard, as described in the README.
