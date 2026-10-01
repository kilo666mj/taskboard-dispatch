# taskboard-dispatch

`taskboard-dispatch` runs [Taskboard](https://github.com/kilo666mj/taskboard)
work on an execution backend. Taskboard stays the queue: a dispatcher pulls
the agent-lane tasks routed to its runner token, claims them, and hands each
one to a backend that starts a worker its own way. The module contains the
shared dispatcher loop, a host-local command backend, and a ready-to-run
binary for that backend.

> **Work in progress.** Interfaces may change before v1. It implements step 2
> of Taskboard's
> [dispatcher design](https://github.com/kilo666mj/taskboard/blob/main/docs/dispatchers.md).

```sh
go get go.michaelspost.com/taskboard-dispatch@latest
```

It requires Go 1.25 or newer and uses the official MCP Go SDK. The
`localexec` backend and the binary build on Unix only.

## How it works

Each pass, a dispatcher:

1. advertises its runner token, extra capabilities, and capacity;
2. checks every worker it supervises. It renews the run's lease while the
   worker is alive, and records a final handoff when a worker exits with its
   run still open;
3. processes pause, cancel, resume, and retry controls through Taskboard's
   acknowledged lifecycle, stopping workers when needed;
4. lists queued agent-lane tasks offered to it and claims admitted ones, up to
   capacity, then launches a worker for each claim.

A worker that exits without completing, blocking, or escalating its task is
not retried. Its lease expires, Taskboard marks the task stale, and a person
decides whether to requeue it. Dispatchers never claim stale tasks.

## Taskboard setup

- Give each dispatcher its own agent credential. Taskboard keeps one worker
  advertisement per principal and accepts run updates only from the principal
  that claimed the run.
- Route work with a runner token. Set the token as a task requirement, or set
  `TASKBOARD_DEFAULT_REQUIREMENTS` on an instance with one backend.
- Size the principal's `max_run_seconds` policy for the longest task,
  including any time spent waiting for capacity.
- Admit only work from people you trust. Agent-lane tasks are editable by
  anyone with access, so privileged runners should check provenance with
  `dispatch.AdmitEditors`.

## Library use

```go
client, err := dispatch.NewClient("https://taskboard.example.com/mcp", dispatch.ClientOptions{
    Token: os.Getenv("TASKBOARD_TOKEN"),
    Name:  "my-dispatcher",
})
if err != nil {
    log.Fatal(err)
}
backend, err := localexec.New(localexec.Config{
    Command:  []string{"/usr/local/bin/run-task"},
    WorkRoot: "/var/lib/my-dispatcher/runs",
    MCPURL:   "https://taskboard.example.com/mcp",
})
if err != nil {
    log.Fatal(err)
}
dispatcher, err := dispatch.New(client, backend, dispatch.Config{
    Runner:     "runner:local",
    Capacity:   2,
    SessionKey: "my-dispatcher@host-a",
    Admit:      dispatch.AdmitEditors("alice-subject"),
})
if err != nil {
    log.Fatal(err)
}
log.Fatal(dispatcher.Run(context.Background()))
```

A new mechanism implements `dispatch.Backend`: `Launch` (idempotent on the run
ID), `Status`, `Stop`, `Forget`, and `Active`, which lists workers that survive
a dispatcher restart so supervision can resume.

## Local command backend

`localexec` runs one command per claimed run in a private directory under the
work root. The directory holds `task.json` (the task, its run, and earlier
handoffs) and `output.log` (stdout and stderr, which stay on the host). The
command receives:

| Variable | Value |
| --- | --- |
| `TASKBOARD_TASK_ID` | Task ID |
| `TASKBOARD_RUN_ID` | Run ID the dispatcher claimed |
| `TASKBOARD_TASK_FILE` | Path to `task.json` |
| `TASKBOARD_MCP_URL` | Taskboard MCP endpoint |
| `TASKBOARD_TOKEN` | Dispatcher credential, only when token passing is enabled |

A worker that should report checklist progress or complete its task itself
needs the token, because Taskboard accepts run updates only from the claiming
principal. Without it, the worker's exit code is all Taskboard learns.
Workers run in their own process group. Stopping sends SIGTERM, then SIGKILL
after the grace period. Local workers end with the dispatcher.

## Running `taskboard-dispatch-local`

```sh
go install go.michaelspost.com/taskboard-dispatch/cmd/taskboard-dispatch-local@latest
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `TASKBOARD_MCP_URL` | required | Taskboard MCP endpoint |
| `TASKBOARD_TOKEN` or `TASKBOARD_TOKEN_FILE` | required | Dispatcher credential |
| `DISPATCH_COMMAND` | required | JSON array argv, for example `["/usr/local/bin/run-task"]` |
| `DISPATCH_WORK_ROOT` | required | Directory for per-run directories |
| `DISPATCH_ALLOWED_EDITORS` | required | Comma-separated principals whose tasks are admitted |
| `DISPATCH_ADMIT_ALL` | `false` | Admit every offered task instead of checking editors |
| `DISPATCH_RUNNER` | `runner:local` | Runner token to advertise |
| `DISPATCH_CAPABILITIES` | none | Extra comma-separated capability tokens |
| `DISPATCH_CAPACITY` | `1` | Maximum concurrent workers |
| `DISPATCH_PASS_TOKEN` | `false` | Pass the credential to workers as `TASKBOARD_TOKEN` |
| `DISPATCH_SESSION_KEY` | host and runner | Stable key that keeps one callsign across runs |
| `DISPATCH_POLL_SECONDS` | `15` | Seconds between passes |
| `DISPATCH_STOP_GRACE_SECONDS` | `30` | Seconds between SIGTERM and SIGKILL |

On SIGTERM or SIGINT the dispatcher stops its workers, records a checkpoint
handoff for each, and exits.

## Testing against a local Taskboard

Unit tests use fakes. To check real tool schemas, run Taskboard on loopback
with `TASKBOARD_ALLOW_INSECURE=true` and a 32-character
`TASKBOARD_AUTH_TOKEN`, create an agent-lane task with `task_create`, and run
the binary against `http://127.0.0.1:<port>/mcp` with `DISPATCH_ADMIT_ALL=true`.

## License

MIT. See [LICENSE](LICENSE).
