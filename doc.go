// Package dispatch runs Taskboard work on an execution backend.
//
// Taskboard is the queue. A Dispatcher pulls the queued agent-lane tasks
// routed to its runner token, claims each one, and hands it to a Backend that
// launches a worker in that mechanism's own way, such as a local process or
// a Kubernetes Job. While the backend reports the worker alive the
// dispatcher renews the run's lease, and it maps human pause, cancel, resume,
// and retry controls onto the backend.
//
// A worker that exits without completing, blocking, or escalating its task
// gets a final handoff describing what was observed, and its lease is left to
// expire. Taskboard then marks the task stale for a person to review; the
// dispatcher never claims stale work, so failures are not retried
// automatically.
//
// Each dispatcher should authenticate with its own Taskboard agent
// principal: Taskboard keeps one worker advertisement per principal and
// requires run updates to come from the principal that claimed the run.
package dispatch
