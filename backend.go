package dispatch

import "context"

// Backend launches and supervises workers on one execution mechanism, such as
// local processes or Kubernetes Jobs. The dispatcher calls it from a single
// goroutine.
type Backend interface {
	// Launch starts a worker for the claimed run. It must be idempotent on
	// the run ID: launching a run that already exists is not an error.
	Launch(ctx context.Context, work Work) error
	// Status reports the worker for runID. An unknown run reports
	// WorkerExited with Detail explaining that it was not found.
	Status(ctx context.Context, runID string) (WorkerStatus, error)
	// Stop signals the worker and returns once it has stopped or ctx ends.
	Stop(ctx context.Context, runID string, reason StopReason) error
	// Forget releases any resources kept for an exited run.
	Forget(ctx context.Context, runID string) error
	// Active lists workers that survived a dispatcher restart, so the
	// dispatcher can resume supervising them.
	Active(ctx context.Context) ([]RunRef, error)
}

// Work is everything a backend needs to start a worker.
type Work struct {
	Task     Task
	Run      AgentRun
	Handoffs []Handoff
}

// RunRef identifies a worker by its task and run.
type RunRef struct {
	TaskID string
	RunID  string
}

// WorkerState is a worker's coarse lifecycle state.
type WorkerState int

const (
	// WorkerPending means the worker was accepted but has not started, for
	// example while a node is provisioned.
	WorkerPending WorkerState = iota
	// WorkerRunning means the worker is executing.
	WorkerRunning
	// WorkerExited means the worker has finished, failed, or is gone.
	WorkerExited
)

func (s WorkerState) String() string {
	switch s {
	case WorkerPending:
		return "pending"
	case WorkerRunning:
		return "running"
	case WorkerExited:
		return "exited"
	default:
		return "unknown"
	}
}

// WorkerStatus describes a worker. ExitCode and Detail are meaningful once
// the worker has exited. Detail must not contain logs or secrets; it is
// recorded in Taskboard handoffs.
type WorkerStatus struct {
	State    WorkerState
	ExitCode int
	Detail   string
}

// StopReason says why the dispatcher is stopping a worker.
type StopReason string

const (
	StopCancel   StopReason = "cancel"
	StopPause    StopReason = "pause"
	StopShutdown StopReason = "shutdown"
	StopLease    StopReason = "lease"
)
