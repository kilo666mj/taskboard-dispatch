package dispatch

import (
	"encoding/json"
	"time"
)

// Task is the subset of a Taskboard task a dispatcher uses. Raw holds the
// complete task as returned by Taskboard so it can be handed to a worker.
type Task struct {
	ID           string          `json:"id"`
	Title        string          `json:"title"`
	Summary      string          `json:"summary,omitempty"`
	Repository   string          `json:"repository,omitempty"`
	Project      string          `json:"project,omitempty"`
	Status       string          `json:"status"`
	Visibility   string          `json:"visibility"`
	Version      int64           `json:"version"`
	CreatedBy    string          `json:"created_by"`
	LastEditedBy string          `json:"last_edited_by"`
	Requirements []string        `json:"requirements,omitempty"`
	Runs         []AgentRun      `json:"runs,omitempty"`
	Raw          json.RawMessage `json:"-"`
}

func (t *Task) UnmarshalJSON(data []byte) error {
	type plain Task
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*t = Task(decoded)
	t.Raw = append(json.RawMessage(nil), data...)
	return nil
}

// RunByID returns the task's run with id, if Taskboard included it.
func (t Task) RunByID(id string) (AgentRun, bool) {
	for _, run := range t.Runs {
		if run.ID == id {
			return run, true
		}
	}
	return AgentRun{}, false
}

// AgentRun is one leased participation in a task.
type AgentRun struct {
	ID           string     `json:"id"`
	TaskID       string     `json:"task_id"`
	Agent        string     `json:"agent"`
	Callsign     string     `json:"callsign"`
	Status       string     `json:"status"`
	LeaseExpires time.Time  `json:"lease_expires_at"`
	StartedAt    time.Time  `json:"started_at"`
	EndedAt      *time.Time `json:"ended_at,omitempty"`
}

// Handoff is a run-scoped snapshot that lets a replacement worker continue.
type Handoff struct {
	ID                string    `json:"id,omitempty"`
	TaskID            string    `json:"task_id,omitempty"`
	RunID             string    `json:"run_id"`
	Kind              string    `json:"kind,omitempty"`
	LastCompletedStep string    `json:"last_completed_step,omitempty"`
	Worktree          string    `json:"worktree,omitempty"`
	Branch            string    `json:"branch,omitempty"`
	Commits           []string  `json:"commits,omitempty"`
	PullRequests      []string  `json:"pull_requests,omitempty"`
	Validation        []string  `json:"validation,omitempty"`
	ReviewFindings    []string  `json:"review_findings,omitempty"`
	Blocker           string    `json:"blocker,omitempty"`
	NextAction        string    `json:"next_action,omitempty"`
	CreatedAt         time.Time `json:"created_at,omitzero"`
}

// Handoff kinds accepted by Taskboard.
const (
	HandoffCheckpoint = "checkpoint"
	HandoffFinal      = "final"
)

// Claim is the result of claiming a task: the task, its fresh run, and the
// handoffs left by earlier runs.
type Claim struct {
	Task     Task      `json:"task"`
	Run      AgentRun  `json:"run"`
	Handoffs []Handoff `json:"handoffs,omitempty"`
}

// Control is a human request to pause, cancel, resume, or retry a run.
type Control struct {
	ID          string    `json:"id"`
	TaskID      string    `json:"task_id"`
	TargetRunID string    `json:"target_run_id"`
	TargetAgent string    `json:"target_agent"`
	Kind        string    `json:"kind"`
	Status      string    `json:"status"`
	Reason      string    `json:"reason,omitempty"`
	TaskVersion int64     `json:"task_version"`
	ExpiresAt   time.Time `json:"expires_at"`
}

// Control kinds and lifecycle states.
const (
	ControlPause  = "pause"
	ControlCancel = "cancel"
	ControlResume = "resume"
	ControlRetry  = "retry"

	ControlRequested    = "requested"
	ControlAcknowledged = "acknowledged"
	ControlAccepted     = "accepted"
	ControlRejected     = "rejected"
	ControlCompleted    = "completed"
)

// Heartbeat is the result of renewing a run lease.
type Heartbeat struct {
	Run             AgentRun  `json:"run"`
	PendingControls []Control `json:"pending_controls,omitempty"`
}

// Advertisement is a dispatcher's live worker-matching input.
type Advertisement struct {
	Capabilities []string `json:"capabilities"`
	Capacity     int      `json:"capacity"`
	TTLSeconds   int      `json:"ttl_seconds,omitempty"`
}
