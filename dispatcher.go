package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"
)

// Taskboard is the set of Taskboard operations a Dispatcher needs. *Client
// implements it; tests may substitute a fake.
type Taskboard interface {
	Advertise(ctx context.Context, advertisement Advertisement) error
	ListPickup(ctx context.Context) ([]Task, error)
	Claim(ctx context.Context, task Task, sessionKey, agent string) (Claim, error)
	Heartbeat(ctx context.Context, taskID, runID string) (Heartbeat, error)
	GetTask(ctx context.Context, taskID string) (Task, error)
	ListControls(ctx context.Context) ([]Control, error)
	UpdateControl(ctx context.Context, control Control, status, note string, taskVersion int64) (Control, error)
	AddHandoff(ctx context.Context, taskID string, handoff Handoff, key string) error
}

// Config configures a Dispatcher.
type Config struct {
	// Runner is this mechanism's routing token, such as runner:local. It is
	// always advertised.
	Runner string
	// Capabilities are further operational tokens the workers provide, such
	// as repo:org/app or harness:codex.
	Capabilities []string
	// Capacity is the maximum number of concurrent workers (1-100).
	Capacity int
	// SessionKey is a stable opaque key for this dispatcher. Taskboard uses
	// it to keep one callsign across the dispatcher's runs.
	SessionKey string
	// Agent is the display name recorded on claimed runs.
	Agent string
	// Admit decides whether to claim a task. Privileged runners should use
	// AdmitEditors. Nil admits every task offered.
	Admit func(Task) bool

	// PollInterval is the time between passes. Default 15s.
	PollInterval time.Duration
	// AdvertiseTTL is the advertisement lifetime, renewed at half of it.
	// Default 2m; Taskboard accepts 30s-1h.
	AdvertiseTTL time.Duration
	// StopTimeout bounds how long a stop waits for a worker. Default 1m.
	StopTimeout time.Duration
	// Logger receives operational logs. Default slog.Default().
	Logger *slog.Logger
}

// AdmitEditors returns an Admit function that claims a task only when both
// its creator and last editor are in allowed. Tasks without recorded
// provenance are refused.
func AdmitEditors(allowed ...string) func(Task) bool {
	return func(task Task) bool {
		return task.CreatedBy != "" && task.LastEditedBy != "" && slices.Contains(allowed, task.CreatedBy) && slices.Contains(allowed, task.LastEditedBy)
	}
}

// Dispatcher pulls the Taskboard work routed to one execution mechanism and
// supervises it through a Backend. Taskboard remains the queue: the
// dispatcher claims a task before launching it, keeps the run's lease alive
// while the backend reports the worker alive, and maps run controls onto the
// backend.
type Dispatcher struct {
	board   Taskboard
	backend Backend
	cfg     Config
	log     *slog.Logger

	runs          map[string]*tracked
	nextAdvertise time.Time
}

type tracked struct {
	taskID string
	runID  string
}

// New validates cfg and returns a Dispatcher.
func New(board Taskboard, backend Backend, cfg Config) (*Dispatcher, error) {
	if board == nil || backend == nil {
		return nil, errors.New("taskboard and backend are required")
	}
	cfg.Runner = strings.TrimSpace(cfg.Runner)
	if !strings.HasPrefix(cfg.Runner, "runner:") || len(cfg.Runner) == len("runner:") {
		return nil, errors.New("runner must be a runner: token, such as runner:local")
	}
	if cfg.Capacity < 1 || cfg.Capacity > 100 {
		return nil, errors.New("capacity must be between 1 and 100")
	}
	if strings.TrimSpace(cfg.SessionKey) == "" {
		return nil, errors.New("session key is required")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 15 * time.Second
	}
	if cfg.AdvertiseTTL == 0 {
		cfg.AdvertiseTTL = 2 * time.Minute
	}
	if cfg.AdvertiseTTL < 30*time.Second || cfg.AdvertiseTTL > time.Hour {
		return nil, errors.New("advertise TTL must be between 30s and 1h")
	}
	if cfg.StopTimeout <= 0 {
		cfg.StopTimeout = time.Minute
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Admit == nil {
		cfg.Admit = func(Task) bool { return true }
	}
	return &Dispatcher{board: board, backend: backend, cfg: cfg, log: cfg.Logger, runs: map[string]*tracked{}}, nil
}

// Run supervises work until ctx ends. On return it stops every worker it
// supervises and records a checkpoint handoff; their leases then expire and
// Taskboard marks the tasks stale for review.
func (d *Dispatcher) Run(ctx context.Context) error {
	active, err := d.backend.Active(ctx)
	if err != nil {
		return fmt.Errorf("list surviving workers: %w", err)
	}
	for _, ref := range active {
		d.runs[ref.RunID] = &tracked{taskID: ref.TaskID, runID: ref.RunID}
		d.log.Info("resumed supervising worker", "task", ref.TaskID, "run", ref.RunID)
	}
	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()
	for {
		d.Pass(ctx)
		select {
		case <-ctx.Done():
			d.shutdown()
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Pass performs one dispatch pass: advertise when due, supervise workers,
// process controls, then claim new work up to capacity. Errors are logged
// and retried on the next pass.
func (d *Dispatcher) Pass(ctx context.Context) {
	d.advertise(ctx)
	d.supervise(ctx)
	d.processControls(ctx)
	d.claim(ctx)
}

func (d *Dispatcher) advertise(ctx context.Context) {
	if time.Now().Before(d.nextAdvertise) {
		return
	}
	capabilities := append([]string{d.cfg.Runner}, d.cfg.Capabilities...)
	err := d.board.Advertise(ctx, Advertisement{Capabilities: capabilities, Capacity: d.cfg.Capacity, TTLSeconds: int(d.cfg.AdvertiseTTL / time.Second)})
	if err != nil {
		d.log.Warn("advertise failed", "error", err)
		return
	}
	d.nextAdvertise = time.Now().Add(d.cfg.AdvertiseTTL / 2)
}

// supervise renews leases for live workers and records a handoff for workers
// that exit without the task reaching a terminal state for their run.
func (d *Dispatcher) supervise(ctx context.Context) {
	for _, run := range d.sortedRuns() {
		status, err := d.backend.Status(ctx, run.runID)
		if err != nil {
			d.log.Warn("worker status failed", "task", run.taskID, "run", run.runID, "error", err)
			continue
		}
		if status.State == WorkerExited {
			d.finish(ctx, run, status)
			continue
		}
		// Pending controls in the response are handled by processControls,
		// which lists every open control once per pass.
		if _, err := d.board.Heartbeat(ctx, run.taskID, run.runID); err != nil {
			if d.runClosed(ctx, run, err) {
				d.log.Warn("run closed in Taskboard; stopping worker", "task", run.taskID, "run", run.runID, "error", err)
				if d.stop(ctx, run, StopLease) == nil {
					d.forget(ctx, run)
				}
				continue
			}
			d.log.Warn("heartbeat failed", "task", run.taskID, "run", run.runID, "error", err)
		}
	}
}

// runClosed reports whether a refused heartbeat means the worker must stop:
// the run ended, was replaced, or reached its maximum duration. Transport
// failures and other errors leave the worker running for the next attempt.
func (d *Dispatcher) runClosed(ctx context.Context, run *tracked, heartbeatErr error) bool {
	var tool *ToolError
	if !errors.As(heartbeatErr, &tool) {
		return false
	}
	if strings.Contains(tool.Message, "maximum run duration") {
		return true
	}
	task, err := d.board.GetTask(ctx, run.taskID)
	if err != nil {
		return false
	}
	current, found := task.RunByID(run.runID)
	return !found || current.EndedAt != nil || current.Status != "active"
}

// finish handles a worker that exited. If its run is still open in
// Taskboard, the worker stopped without completing, blocking, or escalating:
// record what was observed and let the lease expire so the task becomes
// stale for human review. Taskboard never retries automatically.
func (d *Dispatcher) finish(ctx context.Context, run *tracked, status WorkerStatus) {
	task, err := d.board.GetTask(ctx, run.taskID)
	if err != nil {
		d.log.Warn("load task for exited worker failed", "task", run.taskID, "run", run.runID, "error", err)
		return
	}
	current, found := task.RunByID(run.runID)
	if found && current.EndedAt == nil {
		blocker := fmt.Sprintf("Worker exited with code %d before the run ended.", status.ExitCode)
		if status.Detail != "" {
			blocker += " " + status.Detail
		}
		handoff := Handoff{RunID: run.runID, Kind: HandoffFinal, Blocker: clip(blocker, 1000), NextAction: "Review the task and requeue it if another attempt is wanted."}
		if err := d.board.AddHandoff(ctx, run.taskID, handoff, "exit-"+run.runID); err != nil {
			d.log.Warn("record exit handoff failed", "task", run.taskID, "run", run.runID, "error", err)
			return
		}
		d.log.Info("worker exited with the run open; lease will expire", "task", run.taskID, "run", run.runID, "exit_code", status.ExitCode)
	} else {
		d.log.Info("worker finished", "task", run.taskID, "run", run.runID, "exit_code", status.ExitCode)
	}
	d.forget(ctx, run)
}

func (d *Dispatcher) processControls(ctx context.Context) {
	controls, err := d.board.ListControls(ctx)
	if err != nil {
		d.log.Warn("list controls failed", "error", err)
		return
	}
	for _, control := range controls {
		d.handleControl(ctx, control)
	}
}

// handleControl advances one control through Taskboard's acknowledged
// lifecycle: requested, acknowledged, accepted or rejected, then completed.
func (d *Dispatcher) handleControl(ctx context.Context, control Control) {
	if control.Status == ControlRequested {
		updated, err := d.board.UpdateControl(ctx, control, ControlAcknowledged, "", control.TaskVersion)
		if err != nil {
			d.log.Warn("acknowledge control failed", "control", control.ID, "error", err)
			return
		}
		control = updated
	}
	if control.Status == ControlAcknowledged {
		status, note := ControlAccepted, ""
		switch control.Kind {
		case ControlCancel, ControlPause, ControlResume, ControlRetry:
		default:
			status, note = ControlRejected, "Unsupported control kind."
		}
		updated, err := d.board.UpdateControl(ctx, control, status, note, control.TaskVersion)
		if err != nil {
			d.log.Warn("decide control failed", "control", control.ID, "error", err)
			return
		}
		control = updated
	}
	if control.Status != ControlAccepted {
		return
	}
	note := ""
	if run, ok := d.runs[control.TargetRunID]; ok && (control.Kind == ControlCancel || control.Kind == ControlPause) {
		reason := StopCancel
		if control.Kind == ControlPause {
			reason = StopPause
		}
		if err := d.stop(ctx, run, reason); err != nil {
			if _, rejectErr := d.board.UpdateControl(ctx, control, ControlRejected, clip("Worker did not stop: "+err.Error(), 1000), control.TaskVersion); rejectErr != nil {
				d.log.Warn("reject control failed", "control", control.ID, "error", rejectErr)
			}
			return
		}
		d.checkpoint(ctx, run, fmt.Sprintf("Worker stopped by a %s request.", control.Kind))
		d.forget(ctx, run)
		note = "Worker stopped."
	}
	task, err := d.board.GetTask(ctx, control.TaskID)
	if err != nil {
		d.log.Warn("load task for control failed", "control", control.ID, "error", err)
		return
	}
	if _, err := d.board.UpdateControl(ctx, control, ControlCompleted, note, task.Version); err != nil {
		d.log.Warn("complete control failed", "control", control.ID, "error", err)
		return
	}
	d.log.Info("control completed", "control", control.ID, "kind", control.Kind, "task", control.TaskID)
}

func (d *Dispatcher) claim(ctx context.Context) {
	if len(d.runs) >= d.cfg.Capacity {
		return
	}
	tasks, err := d.board.ListPickup(ctx)
	if err != nil {
		d.log.Warn("list pickup work failed", "error", err)
		return
	}
	for _, task := range tasks {
		if len(d.runs) >= d.cfg.Capacity {
			return
		}
		if !d.cfg.Admit(task) {
			continue
		}
		claim, err := d.board.Claim(ctx, task, d.cfg.SessionKey, d.cfg.Agent)
		if err != nil {
			if IsConflict(err) {
				continue
			}
			d.log.Warn("claim failed", "task", task.ID, "error", err)
			continue
		}
		run := &tracked{taskID: claim.Task.ID, runID: claim.Run.ID}
		if err := d.backend.Launch(ctx, Work(claim)); err != nil {
			// The claim cannot be undone. Record why and let the lease
			// expire so the task becomes stale for review.
			d.log.Warn("launch failed; lease will expire", "task", run.taskID, "run", run.runID, "error", err)
			handoff := Handoff{RunID: run.runID, Kind: HandoffFinal, Blocker: clip("Dispatcher could not launch a worker: "+err.Error(), 1000), NextAction: "Check the dispatcher and requeue the task."}
			if err := d.board.AddHandoff(ctx, run.taskID, handoff, "launch-"+run.runID); err != nil {
				d.log.Warn("record launch handoff failed", "task", run.taskID, "run", run.runID, "error", err)
			}
			continue
		}
		d.runs[run.runID] = run
		d.log.Info("launched worker", "task", run.taskID, "run", run.runID, "callsign", claim.Run.Callsign)
	}
}

func (d *Dispatcher) stop(ctx context.Context, run *tracked, reason StopReason) error {
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), d.cfg.StopTimeout)
	defer cancel()
	if err := d.backend.Stop(stopCtx, run.runID, reason); err != nil {
		d.log.Warn("stop worker failed", "task", run.taskID, "run", run.runID, "reason", reason, "error", err)
		return err
	}
	return nil
}

func (d *Dispatcher) checkpoint(ctx context.Context, run *tracked, blocker string) {
	handoff := Handoff{RunID: run.runID, Kind: HandoffCheckpoint, Blocker: blocker}
	if err := d.board.AddHandoff(ctx, run.taskID, handoff, "stop-"+run.runID); err != nil {
		d.log.Warn("record stop handoff failed", "task", run.taskID, "run", run.runID, "error", err)
	}
}

func (d *Dispatcher) forget(ctx context.Context, run *tracked) {
	if err := d.backend.Forget(ctx, run.runID); err != nil {
		d.log.Warn("release worker failed", "task", run.taskID, "run", run.runID, "error", err)
	}
	delete(d.runs, run.runID)
}

func (d *Dispatcher) shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), d.cfg.StopTimeout)
	defer cancel()
	for _, run := range d.sortedRuns() {
		if err := d.stop(ctx, run, StopShutdown); err != nil {
			continue
		}
		d.checkpoint(ctx, run, "Dispatcher shut down and stopped the worker.")
		d.forget(ctx, run)
	}
}

// Supervising returns the run IDs the dispatcher currently supervises.
func (d *Dispatcher) Supervising() []string {
	ids := make([]string, 0, len(d.runs))
	for id := range d.runs {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func (d *Dispatcher) sortedRuns() []*tracked {
	runs := make([]*tracked, 0, len(d.runs))
	for _, id := range d.Supervising() {
		runs = append(runs, d.runs[id])
	}
	return runs
}

// clip shortens value to at most limit bytes without splitting a character.
func clip(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return strings.ToValidUTF8(value[:limit], "")
}
