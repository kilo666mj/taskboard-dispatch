//go:build unix

// Package localexec is a dispatch.Backend that runs one local command per
// claimed Taskboard run.
//
// Each run gets a private working directory under Config.WorkRoot containing
// task.json (the task, its run, and earlier handoffs) and output.log (the
// command's stdout and stderr). The command receives TASKBOARD_TASK_ID,
// TASKBOARD_RUN_ID, TASKBOARD_TASK_FILE, and TASKBOARD_MCP_URL, plus
// TASKBOARD_TOKEN when Config.PassToken is set. Output never leaves the
// host; only exit codes reach Taskboard.
//
// Workers run in their own process group and are stopped with SIGTERM, then
// SIGKILL after Config.StopGrace. They do not survive the dispatcher, so
// Active always reports none.
package localexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sync"
	"syscall"
	"time"

	dispatch "go.michaelspost.com/taskboard-dispatch"
)

// Config configures a Backend.
type Config struct {
	// Command is the argv to run for each task. Required.
	Command []string
	// WorkRoot holds one directory per run. Required.
	WorkRoot string
	// Env adds KEY=value entries to the inherited environment.
	Env []string
	// MCPURL is passed to workers as TASKBOARD_MCP_URL.
	MCPURL string
	// Token is the dispatcher's Taskboard credential. It is passed to
	// workers as TASKBOARD_TOKEN only when PassToken is set, so workers can
	// report progress on the run the dispatcher claimed. Taskboard requires
	// run updates to come from the claiming principal.
	Token     string
	PassToken bool
	// StopGrace is how long a stopped worker has after SIGTERM before
	// SIGKILL. Default 30s.
	StopGrace time.Duration
}

// Backend runs local worker processes.
type Backend struct {
	cfg Config

	mu    sync.Mutex
	procs map[string]*process
}

type process struct {
	cmd      *exec.Cmd
	done     chan struct{}
	exitCode int
	detail   string
}

var runIDPattern = regexp.MustCompile(`^[0-9A-Za-z]{10,64}$`)

// New validates cfg and returns a Backend.
func New(cfg Config) (*Backend, error) {
	if len(cfg.Command) == 0 || cfg.Command[0] == "" {
		return nil, errors.New("command is required")
	}
	if cfg.WorkRoot == "" {
		return nil, errors.New("work root is required")
	}
	if cfg.PassToken && cfg.Token == "" {
		return nil, errors.New("pass token requires a token")
	}
	if cfg.StopGrace <= 0 {
		cfg.StopGrace = 30 * time.Second
	}
	if err := os.MkdirAll(cfg.WorkRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create work root: %w", err)
	}
	return &Backend{cfg: cfg, procs: map[string]*process{}}, nil
}

// Launch starts the command for work.Run. Launching a run that is already
// running is a no-op.
func (b *Backend) Launch(ctx context.Context, work dispatch.Work) error {
	runID := work.Run.ID
	if !runIDPattern.MatchString(runID) {
		return fmt.Errorf("invalid run id %q", runID)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.procs[runID]; ok {
		return nil
	}
	dir := filepath.Join(b.cfg.WorkRoot, runID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create run directory: %w", err)
	}
	taskFile := filepath.Join(dir, "task.json")
	if err := writeTaskFile(taskFile, work); err != nil {
		return err
	}
	output, err := os.OpenFile(filepath.Join(dir, "output.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open output log: %w", err)
	}
	cmd := exec.Command(b.cfg.Command[0], b.cfg.Command[1:]...)
	cmd.Dir = dir
	cmd.Stdout, cmd.Stderr = output, output
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Env = append(os.Environ(), b.cfg.Env...)
	cmd.Env = append(cmd.Env,
		"TASKBOARD_TASK_ID="+work.Task.ID,
		"TASKBOARD_RUN_ID="+runID,
		"TASKBOARD_TASK_FILE="+taskFile,
		"TASKBOARD_MCP_URL="+b.cfg.MCPURL,
	)
	if b.cfg.PassToken {
		cmd.Env = append(cmd.Env, "TASKBOARD_TOKEN="+b.cfg.Token)
	}
	if err := cmd.Start(); err != nil {
		_ = output.Close()
		return fmt.Errorf("start command: %w", err)
	}
	proc := &process{cmd: cmd, done: make(chan struct{})}
	b.procs[runID] = proc
	go func() {
		err := cmd.Wait()
		_ = output.Close()
		b.mu.Lock()
		proc.exitCode, proc.detail = exitStatus(cmd, err)
		b.mu.Unlock()
		close(proc.done)
	}()
	return nil
}

func writeTaskFile(path string, work dispatch.Work) error {
	encoded, err := json.MarshalIndent(struct {
		Task     json.RawMessage    `json:"task"`
		Run      dispatch.AgentRun  `json:"run"`
		Handoffs []dispatch.Handoff `json:"handoffs"`
	}{taskJSON(work.Task), work.Run, work.Handoffs}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode task file: %w", err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		return fmt.Errorf("write task file: %w", err)
	}
	return nil
}

func taskJSON(task dispatch.Task) json.RawMessage {
	if len(task.Raw) > 0 {
		return task.Raw
	}
	encoded, err := json.Marshal(task)
	if err != nil {
		return json.RawMessage("null")
	}
	return encoded
}

func exitStatus(cmd *exec.Cmd, err error) (int, string) {
	if err == nil {
		return 0, ""
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			return -1, "Terminated by signal " + status.Signal().String() + "."
		}
		return exitErr.ExitCode(), ""
	}
	return -1, "Wait failed."
}

// Status reports the worker for runID.
func (b *Backend) Status(_ context.Context, runID string) (dispatch.WorkerStatus, error) {
	b.mu.Lock()
	proc, ok := b.procs[runID]
	b.mu.Unlock()
	if !ok {
		return dispatch.WorkerStatus{State: dispatch.WorkerExited, ExitCode: -1, Detail: "Worker process not found."}, nil
	}
	select {
	case <-proc.done:
		b.mu.Lock()
		defer b.mu.Unlock()
		return dispatch.WorkerStatus{State: dispatch.WorkerExited, ExitCode: proc.exitCode, Detail: proc.detail}, nil
	default:
		return dispatch.WorkerStatus{State: dispatch.WorkerRunning}, nil
	}
}

// Stop sends SIGTERM to the worker's process group, then SIGKILL after
// StopGrace, and waits for it to exit.
func (b *Backend) Stop(ctx context.Context, runID string, _ dispatch.StopReason) error {
	b.mu.Lock()
	proc, ok := b.procs[runID]
	b.mu.Unlock()
	if !ok {
		return nil
	}
	select {
	case <-proc.done:
		return nil
	default:
	}
	group := -proc.cmd.Process.Pid
	if err := syscall.Kill(group, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("signal worker: %w", err)
	}
	grace := time.NewTimer(b.cfg.StopGrace)
	defer grace.Stop()
	select {
	case <-proc.done:
		return nil
	case <-grace.C:
	case <-ctx.Done():
	}
	if err := syscall.Kill(group, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("kill worker: %w", err)
	}
	select {
	case <-proc.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("worker did not exit: %w", ctx.Err())
	}
}

// Forget stops tracking an exited run. Its directory is kept for inspection.
func (b *Backend) Forget(_ context.Context, runID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.procs, runID)
	return nil
}

// Active reports no surviving workers: local workers end with the dispatcher.
func (b *Backend) Active(context.Context) ([]dispatch.RunRef, error) {
	return nil, nil
}
