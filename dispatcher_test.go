package dispatch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"
)

// fakeBoard is an in-memory Taskboard with just enough behavior for the
// dispatcher: claims bump versions and create runs, controls move through
// their lifecycle, and completing cancel or pause ends the run.
type fakeBoard struct {
	mu            sync.Mutex
	tasks         map[string]*Task
	order         []string
	controls      map[string]*Control
	handoffs      []Handoff
	advertised    []Advertisement
	heartbeats    map[string]int
	heartbeatErr  error
	claimConflict map[string]bool
	nextRun       int
}

func newFakeBoard() *fakeBoard {
	return &fakeBoard{tasks: map[string]*Task{}, controls: map[string]*Control{}, heartbeats: map[string]int{}, claimConflict: map[string]bool{}}
}

func (b *fakeBoard) addTask(id string, editors ...string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	creator, editor := "human:owner", "human:owner"
	if len(editors) == 2 {
		creator, editor = editors[0], editors[1]
	}
	b.tasks[id] = &Task{ID: id, Title: "Task " + id, Status: "queued", Visibility: "agent", Version: 1, CreatedBy: creator, LastEditedBy: editor}
	b.order = append(b.order, id)
}

func (b *fakeBoard) Advertise(_ context.Context, advertisement Advertisement) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advertised = append(b.advertised, advertisement)
	return nil
}

func (b *fakeBoard) ListPickup(context.Context) ([]Task, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var tasks []Task
	for _, id := range b.order {
		if task := b.tasks[id]; task.Status == "queued" {
			tasks = append(tasks, *task)
		}
	}
	return tasks, nil
}

func (b *fakeBoard) Claim(_ context.Context, task Task, sessionKey, _ string) (Claim, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if sessionKey == "" {
		return Claim{}, errors.New("missing session key")
	}
	current := b.tasks[task.ID]
	if b.claimConflict[task.ID] || current.Version != task.Version {
		return Claim{}, &ToolError{Tool: "task_claim", Message: "version conflict"}
	}
	b.nextRun++
	run := AgentRun{ID: fmt.Sprintf("run%010d", b.nextRun), TaskID: task.ID, Status: "active", Callsign: "Test"}
	current.Status, current.Version = "active", current.Version+1
	current.Runs = append(current.Runs, run)
	return Claim{Task: *current, Run: run}, nil
}

func (b *fakeBoard) Heartbeat(_ context.Context, taskID, runID string) (Heartbeat, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.heartbeatErr != nil {
		return Heartbeat{}, b.heartbeatErr
	}
	run, ok := b.tasks[taskID].RunByID(runID)
	if !ok || run.EndedAt != nil {
		return Heartbeat{}, &ToolError{Tool: "task_heartbeat", Message: "not found"}
	}
	b.heartbeats[runID]++
	return Heartbeat{Run: run}, nil
}

func (b *fakeBoard) GetTask(_ context.Context, taskID string) (Task, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return *b.tasks[taskID], nil
}

func (b *fakeBoard) ListControls(context.Context) ([]Control, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var controls []Control
	for _, control := range b.controls {
		if control.Status == ControlRequested || control.Status == ControlAcknowledged || control.Status == ControlAccepted {
			controls = append(controls, *control)
		}
	}
	return controls, nil
}

func (b *fakeBoard) UpdateControl(_ context.Context, control Control, status, _ string, taskVersion int64) (Control, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	current := b.controls[control.ID]
	if status == ControlCompleted {
		task := b.tasks[current.TaskID]
		if taskVersion != task.Version {
			return Control{}, &ToolError{Tool: "task_control_update", Message: "version conflict"}
		}
		switch current.Kind {
		case ControlCancel, ControlPause:
			b.endRun(task, current.TargetRunID, map[string]string{ControlCancel: "cancelled", ControlPause: "waiting"}[current.Kind])
		case ControlResume, ControlRetry:
			task.Status = "queued"
		}
		task.Version++
	}
	current.Status = status
	return *current, nil
}

func (b *fakeBoard) AddHandoff(_ context.Context, _ string, handoff Handoff, _ string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handoffs = append(b.handoffs, handoff)
	return nil
}

// endRun must be called with b.mu held.
func (b *fakeBoard) endRun(task *Task, runID, status string) {
	now := time.Now()
	for index := range task.Runs {
		if task.Runs[index].ID == runID {
			task.Runs[index].EndedAt = &now
			task.Runs[index].Status = status
		}
	}
	task.Status = status
}

func (b *fakeBoard) finishTask(taskID, runID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	task := b.tasks[taskID]
	b.endRun(task, runID, "done")
	task.Version++
}

func (b *fakeBoard) addControl(id, kind, taskID, runID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.controls[id] = &Control{ID: id, Kind: kind, Status: ControlRequested, TaskID: taskID, TargetRunID: runID}
}

func (b *fakeBoard) handoffKinds() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var kinds []string
	for _, handoff := range b.handoffs {
		kinds = append(kinds, handoff.Kind+":"+handoff.RunID)
	}
	return kinds
}

type fakeBackend struct {
	workers   map[string]WorkerStatus
	launched  []string
	stopped   []string
	launchErr error
	stopErr   error
	active    []RunRef
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{workers: map[string]WorkerStatus{}}
}

func (b *fakeBackend) Launch(_ context.Context, work Work) error {
	if b.launchErr != nil {
		return b.launchErr
	}
	b.launched = append(b.launched, work.Run.ID)
	b.workers[work.Run.ID] = WorkerStatus{State: WorkerRunning}
	return nil
}

func (b *fakeBackend) Status(_ context.Context, runID string) (WorkerStatus, error) {
	status, ok := b.workers[runID]
	if !ok {
		return WorkerStatus{State: WorkerExited, ExitCode: -1, Detail: "Worker process not found."}, nil
	}
	return status, nil
}

func (b *fakeBackend) Stop(_ context.Context, runID string, _ StopReason) error {
	if b.stopErr != nil {
		return b.stopErr
	}
	b.stopped = append(b.stopped, runID)
	b.workers[runID] = WorkerStatus{State: WorkerExited, ExitCode: -1}
	return nil
}

func (b *fakeBackend) Forget(_ context.Context, runID string) error {
	delete(b.workers, runID)
	return nil
}

func (b *fakeBackend) Active(context.Context) ([]RunRef, error) {
	return b.active, nil
}

func testDispatcher(t *testing.T, board *fakeBoard, backend *fakeBackend, capacity int) *Dispatcher {
	t.Helper()
	dispatcher, err := New(board, backend, Config{
		Runner:     "runner:test",
		Capacity:   capacity,
		SessionKey: "test-session",
		Admit:      AdmitEditors("human:owner"),
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return dispatcher
}

func TestNewValidatesConfig(t *testing.T) {
	board, backend := newFakeBoard(), newFakeBackend()
	for name, cfg := range map[string]Config{
		"runner token": {Runner: "local", Capacity: 1, SessionKey: "key"},
		"capacity":     {Runner: "runner:local", Capacity: 0, SessionKey: "key"},
		"session key":  {Runner: "runner:local", Capacity: 1},
		"ttl":          {Runner: "runner:local", Capacity: 1, SessionKey: "key", AdvertiseTTL: time.Second},
	} {
		if _, err := New(board, backend, cfg); err == nil {
			t.Errorf("%s: invalid config accepted", name)
		}
	}
}

func TestPassAdvertisesClaimsAdmittedWorkUpToCapacity(t *testing.T) {
	board, backend := newFakeBoard(), newFakeBackend()
	board.addTask("task-a")
	board.addTask("task-unknown", "human:stranger", "human:owner")
	board.addTask("task-b")
	board.addTask("task-c")
	dispatcher := testDispatcher(t, board, backend, 2)

	dispatcher.Pass(t.Context())

	if len(board.advertised) != 1 || !slices.Equal(board.advertised[0].Capabilities, []string{"runner:test"}) || board.advertised[0].Capacity != 2 {
		t.Fatalf("advertisement = %+v", board.advertised)
	}
	if !slices.Equal(backend.launched, []string{"run0000000001", "run0000000002"}) {
		t.Fatalf("launched = %v", backend.launched)
	}
	if board.tasks["task-unknown"].Status != "queued" || board.tasks["task-c"].Status != "queued" {
		t.Fatal("dispatcher claimed an unadmitted task or exceeded capacity")
	}

	dispatcher.Pass(t.Context())
	if board.heartbeats["run0000000001"] != 1 || board.heartbeats["run0000000002"] != 1 {
		t.Fatalf("heartbeats = %v", board.heartbeats)
	}
	if len(board.advertised) != 1 {
		t.Fatal("advertisement renewed before half its TTL")
	}
}

func TestClaimConflictIsSkipped(t *testing.T) {
	board, backend := newFakeBoard(), newFakeBackend()
	board.addTask("task-a")
	board.addTask("task-b")
	board.claimConflict["task-a"] = true
	testDispatcher(t, board, backend, 2).Pass(t.Context())
	if !slices.Equal(backend.launched, []string{"run0000000001"}) || board.tasks["task-b"].Status != "active" {
		t.Fatalf("launched = %v", backend.launched)
	}
}

func TestExitedWorkerWithOpenRunRecordsHandoff(t *testing.T) {
	board, backend := newFakeBoard(), newFakeBackend()
	board.addTask("task-a")
	board.addTask("task-b")
	dispatcher := testDispatcher(t, board, backend, 2)
	dispatcher.Pass(t.Context())

	backend.workers["run0000000001"] = WorkerStatus{State: WorkerExited, ExitCode: 3, Detail: "Detail."}
	board.finishTask("task-b", "run0000000002")
	backend.workers["run0000000002"] = WorkerStatus{State: WorkerExited}
	dispatcher.Pass(t.Context())

	if kinds := board.handoffKinds(); !slices.Equal(kinds, []string{"final:run0000000001"}) {
		t.Fatalf("handoffs = %v", kinds)
	}
	if blocker := board.handoffs[0].Blocker; blocker != "Worker exited with code 3 before the run ended. Detail." {
		t.Fatalf("blocker = %q", blocker)
	}
	if len(dispatcher.Supervising()) != 0 {
		t.Fatalf("still supervising %v", dispatcher.Supervising())
	}
}

func TestLaunchFailureRecordsHandoffAndDoesNotSupervise(t *testing.T) {
	board, backend := newFakeBoard(), newFakeBackend()
	board.addTask("task-a")
	backend.launchErr = errors.New("no such command")
	dispatcher := testDispatcher(t, board, backend, 1)
	dispatcher.Pass(t.Context())
	if kinds := board.handoffKinds(); !slices.Equal(kinds, []string{"final:run0000000001"}) {
		t.Fatalf("handoffs = %v", kinds)
	}
	if len(dispatcher.Supervising()) != 0 {
		t.Fatal("failed launch is supervised")
	}
}

func TestCancelControlStopsWorkerAndCompletes(t *testing.T) {
	board, backend := newFakeBoard(), newFakeBackend()
	board.addTask("task-a")
	dispatcher := testDispatcher(t, board, backend, 1)
	dispatcher.Pass(t.Context())

	board.addControl("control-1", ControlCancel, "task-a", "run0000000001")
	dispatcher.Pass(t.Context())

	if board.controls["control-1"].Status != ControlCompleted || board.tasks["task-a"].Status != "cancelled" {
		t.Fatalf("control = %+v task = %+v", board.controls["control-1"], board.tasks["task-a"])
	}
	if !slices.Equal(backend.stopped, []string{"run0000000001"}) || !slices.Equal(board.handoffKinds(), []string{"checkpoint:run0000000001"}) {
		t.Fatalf("stopped = %v handoffs = %v", backend.stopped, board.handoffKinds())
	}
	if len(dispatcher.Supervising()) != 0 {
		t.Fatal("cancelled worker still supervised")
	}
}

func TestStopFailureRejectsControl(t *testing.T) {
	board, backend := newFakeBoard(), newFakeBackend()
	board.addTask("task-a")
	dispatcher := testDispatcher(t, board, backend, 1)
	dispatcher.Pass(t.Context())
	backend.stopErr = errors.New("still running")
	board.addControl("control-1", ControlPause, "task-a", "run0000000001")
	dispatcher.Pass(t.Context())
	if board.controls["control-1"].Status != ControlRejected || len(dispatcher.Supervising()) != 1 {
		t.Fatalf("control = %+v supervising = %v", board.controls["control-1"], dispatcher.Supervising())
	}
}

func TestResumeControlQueuesTaskForFreshClaim(t *testing.T) {
	board, backend := newFakeBoard(), newFakeBackend()
	board.addTask("task-a")
	board.tasks["task-a"].Status = "waiting"
	board.addControl("control-1", ControlResume, "task-a", "run-ended")
	dispatcher := testDispatcher(t, board, backend, 1)
	dispatcher.Pass(t.Context())
	if board.controls["control-1"].Status != ControlCompleted {
		t.Fatalf("control = %+v", board.controls["control-1"])
	}
	if !slices.Equal(backend.launched, []string{"run0000000001"}) {
		t.Fatalf("resumed task was not claimed afresh: %v", backend.launched)
	}
}

func TestHeartbeatRefusalStopsWorkerOnlyWhenRunEnded(t *testing.T) {
	board, backend := newFakeBoard(), newFakeBackend()
	board.addTask("task-a")
	dispatcher := testDispatcher(t, board, backend, 1)
	dispatcher.Pass(t.Context())

	board.heartbeatErr = errors.New("connection reset")
	dispatcher.Pass(t.Context())
	board.heartbeatErr = &ToolError{Tool: "task_heartbeat", Message: "database is locked"}
	dispatcher.Pass(t.Context())
	if len(backend.stopped) != 0 {
		t.Fatalf("transient heartbeat failure stopped the worker: %v", backend.stopped)
	}

	board.heartbeatErr = nil
	board.mu.Lock()
	board.endRun(board.tasks["task-a"], "run0000000001", "stale")
	board.mu.Unlock()
	dispatcher.Pass(t.Context())
	if !slices.Equal(backend.stopped, []string{"run0000000001"}) || len(dispatcher.Supervising()) != 0 {
		t.Fatalf("stopped = %v supervising = %v", backend.stopped, dispatcher.Supervising())
	}
}

func TestRunResumesSurvivingWorkersAndStopsThemOnShutdown(t *testing.T) {
	board, backend := newFakeBoard(), newFakeBackend()
	board.addTask("task-a")
	claim, err := board.Claim(t.Context(), *board.tasks["task-a"], "key", "")
	if err != nil {
		t.Fatal(err)
	}
	backend.workers[claim.Run.ID] = WorkerStatus{State: WorkerRunning}
	backend.active = []RunRef{{TaskID: "task-a", RunID: claim.Run.ID}}
	dispatcher := testDispatcher(t, board, backend, 1)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := dispatcher.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v", err)
	}
	if board.heartbeats[claim.Run.ID] != 1 {
		t.Fatal("surviving worker was not supervised")
	}
	if !slices.Equal(backend.stopped, []string{claim.Run.ID}) || !slices.Equal(board.handoffKinds(), []string{"checkpoint:" + claim.Run.ID}) {
		t.Fatalf("stopped = %v handoffs = %v", backend.stopped, board.handoffKinds())
	}
}

func TestAdmitEditorsRequiresBothProvenanceFields(t *testing.T) {
	admit := AdmitEditors("human:owner", "human:reviewer")
	for _, test := range []struct {
		creator, editor string
		want            bool
	}{
		{"human:owner", "human:reviewer", true},
		{"human:owner", "", false},
		{"", "human:owner", false},
		{"human:owner", "human:stranger", false},
	} {
		if got := admit(Task{CreatedBy: test.creator, LastEditedBy: test.editor}); got != test.want {
			t.Errorf("admit(%q, %q) = %v", test.creator, test.editor, got)
		}
	}
}

func TestClipKeepsValidUTF8(t *testing.T) {
	if got := clip("ab€", 3); got != "ab" {
		t.Fatalf("clip = %q", got)
	}
}
