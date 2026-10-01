//go:build unix

package localexec

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dispatch "go.michaelspost.com/taskboard-dispatch"
)

func work(runID string) dispatch.Work {
	return dispatch.Work{
		Task:     dispatch.Task{ID: "task-1", Title: "Example", Version: 2, Raw: json.RawMessage(`{"id":"task-1","title":"Example"}`)},
		Run:      dispatch.AgentRun{ID: runID, TaskID: "task-1"},
		Handoffs: []dispatch.Handoff{{RunID: "run0000000000", Kind: "stale", NextAction: "Continue."}},
	}
}

func waitExited(t *testing.T, backend *Backend, runID string) dispatch.WorkerStatus {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, err := backend.Status(t.Context(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if status.State == dispatch.WorkerExited {
			return status
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("worker %s did not exit", runID)
	return dispatch.WorkerStatus{}
}

func TestLaunchPassesTaskContextAndReportsExitCode(t *testing.T) {
	root := t.TempDir()
	script := `printf '%s|%s|%s|%s|%s' "$TASKBOARD_TASK_ID" "$TASKBOARD_RUN_ID" "$TASKBOARD_MCP_URL" "${TASKBOARD_TOKEN-unset}" "$EXTRA" > env.txt; cp "$TASKBOARD_TASK_FILE" copy.json; exit 3`
	backend, err := New(Config{Command: []string{"/bin/sh", "-c", script}, WorkRoot: root, MCPURL: "https://taskboard.example/mcp", Token: "secret", Env: []string{"EXTRA=yes"}})
	if err != nil {
		t.Fatal(err)
	}
	runID := "run0000000001"
	if err := backend.Launch(t.Context(), work(runID)); err != nil {
		t.Fatal(err)
	}
	if err := backend.Launch(t.Context(), work(runID)); err != nil {
		t.Fatalf("relaunch of the same run: %v", err)
	}
	status := waitExited(t, backend, runID)
	if status.ExitCode != 3 {
		t.Fatalf("status = %+v", status)
	}
	env, err := os.ReadFile(filepath.Join(root, runID, "env.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(env) != "task-1|run0000000001|https://taskboard.example/mcp|unset|yes" {
		t.Fatalf("worker environment = %q", env)
	}
	var file struct {
		Task     map[string]any     `json:"task"`
		Run      dispatch.AgentRun  `json:"run"`
		Handoffs []dispatch.Handoff `json:"handoffs"`
	}
	data, err := os.ReadFile(filepath.Join(root, runID, "copy.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatal(err)
	}
	if file.Task["title"] != "Example" || file.Run.ID != runID || len(file.Handoffs) != 1 || file.Handoffs[0].NextAction != "Continue." {
		t.Fatalf("task file = %+v", file)
	}
	info, err := os.Stat(filepath.Join(root, runID, "task.json"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("task file mode = %v, %v", info, err)
	}
}

func TestPassTokenExposesCredential(t *testing.T) {
	root := t.TempDir()
	backend, err := New(Config{Command: []string{"/bin/sh", "-c", `printf '%s' "$TASKBOARD_TOKEN" > token.txt`}, WorkRoot: root, Token: "secret", PassToken: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Launch(t.Context(), work("run0000000002")); err != nil {
		t.Fatal(err)
	}
	waitExited(t, backend, "run0000000002")
	if token, _ := os.ReadFile(filepath.Join(root, "run0000000002", "token.txt")); string(token) != "secret" {
		t.Fatalf("token = %q", token)
	}
}

func TestStopTerminatesProcessGroup(t *testing.T) {
	backend, err := New(Config{Command: []string{"/bin/sh", "-c", "sleep 30 & wait"}, WorkRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Launch(t.Context(), work("run0000000003")); err != nil {
		t.Fatal(err)
	}
	if status, _ := backend.Status(t.Context(), "run0000000003"); status.State != dispatch.WorkerRunning {
		t.Fatalf("status = %+v", status)
	}
	if err := backend.Stop(t.Context(), "run0000000003", dispatch.StopCancel); err != nil {
		t.Fatal(err)
	}
	status := waitExited(t, backend, "run0000000003")
	if !strings.Contains(status.Detail, "signal") {
		t.Fatalf("status = %+v", status)
	}
}

func TestStopKillsWorkerThatIgnoresTerm(t *testing.T) {
	backend, err := New(Config{Command: []string{"/bin/sh", "-c", `trap '' TERM; while :; do sleep 0.05; done`}, WorkRoot: t.TempDir(), StopGrace: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Launch(t.Context(), work("run0000000004")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started := time.Now()
	if err := backend.Stop(ctx, "run0000000004", dispatch.StopShutdown); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 200*time.Millisecond {
		t.Fatalf("worker was killed before the grace period: %v", elapsed)
	}
	if status := waitExited(t, backend, "run0000000004"); !strings.Contains(status.Detail, "killed") {
		t.Fatalf("status = %+v", status)
	}
}

func TestUnknownRunReportsExitedAndForgetReleases(t *testing.T) {
	backend, err := New(Config{Command: []string{"/bin/true"}, WorkRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if status, _ := backend.Status(t.Context(), "run0000000009"); status.State != dispatch.WorkerExited {
		t.Fatalf("unknown status = %+v", status)
	}
	if err := backend.Launch(t.Context(), work("run0000000005")); err != nil {
		t.Fatal(err)
	}
	waitExited(t, backend, "run0000000005")
	if err := backend.Forget(t.Context(), "run0000000005"); err != nil {
		t.Fatal(err)
	}
	if status, _ := backend.Status(t.Context(), "run0000000005"); status.Detail != "Worker process not found." {
		t.Fatalf("forgotten status = %+v", status)
	}
	if active, _ := backend.Active(t.Context()); len(active) != 0 {
		t.Fatalf("active = %v", active)
	}
}

func TestLaunchRejectsUnsafeRunID(t *testing.T) {
	backend, err := New(Config{Command: []string{"/bin/true"}, WorkRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Launch(t.Context(), work("../escape")); err == nil {
		t.Fatal("path-like run id accepted")
	}
}

func TestNewValidatesConfig(t *testing.T) {
	for name, cfg := range map[string]Config{
		"command":   {WorkRoot: t.TempDir()},
		"work root": {Command: []string{"/bin/true"}},
		"token":     {Command: []string{"/bin/true"}, WorkRoot: t.TempDir(), PassToken: true},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: invalid config accepted", name)
		}
	}
}
