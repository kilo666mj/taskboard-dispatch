package dispatch

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type listArgs struct {
	Statuses   []string `json:"statuses"`
	Visibility string   `json:"visibility"`
	Limit      int      `json:"limit"`
	Cursor     string   `json:"cursor,omitempty"`
}

type claimArgs struct {
	TaskID          string `json:"task_id"`
	ExpectedVersion int64  `json:"expected_version"`
	AgentSessionKey string `json:"agent_session_key"`
	Agent           string `json:"agent"`
	IdempotencyKey  string `json:"idempotency_key"`
}

type controlArgs struct {
	ControlID   string `json:"control_id"`
	Status      string `json:"status"`
	OutcomeNote string `json:"outcome_note,omitempty"`
	// Required like Taskboard's schema, even before completion.
	ExpectedVersion int64  `json:"expected_version"`
	IdempotencyKey  string `json:"idempotency_key"`
}

type empty struct{}

// fakeMCPTaskboard serves a few Taskboard tools over Streamable HTTP so the
// client is exercised through the real SDK transport.
func fakeMCPTaskboard(t *testing.T, token string) (*httptest.Server, *recorded) {
	t.Helper()
	seen := &recorded{}
	server := mcp.NewServer(&mcp.Implementation{Name: "fake-taskboard"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "task_list"}, func(_ context.Context, _ *mcp.CallToolRequest, input listArgs) (*mcp.CallToolResult, map[string]any, error) {
		seen.add(input)
		if input.Cursor == "" {
			return nil, map[string]any{"tasks": []map[string]any{{"id": "task-1", "version": 2, "created_by": "human:owner"}}, "next_cursor": "page-2"}, nil
		}
		return nil, map[string]any{"tasks": []map[string]any{{"id": "task-2", "version": 5, "requirements": []string{"runner:local"}}}}, nil
	})
	mcp.AddTool(server, &mcp.Tool{Name: "task_claim"}, func(_ context.Context, _ *mcp.CallToolRequest, input claimArgs) (*mcp.CallToolResult, map[string]any, error) {
		seen.add(input)
		if input.TaskID == "task-conflict" {
			return nil, nil, errors.New("task version conflict")
		}
		return nil, map[string]any{"task": map[string]any{"id": input.TaskID, "version": input.ExpectedVersion + 1}, "run": map[string]any{"id": "run0000000001", "task_id": input.TaskID, "status": "active"}}, nil
	})
	mcp.AddTool(server, &mcp.Tool{Name: "task_control_update"}, func(_ context.Context, _ *mcp.CallToolRequest, input controlArgs) (*mcp.CallToolResult, map[string]any, error) {
		seen.add(input)
		return nil, map[string]any{"control": map[string]any{"id": input.ControlID, "status": input.Status}}, nil
	})
	mcp.AddTool(server, &mcp.Tool{Name: "task_handoff_add"}, func(_ context.Context, _ *mcp.CallToolRequest, input map[string]any) (*mcp.CallToolResult, empty, error) {
		seen.add(input)
		return nil, empty{}, nil
	})
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{Stateless: true})
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(httpServer.Close)
	return httpServer, seen
}

type recorded struct {
	mu    sync.Mutex
	calls []any
}

func (r *recorded) add(call any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
}

func (r *recorded) all() []any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

func testClient(t *testing.T, endpoint, token string) *Client {
	t.Helper()
	client, err := NewClient(endpoint, ClientOptions{Token: token, Name: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestClientListsQueuedPickupAcrossPages(t *testing.T) {
	server, seen := fakeMCPTaskboard(t, "secret-token")
	tasks, err := testClient(t, server.URL, "secret-token").ListPickup(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 || tasks[0].ID != "task-1" || tasks[0].CreatedBy != "human:owner" || tasks[1].Version != 5 || !slices.Equal(tasks[1].Requirements, []string{"runner:local"}) {
		t.Fatalf("tasks = %+v", tasks)
	}
	if len(tasks[1].Raw) == 0 {
		t.Fatal("raw task JSON was not kept")
	}
	calls := seen.all()
	first, second := calls[0].(listArgs), calls[1].(listArgs)
	if !slices.Equal(first.Statuses, []string{"queued"}) || first.Visibility != "agent" || first.Cursor != "" || second.Cursor != "page-2" {
		t.Fatalf("list calls = %+v", calls)
	}
}

func TestClientClaimSendsVersionSessionAndIdempotencyKey(t *testing.T) {
	server, seen := fakeMCPTaskboard(t, "secret-token")
	client := testClient(t, server.URL, "secret-token")
	claim, err := client.Claim(t.Context(), Task{ID: "task-1", Version: 2}, "session-key", "dispatcher")
	if err != nil {
		t.Fatal(err)
	}
	if claim.Run.ID != "run0000000001" || claim.Task.Version != 3 {
		t.Fatalf("claim = %+v", claim)
	}
	call := seen.all()[0].(claimArgs)
	if call.ExpectedVersion != 2 || call.AgentSessionKey != "session-key" || call.Agent != "dispatcher" || call.IdempotencyKey != "claim-task-1-v2" {
		t.Fatalf("claim call = %+v", call)
	}

	_, err = client.Claim(t.Context(), Task{ID: "task-conflict", Version: 1}, "session-key", "")
	var tool *ToolError
	if !errors.As(err, &tool) || !IsConflict(err) {
		t.Fatalf("conflict error = %#v", err)
	}
}

func TestClientControlCompletionCarriesTaskVersion(t *testing.T) {
	server, seen := fakeMCPTaskboard(t, "secret-token")
	client := testClient(t, server.URL, "secret-token")
	if _, err := client.UpdateControl(t.Context(), Control{ID: "control-1"}, ControlAcknowledged, "", 4); err != nil {
		t.Fatal(err)
	}
	control, err := client.UpdateControl(t.Context(), Control{ID: "control-1"}, ControlCompleted, "Worker stopped.", 7)
	if err != nil || control.Status != ControlCompleted {
		t.Fatalf("control = %+v, %v", control, err)
	}
	calls := seen.all()
	acknowledged, completed := calls[0].(controlArgs), calls[1].(controlArgs)
	if acknowledged.ExpectedVersion != 4 || acknowledged.IdempotencyKey != "control-control-1-acknowledged" {
		t.Fatalf("acknowledge call = %+v", acknowledged)
	}
	if completed.ExpectedVersion != 7 || completed.OutcomeNote != "Worker stopped." {
		t.Fatalf("complete call = %+v", completed)
	}
}

func TestClientHandoffOmitsEmptyFields(t *testing.T) {
	server, seen := fakeMCPTaskboard(t, "secret-token")
	err := testClient(t, server.URL, "secret-token").AddHandoff(t.Context(), "task-1", Handoff{RunID: "run0000000001", Kind: HandoffFinal, Blocker: "Exited."}, "exit-run0000000001")
	if err != nil {
		t.Fatal(err)
	}
	call := seen.all()[0].(map[string]any)
	if call["blocker"] != "Exited." || call["kind"] != HandoffFinal || call["idempotency_key"] != "exit-run0000000001" {
		t.Fatalf("handoff call = %v", call)
	}
	for _, field := range []string{"branch", "commits", "next_action"} {
		if _, ok := call[field]; ok {
			t.Errorf("empty field %q was sent", field)
		}
	}
}

func TestClientRejectsWrongToken(t *testing.T) {
	server, _ := fakeMCPTaskboard(t, "secret-token")
	if _, err := testClient(t, server.URL, "wrong-token").ListPickup(t.Context()); err == nil {
		t.Fatal("wrong token was accepted")
	}
}

func TestNewClientRequiresEndpointAndToken(t *testing.T) {
	if _, err := NewClient("", ClientOptions{Token: "token"}); err == nil {
		t.Error("missing endpoint accepted")
	}
	if _, err := NewClient("https://taskboard.example/mcp", ClientOptions{}); err == nil {
		t.Error("missing token accepted")
	}
}
