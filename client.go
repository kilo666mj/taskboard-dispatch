package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ToolError is an error reported by a Taskboard tool, such as a version
// conflict or a forbidden operation, as opposed to a transport failure.
type ToolError struct {
	Tool    string
	Message string
}

func (e *ToolError) Error() string {
	return fmt.Sprintf("taskboard %s: %s", e.Tool, e.Message)
}

// IsConflict reports whether err is a Taskboard optimistic-version conflict.
func IsConflict(err error) bool {
	var tool *ToolError
	return errors.As(err, &tool) && strings.Contains(strings.ToLower(tool.Message), "conflict")
}

// Client calls Taskboard's MCP tools with one agent credential. Taskboard
// accepts agent credentials only on its /mcp endpoint.
type Client struct {
	endpoint   string
	httpClient *http.Client
	client     *mcp.Client

	mu      sync.Mutex
	session *mcp.ClientSession
}

// ClientOptions configures a Client.
type ClientOptions struct {
	// Token is the dispatcher principal's bearer credential.
	Token string
	// HTTPClient is used for requests. Its transport receives the bearer
	// header. Defaults to http.DefaultClient's transport.
	HTTPClient *http.Client
	// Name and Version identify the dispatcher in run metadata.
	Name    string
	Version string
}

// NewClient returns a client for the Taskboard MCP endpoint, for example
// https://taskboard.example.com/mcp.
func NewClient(endpoint string, options ClientOptions) (*Client, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, errors.New("taskboard endpoint is required")
	}
	if strings.TrimSpace(options.Token) == "" {
		return nil, errors.New("taskboard token is required")
	}
	base := http.DefaultTransport
	timeout := http.DefaultClient.Timeout
	if options.HTTPClient != nil {
		if options.HTTPClient.Transport != nil {
			base = options.HTTPClient.Transport
		}
		timeout = options.HTTPClient.Timeout
	}
	name := options.Name
	if name == "" {
		name = "taskboard-dispatch"
	}
	return &Client{
		endpoint:   endpoint,
		httpClient: &http.Client{Timeout: timeout, Transport: bearerTransport{token: strings.TrimSpace(options.Token), base: base}},
		client:     mcp.NewClient(&mcp.Implementation{Name: name, Version: options.Version}, nil),
	}, nil
}

type bearerTransport struct {
	token string
	base  http.RoundTripper
}

func (t bearerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	request.Header.Set("Authorization", "Bearer "+t.token)
	return t.base.RoundTrip(request)
}

// Close ends the MCP session.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session == nil {
		return nil
	}
	err := c.session.Close()
	c.session = nil
	return err
}

func (c *Client) connect(ctx context.Context) (*mcp.ClientSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session != nil {
		return c.session, nil
	}
	session, err := c.client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: c.endpoint, HTTPClient: c.httpClient, DisableStandaloneSSE: true}, nil)
	if err != nil {
		return nil, fmt.Errorf("connect to taskboard: %w", err)
	}
	c.session = session
	return session, nil
}

// reset drops a session after a transport failure so the next call reconnects.
func (c *Client) reset(session *mcp.ClientSession) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session == session {
		_ = c.session.Close()
		c.session = nil
	}
}

func (c *Client) call(ctx context.Context, tool string, arguments any, output any) error {
	session, err := c.connect(ctx)
	if err != nil {
		return err
	}
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: arguments})
	if err != nil {
		c.reset(session)
		return fmt.Errorf("taskboard %s: %w", tool, err)
	}
	if result.IsError {
		return &ToolError{Tool: tool, Message: resultText(result)}
	}
	if output == nil {
		return nil
	}
	encoded, err := json.Marshal(result.StructuredContent)
	if err != nil {
		return fmt.Errorf("taskboard %s: encode result: %w", tool, err)
	}
	if err := json.Unmarshal(encoded, output); err != nil {
		return fmt.Errorf("taskboard %s: decode result: %w", tool, err)
	}
	return nil
}

func resultText(result *mcp.CallToolResult) string {
	var parts []string
	for _, content := range result.Content {
		if text, ok := content.(*mcp.TextContent); ok {
			parts = append(parts, text.Text)
		}
	}
	if len(parts) == 0 {
		return "tool error"
	}
	return strings.Join(parts, "; ")
}

// Advertise publishes the dispatcher's operational capabilities and capacity.
func (c *Client) Advertise(ctx context.Context, advertisement Advertisement) error {
	return c.call(ctx, "worker_advertise", advertisement, nil)
}

// ListPickup returns every queued agent-lane task offered to this dispatcher,
// following Taskboard's cursor until the listing is exhausted. Stale tasks are
// deliberately excluded: they wait for a person to review and requeue them,
// so a failing worker is never retried automatically.
func (c *Client) ListPickup(ctx context.Context) ([]Task, error) {
	var tasks []Task
	cursor := ""
	for {
		var page struct {
			Tasks      []Task `json:"tasks"`
			NextCursor string `json:"next_cursor"`
		}
		arguments := map[string]any{"statuses": []string{"queued"}, "visibility": "agent", "limit": 200}
		if cursor != "" {
			arguments["cursor"] = cursor
		}
		if err := c.call(ctx, "task_list", arguments, &page); err != nil {
			return nil, err
		}
		tasks = append(tasks, page.Tasks...)
		if page.NextCursor == "" {
			return tasks, nil
		}
		cursor = page.NextCursor
	}
}

// Claim takes a queued or stale task at its current version and starts a run.
func (c *Client) Claim(ctx context.Context, task Task, sessionKey, agent string) (Claim, error) {
	var claim Claim
	err := c.call(ctx, "task_claim", map[string]any{
		"task_id":           task.ID,
		"expected_version":  task.Version,
		"agent_session_key": sessionKey,
		"agent":             agent,
		"idempotency_key":   fmt.Sprintf("claim-%s-v%d", task.ID, task.Version),
	}, &claim)
	return claim, err
}

// Heartbeat renews a run lease and returns controls addressed to that run.
func (c *Client) Heartbeat(ctx context.Context, taskID, runID string) (Heartbeat, error) {
	var heartbeat Heartbeat
	err := c.call(ctx, "task_heartbeat", map[string]any{"task_id": taskID, "run_id": runID}, &heartbeat)
	return heartbeat, err
}

// GetTask returns a task with its runs.
func (c *Client) GetTask(ctx context.Context, taskID string) (Task, error) {
	var output struct {
		Task Task `json:"task"`
	}
	err := c.call(ctx, "task_get", map[string]any{"task_id": taskID}, &output)
	return output.Task, err
}

// ListControls returns open controls addressed to this principal's runs,
// including resume and retry requests for runs that have ended.
func (c *Client) ListControls(ctx context.Context) ([]Control, error) {
	var output struct {
		Controls []Control `json:"controls"`
	}
	err := c.call(ctx, "task_control_list", map[string]any{"limit": 200}, &output)
	return output.Controls, err
}

// UpdateControl moves a control to status. Taskboard's schema requires
// expected_version on every transition but checks it only on completion,
// which needs the task's current version.
func (c *Client) UpdateControl(ctx context.Context, control Control, status, note string, taskVersion int64) (Control, error) {
	var output struct {
		Control Control `json:"control"`
	}
	arguments := map[string]any{
		"control_id":       control.ID,
		"status":           status,
		"expected_version": taskVersion,
		"idempotency_key":  fmt.Sprintf("control-%s-%s", control.ID, status),
	}
	if note != "" {
		arguments["outcome_note"] = note
	}
	err := c.call(ctx, "task_control_update", arguments, &output)
	return output.Control, err
}

// AddHandoff records a handoff for a run. key makes retries idempotent.
func (c *Client) AddHandoff(ctx context.Context, taskID string, handoff Handoff, key string) error {
	arguments := map[string]any{"task_id": taskID, "run_id": handoff.RunID, "kind": handoff.Kind, "idempotency_key": key}
	for name, value := range map[string]string{
		"last_completed_step": handoff.LastCompletedStep,
		"worktree":            handoff.Worktree,
		"branch":              handoff.Branch,
		"blocker":             handoff.Blocker,
		"next_action":         handoff.NextAction,
	} {
		if value != "" {
			arguments[name] = value
		}
	}
	for name, values := range map[string][]string{
		"commits":         handoff.Commits,
		"pull_requests":   handoff.PullRequests,
		"validation":      handoff.Validation,
		"review_findings": handoff.ReviewFindings,
	} {
		if len(values) > 0 {
			arguments[name] = values
		}
	}
	return c.call(ctx, "task_handoff_add", arguments, nil)
}
