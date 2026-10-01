//go:build unix

// Command taskboard-dispatch-local claims Taskboard work routed to a runner
// token and runs a local command for each task. Configuration comes from the
// environment; see the repository README.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	dispatch "go.michaelspost.com/taskboard-dispatch"
	"go.michaelspost.com/taskboard-dispatch/localexec"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(logger); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("dispatcher stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := load()
	if err != nil {
		return err
	}
	client, err := dispatch.NewClient(cfg.endpoint, dispatch.ClientOptions{Token: cfg.token, Name: "taskboard-dispatch-local", Version: version()})
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	backend, err := localexec.New(localexec.Config{
		Command:   cfg.command,
		WorkRoot:  cfg.workRoot,
		MCPURL:    cfg.endpoint,
		Token:     cfg.token,
		PassToken: cfg.passToken,
		StopGrace: cfg.stopGrace,
	})
	if err != nil {
		return err
	}
	admit := dispatch.AdmitEditors(cfg.allowedEditors...)
	if cfg.admitAll {
		admit = nil
	}
	dispatcher, err := dispatch.New(client, backend, dispatch.Config{
		Runner:       cfg.runner,
		Capabilities: cfg.capabilities,
		Capacity:     cfg.capacity,
		SessionKey:   cfg.sessionKey,
		Agent:        "taskboard-dispatch-local",
		Admit:        admit,
		PollInterval: cfg.poll,
		StopTimeout:  cfg.stopGrace + 10*time.Second,
		Logger:       logger,
	})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	logger.Info("dispatcher starting", "runner", cfg.runner, "capacity", cfg.capacity, "version", version())
	return dispatcher.Run(ctx)
}

type settings struct {
	endpoint       string
	token          string
	runner         string
	capabilities   []string
	capacity       int
	sessionKey     string
	allowedEditors []string
	admitAll       bool
	command        []string
	workRoot       string
	passToken      bool
	poll           time.Duration
	stopGrace      time.Duration
}

func load() (settings, error) {
	cfg := settings{
		endpoint:       strings.TrimSpace(os.Getenv("TASKBOARD_MCP_URL")),
		runner:         env("DISPATCH_RUNNER", "runner:local"),
		capabilities:   split(os.Getenv("DISPATCH_CAPABILITIES")),
		allowedEditors: split(os.Getenv("DISPATCH_ALLOWED_EDITORS")),
		workRoot:       strings.TrimSpace(os.Getenv("DISPATCH_WORK_ROOT")),
	}
	token, err := secret("TASKBOARD_TOKEN")
	if err != nil {
		return cfg, err
	}
	cfg.token = token
	if cfg.capacity, err = number("DISPATCH_CAPACITY", 1); err != nil {
		return cfg, err
	}
	seconds, err := number("DISPATCH_POLL_SECONDS", 15)
	if err != nil {
		return cfg, err
	}
	cfg.poll = time.Duration(seconds) * time.Second
	if seconds, err = number("DISPATCH_STOP_GRACE_SECONDS", 30); err != nil {
		return cfg, err
	}
	cfg.stopGrace = time.Duration(seconds) * time.Second
	if cfg.admitAll, err = boolean("DISPATCH_ADMIT_ALL"); err != nil {
		return cfg, err
	}
	if cfg.passToken, err = boolean("DISPATCH_PASS_TOKEN"); err != nil {
		return cfg, err
	}
	if raw := strings.TrimSpace(os.Getenv("DISPATCH_COMMAND")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &cfg.command); err != nil {
			return cfg, errors.New("DISPATCH_COMMAND must be a JSON array of strings")
		}
	}
	cfg.sessionKey = strings.TrimSpace(os.Getenv("DISPATCH_SESSION_KEY"))
	if cfg.sessionKey == "" {
		host, err := os.Hostname()
		if err != nil {
			return cfg, fmt.Errorf("DISPATCH_SESSION_KEY is unset and the hostname is unavailable: %w", err)
		}
		cfg.sessionKey = "taskboard-dispatch-local:" + host + ":" + cfg.runner
	}
	switch {
	case cfg.endpoint == "":
		return cfg, errors.New("TASKBOARD_MCP_URL is required")
	case len(cfg.command) == 0:
		return cfg, errors.New("DISPATCH_COMMAND is required")
	case cfg.workRoot == "":
		return cfg, errors.New("DISPATCH_WORK_ROOT is required")
	case len(cfg.allowedEditors) == 0 && !cfg.admitAll:
		return cfg, errors.New("DISPATCH_ALLOWED_EDITORS is required unless DISPATCH_ADMIT_ALL=true")
	}
	return cfg, nil
}

// secret reads NAME, or the file named by NAME_FILE.
func secret(name string) (string, error) {
	if path := strings.TrimSpace(os.Getenv(name + "_FILE")); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read %s_FILE: %w", name, err)
		}
		return strings.TrimSpace(string(data)), nil
	}
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value, nil
	}
	return "", fmt.Errorf("%s or %s_FILE is required", name, name)
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func number(name string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func boolean(name string) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return false, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", name)
	}
	return value, nil
}

func split(value string) []string {
	var result []string
	for _, part := range strings.Split(value, ",") {
		if item := strings.TrimSpace(part); item != "" {
			result = append(result, item)
		}
	}
	return result
}

func version() string {
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" {
		return info.Main.Version
	}
	return "devel"
}
