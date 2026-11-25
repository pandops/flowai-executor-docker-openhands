// Pure unit tests for the executor package. Integration / multi-component
// scenarios live under /autotests/executor_docker_openhands.
package executor

import (
	"os"
	"strings"
	"testing"
	"time"
)

// setRequiredEnv sets the one setting the operator must configure
// explicitly.
func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DOCKER_SOCKET_PATH", "/run/flowai/runtime.sock")
}

func TestConfigValidateRejectsBadRange(t *testing.T) {
	cfg := &Config{
		MaxParallelTasks:    5,
		OpenHandsPortStart:  19000,
		OpenHandsPortEnd:    19001,
		OpenHandsWorkspace:  "/workspace/project",
		OpenHandsLLMUsageID: "u",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected error for too-small port range")
	}
}

func TestConfigValidateRejectsMissingV1Agent(t *testing.T) {
	cfg := &Config{
		MaxParallelTasks:   1,
		OpenHandsPortStart: 19000,
		OpenHandsPortEnd:   19000,
		OpenHandsWorkspace: "/workspace/project",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected error for missing V1 agent profile/model")
	}
}

func TestConfigLoadFromYAML(t *testing.T) {
	setRequiredEnv(t)
	tmp := t.TempDir()
	path := tmp + "/cfg.yaml"
	yaml := `max_parallel_tasks: 4
openhands_host_port_start: 20000
openhands_host_port_end: 20010
`
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.MaxParallelTasks != 4 {
		t.Fatalf("max_parallel_tasks: %d", cfg.MaxParallelTasks)
	}
	if cfg.OpenHandsPortStart != 20000 {
		t.Fatalf("port start: %d", cfg.OpenHandsPortStart)
	}
}

func TestStateStringValues(t *testing.T) {
	states := []State{StateStarting, StateReady, StateConnectionLoss, StateStopping, StateStopped, StateFailed}
	for _, s := range states {
		if string(s) == "" {
			t.Fatalf("empty state value")
		}
	}
}

func TestLoadConfigGeneratesOpenHandsAPIKey(t *testing.T) {
	setRequiredEnv(t)
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.OpenHandsAPIKey == "" {
		t.Fatalf("expected OpenHandsAPIKey to be auto-generated when none configured")
	}
}

func TestLoadConfigPreservesExplicitOpenHandsAPIKey(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("OPENHANDS_API_KEY", "operator-supplied-key")
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.OpenHandsAPIKey != "operator-supplied-key" {
		t.Fatalf("expected OpenHandsAPIKey = %q, got %q",
			"operator-supplied-key", cfg.OpenHandsAPIKey)
	}
}

// validConfig returns a baseline cfg that satisfies every Validate rule.
func validConfig() *Config {
	cfg := defaultConfig()
	cfg.DockerSocketPath = "/run/flowai/runtime.sock"
	cfg.MaxParallelTasks = 1
	cfg.OpenHandsPortStart = 19000
	cfg.OpenHandsPortEnd = 19001
	return cfg
}

func TestDockerEnvWithSessionAPIKey(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		key    string
		want   map[string]string
	}{
		{
			name: "override caller-provided with configured key",
			values: map[string]string{
				"SESSION_API_KEY": "caller-supplied",
				"FOO":             "bar",
			},
			key: "configured-key",
			want: map[string]string{
				"SESSION_API_KEY": "configured-key",
				"FOO":             "bar",
			},
		},
		{
			name: "inject key when caller did not provide one",
			values: map[string]string{
				"OTHER": "value",
			},
			key: "configured-key",
			want: map[string]string{
				"SESSION_API_KEY": "configured-key",
				"OTHER":           "value",
			},
		},
		{
			name: "strip caller-provided when configured key is empty",
			values: map[string]string{
				"SESSION_API_KEY": "caller-supplied",
				"FOO":             "bar",
			},
			key: "",
			want: map[string]string{
				"FOO": "bar",
			},
		},
		{
			name: "empty key and absent caller value keeps SESSION_API_KEY absent",
			values: map[string]string{
				"FOO": "bar",
			},
			key: "",
			want: map[string]string{
				"FOO": "bar",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dockerEnvWithSessionAPIKey(tt.values, tt.key)
			parsed := map[string]string{}
			for _, entry := range got {
				eq := strings.IndexByte(entry, '=')
				if eq < 0 {
					t.Fatalf("entry %q missing '=' separator", entry)
				}
				parsed[entry[:eq]] = entry[eq+1:]
			}
			if len(parsed) != len(tt.want) {
				t.Fatalf("entry count = %d, want %d (entries=%v)",
					len(parsed), len(tt.want), parsed)
			}
			for k, v := range tt.want {
				if parsed[k] != v {
					t.Fatalf("entry %q = %q, want %q", k, parsed[k], v)
				}
			}
		})
	}
}

func TestDockerEnvWithSessionAPIKeyDoesNotMutateInput(t *testing.T) {
	values := map[string]string{
		"SESSION_API_KEY": "caller-supplied",
		"FOO":             "bar",
	}
	snapshot := map[string]string{
		"SESSION_API_KEY": "caller-supplied",
		"FOO":             "bar",
	}
	_ = dockerEnvWithSessionAPIKey(values, "configured-key")
	if len(values) != len(snapshot) {
		t.Fatalf("input size changed: got %d, want %d", len(values), len(snapshot))
	}
	for k, want := range snapshot {
		if got := values[k]; got != want {
			t.Fatalf("input mutated at %q: got %q, want %q", k, got, want)
		}
	}
}

// TestConfigValidateRejectsNegativeCleanupDelay asserts the v0005
// contract: cleanup delays are non-negative.
func TestConfigValidateRejectsNegativeCleanupDelay(t *testing.T) {
	cases := []struct {
		name     string
		finished time.Duration
		failed   time.Duration
	}{
		{"negative finished", -1 * time.Second, 0},
		{"negative failed", 0, -1 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.FinishedCleanupDelay = tc.finished
			cfg.FailedCleanupDelay = tc.failed
			if err := cfg.Validate(); err == nil {
				t.Fatalf("expected negative cleanup_delay to be rejected")
			}
		})
	}
}

// TestConfigLoadHonoursCleanupDelayDefaults asserts the YAML+env
// path applies the documented defaults (each 0s) when unset.
func TestConfigLoadHonoursCleanupDelayDefaults(t *testing.T) {
	setRequiredEnv(t)
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.FinishedCleanupDelay != 0 {
		t.Fatalf("FinishedCleanupDelay default = %s, want 0s", cfg.FinishedCleanupDelay)
	}
	if cfg.FailedCleanupDelay != 0 {
		t.Fatalf("FailedCleanupDelay default = %s, want 0s", cfg.FailedCleanupDelay)
	}
}

// TestConfigLoadHonoursCleanupDelayEnv asserts the YAML+env path
// applies the v0005 env overrides for cleanup delays.
func TestConfigLoadHonoursCleanupDelayEnv(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("EXECUTOR_FINISHED_CLEANUP_DELAY", "30s")
	t.Setenv("EXECUTOR_FAILED_CLEANUP_DELAY", "5s")
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.FinishedCleanupDelay != 30*time.Second {
		t.Fatalf("FinishedCleanupDelay env = %s, want 30s", cfg.FinishedCleanupDelay)
	}
	if cfg.FailedCleanupDelay != 5*time.Second {
		t.Fatalf("FailedCleanupDelay env = %s, want 5s", cfg.FailedCleanupDelay)
	}
}

// TestSlotAcceptedTerminalRecord pins the slot's accepted-terminal
// record so review can match v0005 cleanup-delay selection by event
// type without touching real Docker.
func TestSlotAcceptedTerminalRecord(t *testing.T) {
	slot := &taskSlot{}
	if event, at := slot.acceptedTerminalRecord(); event != "" || !at.IsZero() {
		t.Fatalf("expected no accepted terminal on a fresh slot, got event=%q at=%s", event, at)
	}
	slot.recordAcceptedTerminal("finished")
	event, at := slot.acceptedTerminalRecord()
	if event != "finished" {
		t.Fatalf("event = %q, want %q", event, "finished")
	}
	if at.IsZero() {
		t.Fatalf("accepted_at should be populated after recordAcceptedTerminal")
	}
}
