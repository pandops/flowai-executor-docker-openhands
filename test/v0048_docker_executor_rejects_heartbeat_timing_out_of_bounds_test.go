package test

import (
	"strings"
	"testing"

	"github.com/pandops/flowai-executor-docker-openhands/internal/executor"
)

// Configuration validation: the heartbeat timing the Executor reports to
// executor-registry is whole seconds within the heartbeat schema's bounds
// (interval_seconds and wait_seconds at least 1, miss_limit 1 to 1000); the
// K8s Executor applies the same rule.
func TestV0048DockerExecutorRejectsHeartbeatTimingOutOfBounds(t *testing.T) {
	for _, c := range []struct {
		name, key string
		edit      func(*opts)
	}{
		{"sub-second heartbeat_interval", "heartbeat_interval", func(o *opts) { o.heartbeat = "500ms" }},
		{"fractional heartbeat_interval", "heartbeat_interval", func(o *opts) { o.heartbeat = "1500ms" }},
		{"sub-second connection_wait", "connection_wait", func(o *opts) { o.wait = "999ms" }},
		{"heartbeat_miss_limit above 1000", "heartbeat_miss_limit", func(o *opts) { o.missLimit = 1001 }},
	} {
		t.Run(c.name+" is rejected", func(t *testing.T) {
			h := newHarness(t, c.edit)
			if _, err := executor.LoadConfig(h.cfgPath); err == nil || !strings.Contains(err.Error(), c.key) {
				t.Fatalf("LoadConfig = %v, want an error naming %s", err, c.key)
			}
		})
	}
	t.Run("one-second bounds are accepted", func(t *testing.T) {
		h := newHarness(t, func(o *opts) { o.heartbeat, o.wait, o.missLimit = "1s", "1s", 1000 })
		if _, err := executor.LoadConfig(h.cfgPath); err != nil {
			t.Fatalf("LoadConfig = %v, want accepted", err)
		}
	})
}
