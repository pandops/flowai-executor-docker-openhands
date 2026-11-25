package test

import (
	"encoding/json"
	"testing"
	"time"

	fakebroker "github.com/pandops/flowai-executor-docker-openhands/internal/mocks/broker"
)

const commandCreateSubject = "$JS.API.CONSUMER.CREATE.TASK_MANAGER_COMMAND.commands-7.task-manager.command.7.sub"

// Requirement: Concrete Executors create their own command consumer.
func TestV0048ConcreteExecutorsCreateTheirOwnCommandConsumer(t *testing.T) {
	t.Parallel()

	t.Run("Executor creates its own command consumer on first connect", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		eventually(t, "task pull", func() bool { return len(h.taskFetches()) > 0 })
		ops := h.broker.Ops()
		create := opIndex(ops, func(op fakebroker.Op) bool {
			return op.Kind == fakebroker.OpRequest && op.Subject == commandCreateSubject
		})
		pull := opIndex(ops, func(op fakebroker.Op) bool { return op.Kind == fakebroker.OpFetch && op.Stream == streamTask })
		if create < 0 || create > pull {
			t.Fatalf("create at %d, first task pull at %d", create, pull)
		}
		var req struct {
			Stream string         `json:"stream_name"`
			Action string         `json:"action"`
			Config map[string]any `json:"config"`
		}
		if err := json.Unmarshal(ops[create].Data, &req); err != nil {
			t.Fatalf("create body: %v", err)
		}
		c := req.Config
		if req.Stream != streamCommand || c["durable_name"] != "commands-7" || c["filter_subject"] != "task-manager.command.7.sub" ||
			c["deliver_policy"] != "all" || c["ack_policy"] != "explicit" ||
			asInt(c["ack_wait"]) != int64(30*time.Second) || asInt(c["inactive_threshold"]) != int64(24*time.Hour) {
			t.Fatalf("consumer create = %s", ops[create].Data)
		}
		if md, ok := c["max_deliver"]; ok && asInt(md) > 0 {
			t.Fatalf("max_deliver = %v, want unlimited", md)
		}
		eventually(t, "command pull", func() bool {
			for _, op := range h.broker.Fetches() {
				if op.Stream == streamCommand && op.Consumer == "commands-7" {
					return true
				}
			}
			return false
		})
	})
}
