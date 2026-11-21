package test

import (
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// Requirement: Concrete Executors enforce max_parallel_tasks slots locally.
func TestV0048ConcreteExecutorsEnforceMaxParallelTasksSlotsLocally(t *testing.T) {
	t.Parallel()

	t.Run("Local capacity is reached", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.runTask(newTask(1120))
		h.runTask(newTask(1121))
		pulls := len(h.taskFetches())
		h.enqueueTask(newTask(1122))
		never(t, "pull with every slot occupied", 1500*time.Millisecond, func() bool { return h.pullsSince(pulls) > 0 })
		if len(h.docker.specsSnapshot()) != 2 {
			t.Fatalf("started %d runtimes", len(h.docker.specsSnapshot()))
		}
		h.terminal(1120, "finished")
		h.waitTaskKind(1122, "running")
	})

	t.Run("Pending task occupies a slot", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, func(o *opts) { o.maxParallel = 1 })
		var ok atomic.Bool
		h.setCheck(func(b map[string]any) (int, any, error) {
			if ok.Load() {
				return http.StatusOK, map[string]any{"result": "ok", "model_alias": "3/gpt-4o"}, nil
			}
			return checkTemporary("budget_exhausted")(b)
		})
		h.start()
		h.enqueueTask(newTask(1123))
		h.waitTaskKind(1123, "pending")
		h.enqueueTask(newTask(1124))
		never(t, "second task pulled while pending", 1500*time.Millisecond, func() bool { return h.hasTaskKind(1124, "created") })
		ok.Store(true)
		h.waitTaskKind(1123, "running")
		never(t, "second task pulled while running", time.Second, func() bool { return h.hasTaskKind(1124, "created") })
	})

	t.Run("Capacity observation is informational", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, func(o *opts) { o.maxParallel = 1 })
		// The reply carries extra fields that look like capacity decisions
		// (free slots, a larger limit, an accept flag), as the K8s test does;
		// none of them is a capacity decision.
		var replied atomic.Int64
		h.setHeartbeat(func(b map[string]any) (int, any, error) {
			r := heartbeatReplyListing(b)
			r["connection_state"] = "connected"
			r["max_parallel_tasks"] = 5
			r["available_capacity"] = 4
			r["slots_in_use"] = 0
			r["accept_tasks"] = true
			replied.Add(1)
			return http.StatusOK, r, nil
		})
		h.start()
		h.runTask(newTask(1125))
		hbs := h.waitHeartbeats(len(h.heartbeats()) + 1)
		if asInt(hbs[len(hbs)-1]["slots_in_use"]) != 1 || asInt(hbs[len(hbs)-1]["max_parallel_tasks"]) != 1 {
			t.Fatalf("heartbeat = %v", hbs[len(hbs)-1])
		}
		// Every slot is in use and the replies claim free capacity: the
		// Executor does not pull the next task.
		pulls := len(h.taskFetches())
		h.enqueueTask(newTask(1126))
		before := replied.Load()
		never(t, "pull because a reply field suggested capacity", 2500*time.Millisecond, func() bool {
			return h.pullsSince(pulls) > 0 || h.hasTaskKind(1126, "created")
		})
		if replied.Load() < before+2 {
			t.Fatalf("only %d capacity-looking replies during the wait", replied.Load()-before)
		}
		if len(h.docker.specsSnapshot()) != 1 {
			t.Fatalf("started %d runtimes", len(h.docker.specsSnapshot()))
		}
		h.terminal(1125, "finished")
		h.waitTaskKind(1126, "running") // the Executor decided on its own
	})
}
