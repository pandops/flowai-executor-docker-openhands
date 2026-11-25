package test

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Requirement: Docker Executor shuts down gracefully.
func TestV0048DockerExecutorShutsDownGracefully(t *testing.T) {
	t.Parallel()

	t.Run("SIGTERM during idle polling", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.waitFirstPull(t)
		if err := h.sigterm(); err != nil && !errIs(err, context.Canceled) {
			t.Fatalf("Run = %v", err)
		}
		if k := strings.Join(h.selfKinds(), ","); k != "executor_started,executor_stopping,executor_stopped" {
			t.Fatalf("self events = %s", k)
		}
	})

	t.Run("SIGTERM during in-flight tasks", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, func(o *opts) { o.drain = "2s" })
		h.start()
		h.runTask(newTask(1170))
		h.runTask(newTask(1171))
		hbs, pulls := len(h.heartbeats()), len(h.taskFetches())
		go func() {
			time.Sleep(500 * time.Millisecond)
			h.terminal(1170, "finished")
		}()
		start := time.Now()
		h.sigterm()
		if d := time.Since(start); d < 1800*time.Millisecond || d > 6*time.Second {
			t.Fatalf("drain took %s, want about the 2 s deadline", d)
		}
		if !h.hasTaskKind(1170, "finished") {
			t.Fatalf("finished not published for the task that finished: %v", h.kindsFor(1170))
		}
		r := h.waitTaskKind(1171, "released")
		if r.Body["reason"] != "executor_shutdown" {
			t.Fatalf("released = %s", r.Raw)
		}
		if h.docker.ContainerCount() != 0 {
			t.Fatalf("%d containers left", h.docker.ContainerCount())
		}
		if len(h.heartbeats()) > hbs+1 || len(h.taskFetches()) > pulls+1 {
			t.Fatalf("heartbeats or pulls continued during the drain")
		}
		kinds := h.selfKinds()
		if kinds[len(kinds)-1] != "executor_stopped" {
			t.Fatalf("last self event = %v", kinds)
		}
		stopping := h.waitSelfKind("executor_stopping")
		if stopping.Seq > r.Seq {
			t.Fatalf("executor_stopping after the release")
		}
	})

	t.Run("SIGTERM with created and pending claims", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, func(o *opts) { o.drain = "5s" })
		h.setClaimToken(func(b map[string]any) (int, any, error) {
			if asInt(b["body"].(map[string]any)["task_id"]) == 1172 {
				return claimNotProcessed(b)
			}
			return 200, tokenReply(b, "ct-z"), nil
		})
		h.setCheck(checkTemporary("no_active_model"))
		h.start()
		h.enqueueTask(newTask(1172))
		h.waitTaskKind(1172, "created")
		h.enqueueTask(newTask(1173))
		h.waitTaskKind(1173, "pending")
		start := time.Now()
		h.sigterm()
		if time.Since(start) > 3*time.Second {
			t.Fatalf("waited for the drain deadline without running tasks")
		}
		for _, id := range []int64{1172, 1173} {
			if r := h.waitTaskKind(id, "released"); r.Body["reason"] != "executor_shutdown" {
				t.Fatalf("released = %s", r.Raw)
			}
		}
		if len(h.docker.specsSnapshot()) != 0 {
			t.Fatalf("runtime started")
		}
	})
}
