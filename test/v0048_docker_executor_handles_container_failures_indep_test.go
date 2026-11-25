package test

import (
	"errors"
	"net/http"
	"testing"
	"time"
)

// Requirement: Docker Executor handles container failures independently.
func TestV0048DockerExecutorHandlesContainerFailuresIndependently(t *testing.T) {
	t.Parallel()

	t.Run("One OpenHands container exits unexpectedly", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.runTask(newTask(1150))
		h.runTask(newTask(1151))
		ref, _ := h.docker.containerForTask(1150)
		h.docker.EmitDie(ref.Name)
		f := h.waitTaskKind(1150, "failed")
		if f.Body["failure_reason"] != "container_exited_before_finish" || f.Body["retryable"] != true {
			t.Fatalf("failed = %s", f.Raw)
		}
		eventually(t, "container removed", func() bool { _, ok := h.docker.containerForTask(1150); return !ok })
		if _, ok := h.docker.containerForTask(1151); !ok {
			t.Fatalf("other container stopped")
		}
		h.enqueueTask(newTask(1152))
		h.waitTaskKind(1152, "running")
		if h.hasTaskKind(1151, "failed") {
			t.Fatalf("other task failed")
		}
	})

	t.Run("Docker daemon or Executor process fails", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.waitSelfKind("executor_started")
		eventually(t, "pulling", func() bool { return len(h.taskFetches()) > 0 })
		h.docker.BreakEvents(errors.New("docker daemon unreachable"))
		h.waitSelfKind("executor_failed")
		pulls := len(h.taskFetches())
		never(t, "pull after supervision failed", 1500*time.Millisecond, func() bool { return h.pullsSince(pulls) > 1 })
		if code, body := h.probe("/v1/readyz"); code != http.StatusServiceUnavailable || body["can_supervise"] != false {
			t.Fatalf("readyz = %d %v", code, body)
		}
	})
}
