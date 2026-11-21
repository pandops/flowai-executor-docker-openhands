package test

import (
	"sync"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-docker-openhands/internal/broker"
)

// Requirement: K8s Executor runs only Kubernetes Pods — the two scenarios
// that state the Docker OpenHands Executor's behavior.
func TestV0048K8sExecutorRunsOnlyKubernetesPodsDockerRows(t *testing.T) {
	t.Parallel()

	t.Run("Docker OpenHands uses the same terminal cleanup delay", func(t *testing.T) {
		t.Parallel()
		// Distinct delays prove selection by terminal kind; the terminal
		// publish is acknowledged 1 s after the terminal signal, so a timer
		// started at the signal is told apart from one started at the ack.
		h := newHarness(t, func(o *opts) {
			o.maxParallel = 1
			o.finishedDelay = "300ms"
			o.failedDelay = "1500ms"
			o.publishAck = "3s"
		})
		var mu sync.Mutex
		ackAt := map[string]time.Time{}
		presentAtAck := map[string]bool{}
		containerOf := map[string]string{}
		h.broker.OnPublish = func(m broker.Message) {
			kind := ""
			switch m.Subject {
			case "executor.event.7.finished.pub":
				kind = "finished"
			case "executor.event.7.failed.pub":
				kind = "failed"
			default:
				return
			}
			time.Sleep(time.Second)
			mu.Lock()
			defer mu.Unlock()
			if _, seen := ackAt[kind]; !seen {
				ackAt[kind] = time.Now()
				presentAtAck[kind] = h.docker.HasContainer(containerOf[kind])
			}
		}
		at := func(kind string) (time.Time, bool) {
			mu.Lock()
			defer mu.Unlock()
			return ackAt[kind], presentAtAck[kind]
		}

		h.start()
		h.runTask(newTask(1210))
		fin, _ := h.docker.containerForTask(1210)
		mu.Lock()
		containerOf["finished"] = fin.ID
		mu.Unlock()
		h.enqueueTask(newTask(1211))
		h.terminal(1210, "finished")
		h.waitTaskKind(1210, "finished")
		finAck, present := at("finished")
		if !present {
			t.Fatalf("finished container removed before its terminal event was acknowledged")
		}
		for time.Since(finAck) < 200*time.Millisecond {
			if !h.docker.HasContainer(fin.ID) {
				t.Fatalf("finished container removed %s after the ack, before finished_cleanup_delay", time.Since(finAck))
			}
			if h.hasTaskKind(1211, "created") {
				t.Fatalf("slot freed during the cleanup delay")
			}
			time.Sleep(20 * time.Millisecond)
		}
		for h.docker.HasContainer(fin.ID) {
			if time.Since(finAck) > 1200*time.Millisecond {
				t.Fatalf("finished container kept %s after the ack; finished_cleanup_delay is 300ms", time.Since(finAck))
			}
			time.Sleep(20 * time.Millisecond)
		}

		h.waitTaskKind(1211, "running")
		failed, _ := h.docker.containerForTask(1211)
		mu.Lock()
		containerOf["failed"] = failed.ID
		mu.Unlock()
		h.terminal(1211, "failed")
		h.waitTaskKind(1211, "failed")
		failAck, present := at("failed")
		if !present {
			t.Fatalf("failed container removed before its terminal event was acknowledged")
		}
		for time.Since(failAck) < 1300*time.Millisecond {
			if !h.docker.HasContainer(failed.ID) {
				t.Fatalf("failed container removed %s after the ack, before failed_cleanup_delay (1.5s)", time.Since(failAck))
			}
			time.Sleep(20 * time.Millisecond)
		}
		eventually(t, "failed container removed after failed_cleanup_delay", func() bool { return !h.docker.HasContainer(failed.ID) })
		for _, id := range []int64{1210, 1211} {
			n := 0
			for _, k := range h.kindsFor(id) {
				if k == "finished" || k == "failed" {
					n++
				}
			}
			if n != 1 {
				t.Fatalf("task %d has %d terminal events", id, n)
			}
		}
	})

	t.Run("Docker container exits before OpenHands finishes", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.runTask(newTask(1212))
		ref, _ := h.docker.containerForTask(1212)
		h.docker.EmitDie(ref.Name) // exit code 0 or not: not success
		f := h.waitTaskKind(1212, "failed")
		if f.Body["failure_reason"] != "container_exited_before_finish" || f.Body["retryable"] != true {
			t.Fatalf("failed = %s", f.Raw)
		}
		time.Sleep(500 * time.Millisecond)
		if h.hasTaskKind(1212, "finished") || len(h.docker.specsSnapshot()) != 1 || h.docker.openHandsFor(1212).StartCallCount() != 1 {
			t.Fatalf("execution resumed, recreated or treated as success: %v", h.kindsFor(1212))
		}
	})
}
