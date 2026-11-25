package test

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-docker-openhands/internal/dockerclient"
)

// Requirement: Docker Executor runs bounded OpenHands containers.
func TestV0048DockerExecutorRunsBoundedOpenHandsContainers(t *testing.T) {
	t.Parallel()

	t.Run("Executor starts a same-team, same-tag task up to capacity", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		tk := newTask(1130)
		tk.ImageSource = "team_default"
		h.runTask(tk)
		spec, ok := h.docker.specFor(1130)
		if !ok || spec.Image != *tk.Image {
			t.Fatalf("spec = %+v", spec)
		}
		labels := map[string]string{
			dockerclient.LabelExecutorID: "7", dockerclient.LabelRuntime: "openhands", dockerclient.LabelTaskID: "1130",
			dockerclient.LabelTeamID: "3", "flowai.attempt": "1", "flowai.claim_seq": "1",
			dockerclient.LabelResolvedImageSource: "team_default",
		}
		for k, v := range labels {
			if spec.Labels[k] != v {
				t.Fatalf("label %s = %q, want %q (labels %v)", k, spec.Labels[k], v, spec.Labels)
			}
		}
		if id, err := strconv.ParseInt(spec.Labels[dockerclient.LabelCleanupID], 10, 64); err != nil || id <= 0 {
			t.Fatalf("cleanup_id label %q", spec.Labels[dockerclient.LabelCleanupID])
		}
		for _, gone := range []string{"flowai.executor_scope", "flowai.command_id"} {
			if _, has := spec.Labels[gone]; has {
				t.Fatalf("removed label %s present", gone)
			}
		}
		h.runTask(newTask(1131))
		s2, _ := h.docker.specFor(1131)
		if len(spec.Ports) == 0 || len(s2.Ports) == 0 || spec.Ports[0].HostPort == s2.Ports[0].HostPort {
			t.Fatalf("host ports not distinct: %+v %+v", spec.Ports, s2.Ports)
		}
		h.terminal(1130, "finished")
		h.waitTaskKind(1130, "finished")
	})

	t.Run("Executor rejects a different image than task-manager resolved", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, func(o *opts) { o.extraYAML = "openhands_image: ghcr.io/openhands/agent-server:local\n" })
		h.start()
		tk := newTask(1132)
		tk.Image = strp("registry.example/acme/other-runtime@sha256:" + "b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0")
		h.runTask(tk)
		if spec, _ := h.docker.specFor(1132); spec.Image != *tk.Image {
			t.Fatalf("image %q, want the resolved image verbatim", spec.Image)
		}
		tk2 := newTask(1133)
		tk2.Image = strp("registry.example/acme/missing:1")
		h.docker.mu.Lock()
		h.docker.pullErr[*tk2.Image] = errors.New("not found")
		h.docker.mu.Unlock()
		h.enqueueTask(tk2)
		f := h.waitTaskKind(1133, "failed")
		if f.Body["failure_reason"] != "image_pull_failed" || f.Body["retryable"] != true {
			t.Fatalf("failed = %s", f.Raw)
		}
		for _, s := range h.docker.specsSnapshot() {
			if s.Image == "ghcr.io/openhands/agent-server:local" {
				t.Fatalf("local image substituted")
			}
		}
	})

	t.Run("Executor respects FIFO", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, func(o *opts) { o.maxParallel = 3 })
		for _, id := range []int64{1136, 1134, 1135} {
			h.enqueueTask(newTask(id))
		}
		h.start()
		var seqs []int64
		for _, id := range []int64{1136, 1134, 1135} {
			seqs = append(seqs, h.waitTaskKind(id, "created").Seq)
		}
		if seqs[0] >= seqs[1] || seqs[1] >= seqs[2] {
			t.Fatalf("created order %v does not follow delivery order", seqs)
		}
	})

	t.Run("Executor leaves excess tasks", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		for id := int64(1137); id < 1142; id++ {
			h.enqueueTask(newTask(id))
		}
		h.start()
		h.waitTaskKind(1138, "running")
		time.Sleep(500 * time.Millisecond)
		if w := h.broker.Waiting(streamTask, taskConsumerName(teamID, tagName)); w != 3 {
			t.Fatalf("waiting = %d, want 3 left", w)
		}
	})

	t.Run("Executor reports capacity as an observation", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, func(o *opts) { o.maxParallel = 2 })
		h.start()
		h.runTask(newTask(1142))
		hb := h.waitHeartbeats(len(h.heartbeats()) + 1)
		last := hb[len(hb)-1]
		if asInt(last["max_parallel_tasks"]) != 2 || asInt(last["slots_in_use"]) != 1 {
			t.Fatalf("heartbeat = %v", last)
		}
	})
}
