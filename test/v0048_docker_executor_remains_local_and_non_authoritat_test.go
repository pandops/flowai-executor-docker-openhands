package test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pandops/flowai-executor-docker-openhands/internal/broker"
	fakebroker "github.com/pandops/flowai-executor-docker-openhands/internal/mocks/broker"
)

// Requirement: Docker Executor remains local and non-authoritative.
func TestV0048DockerExecutorRemainsLocalAndNonAuthoritative(t *testing.T) {
	t.Parallel()

	t.Run("Executor does not own task selection or tenancy", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		// A message on another team's consumer is never pulled.
		h.broker.MarkConsumer(streamTask, taskConsumerName(4, tagName))
		other := h.broker.Enqueue(streamTask, taskConsumerName(4, tagName), "task-manager.task.4.gpu.1200.sub", mustJSON(newTask(1200).body()))
		h.setClaimToken(func(b map[string]any) (int, any, error) {
			if asInt(b["body"].(map[string]any)["task_id"]) == 1201 {
				return claimRejected(b)
			}
			return 200, tokenReply(b, "ct-ok"), nil
		})
		h.start()
		h.enqueueTask(newTask(1201))
		h.enqueueTask(newTask(1202))
		h.waitTaskKind(1202, "running")
		if other.Taken() {
			t.Fatalf("pulled from another team's consumer")
		}
		for _, op := range h.broker.Fetches() {
			if op.Stream == streamTask && op.Consumer != taskConsumerName(teamID, tagName) {
				t.Fatalf("pulled %s", op.Consumer)
			}
		}
		// task-manager's answer is the only claim decision.
		if _, ok := h.docker.specFor(1201); ok {
			t.Fatalf("ran a task task-manager rejected")
		}
	})

	t.Run("Executor does not persist task history or secrets", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.runTask(newTask(1203))
		spec, _ := h.docker.specFor(1203)
		env := strings.Join(spec.Env, "\n")
		if !strings.Contains(env, "DEPLOY_TOKEN=s3cr3t-value-1203") {
			t.Fatalf("opened value not passed to the runtime: %v", spec.Env)
		}
		h.terminal(1203, "finished")
		h.waitTaskKind(1203, "finished")
		h.sigterm()
		secrets := []string{"s3cr3t-value-1203", "sk-task-key-1203", "ct-1203-1-1"}
		_ = filepath.Walk(h.dir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || path == h.cfgPath {
				return nil //nolint:nilerr // files that vanish during the walk hold nothing
			}
			raw, _ := os.ReadFile(path)
			for _, s := range secrets {
				if strings.Contains(string(raw), s) {
					t.Errorf("%s persists %q", path, s)
				}
			}
			return nil
		})
		store := h.openCache()
		defer store.Close()
		if claims, _ := store.ListClaims(context.Background()); len(claims) != 0 {
			t.Fatalf("finished claim kept as history: %+v", claims)
		}
		_ = broker.Message{}
		_ = fakebroker.OpAck
	})
}
