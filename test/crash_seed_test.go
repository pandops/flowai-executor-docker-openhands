package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-docker-openhands/internal/cache"
	"github.com/pandops/flowai-executor-docker-openhands/internal/dockerclient"
	"github.com/pandops/flowai-executor-docker-openhands/internal/executor"
)

// The state a crashed Executor process leaves behind is seeded directly:
// its cleanup_id file, its still-running containers, its recovery cache
// and outbox, and its publisher_seq state file.

const seededCleanupID = "4611686018427387905"

func (h *harness) writeCleanupID(t *testing.T, id string) {
	t.Helper()
	if err := os.MkdirAll(h.cleanupDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(h.cleanupDir, executor.CleanupIDFileName), []byte(id), 0o600); err != nil {
		t.Fatal(err)
	}
}

// leftover starts a container as the previous process would have.
func (h *harness) leftover(t *testing.T, taskID int64, cleanupID string) dockerclient.ContainerRef {
	t.Helper()
	ref, err := h.docker.FakeDocker.StartContainer(context.Background(), dockerclient.ContainerSpec{
		Name:  fmt.Sprintf("flowai-task-%d", taskID),
		Image: "ghcr.io/acme/agent-runtime:1",
		Labels: map[string]string{
			dockerclient.LabelExecutorID: strconv.FormatInt(executorID, 10), dockerclient.LabelCleanupID: cleanupID,
			dockerclient.LabelRuntime: "openhands", dockerclient.LabelTaskID: strconv.FormatInt(taskID, 10),
			dockerclient.LabelTeamID: "3", "flowai.attempt": "1", "flowai.claim_seq": "1",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return *ref
}

func crashedClaim(taskID int64, state, containerID string) cache.ClaimRecord {
	return cache.ClaimRecord{
		TaskID: taskID, Attempt: 1, ClaimSeq: 1, TeamID: teamID, ExecutorID: executorID, FlowID: 42,
		State: state, CreatedPublisherSeq: 10, ClaimAckedAt: time.Now().Add(-5 * time.Minute),
		ContainerID: containerID, ConversationID: fmt.Sprintf("conv-%d", taskID),
	}
}

// seedCache writes claims, outbox events and the last successful
// heartbeat into the Executor's cache while it is not running.
func (h *harness) seedCache(t *testing.T, lastHeartbeat time.Time, claims []cache.ClaimRecord, outbox []cache.OutboxRecord) {
	t.Helper()
	store := h.openCache()
	defer store.Close()
	ctx := context.Background()
	if err := store.PutExecutorID(ctx, executorID); err != nil {
		t.Fatalf("seed executor_id: %v", err)
	}
	if err := store.PutLastHeartbeat(ctx, lastHeartbeat); err != nil {
		t.Fatalf("seed last heartbeat: %v", err)
	}
	for _, c := range claims {
		if err := store.PutClaim(ctx, c); err != nil {
			t.Fatalf("seed claim: %v", err)
		}
	}
	for _, o := range outbox {
		if err := store.PutOutbox(ctx, o); err != nil {
			t.Fatalf("seed outbox: %v", err)
		}
	}
}

// outboxEvent builds a stored Executor event for the seeded outbox.
func outboxEvent(seq, taskID int64, kind, state string) cache.OutboxRecord {
	key := cache.ClaimKey{TaskID: taskID, Attempt: 1, ClaimSeq: 1}
	body := fmt.Sprintf(`{"publisher":"executor","publisher_seq":%d,"team_id":3,"executor_id":7,"kind":%q,"occurred_at":"2026-11-07T10:00:00.000Z","task_id":%d,"attempt":1,"claim_seq":1}`, seq, kind, taskID)
	return cache.OutboxRecord{PublisherSeq: seq, Subject: "executor.event.7." + kind + ".pub", Kind: kind, Claim: &key, Body: []byte(body), State: state}
}

// firstTaskPullIndex returns the op index of the first task pull.
func (h *harness) waitFirstPull(t *testing.T) {
	t.Helper()
	eventually(t, "first task pull", func() bool { return len(h.taskFetches()) > 0 })
}
