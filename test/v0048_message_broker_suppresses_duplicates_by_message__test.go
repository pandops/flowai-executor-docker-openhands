package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-docker-openhands/internal/cache"
)

// Requirement: message-broker suppresses duplicates by publisher sequence —
// the Executor's message ids and its sequence across restarts.
func TestV0048MessageBrokerSuppressesDuplicatesByPublisherSequence(t *testing.T) {
	t.Parallel()

	t.Run("Equal sequence numbers of two publishers are distinct messages", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.writeCleanupID(t, seededCleanupID)
		ref := h.leftover(t, 1270, seededCleanupID)
		// A crashed process left publisher_seq 40 unsent; task-manager's
		// own message 40 is the task message below.
		h.seedCache(t, time.Now().Add(-time.Minute),
			[]cache.ClaimRecord{crashedClaim(1270, cache.ClaimRunning, ref.ID)},
			[]cache.OutboxRecord{outboxEvent(40, 1270, "running", cache.OutboxPending)})
		h.writeStateSeq(t, 40)
		tk := newTask(1271)
		b := tk.body()
		b["publisher_seq"] = 40
		tk.raw = b
		h.start()
		h.runTask(tk)
		var ev40 *event
		for _, ev := range h.events() {
			if ev.Seq == 40 {
				e := ev
				ev40 = &e
			}
		}
		if ev40 == nil || ev40.MsgID != "executor-3-7-40" {
			t.Fatalf("Executor message 40 = %+v, want Nats-Msg-Id executor-3-7-40", ev40)
		}
		for _, ev := range h.events() {
			if want := fmt.Sprintf("executor-3-7-%d", ev.Seq); ev.MsgID != want {
				t.Fatalf("Nats-Msg-Id %q, want %q", ev.MsgID, want)
			}
		}
		if !h.hasTaskKind(1271, "running") {
			t.Fatalf("task-manager message 40 was not processed")
		}
	})

	t.Run("Executor sequence continues after restart", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.writeStateSeq(t, 900) // synced counter of a crashed process
		h.start()
		first := h.waitSelfKind("executor_started")
		if first.Seq <= 900 {
			t.Fatalf("first publisher_seq after restart = %d, want > 900", first.Seq)
		}
		h.runTask(newTask(1272))
		if s := h.stateSeq(t); s < maxSeq(h.events()) {
			t.Fatalf("state file %d behind published %d", s, maxSeq(h.events()))
		}
		before := maxSeq(h.events())
		h.sigterm()
		h.start()
		eventually(t, "second executor_started", func() bool {
			n := 0
			for _, k := range h.selfKinds() {
				if k == "executor_started" {
					n++
				}
			}
			return n == 2
		})
		seen := map[int64]bool{}
		for _, ev := range h.events() {
			if seen[ev.Seq] {
				t.Fatalf("publisher_seq %d reused", ev.Seq)
			}
			seen[ev.Seq] = true
		}
		var second event
		for _, ev := range h.events() {
			if ev.Kind == "executor_started" {
				second = ev
			}
		}
		if second.Seq <= before {
			t.Fatalf("restart published %d after %d", second.Seq, before)
		}
	})
}
