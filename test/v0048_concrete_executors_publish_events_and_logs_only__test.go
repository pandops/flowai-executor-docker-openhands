package test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-docker-openhands/internal/broker"
	"github.com/pandops/flowai-executor-docker-openhands/internal/executor"
)

// statePath is the publisher_seq state file next to the configuration.
func (h *harness) statePath() string {
	return filepath.Join(filepath.Dir(h.cfgPath), executor.StateFileName)
}

// stateSeq reads the persisted publisher_seq high-water mark.
func (h *harness) stateSeq(t *testing.T) int64 {
	t.Helper()
	raw, err := os.ReadFile(h.statePath())
	if err != nil {
		return 0
	}
	var st struct {
		Reserved int64 `json:"publisher_seq_reserved"`
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatalf("state file %s: %v", raw, err)
	}
	return st.Reserved
}

func (h *harness) writeStateSeq(t *testing.T, n int64) {
	t.Helper()
	if err := os.WriteFile(h.statePath(), []byte(fmt.Sprintf(`{"publisher_seq_reserved":%d}`, n)), 0o600); err != nil {
		t.Fatal(err)
	}
}

func maxSeq(evs []event) int64 {
	var m int64
	for _, ev := range evs {
		if ev.Seq > m {
			m = ev.Seq
		}
	}
	return m
}

// Requirement: Concrete Executors publish events and logs only through the broker.
func TestV0048ConcreteExecutorsPublishEventsAndLogsOnlyThroughTheBroker(t *testing.T) {
	t.Parallel()

	t.Run("Executor restart continues its publisher sequence", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.writeStateSeq(t, 41)
		h.start()
		first := h.waitSelfKind("executor_started")
		if first.Seq < 42 || first.MsgID != fmt.Sprintf("executor-3-7-%d", first.Seq) {
			t.Fatalf("first message seq %d id %s, want >= 42", first.Seq, first.MsgID)
		}
		h.runTask(newTask(1081))
		h.sigterm()
		before := maxSeq(h.events())
		count := len(h.events())
		h.start()
		eventually(t, "started again", func() bool { return len(h.events()) > count })
		seen := map[int64]bool{}
		for i, ev := range h.events() {
			if seen[ev.Seq] {
				t.Fatalf("publisher_seq %d reused", ev.Seq)
			}
			seen[ev.Seq] = true
			if i >= count && ev.Seq <= before {
				t.Fatalf("new message seq %d not above %d", ev.Seq, before)
			}
		}
	})

	t.Run("Counter is persisted before a publish", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		var mu sync.Mutex
		var bad []string
		h.broker.OnPublish = func(m broker.Message) {
			var b struct {
				Seq int64 `json:"publisher_seq"`
			}
			_ = json.Unmarshal(m.Data, &b)
			if got := h.stateSeq(t); got < b.Seq {
				mu.Lock()
				bad = append(bad, fmt.Sprintf("seq %d published with state %d", b.Seq, got))
				mu.Unlock()
			}
		}
		h.start()
		h.runTask(newTask(1082))
		mu.Lock()
		defer mu.Unlock()
		if len(bad) != 0 {
			t.Fatalf("counter not persisted first: %v", bad)
		}
		if info, err := os.Stat(h.statePath()); err != nil || info.Mode().Perm()&0o077 != 0 {
			t.Fatalf("state file %v, %v; want owner-only", info, err)
		}
	})

	t.Run("Executor records terminal state", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, func(o *opts) { o.maxParallel = 1 })
		h.start()
		h.runTask(newTask(1083))
		h.terminal(1083, "finished")
		fin := h.waitTaskKind(1083, "finished")
		evs := h.eventsFor(1083)
		var last time.Time
		for i, ev := range evs {
			at, err := time.Parse(time.RFC3339Nano, ev.Body["occurred_at"].(string))
			if err != nil || at.Before(last) || (i > 0 && ev.Seq <= evs[i-1].Seq) {
				t.Fatalf("lifecycle order broken at %s", ev.Raw)
			}
			last = at
		}
		if k := strings.Join(h.kindsFor(1083), ","); k != "created,running,finished" || fin.Seq <= evs[1].Seq {
			t.Fatalf("kinds = %s", k)
		}
		h.enqueueTask(newTask(1084))
		h.waitTaskKind(1084, "created") // slot freed after terminal handling
	})

	t.Run("Executor records lifecycle self events", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		st := h.waitSelfKind("executor_started")
		if st.Body["executor_type"] != execType || asInt(st.Body["executor_id"]) != executorID || asInt(st.Body["team_id"]) != teamID {
			t.Fatalf("executor_started = %s", st.Raw)
		}
		h.sigterm()
		if k := strings.Join(h.selfKinds(), ","); k != "executor_started,executor_stopping,executor_stopped" {
			t.Fatalf("self events = %s", k)
		}
		// executor_failed when Docker supervision fails.
		h2 := newHarness(t, nil)
		h2.start()
		h2.waitSelfKind("executor_started")
		h2.docker.BreakEvents(fmt.Errorf("docker daemon gone"))
		f := h2.waitSelfKind("executor_failed")
		if f.Body["phase"] == "" {
			t.Fatalf("executor_failed = %s", f.Raw)
		}
	})

	t.Run("Events carry no secret material", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		tk := newTask(1085)
		h.runTask(tk)
		h.terminal(1085, "finished")
		h.waitTaskKind(1085, "finished")
		h.sigterm()
		secrets := []string{"sk-task-key-1085", "s3cr3t-value-1085", "ct-1085-1-1", h.token, "test-session-key"}
		if l := h.leaks(secrets...); len(l) != 0 {
			t.Fatalf("secret material on the broker: %v", l)
		}
		logs := h.logs.String()
		for _, s := range secrets {
			if strings.Contains(logs, s) {
				t.Fatalf("log carries %q", s)
			}
		}
	})

	t.Run("Executor publishes only its own release reasons", func(t *testing.T) {
		t.Parallel()
		// pending_timeout
		h := newHarness(t, nil)
		h.setCheck(checkTemporary("no_active_model"))
		h.start()
		tk := newTask(1086)
		tk.PendingLimit = 1
		h.enqueueTask(tk)
		if r := h.waitTaskKind(1086, "released"); r.Body["reason"] != "pending_timeout" {
			t.Fatalf("released = %s", r.Raw)
		}
		h.sigterm()
		// executor_shutdown for created, pending and running.
		h2 := newHarness(t, func(o *opts) { o.maxParallel = 3; o.drain = "1s" })
		h2.setClaimToken(func(b map[string]any) (int, any, error) {
			if asInt(b["body"].(map[string]any)["task_id"]) == 1088 {
				return claimNotProcessed(b) // stays created
			}
			return 200, tokenReply(b, "ct-x"), nil
		})
		h2.setCheck(func(b map[string]any) (int, any, error) {
			if asInt(b["body"].(map[string]any)["task_id"]) == 1089 {
				return checkTemporary("no_active_model")(b) // stays pending
			}
			return 200, map[string]any{"result": "ok", "model_alias": "3/gpt-4o"}, nil
		})
		h2.start()
		h2.runTask(newTask(1087)) // running, not terminal at the deadline
		h2.enqueueTask(newTask(1088))
		h2.waitTaskKind(1088, "created")
		h2.enqueueTask(newTask(1089))
		h2.waitTaskKind(1089, "pending")
		h2.sigterm()
		for _, id := range []int64{1087, 1088, 1089} {
			r := h2.waitTaskKind(id, "released")
			if r.Body["reason"] != "executor_shutdown" {
				t.Fatalf("task %d released = %s", id, r.Raw)
			}
		}
		for _, hh := range []*harness{h, h2} {
			for _, ev := range hh.events() {
				if ev.Kind == "released" && (ev.Body["reason"] == "executor_lost" || ev.Body["reason"] == "claim_missing") {
					t.Fatalf("task-manager release reason published: %s", ev.Raw)
				}
			}
		}
	})
}
