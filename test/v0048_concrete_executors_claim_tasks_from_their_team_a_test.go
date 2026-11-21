package test

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	fakebroker "github.com/pandops/flowai-executor-docker-openhands/internal/mocks/broker"
)

func claimRejected(map[string]any) (int, any, error) {
	return http.StatusConflict, map[string]any{"code": "claim_rejected", "message": "another claim won", "request_id": nil}, nil
}

func claimNotProcessed(map[string]any) (int, any, error) {
	return http.StatusConflict, map[string]any{"code": "claim_not_processed", "message": "created not processed", "request_id": nil}, nil
}

// Requirement: Concrete Executors claim tasks from their team and tag consumer.
func TestV0048ConcreteExecutorsClaimTasksFromTheirTeamAndTagConsumer(t *testing.T) {
	t.Parallel()

	t.Run("Executor wins a claim", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		tk := newTask(1030)
		d := h.enqueueTask(tk)
		created := h.waitTaskKind(tk.TaskID, "created")
		if created.Attempt != 1 || created.ClaimSeq != 1 {
			t.Fatalf("created = %s", created.Raw)
		}
		eventually(t, "task message acked", d.Acked)
		h.waitTaskKind(tk.TaskID, "running")
		if calls := h.env.callsFor(tk.TaskID); len(calls) != 1 || calls[0].Auth != "Bearer ct-1030-1-1" {
			t.Fatalf("env open calls = %+v", calls)
		}
		hb := h.waitHeartbeats(len(h.heartbeats()) + 1)
		if got := asInt(hb[len(hb)-1]["slots_in_use"]); got != 1 {
			t.Fatalf("slots_in_use = %d, want 1", got)
		}
	})

	t.Run("Executor loses a claim", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.setClaimToken(claimRejected)
		h.start()
		tk := newTask(1031)
		h.enqueueTask(tk)
		h.waitTaskKind(tk.TaskID, "created")
		eventually(t, "claim-token request", func() bool { return len(h.requestsOf("claim-token")) > 0 })
		never(t, "event after a lost claim", time.Second, func() bool { return len(h.kindsFor(tk.TaskID)) > 1 })
		if _, ok := h.docker.specFor(tk.TaskID); ok {
			t.Fatalf("container started for a lost claim")
		}
		eventually(t, "slot freed", func() bool {
			hbs := h.heartbeats()
			return len(hbs) > 0 && asInt(hbs[len(hbs)-1]["slots_in_use"]) == 0 && len(claimsIn(hbs[len(hbs)-1])) == 0
		})
	})

	t.Run("Claim token requested before any check", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		tk := newTask(1032)
		d := h.enqueueTask(tk)
		created := h.waitTaskKind(tk.TaskID, "created")
		h.waitTaskKind(tk.TaskID, "running")
		ops := h.broker.Ops()
		pubCreated := opIndex(ops, func(op fakebroker.Op) bool {
			return op.Kind == fakebroker.OpPublish && op.Acked && strings.HasSuffix(op.Subject, ".created.pub")
		})
		ack := opIndex(ops, func(op fakebroker.Op) bool { return op.Kind == fakebroker.OpAck && op.Stream == streamTask })
		claim := opIndex(ops, func(op fakebroker.Op) bool { return strings.HasSuffix(op.Subject, ".claim-token.pub") })
		check := opIndex(ops, func(op fakebroker.Op) bool { return strings.HasSuffix(op.Subject, ".check.pub") })
		if pubCreated >= ack || ack >= claim || claim >= check || !d.Acked() {
			t.Fatalf("order created=%d ack=%d claim=%d check=%d", pubCreated, ack, claim, check)
		}
		reqs := h.requestsOf("claim-token")
		b := bodyOf(reqs[0])
		if asInt(b["task_id"]) != tk.TaskID || asInt(b["attempt"]) != 1 || asInt(b["claim_seq"]) != 1 || asInt(b["created_publisher_seq"]) != created.Seq {
			t.Fatalf("claim-token body = %s", reqs[0].Raw)
		}
		if env := h.env.callsFor(tk.TaskID); len(env) == 0 || env[0].At.Before(reqs[0].At) {
			t.Fatalf("env open before claim-token request")
		}
	})

	t.Run("task-manager has not yet processed the created event", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		var n atomic.Int32
		h.setClaimToken(func(body map[string]any) (int, any, error) {
			if n.Add(1) <= 3 {
				return claimNotProcessed(body)
			}
			b := body["body"].(map[string]any)
			return http.StatusOK, tokenReply(body, "ct-late-"+strings.TrimSpace(asString(b["task_id"]))), nil
		})
		h.start()
		tk := newTask(1033)
		h.enqueueTask(tk)
		h.waitTaskKind(tk.TaskID, "running")
		reqs := h.requestsOf("claim-token")
		if len(reqs) != 4 {
			t.Fatalf("claim-token requests = %d, want 4", len(reqs))
		}
		for i := 1; i < len(reqs); i++ {
			gap := reqs[i].At.Sub(reqs[i-1].At)
			if gap < 900*time.Millisecond || gap > 1600*time.Millisecond {
				t.Fatalf("repeat %d after %s, want about 1s", i, gap)
			}
			if string(reqs[i].Raw) != string(reqs[0].Raw) {
				t.Fatalf("repeat body changed: %s vs %s", reqs[i].Raw, reqs[0].Raw)
			}
		}
		if calls := h.env.callsFor(tk.TaskID); len(calls) == 0 || calls[0].Auth != "Bearer ct-late-1033" {
			t.Fatalf("checks did not use the late token: %+v", calls)
		}
	})

	t.Run("Claim token never arrives within 10 s", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.setClaimToken(claimNotProcessed)
		h.start()
		tk := newTask(1034)
		h.enqueueTask(tk)
		h.waitTaskKind(tk.TaskID, "created")
		eventually(t, "claim dropped", func() bool {
			hbs := h.heartbeats()
			return len(h.requestsOf("claim-token")) > 0 && len(hbs) > 0 && len(claimsIn(hbs[len(hbs)-1])) == 0 &&
				time.Since(h.requestsOf("claim-token")[0].At) > 10*time.Second
		})
		reqs := h.requestsOf("claim-token")
		if span := reqs[len(reqs)-1].At.Sub(reqs[0].At); span > 10500*time.Millisecond || len(reqs) < 9 || len(reqs) > 12 {
			t.Fatalf("%d requests over %s, want repeats every 1 s for 10 s", len(reqs), span)
		}
		if len(h.kindsFor(tk.TaskID)) != 1 {
			t.Fatalf("events for a dropped claim: %v", h.kindsFor(tk.TaskID))
		}
		if _, ok := h.docker.specFor(tk.TaskID); ok {
			t.Fatalf("runtime started")
		}
	})

	t.Run("Executor pulls only up to free slots", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, func(o *opts) { o.maxParallel = 3 })
		h.start()
		h.runTask(newTask(1035))
		h.runTask(newTask(1036))
		for id := int64(1037); id <= 1041; id++ {
			h.enqueueTask(newTask(id))
		}
		h.waitTaskKind(1037, "created")
		time.Sleep(500 * time.Millisecond)
		if w := h.broker.Waiting(streamTask, taskConsumerName(teamID, tagName)); w != 4 {
			t.Fatalf("waiting = %d, want 4 left on the consumer", w)
		}
		for _, b := range h.taskFetches() {
			if b > 3 {
				t.Fatalf("pull batch %d exceeds max_parallel_tasks", b)
			}
		}
		fetches := h.taskFetches()
		if fetches[len(fetches)-1] > 1 {
			// after three slots are used, a pull may only happen with a free slot
			t.Fatalf("pull batch %d with no free slot", fetches[len(fetches)-1])
		}
	})

	t.Run("Executor takes tasks in queue order", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		// A's first claim-token reply is slow, as on a loaded host: the
		// Executor does not move on to B before that exchange ends.
		var aReplied atomic.Int64
		h.setClaimToken(func(body map[string]any) (int, any, error) {
			b := body["body"].(map[string]any)
			if fmt.Sprint(b["task_id"]) == "1042" {
				time.Sleep(300 * time.Millisecond)
				aReplied.CompareAndSwap(0, time.Now().UnixNano())
			}
			return http.StatusOK, tokenReply(body, fmt.Sprintf("ct-%v-%v-%v", b["task_id"], b["attempt"], b["claim_seq"])), nil
		})
		h.enqueueTask(newTask(1042))
		h.enqueueTask(newTask(1043))
		h.start()
		a := h.waitTaskKind(1042, "created")
		b := h.waitTaskKind(1043, "created")
		if a.Seq >= b.Seq {
			t.Fatalf("created for B (seq %d) before A (seq %d)", b.Seq, a.Seq)
		}
		eventually(t, "claim-token requests for A and B", func() bool { return len(h.requestsOf("claim-token")) >= 2 })
		for _, r := range h.requestsOf("claim-token") {
			if fmt.Sprint(bodyOf(r)["task_id"]) != "1043" {
				continue
			}
			if done := aReplied.Load(); done == 0 || r.At.UnixNano() < done {
				t.Fatalf("claim-token request for B sent before A's first claim-token exchange ended")
			}
			break
		}
	})

	for _, c := range []struct {
		name string
		mut  func(map[string]any)
	}{
		{"Task message with a non-integer identifier is refused", func(m map[string]any) { m["task_id"] = "1010" }},
		{"Task message with a non-integer identifier is refused (zero)", func(m map[string]any) { m["task_id"] = 0 }},
		{"Foreign task message is refused", func(m map[string]any) { m["team_id"] = 4 }},
		{"Foreign task message is refused (tag)", func(m map[string]any) { m["execution"].(map[string]any)["required_tag"] = "cpu" }},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, nil)
			h.start()
			tk := newTask(1044)
			body := tk.body()
			c.mut(body)
			tk.raw = body
			d := h.enqueueTask(tk)
			eventually(t, "message terminated", d.Termed)
			pv := h.waitSelfKind("protocol_violation")
			if pv.Body["reason"] == "" {
				t.Fatalf("protocol_violation without reason: %s", pv.Raw)
			}
			if d.Acked() {
				t.Fatalf("message acked instead of terminated")
			}
			for _, ev := range h.events() {
				if ev.Kind == "created" {
					t.Fatalf("created published for a refused message")
				}
			}
			if len(h.docker.specsSnapshot()) != 0 || len(h.requestsOf("claim-token")) != 0 {
				t.Fatalf("refused message reached claim or runtime")
			}
		})
	}
}

func asString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case float64:
		return strconv.FormatInt(int64(s), 10)
	}
	return ""
}
