package test

import (
	"errors"
	"testing"
	"time"

	fakebroker "github.com/pandops/flowai-executor-docker-openhands/internal/mocks/broker"
)

// commandAckIndex is the op index of the acknowledgement of a command.
func commandAckIndex(ops []fakebroker.Op) int {
	return opIndex(ops, func(op fakebroker.Op) bool {
		return op.Kind == fakebroker.OpAck && op.Stream == streamCommand && op.Consumer == commandConsumerName(executorID)
	})
}

func (h *harness) slotsFree(t *testing.T) {
	t.Helper()
	eventually(t, "slot freed", func() bool {
		hbs := h.heartbeats()
		return len(hbs) > 0 && asInt(hbs[len(hbs)-1]["slots_in_use"]) == 0 && len(claimsIn(hbs[len(hbs)-1])) == 0
	})
}

// Requirement: Concrete Executors report cancellation progress.
func TestV0048ConcreteExecutorsReportCancellationProgress(t *testing.T) {
	t.Parallel()

	t.Run("Cancellation reaches OpenHands", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		tk := newTask(1250)
		h.runTask(tk)
		ref, _ := h.docker.containerForTask(1250)
		cmd := h.enqueueCancel(tk, 77)
		ack := h.waitTaskKind(1250, "control_acknowledged")
		c := h.waitTaskKind(1250, "cancelled")
		if asInt(ack.Body["control_id"]) != 77 || ack.Body["control"] != "cancel" || asInt(c.Body["control_id"]) != 77 {
			t.Fatalf("control_id not echoed: %s / %s", ack.Raw, c.Raw)
		}
		if ack.Seq >= c.Seq {
			t.Fatalf("cancelled (%d) before control_acknowledged (%d)", c.Seq, ack.Seq)
		}
		if h.docker.openHandsFor(1250).PauseCallCount() != 1 {
			t.Fatalf("pause not sent to the task's OpenHands")
		}
		if h.docker.HasContainer(ref.ID) {
			t.Fatalf("container kept after cancel")
		}
		eventually(t, "command acknowledged", cmd.Acked)
		ops := h.broker.Ops()
		if a, p := commandAckIndex(ops), opIndexSubject(ops, "executor.event.7.cancelled.pub"); a < p {
			t.Fatalf("command acknowledged (%d) before cancelled was published (%d)", a, p)
		}
		h.slotsFree(t)
		if h.hasTaskKind(1250, "finished") || h.hasTaskKind(1250, "failed") {
			t.Fatalf("second terminal event: %v", h.kindsFor(1250))
		}
	})

	t.Run("Cancellation delivery fails", func(t *testing.T) {
		t.Parallel()
		for _, mode := range []string{"rejected", "unreachable"} {
			t.Run(mode, func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, nil)
				h.start()
				tk := newTask(1251)
				h.runTask(tk)
				ref, _ := h.docker.containerForTask(1251)
				oh := h.docker.openHandsFor(1251)
				if mode == "rejected" {
					oh.SetPauseErr(errors.New("conversation cannot be paused"))
				} else {
					oh.Stop()
				}
				cmd := h.enqueueCancel(tk, 78)
				f := h.waitTaskKind(1251, "control_failed")
				if asInt(f.Body["control_id"]) != 78 || f.Body["reason"] == nil || f.Body["reason"] == "" {
					t.Fatalf("control_failed = %s", f.Raw)
				}
				eventually(t, "command acknowledged", cmd.Acked)
				never(t, "cancelled after a failed pause", 500*time.Millisecond, func() bool { return h.hasTaskKind(1251, "cancelled") })
				if mode == "rejected" && !h.docker.HasContainer(ref.ID) {
					t.Fatalf("task stopped after a rejected pause")
				}
			})
		}
	})

	t.Run("Cancel of a pending claim", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.setCheck(checkTemporary("no_active_model"))
		h.start()
		tk := newTask(1252)
		h.enqueueTask(tk)
		h.waitTaskKind(1252, "pending")
		cmd := h.enqueueCancel(tk, 79)
		c := h.waitTaskKind(1252, "cancelled")
		if asInt(c.Body["control_id"]) != 79 {
			t.Fatalf("cancelled = %s", c.Raw)
		}
		eventually(t, "command acknowledged", cmd.Acked)
		h.slotsFree(t)
		checks := len(h.requestsOf("check"))
		time.Sleep(4 * 200 * time.Millisecond) // four recheck intervals
		if n := len(h.requestsOf("check")); n != checks {
			t.Fatalf("checks continued after cancel: %d -> %d", checks, n)
		}
		if _, ok := h.docker.specFor(1252); ok {
			t.Fatalf("runtime started for a cancelled pending claim")
		}
	})

	t.Run("Cancel for a claim the Executor does not hold", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.runTask(newTask(1253))
		other := newTask(1254)
		cmd := h.enqueueCancel(other, 80)
		eventually(t, "command acknowledged", cmd.Acked)
		time.Sleep(200 * time.Millisecond)
		if len(h.eventsFor(1254)) != 0 || h.hasTaskKind(1253, "cancelled") || h.docker.openHandsFor(1253).PauseCallCount() != 0 {
			t.Fatalf("cancel of a foreign claim had an effect")
		}
	})
}
