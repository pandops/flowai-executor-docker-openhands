package test

import (
	"context"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-docker-openhands/internal/cache"
)

// Requirement: Docker Executor cleans up on startup.
func TestV0048DockerExecutorCleansUpOnStartup(t *testing.T) {
	t.Parallel()

	t.Run("Executor finds leftover containers on startup", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.writeCleanupID(t, seededCleanupID)
		a := h.leftover(t, 1160, seededCleanupID)
		b := h.leftover(t, 1161, seededCleanupID)
		foreign := h.leftover(t, 1162, "1234")
		removedBeforePull := make(chan bool, 1)
		h.broker.OnFetch = func(stream, consumer string, batch int) {
			if stream == streamTask {
				select {
				case removedBeforePull <- !h.docker.HasContainer(a.ID) && !h.docker.HasContainer(b.ID):
				default:
				}
			}
		}
		h.start()
		h.waitFirstPull(t)
		if !<-removedBeforePull {
			t.Fatalf("leftover containers still present at the first pull")
		}
		if !h.docker.HasContainer(foreign.ID) {
			t.Fatalf("container of another cleanup_id removed")
		}
	})

	t.Run("Restart within W releases cached claims", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.writeCleanupID(t, seededCleanupID)
		ref := h.leftover(t, 1163, seededCleanupID)
		h.seedCache(t, time.Now().Add(-2*time.Minute), []cache.ClaimRecord{crashedClaim(1163, cache.ClaimRunning, ref.ID)}, nil)
		h.writeStateSeq(t, 100)
		h.start()
		r := h.waitTaskKind(1163, "released")
		if r.Body["reason"] != "executor_restarted" {
			t.Fatalf("released = %s", r.Raw)
		}
		if h.docker.HasContainer(ref.ID) {
			t.Fatalf("container kept")
		}
		h.waitFirstPull(t)
		hbs := h.waitHeartbeats(1)
		if len(claimsIn(hbs[0])) != 0 {
			t.Fatalf("restarted Executor holds claims: %v", claimsIn(hbs[0]))
		}
		ops := h.broker.Ops()
		rel := opIndexSubject(ops, "executor.event.7.released.pub")
		pull := opIndexFetch(ops, streamTask)
		if rel < 0 || pull < rel {
			t.Fatalf("released at %d after first pull %d", rel, pull)
		}
	})

	t.Run("Start after a graceful shutdown releases nothing again", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.setCheck(checkTemporary("no_active_model"))
		h.start()
		h.enqueueTask(newTask(1164))
		h.waitTaskKind(1164, "pending")
		h.sigterm()
		if r := h.waitTaskKind(1164, "released"); r.Body["reason"] != "executor_shutdown" {
			t.Fatalf("released = %s", r.Raw)
		}
		store := h.openCache()
		claims, err := store.ListClaims(context.Background())
		store.Close()
		if err != nil || len(claims) != 0 {
			t.Fatalf("cache after graceful shutdown = %+v, %v", claims, err)
		}
		h.start()
		h.waitFirstPull(t)
		n := 0
		for _, ev := range h.eventsFor(1164) {
			if ev.Kind == "released" {
				n++
				if ev.Body["reason"] == "executor_restarted" {
					t.Fatalf("executor_restarted after a graceful shutdown")
				}
			}
		}
		if n != 1 {
			t.Fatalf("released %d times", n)
		}
	})
}
