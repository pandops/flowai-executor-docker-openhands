package test

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-docker-openhands/internal/cache"
	"github.com/pandops/flowai-executor-docker-openhands/internal/executor"
	fakebroker "github.com/pandops/flowai-executor-docker-openhands/internal/mocks/broker"
)

// Requirement: Concrete Executors reconcile after restart without
// reassignment, in its Docker form (the K8s Pod scenarios name Pods; the
// Docker Executor removes containers instead).
func TestV0048ConcreteExecutorsReconcileAfterRestartWithoutReassignment(t *testing.T) {
	t.Parallel()

	t.Run("Restart after W drops cached claims", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil) // W = 600s
		h.writeCleanupID(t, seededCleanupID)
		ref := h.leftover(t, 1220, seededCleanupID)
		h.seedCache(t, time.Now().Add(-12*time.Minute),
			[]cache.ClaimRecord{crashedClaim(1220, cache.ClaimRunning, ref.ID)},
			[]cache.OutboxRecord{outboxEvent(11, 1220, "running", cache.OutboxPending)})
		h.writeStateSeq(t, 100)
		h.start()
		h.waitFirstPull(t)
		if h.docker.HasContainer(ref.ID) {
			t.Fatalf("container of a claim cached before W kept")
		}
		hb := h.waitHeartbeats(1)[0]
		if len(claimsIn(hb)) != 0 {
			t.Fatalf("claims held after W: %v", claimsIn(hb))
		}
		never(t, "event for a claim dropped after W", 500*time.Millisecond, func() bool { return len(h.eventsFor(1220)) > 0 })
		h.sigterm()
		store := h.openCache()
		defer store.Close()
		ctx := context.Background()
		claims, _ := store.ListClaims(ctx)
		pending, _ := store.PendingOutbox(ctx)
		if len(claims) != 0 || len(pending) != 0 {
			t.Fatalf("cache after W: claims %+v, outbox %+v", claims, pending)
		}
	})

	t.Run("Docker Executor keeps its token and cache on volumes", func(t *testing.T) {
		t.Parallel()
		// Empty cache volume, mounted configuration without a token and
		// world-readable; the first start runs with --token.
		h := newHarness(t, func(o *opts) { o.tokenInYAML = false; o.cfgMode = 0o644 })
		if entries, _ := os.ReadDir(h.cacheDir); len(entries) != 0 {
			t.Fatalf("cache volume not empty")
		}
		cacheBeforePull := make(chan bool, 1)
		h.broker.OnFetch = func(stream, consumer string, batch int) {
			if stream == streamTask {
				_, err := os.Stat(filepath.Join(h.cacheDir, "executor.db"))
				select {
				case cacheBeforePull <- err == nil:
				default:
				}
			}
		}
		h.startCLI("--config", h.cfgPath, "--token", h.token)
		info, err := os.Stat(h.cfgPath)
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("config file mode = %v, %v; want 0600", info.Mode().Perm(), err)
		}
		raw, _ := os.ReadFile(h.cfgPath)
		if !strings.Contains(string(raw), h.token) || !strings.Contains(string(raw), "env_registry_url:") {
			t.Fatalf("token not stored in the YAML, or other keys lost:\n%s", raw)
		}
		h.waitFirstPull(t)
		if !<-cacheBeforePull {
			t.Fatalf("recovery cache not created before the first pull")
		}
		if info, err := os.Stat(h.cacheDir); err != nil || info.Mode().Perm() != 0o700 {
			t.Fatalf("cache dir mode = %v, %v; want owner-only", info.Mode().Perm(), err)
		}
		h.sigterm()

		// Restart: no --token; the stored JWT and the cache are reused.
		h.broker.OnFetch = nil
		pulls := len(h.taskFetches())
		h.startCLI("--config", h.cfgPath)
		eventually(t, "first pull after restart", func() bool { return len(h.taskFetches()) > pulls })
		dials := h.broker.Dials()
		if len(dials) != 2 || dials[0].Token != h.token || dials[1].Token != h.token {
			t.Fatalf("dials = %d; restart did not reuse the stored JWT", len(dials))
		}
		h.sigterm()
		store := h.openCache()
		defer store.Close()
		if id, err := store.ExecutorID(context.Background()); err != nil || id != executorID {
			t.Fatalf("cached executor_id = %d, %v", id, err)
		}
	})

	t.Run("Second process cannot use the same cache volume", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.writeCleanupID(t, seededCleanupID)
		ref := h.leftover(t, 1221, seededCleanupID)
		holder := h.openCache() // the first process holds executor.lock
		defer holder.Close()
		h.start()
		err, exited := h.waitExit(5 * time.Second)
		if !exited || !errIs(err, executor.ErrCacheLocked) {
			t.Fatalf("Run = %v (exited %v), want ErrCacheLocked", err, exited)
		}
		if code, _ := h.probe("/v1/readyz"); code != http.StatusServiceUnavailable {
			t.Fatalf("readyz = %d with the cache lock held elsewhere", code)
		}
		for _, op := range h.broker.Ops() {
			if op.Kind == fakebroker.OpDial || op.Kind == fakebroker.OpPublish || op.Kind == fakebroker.OpRequest || op.Kind == fakebroker.OpFetch {
				t.Fatalf("broker activity without the cache lock: %+v", op)
			}
		}
		if !h.docker.HasContainer(ref.ID) || len(h.docker.specsSnapshot()) != 0 {
			t.Fatalf("container runtime mutated without the cache lock")
		}
	})

	t.Run("Restart does not duplicate a running event", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.writeCleanupID(t, seededCleanupID)
		ref := h.leftover(t, 1222, seededCleanupID)
		h.seedCache(t, time.Now().Add(-3*time.Minute),
			[]cache.ClaimRecord{crashedClaim(1222, cache.ClaimRunning, ref.ID)},
			[]cache.OutboxRecord{outboxEvent(10, 1222, "created", cache.OutboxSent), outboxEvent(11, 1222, "running", cache.OutboxSent)})
		h.writeStateSeq(t, 100)
		h.start()
		r := h.waitTaskKind(1222, "released")
		if r.Body["reason"] != "executor_restarted" {
			t.Fatalf("released = %s", r.Raw)
		}
		h.waitFirstPull(t)
		time.Sleep(300 * time.Millisecond)
		if k := h.kindsFor(1222); len(k) != 1 || k[0] != "released" {
			t.Fatalf("events after restart = %v; want only released", k)
		}
		if h.docker.HasContainer(ref.ID) || len(h.docker.specsSnapshot()) != 0 {
			t.Fatalf("container kept or recreated after restart")
		}
		for _, op := range h.broker.Requests() {
			if strings.Contains(op.Subject, "claim-token") {
				t.Fatalf("cached claim re-claimed after restart")
			}
		}
	})

	t.Run("Restart republishes pending outbox events with their original publisher_seq", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.writeCleanupID(t, seededCleanupID)
		ref := h.leftover(t, 1223, seededCleanupID)
		h.seedCache(t, time.Now().Add(-3*time.Minute),
			[]cache.ClaimRecord{crashedClaim(1223, cache.ClaimRunning, ref.ID)},
			[]cache.OutboxRecord{outboxEvent(10, 1223, "created", cache.OutboxSent), outboxEvent(11, 1223, "running", cache.OutboxPending)})
		h.writeStateSeq(t, 100)
		h.start()
		rel := h.waitTaskKind(1223, "released")
		evs := h.eventsFor(1223)
		if len(evs) != 2 || evs[0].Kind != "running" || evs[0].Seq != 11 || evs[0].MsgID != "executor-3-7-11" {
			t.Fatalf("republished outbox = %+v", evs)
		}
		if rel.Seq <= 100 {
			t.Fatalf("released took publisher_seq %d at or below the stored counter", rel.Seq)
		}
		h.sigterm()
		store := h.openCache()
		defer store.Close()
		if p, _ := store.PendingOutbox(context.Background()); len(p) != 0 {
			t.Fatalf("outbox still pending after acknowledgement: %+v", p)
		}
	})
}
