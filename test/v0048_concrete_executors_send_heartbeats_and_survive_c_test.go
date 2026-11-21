package test

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-docker-openhands/internal/executor"
	"github.com/pandops/flowai-executor-docker-openhands/internal/netaddr"
)

func heartbeatListing(ids ...int64) func(map[string]any) (int, any, error) {
	return func(map[string]any) (int, any, error) {
		claims := []any{}
		for _, id := range ids {
			claims = append(claims, map[string]any{"task_id": id, "attempt": 1, "claim_seq": 1})
		}
		return http.StatusOK, map[string]any{"connection_state": "connection_lost", "claims_known": true, "claims": claims}, nil
	}
}

func claimsUnknown(map[string]any) (int, any, error) {
	return http.StatusOK, map[string]any{"connection_state": "connection_lost", "claims_known": false}, nil
}

// enterLoss makes heartbeats unanswered and waits until both agents pause.
func (h *harness) enterLoss(t *testing.T, ids ...int64) {
	t.Helper()
	h.setHeartbeat(noReply)
	for _, id := range ids {
		oh := h.docker.openHandsFor(id)
		eventually(t, "agent paused", func() bool { return oh.PauseCallCount() > 0 })
	}
}

func (h *harness) pullsSince(n int) int { return len(h.taskFetches()) - n }

// Requirement: Concrete Executors send heartbeats and survive connection loss.
func TestV0048ConcreteExecutorsSendHeartbeatsAndSurviveConnectionLoss(t *testing.T) {
	t.Parallel()

	t.Run("Three missed heartbeats pause the agents", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.runTask(newTask(1100))
		h.runTask(newTask(1101))
		n := len(h.heartbeats())
		h.setHeartbeat(noReply)
		h.waitHeartbeats(n + 2)
		if h.docker.openHandsFor(1100).PauseCallCount() != 0 {
			t.Fatalf("paused after two misses")
		}
		h.enterLoss(t, 1100, 1101)
		if len(h.heartbeats()) < n+3 {
			t.Fatalf("paused before the third miss")
		}
		for _, id := range []int64{1100, 1101} {
			if _, ok := h.docker.containerForTask(id); !ok {
				t.Fatalf("container of %d removed", id)
			}
		}
		pulls := len(h.taskFetches())
		h.waitHeartbeats(len(h.heartbeats()) + 2) // heartbeats continue
		if h.pullsSince(pulls) > 0 {
			t.Fatalf("pulled during connection loss")
		}
	})

	t.Run("Connection restored within W", func(t *testing.T) {
		t.Parallel()
		// A third slot stays free, so resumed pulling is observable.
		h := newHarness(t, func(o *opts) { o.maxParallel = 3 })
		h.start()
		h.runTask(newTask(1102))
		h.runTask(newTask(1103))
		h.enterLoss(t, 1102, 1103)
		pulls := len(h.taskFetches())
		h.setHeartbeat(heartbeatListing(1102, 1103))
		lost := h.waitSelfKind("executor_connection_lost")
		rest := h.waitSelfKind("executor_connection_restored")
		if lost.Body["lost_at"] == nil || rest.Body["restored_at"] == nil || rest.Seq <= lost.Seq {
			t.Fatalf("observations = %s / %s", lost.Raw, rest.Raw)
		}
		for _, id := range []int64{1102, 1103} {
			oh := h.docker.openHandsFor(id)
			eventually(t, "continue sent", func() bool { return oh.RunCallCount() == 1 })
		}
		eventually(t, "pulling resumed", func() bool { return h.pullsSince(pulls) > 0 })
	})

	t.Run("Reply omits a claim after a late restore", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.runTask(newTask(1104))
		h.runTask(newTask(1105))
		h.enterLoss(t, 1104, 1105)
		before := len(h.eventsFor(1105))
		h.setHeartbeat(heartbeatListing(1104))
		h.waitSelfKind("executor_connection_restored")
		eventually(t, "1104 continued", func() bool { return h.docker.openHandsFor(1104).RunCallCount() == 1 })
		eventually(t, "1105 removed", func() bool { _, ok := h.docker.containerForTask(1105); return !ok })
		if h.docker.openHandsFor(1105).RunCallCount() != 0 || len(h.eventsFor(1105)) != before {
			t.Fatalf("omitted claim continued or got events: %v", h.kindsFor(1105))
		}
		h.setHeartbeat(func(b map[string]any) (int, any, error) { return http.StatusOK, heartbeatReplyListing(b), nil })
		eventually(t, "omitted claim gone from heartbeats", func() bool {
			hbs := h.heartbeats()
			return strings.Join(claimsIn(hbs[len(hbs)-1]), ",") == "1104/1/1=running"
		})
	})

	t.Run("Reply without known claims changes no child", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, func(o *opts) { o.wait = "4s" })
		h.start()
		h.runTask(newTask(1106))
		h.enterLoss(t, 1106)
		pulls := len(h.taskFetches())
		h.setHeartbeat(claimsUnknown)
		time.Sleep(6 * time.Second) // past W
		if _, ok := h.docker.containerForTask(1106); !ok {
			t.Fatalf("container removed at W although heartbeats succeeded")
		}
		oh := h.docker.openHandsFor(1106)
		if oh.RunCallCount() != 0 || h.pullsSince(pulls) != 0 || len(h.selfKinds()) != 1 {
			t.Fatalf("changed state on claims_known=false: run=%d pulls=%d self=%v", oh.RunCallCount(), h.pullsSince(pulls), h.selfKinds())
		}
		h.setHeartbeat(heartbeatListing(1106))
		h.waitSelfKind("executor_connection_restored")
		eventually(t, "continue", func() bool { return oh.RunCallCount() == 1 })
		eventually(t, "pull resumed", func() bool { return h.pullsSince(pulls) > 0 })
	})

	t.Run("Just-published claim is not dropped by a heartbeat reply", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		release := make(chan struct{})
		var acked atomic.Value
		h.setClaimToken(func(b map[string]any) (int, any, error) {
			<-release
			acked.Store(time.Now())
			return http.StatusOK, tokenReply(b, "ct-1107"), nil
		})
		// Every reply omits every claim.
		h.setHeartbeat(func(map[string]any) (int, any, error) {
			return http.StatusOK, map[string]any{"connection_state": "connected", "claims_known": true, "claims": []any{}}, nil
		})
		h.start()
		h.enqueueTask(newTask(1107))
		h.waitTaskKind(1107, "created")
		n := len(h.heartbeats())
		hbs := h.waitHeartbeats(n + 2)
		if got := claimsIn(hbs[len(hbs)-1]); len(got) != 1 || got[0] != "1107/1/1=created" {
			t.Fatalf("claim dropped before its claim-token reply: %v", got)
		}
		close(release)
		eventually(t, "claim dropped after a heartbeat sent after the ack", func() bool {
			hbs := h.heartbeats()
			return len(claimsIn(hbs[len(hbs)-1])) == 0
		})
		at := acked.Load().(time.Time)
		// Heartbeats from before the task arrived carry no claim at all.
		for _, r := range h.requestsOf("heartbeat")[n:] {
			if r.At.Before(at) && len(claimsIn(bodyOf(r))) == 0 {
				t.Fatalf("claim missing from a heartbeat sent before the ack")
			}
		}
		if _, ok := h.docker.containerForTask(1107); ok {
			t.Fatalf("container of a dropped claim kept")
		}
		for _, k := range h.kindsFor(1107) {
			if k == "released" || k == "failed" || k == "finished" || k == "cancelled" {
				t.Fatalf("event %s for a dropped claim", k)
			}
		}
	})

	t.Run("Connection not restored within W", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, func(o *opts) { o.wait = "4s" })
		h.start()
		h.runTask(newTask(1108))
		h.runTask(newTask(1109))
		h.enterLoss(t, 1108, 1109)
		eventually(t, "children removed at W", func() bool {
			_, a := h.docker.containerForTask(1108)
			_, b := h.docker.containerForTask(1109)
			return !a && !b
		})
		pulls := len(h.taskFetches())
		h.setHeartbeat(func(b map[string]any) (int, any, error) { return http.StatusOK, heartbeatReplyListing(b), nil })
		h.waitSelfKind("executor_connection_lost")
		h.waitSelfKind("executor_connection_restored")
		eventually(t, "pull", func() bool { return h.pullsSince(pulls) > 0 })
		hbs := h.heartbeats()
		if len(claimsIn(hbs[len(hbs)-1])) != 0 {
			t.Fatalf("claims held after W: %v", claimsIn(hbs[len(hbs)-1]))
		}
		for _, id := range []int64{1108, 1109} {
			if h.hasTaskKind(id, "released") {
				t.Fatalf("released published for a claim dropped at W")
			}
		}
	})

	t.Run("Heartbeat carries claims and capacity", func(t *testing.T) {
		t.Parallel()
		// Default timing: the three keys are absent from the configuration.
		h := newHarness(t, func(o *opts) {
			o.maxParallel = 4
			o.omitKeys = []string{"heartbeat_interval", "heartbeat_miss_limit", "connection_wait"}
		})
		cfg, err := executor.LoadConfig(h.cfgPath)
		if err != nil || cfg.HeartbeatInterval != 15*time.Second || cfg.HeartbeatMissLimit != 3 || cfg.ConnectionWait != 600*time.Second {
			t.Fatalf("default timing = %v / %v / %v, %v; want 15s / 3 / 600s", cfg.HeartbeatInterval, cfg.HeartbeatMissLimit, cfg.ConnectionWait, err)
		}
		// 1110 stays created: its first claim-token request is answered
		// claim_not_processed (the Executor takes the next message only after
		// that exchange) and the first repeat's reply is held for the whole
		// case (an unanswered claim would be dropped after 10 s, before the
		// second heartbeat at 15 s).
		hold := make(chan struct{})
		var asked atomic.Int64
		h.setClaimToken(func(b map[string]any) (int, any, error) {
			if asInt(b["body"].(map[string]any)["task_id"]) == 1110 {
				if asked.Add(1) > 1 {
					<-hold
				}
				return claimNotProcessed(b)
			}
			return 200, tokenReply(b, "ct-y"), nil
		})
		h.setCheck(func(b map[string]any) (int, any, error) {
			if asInt(b["body"].(map[string]any)["task_id"]) == 1111 {
				return checkTemporary("no_active_model")(b)
			}
			return 200, map[string]any{"result": "ok", "model_alias": "3/gpt-4o"}, nil
		})
		h.start()
		t.Cleanup(func() { close(hold) }) // runs before the harness stops the Executor
		h.waitHeartbeats(1)
		h.enqueueTask(newTask(1110))
		h.enqueueTask(newTask(1111))
		h.enqueueTask(newTask(1112))
		h.waitTaskKind(1111, "pending")
		h.waitTaskKind(1112, "running")
		// The second heartbeat comes one default interval (15 s) later.
		deadline := time.Now().Add(25 * time.Second)
		for len(h.heartbeats()) < 2 && time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
		}
		hbs := h.heartbeats()
		if len(hbs) < 2 {
			t.Fatalf("second heartbeat not sent within 25 s")
		}
		if reqs := h.requestsOf("heartbeat"); reqs[1].At.Sub(reqs[0].At) < 14*time.Second {
			t.Fatalf("heartbeats %s apart, want the 15 s default", reqs[1].At.Sub(reqs[0].At))
		}
		hb := hbs[1]
		if asInt(hb["interval_seconds"]) != 15 || asInt(hb["miss_limit"]) != 3 || asInt(hb["wait_seconds"]) != 600 ||
			asInt(hb["max_parallel_tasks"]) != 4 || asInt(hb["slots_in_use"]) != 3 || hb["executor_type"] != execType {
			t.Fatalf("heartbeat = %v", hb)
		}
		if got := strings.Join(claimsIn(hb), ","); got != "1110/1/1=created,1111/1/1=pending,1112/1/1=running" {
			t.Fatalf("claims = %s", got)
		}
		for _, r := range h.requestsOf("heartbeat") {
			if strings.Contains(string(r.Raw), "ct-") {
				t.Fatalf("heartbeat carries a claim token")
			}
			h.schema.validate(t, erHeartbeatReqSchema, r.Raw)
		}
	})

	t.Run("Configured miss limit decides connection loss", func(t *testing.T) {
		t.Parallel()
		// Scaled from 10 s to 1 s per heartbeat; the miss limit is 5.
		h := newHarness(t, func(o *opts) { o.heartbeat = "1s"; o.missLimit = 5 })
		h.start()
		h.runTask(newTask(1113))
		n := len(h.heartbeats())
		h.setHeartbeat(noReply)
		oh := h.docker.openHandsFor(1113)
		h.waitHeartbeats(n + 4)
		time.Sleep(200 * time.Millisecond)
		if oh.PauseCallCount() != 0 {
			t.Fatalf("connection loss after four misses")
		}
		eventually(t, "loss after the fifth miss", func() bool { return oh.PauseCallCount() > 0 })
		for _, hb := range h.heartbeats() {
			if asInt(hb["interval_seconds"]) != 1 || asInt(hb["miss_limit"]) != 5 {
				t.Fatalf("heartbeat timing = %v", hb)
			}
		}
	})

	t.Run("Heartbeat reports the primary interface address", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.net.set(netaddr.Snapshot{DefaultIface4: "eth0", DefaultIface6: "eth0", Addrs: map[string][]netipPrefix{
			"eth0":    prefixes(t, "192.0.2.10/24", "2001:db8:4::21/64"),
			"docker0": prefixes(t, "172.17.0.1/16"),
		}})
		h.start()
		if hb := h.waitHeartbeats(1)[0]; hb["ip"] != "192.0.2.10" {
			t.Fatalf("ip = %v", hb["ip"])
		}
	})

	t.Run("IPv6-only primary interface", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.net.set(netaddr.Snapshot{DefaultIface6: "eth0", Addrs: map[string][]netipPrefix{
			"eth0": prefixes(t, "fe80::1/64", "2001:db8:4:0:0:0:0:21/64"),
		}})
		h.start()
		if hb := h.waitHeartbeats(1)[0]; hb["ip"] != "2001:db8:4::21" {
			t.Fatalf("ip = %v", hb["ip"])
		}
	})

	t.Run("Changed address is reported in the next heartbeat", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.net.set(netaddr.Snapshot{DefaultIface4: "eth0", Addrs: map[string][]netipPrefix{"eth0": prefixes(t, "10.42.0.9/24")}})
		h.start()
		if hb := h.waitHeartbeats(1)[0]; hb["ip"] != "10.42.0.9" {
			t.Fatalf("ip = %v", hb["ip"])
		}
		h.net.set(netaddr.Snapshot{DefaultIface4: "eth0", Addrs: map[string][]netipPrefix{"eth0": prefixes(t, "10.42.0.17/24")}})
		eventually(t, "new address", func() bool {
			hbs := h.heartbeats()
			return hbs[len(hbs)-1]["ip"] == "10.42.0.17"
		})
	})

	t.Run("No default route still sends the heartbeat", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.net.set(netaddr.Snapshot{Addrs: map[string][]netipPrefix{"eth0": prefixes(t, "10.0.0.5/24")}})
		h.start()
		h.runTask(newTask(1114))
		hbs := h.waitHeartbeats(3)
		for i, hb := range hbs {
			if v, ok := hb["ip"]; !ok || v != nil {
				t.Fatalf("heartbeat %d ip = %v (present %v), want null", i, v, ok)
			}
		}
		reqs := h.requestsOf("heartbeat")
		for i := 1; i < len(reqs); i++ {
			if gap := reqs[i].At.Sub(reqs[i-1].At); gap > 1500*time.Millisecond {
				t.Fatalf("heartbeat late by %s", gap)
			}
		}
		warns := strings.Count(h.logs.String(), "heartbeat_ip_unavailable")
		if warns < 1 || warns > len(reqs)+1 {
			t.Fatalf("%d heartbeat_ip_unavailable warnings for %d heartbeats", warns, len(reqs))
		}
		if code, _ := h.probe("/v1/readyz"); code != http.StatusOK {
			t.Fatalf("readyz = %d; no address must not affect liveness or claims", code)
		}
	})
}
