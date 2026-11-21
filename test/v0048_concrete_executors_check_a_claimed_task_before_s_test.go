package test

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pandops/flowai-executor-docker-openhands/internal/executor"
)

func checkTemporary(reason string) func(map[string]any) (int, any, error) {
	return func(map[string]any) (int, any, error) {
		return http.StatusOK, map[string]any{"result": "temporary", "reason": reason}, nil
	}
}

func checkError(status int, code string) func(map[string]any) (int, any, error) {
	return func(map[string]any) (int, any, error) {
		return status, map[string]any{"code": code, "message": code, "request_id": nil}, nil
	}
}

// pendingReasons returns the reasons of every pending event of a task.
func (h *harness) pendingReasons(taskID int64) []string {
	var out []string
	for _, ev := range h.eventsFor(taskID) {
		if ev.Kind == "pending" {
			out = append(out, ev.Body["reason"].(string))
		}
	}
	return out
}

// assertDropped checks a claim the Executor took and then dropped. A heartbeat
// sent before the claim also has no claims, so the drop counts only on a
// heartbeat sent after the claim's created event.
func (h *harness) assertDropped(t *testing.T, taskID int64) {
	t.Helper()
	h.waitTaskKind(taskID, "created")
	before := len(h.heartbeats())
	eventually(t, "claim dropped from heartbeats", func() bool {
		hbs := h.heartbeats()
		return len(hbs) > before && len(claimsIn(hbs[len(hbs)-1])) == 0 && asInt(hbs[len(hbs)-1]["slots_in_use"]) == 0
	})
	if k := h.kindsFor(taskID); len(k) != 1 || k[0] != "created" {
		t.Fatalf("events for a dropped claim: %v", k)
	}
	if _, ok := h.docker.specFor(taskID); ok {
		t.Fatalf("runtime started for a dropped claim")
	}
}

// Requirement: Concrete Executors check a claimed task before starting the agent.
func TestV0048ConcreteExecutorsCheckAClaimedTaskBeforeStartingTheAgent(t *testing.T) {
	t.Parallel()

	t.Run("Model check answers claim_not_current", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.setCheck(checkError(http.StatusNotFound, "claim_not_current"))
		h.start()
		h.enqueueTask(newTask(1050))
		h.waitTaskKind(1050, "created")
		h.assertDropped(t, 1050)
		h.sigterm()
		store := h.openCache()
		defer store.Close()
		if claims, err := store.ListClaims(t.Context()); err != nil || len(claims) != 0 {
			t.Fatalf("cache still holds %+v, %v", claims, err)
		}
	})

	t.Run("Paused models keep the task pending", func(t *testing.T) {
		t.Parallel()
		// Default recheck interval: the key is absent from the configuration.
		h := newHarness(t, func(o *opts) { o.omitKeys = []string{"pending_recheck_interval"} })
		if cfg, err := executor.LoadConfig(h.cfgPath); err != nil || cfg.PendingRecheckInterval != 30*time.Second {
			t.Fatalf("default pending_recheck_interval = %v, %v; want 30s", cfg.PendingRecheckInterval, err)
		}
		h.setCheck(checkTemporary("all_selected_models_paused"))
		h.start()
		h.enqueueTask(newTask(1052))
		ev := h.waitTaskKind(1052, "pending")
		if ev.Body["reason"] != "all_selected_models_paused" {
			t.Fatalf("pending = %s", ev.Raw)
		}
		never(t, "recheck before 30 s", 2*time.Second, func() bool { return len(h.requestsOf("check")) > 1 })
		hbs := h.heartbeats()
		last := hbs[len(hbs)-1]
		if got := strings.Join(claimsIn(last), ","); got != "1052/1/1=pending" || asInt(last["slots_in_use"]) != 1 {
			t.Fatalf("heartbeat claims %s, slots_in_use %v; want the pending claim holding its slot", got, last["slots_in_use"])
		}
		if _, ok := h.docker.specFor(1052); ok {
			t.Fatalf("runtime started")
		}
	})

	t.Run("Paused models keep the task pending (recheck happens)", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, func(o *opts) { o.recheck = "300ms" })
		h.setCheck(checkTemporary("all_selected_models_paused"))
		h.start()
		h.enqueueTask(newTask(1058))
		h.waitTaskKind(1058, "pending")
		eventually(t, "recheck after the interval", func() bool { return len(h.requestsOf("check")) >= 3 })
		if r := h.pendingReasons(1058); len(r) != 1 {
			t.Fatalf("pending republished with an unchanged reason: %v", r)
		}
		if _, ok := h.docker.specFor(1058); ok {
			t.Fatalf("runtime started while paused")
		}
	})

	t.Run("Model check carries no claim token", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.runTask(newTask(1053))
		reqs := h.requestsOf("check")
		if len(reqs) == 0 || reqs[0].Subject != "model-registry.executor.7.check.pub" {
			t.Fatalf("check requests = %+v", reqs)
		}
		b := bodyOf(reqs[0])
		if len(b) != 3 || asInt(b["task_id"]) != 1053 || asInt(b["attempt"]) != 1 || asInt(b["claim_seq"]) != 1 {
			t.Fatalf("check body = %s", reqs[0].Raw)
		}
		if strings.Contains(string(reqs[0].Raw), "ct-1053") || strings.Contains(strings.ToLower(reqs[0].Header.Get("Authorization")), "ct-") {
			t.Fatalf("check carries the claim token")
		}
		h.schema.validate(t, mrCheckReqSchema, reqs[0].Raw)
	})

	t.Run("Pending task starts once the reason clears", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		var ok atomic.Bool
		h.setCheck(func(b map[string]any) (int, any, error) {
			if ok.Load() {
				return http.StatusOK, map[string]any{"result": "ok", "model_alias": "3/gpt-4o"}, nil
			}
			return checkTemporary("no_active_model")(b)
		})
		h.start()
		h.enqueueTask(newTask(1055))
		h.waitTaskKind(1055, "pending")
		ok.Store(true)
		h.waitTaskKind(1055, "running")
		if k := h.kindsFor(1055); strings.Join(k, ",") != "created,pending,running" {
			t.Fatalf("kinds = %v", k)
		}
	})

	t.Run("Pending limit releases the task", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, func(o *opts) { o.maxParallel = 1 })
		h.setCheck(checkTemporary("budget_exhausted"))
		h.start()
		tk := newTask(1056)
		tk.PendingLimit = 1
		h.enqueueTask(tk)
		r := h.waitTaskKind(1056, "released")
		if r.Body["reason"] != "pending_timeout" {
			t.Fatalf("released = %s", r.Raw)
		}
		if _, ok := h.docker.specFor(1056); ok {
			t.Fatalf("runtime started")
		}
		h.enqueueTask(newTask(1057))
		h.waitTaskKind(1057, "created") // the slot was freed
	})

	t.Run("Missing image fails without retry", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, func(o *opts) { o.maxParallel = 1 })
		h.start()
		tk := newTask(1058)
		tk.Image = nil
		h.enqueueTask(tk)
		f := h.waitTaskKind(1058, "failed")
		if f.Body["failure_reason"] != "image_missing" || f.Body["retryable"] != false {
			t.Fatalf("failed = %s", f.Raw)
		}
		if _, ok := h.docker.specFor(1058); ok {
			t.Fatalf("runtime started")
		}
		h.enqueueTask(newTask(1059))
		h.waitTaskKind(1059, "created")
	})

	t.Run("Missing required launch parameter fails without retry", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.env.set(func(int64) (int, any) {
			b := edgeError("configuration_error", "missing_required_parameter")
			b["request_id"] = 1
			b["missing_parameter"] = "REPO_URL"
			return http.StatusUnprocessableEntity, b
		})
		h.start()
		h.enqueueTask(newTask(1060))
		f := h.waitTaskKind(1060, "failed")
		if f.Body["failure_reason"] != "missing_required_parameter" || f.Body["retryable"] != false {
			t.Fatalf("failed = %s", f.Raw)
		}
		if _, ok := h.docker.specFor(1060); ok {
			t.Fatalf("runtime started")
		}
	})
}
