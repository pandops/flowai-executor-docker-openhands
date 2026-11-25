package test

import (
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// startLLM returns agent.llm of the task's OpenHands start request.
func (h *harness) startLLM(t *testing.T, taskID int64) map[string]any {
	t.Helper()
	body := h.waitOpenHands(taskID).StartBodySnapshot()
	agent, _ := body["agent"].(map[string]any)
	llm, _ := agent["llm"].(map[string]any)
	if llm == nil {
		t.Fatalf("start body has no agent.llm: %v", body)
	}
	return llm
}

// assertFailedNoRetry checks a failed event with retryable = false and no runtime.
func (h *harness) assertFailedNoRetry(t *testing.T, taskID int64, reason string) {
	t.Helper()
	f := h.waitTaskKind(taskID, "failed")
	if f.Body["failure_reason"] != reason || f.Body["retryable"] != false {
		t.Fatalf("failed = %s, want %s and retryable=false", f.Raw, reason)
	}
	if _, ok := h.docker.specFor(taskID); ok {
		t.Fatalf("runtime started for task %d", taskID)
	}
	eventually(t, "slot freed", func() bool {
		hbs := h.heartbeats()
		return len(hbs) > 0 && asInt(hbs[len(hbs)-1]["slots_in_use"]) == 0
	})
}

// Requirement: Concrete Executors consume resolved launch parameters.
func TestV0048ConcreteExecutorsConsumeResolvedLaunchParameters(t *testing.T) {
	t.Parallel()

	t.Run("Claimed task has merged parameters", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.runTask(newTask(1230))
		h.runTask(newTask(1231))
		for _, id := range []int64{1230, 1231} {
			calls := h.env.callsFor(id)
			if len(calls) != 1 {
				t.Fatalf("task %d opened its snapshot %d times", id, len(calls))
			}
			if want := "Bearer " + "ct-" + strconv.FormatInt(id, 10) + "-1-1"; calls[0].Auth != want {
				t.Fatalf("task %d open used %q, want its own claim token", id, calls[0].Auth)
			}
		}
		a, _ := h.docker.specFor(1230)
		b, _ := h.docker.specFor(1231)
		if !slices.Contains(a.Env, "DEPLOY_TOKEN=s3cr3t-value-1230") || !slices.Contains(a.Env, "REPO_URL=https://git.example/acme/repo") {
			t.Fatalf("task 1230 env = %v", a.Env)
		}
		if slices.Contains(b.Env, "DEPLOY_TOKEN=s3cr3t-value-1230") || slices.Contains(a.Env, "DEPLOY_TOKEN=s3cr3t-value-1231") {
			t.Fatalf("values crossed between tasks")
		}
		if !slices.Contains(a.Networks, "flowai-litellm-data") {
			t.Fatalf("runtime does not join the LiteLLM data network: %v", a.Networks)
		}
		if l := h.leaks("s3cr3t-value-1230", "s3cr3t-value-1231"); len(l) != 0 {
			t.Fatalf("opened values on the broker: %v", l)
		}
	})

	t.Run("Claim token is no longer current", func(t *testing.T) {
		t.Parallel()
		for _, edge := range []string{"env", "model"} {
			t.Run(edge, func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, nil)
				notCurrent := func(int64) (int, any) { return http.StatusNotFound, edgeError("claim_not_current", "") }
				if edge == "env" {
					h.env.set(notCurrent)
				} else {
					h.model.set(notCurrent)
				}
				h.start()
				h.enqueueTask(newTask(1232))
				h.assertDropped(t, 1232)
			})
		}
	})

	t.Run("Model key edge reports a configuration error", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.model.set(func(int64) (int, any) {
			return http.StatusUnprocessableEntity, edgeError("configuration_error", "llm_configuration_incomplete")
		})
		h.start()
		h.enqueueTask(newTask(1233))
		h.assertFailedNoRetry(t, 1233, "llm_configuration_incomplete")
	})

	t.Run("Task key edge supplies the LiteLLM configuration", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.runTask(newTask(1234))
		llm := h.startLLM(t, 1234)
		if llm["model"] != "openai/3/gpt-4o" || llm["api_key"] != "sk-task-key-1234" || llm["base_url"] != "http://litellm-gateway-data:4000/v1" {
			t.Fatalf("agent.llm = %v", llm)
		}
		if calls := h.model.callsFor(1234); len(calls) != 1 || calls[0].Auth != "Bearer ct-1234-1-1" {
			t.Fatalf("key edge calls = %+v", calls)
		}
		h.terminal(1234, "finished")
		h.waitTaskKind(1234, "finished")
		h.sigterm()
		if l := h.leaks("sk-task-key-1234", "ct-1234-1-1"); len(l) != 0 {
			t.Fatalf("key or claim token published: %v", l)
		}
		if strings.Contains(h.logs.String(), "sk-task-key-1234") || strings.Contains(h.logs.String(), "ct-1234-1-1") {
			t.Fatalf("key or claim token logged")
		}
		_ = filepath.Walk(h.cacheDir, func(p string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				if raw, _ := os.ReadFile(p); strings.Contains(string(raw), "sk-task-key-1234") {
					t.Errorf("%s persists the key", p)
				}
			}
			return nil
		})
	})

	t.Run("Rate-limited model calls are retried", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.runTask(newTask(1235))
		llm := h.startLLM(t, 1235)
		if asInt(llm["num_retries"]) != 8 || asInt(llm["retry_min_wait"]) != 8 || asInt(llm["retry_max_wait"]) != 60 {
			t.Fatalf("retry policy = %v %v %v, want 8 retries waiting 8..60s", llm["num_retries"], llm["retry_min_wait"], llm["retry_max_wait"])
		}
		if llm["model"] != "openai/3/gpt-4o" || llm["api_key"] != "sk-task-key-1235" {
			t.Fatalf("retries do not use the same alias and key: %v", llm)
		}
		// A rate-limit observation alone does not end the task.
		oh := h.waitOpenHands(1235)
		_ = oh.PushEvent("conv-1235", map[string]any{"kind": "ConversationErrorEvent", "code": "RateLimitError", "detail": "429 rpm limit"})
		never(t, "terminal event while OpenHands retries", 500*time.Millisecond, func() bool {
			return h.hasTaskKind(1235, "failed") || h.hasTaskKind(1235, "finished")
		})
		h.terminal(1235, "error")
		if f := h.waitTaskKind(1235, "failed"); f.Body["retryable"] != true {
			t.Fatalf("failed after exhausted retries = %s", f.Raw)
		}
	})

	t.Run("Model configuration is incomplete", func(t *testing.T) {
		t.Parallel()
		for i, missing := range []string{"OPENAI_API_KEY", "OPENAI_BASE_URL", "OPENAI_MODEL"} {
			id := int64(1236 + i)
			t.Run(missing, func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, nil)
				h.model.set(func(taskID int64) (int, any) {
					v := defaultLLM(taskID)
					delete(v, missing)
					return http.StatusOK, v
				})
				h.start()
				h.enqueueTask(newTask(id))
				h.assertFailedNoRetry(t, id, "llm_configuration_incomplete")
			})
		}
	})
}

// Not parallel: t.Setenv sets process-level LLM settings, so a fallback to
// them would be visible as a started runtime.
func TestV0048ConcreteExecutorsConsumeResolvedLaunchParametersNoProcessFallback(t *testing.T) {
	t.Run("Model configuration is incomplete", func(t *testing.T) {
		for _, kv := range [][2]string{
			{"OPENHANDS_LLM_MODEL", "openai/process-model"},
			{"OPENHANDS_LLM_API_KEY", "sk-process-key"},
			{"OPENHANDS_LLM_BASE_URL", "http://process-llm:4000/v1"},
			{"OPENAI_MODEL", "process-model"},
			{"OPENAI_API_KEY", "sk-process-key"},
			{"OPENAI_BASE_URL", "http://process-llm:4000/v1"},
			{"LITELLM_API_KEY", "sk-process-key"},
		} {
			t.Setenv(kv[0], kv[1])
		}
		for i, missing := range []string{"OPENAI_API_KEY", "OPENAI_BASE_URL", "OPENAI_MODEL"} {
			id := int64(1290 + i)
			h := newHarness(t, nil)
			h.model.set(func(taskID int64) (int, any) {
				v := defaultLLM(taskID)
				delete(v, missing)
				return http.StatusOK, v
			})
			h.start()
			h.enqueueTask(newTask(id))
			h.assertFailedNoRetry(t, id, "llm_configuration_incomplete")
			if oh := h.docker.openHandsFor(id); oh != nil && oh.StartCallCount() != 0 {
				t.Fatalf("%s missing: conversation started with process settings", missing)
			}
			h.sigterm()
		}
	})
}
