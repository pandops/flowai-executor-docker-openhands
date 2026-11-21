package test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/pandops/flowai-executor-docker-openhands/internal/dockerclient"
)

// Requirement: Concrete Executors mark every failure as retryable or not.
func TestV0048ConcreteExecutorsMarkEveryFailureAsRetryableOrNot(t *testing.T) {
	t.Parallel()

	t.Run("Runtime failure is retryable", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.runTask(newTask(1070))
		ref, _ := h.docker.containerForTask(1070)
		h.docker.EmitDie(ref.Name)
		f := h.waitTaskKind(1070, "failed")
		if f.Body["failure_reason"] != "container_exited_before_finish" || f.Body["retryable"] != true {
			t.Fatalf("failed = %s", f.Raw)
		}
	})

	t.Run("Runtime failure is retryable (OpenHands terminal statuses)", func(t *testing.T) {
		t.Parallel()
		for i, status := range []string{"failed", "error", "stuck", "paused"} {
			h := newHarness(t, nil)
			h.start()
			id := int64(1071 + i)
			h.runTask(newTask(id))
			h.terminal(id, status)
			f := h.waitTaskKind(id, "failed")
			if f.Body["failure_reason"] != "openhands_"+status || f.Body["retryable"] != true {
				t.Fatalf("failed = %s", f.Raw)
			}
			h.sigterm()
		}
	})

	t.Run("Runtime failure is retryable (image pull and LiteLLM route)", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		tk := newTask(1075)
		h.docker.mu.Lock()
		h.docker.pullErr[*tk.Image] = errors.New("manifest unknown")
		h.docker.mu.Unlock()
		h.start()
		h.enqueueTask(tk)
		f := h.waitTaskKind(1075, "failed")
		if f.Body["failure_reason"] != "image_pull_failed" || f.Body["retryable"] != true {
			t.Fatalf("failed = %s", f.Raw)
		}
		// Missing LiteLLM data network (replaces TestV0022_13_DockerMissingDataNetworkFailsWithLLMRouteUnavailable).
		h.docker.mu.Lock()
		delete(h.docker.pullErr, *tk.Image) // the image pulls again; joining the network fails
		h.docker.startErr = dockerclient.ErrNetworkUnavailable
		h.docker.mu.Unlock()
		h.enqueueTask(newTask(1076))
		f = h.waitTaskKind(1076, "failed")
		if f.Body["failure_reason"] != "llm_route_unavailable" || f.Body["retryable"] != true {
			t.Fatalf("failed = %s", f.Raw)
		}
	})

	t.Run("Runtime failure is retryable (budget exceeded)", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.runTask(newTask(1077))
		oh := h.waitOpenHands(1077)
		_ = oh.PushEvent("conv-1077", map[string]any{"kind": "ConversationErrorEvent", "code": "LLMBadRequestError", "detail": "litellm.BadRequestError: Budget has been exceeded! Current cost: 1.2"})
		_ = oh.PushTerminal("conv-1077", "error")
		f := h.waitTaskKind(1077, "failed")
		if f.Body["failure_reason"] != "llm_budget_exceeded" || f.Body["retryable"] != true || f.Body["openhands_error_code"] != "LLMBadRequestError" {
			t.Fatalf("failed = %s", f.Raw)
		}
	})

	t.Run("Configuration error is not retryable", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.model.set(func(id int64) (int, any) {
			v := defaultLLM(id)
			delete(v, "OPENAI_MODEL")
			return http.StatusOK, v
		})
		h.start()
		h.enqueueTask(newTask(1078))
		f := h.waitTaskKind(1078, "failed")
		if f.Body["failure_reason"] != "llm_configuration_incomplete" || f.Body["retryable"] != false {
			t.Fatalf("failed = %s", f.Raw)
		}
		if _, ok := h.docker.specFor(1078); ok {
			t.Fatalf("runtime started")
		}
	})
}
