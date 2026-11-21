package test

import (
	"encoding/json"
	"strings"
	"testing"
)

// startText returns the initial message text of a task's conversation.
func (h *harness) startText(t *testing.T, taskID int64) string {
	t.Helper()
	body := h.waitOpenHands(taskID).StartBodySnapshot()
	im, _ := body["initial_message"].(map[string]any)
	parts, _ := im["content"].([]any)
	if len(parts) != 1 {
		t.Fatalf("initial_message = %v", im)
	}
	text, _ := parts[0].(map[string]any)["text"].(string)
	return text
}

// Requirement: Concrete OpenHands Executors consume pinned task configuration.
func TestV0048ConcreteOpenHandsExecutorsConsumePinnedTaskConfiguration(t *testing.T) {
	t.Parallel()

	t.Run("Claimed task uses its pinned prompt and model", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		tk := newTask(1260)
		tk.Prompt = strp("Rendered: fix the flaky test in pkg/a")
		h.runTask(tk)
		if got := h.startText(t, 1260); got != "Rendered: fix the flaky test in pkg/a" {
			t.Fatalf("prompt = %q", got)
		}
		body := h.docker.openHandsFor(1260).StartBodySnapshot()
		agent, _ := body["agent"].(map[string]any)
		llm := h.startLLM(t, 1260)
		if agent["kind"] != "Agent" || llm["model"] != "openai/3/gpt-4o" || llm["api_key"] != "sk-task-key-1260" ||
			llm["base_url"] != "http://litellm-gateway-data:4000/v1" {
			t.Fatalf("agent = %v", agent)
		}
	})

	t.Run("Task snapshot without an agent field starts OpenHands", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		tk := newTask(1261)
		b := tk.body()
		if _, ok := b["execution"].(map[string]any)["agent_id"]; ok {
			t.Fatalf("fixture carries agent_id")
		}
		tk.raw = b
		h.runTask(tk)
		if got := h.startText(t, 1261); got != "Fix issue 1261" {
			t.Fatalf("prompt = %q", got)
		}
		if h.hasTaskKind(1261, "failed") {
			t.Fatalf("claim failed for a missing agent")
		}
	})

	t.Run("Incomplete pinned revision fails without retry", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		tk := newTask(1262)
		tk.Prompt = nil
		h.enqueueTask(tk)
		// No container means no OpenHands conversation was submitted.
		h.assertFailedNoRetry(t, 1262, "invalid_flow_revision")
	})
}

// Not parallel: t.Setenv changes the process environment.
func TestV0048ConcreteOpenHandsExecutorsIgnoreProcessAgentProfile(t *testing.T) {
	t.Run("Process agent profile conflicts with task configuration", func(t *testing.T) {
		t.Setenv("OPENHANDS_AGENT_PROFILE_ID", "stale-profile")
		t.Setenv("OPENHANDS_LLM_MODEL", "openai/process-model")
		t.Setenv("OPENHANDS_LLM_API_KEY", "sk-process-key")
		t.Setenv("OPENHANDS_LLM_BASE_URL", "http://process-llm:4000/v1")
		h := newHarness(t, nil)
		h.start()
		h.runTask(newTask(1263))
		llm := h.startLLM(t, 1263)
		if llm["model"] != "openai/3/gpt-4o" || llm["api_key"] != "sk-task-key-1263" || llm["base_url"] != "http://litellm-gateway-data:4000/v1" {
			t.Fatalf("agent.llm = %v", llm)
		}
		raw, _ := json.Marshal(h.docker.openHandsFor(1263).StartBodySnapshot())
		for _, s := range []string{"stale-profile", "profile", "process-model", "sk-process-key", "process-llm"} {
			if strings.Contains(string(raw), s) {
				t.Fatalf("start body uses process setting %q: %s", s, raw)
			}
		}
		if got := h.startText(t, 1263); got != "Fix issue 1263" {
			t.Fatalf("prompt = %q", got)
		}
	})
}
