package executor

import (
	"sort"
	"strings"

	"github.com/pandops/flowai-executor-docker-openhands/internal/openhands"
)

func envToDockerEnv(values map[string]string) []string {
	if len(values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(values))
	for _, k := range keys {
		out = append(out, k+"="+values[k])
	}
	return out
}

// dockerEnvWithSessionAPIKey defensively copies the supplied env
// map and forces SESSION_API_KEY to the configured key when non-
// empty. When the configured key is empty, any caller-provided
// SESSION_API_KEY is stripped so the helper remains the single owner
// of that variable. The input map is never mutated.
func dockerEnvWithSessionAPIKey(values map[string]string, key string) []string {
	copied := make(map[string]string, len(values))
	for k, v := range values {
		copied[k] = v
	}
	if key == "" {
		delete(copied, "SESSION_API_KEY")
	} else {
		copied["SESSION_API_KEY"] = key
	}
	return envToDockerEnv(copied)
}

func (e *Executor) conversationConfig(prompt, taskAPIKey, taskBaseURL, taskModel string) openhands.ConversationConfig {
	cfg := openhands.ConversationConfig{
		Workspace: openhands.Workspace{
			Kind:       "LocalWorkspace",
			WorkingDir: e.cfg.OpenHandsWorkspace,
		},
		InitialMessage: openhands.InitialMessage{
			Role: "user",
			Content: []openhands.ContentPart{
				{Type: "text", Text: prompt},
			},
			Run: e.cfg.OpenHandsInitialRun,
		},
	}
	cfg.Agent = &openhands.Agent{
		Kind:  "Agent",
		Tools: []openhands.Tool{{Name: "terminal"}},
		LLM: openhands.LLM{
			Model:   openAICompatibleModel(taskModel),
			APIKey:  taskAPIKey,
			UsageID: e.cfg.OpenHandsLLMUsageID,
			BaseURL: taskBaseURL,
			// 429s from the LiteLLM RPM/TPM limits are retried by the agent
			// for longer than a one-minute window (8+16+32+60+60... s).
			NumRetries:   intPtr(llmNumRetries),
			RetryMinWait: intPtr(llmRetryMinWait),
			RetryMaxWait: intPtr(llmRetryMaxWait),
		},
	}
	return cfg
}

func taskStatusFromValue(value any) string {
	switch typed := value.(type) {
	case map[string]any:
		if raw, ok := typed["execution_status"].(string); ok {
			if status := strings.ToLower(strings.TrimSpace(raw)); status != "" {
				return status
			}
		}
		for _, nested := range typed {
			if status := taskStatusFromValue(nested); status != "" {
				return status
			}
		}
		for _, key := range []string{"status", "kind", "type"} {
			if raw, ok := typed[key].(string); ok {
				if status := strings.ToLower(strings.TrimSpace(raw)); status != "" {
					return status
				}
			}
		}
	case []any:
		for _, nested := range typed {
			if status := taskStatusFromValue(nested); status != "" {
				return status
			}
		}
	}
	return ""
}

// isTerminalTaskStatus reports whether the task status signals a
// terminal conversation state.
func isTerminalTaskStatus(status string) bool {
	switch status {
	case "finished", "failed", "error", "stuck", "paused":
		return true
	}
	return false
}

func httpAPIHeaders(apiKey string) map[string][]string {
	if apiKey == "" {
		return nil
	}
	return map[string][]string{"X-Session-API-Key": {apiKey}}
}

// openAICompatibleModel names the task's LiteLLM alias for OpenHands' LiteLLM
// SDK, which calls the OpenAI-compatible data plane with the "openai/"
// provider prefix and sends the alias itself as the model.
func openAICompatibleModel(alias string) string {
	if strings.HasPrefix(alias, "openai/") {
		return alias
	}
	return "openai/" + alias
}

func OpenHandsWSURL(containerURL, conversationID string) string {
	base := strings.TrimRight(containerURL, "/")
	if strings.HasPrefix(base, "https://") {
		base = "wss://" + strings.TrimPrefix(base, "https://")
	} else if strings.HasPrefix(base, "http://") {
		base = "ws://" + strings.TrimPrefix(base, "http://")
	}
	return base + "/sockets/events/" + conversationID
}
