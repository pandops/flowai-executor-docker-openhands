package executor

import "testing"

func TestTaskStatusFromValueFindsNestedExecutionStatus(t *testing.T) {
	event := map[string]any{
		"kind":    "ConversationStateUpdateEvent",
		"payload": map[string]any{"execution_status": " FINISHED "},
	}
	if got := taskStatusFromValue(event); got != "finished" {
		t.Fatalf("taskStatusFromValue() = %q, want finished", got)
	}
}
