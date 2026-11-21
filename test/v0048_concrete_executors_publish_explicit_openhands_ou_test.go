package test

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// logsOf returns the log events of a task in stream order.
func (h *harness) logsOf(taskID int64) []event {
	var out []event
	for _, ev := range h.eventsFor(taskID) {
		if ev.Kind == "log" {
			out = append(out, ev)
		}
	}
	return out
}

// Requirement: Concrete Executors publish explicit OpenHands output.
func TestV0048ConcreteExecutorsPublishExplicitOpenHandsOutput(t *testing.T) {
	t.Parallel()

	t.Run("OpenHands publishes assistant and tool output", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.runTask(newTask(1240))
		oh := h.waitOpenHands(1240)
		frames := []any{
			map[string]any{"kind": "MessageEvent", "source": "user", "llm_message": map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": "Fix issue 1240"}}}},
			map[string]any{"kind": "MessageEvent", "source": "agent", "llm_message": map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": "I will run the tests first."}}}},
			map[string]any{"kind": "ObservationEvent", "source": "environment", "observation": map[string]any{"kind": "TerminalObservation", "content": []any{map[string]any{"type": "text", "text": "ok  3 passed"}}}},
			map[string]any{"type": "conversation.status", "execution_status": "finished", "kind": "finished", "status": "finished"},
		}
		for _, f := range frames {
			if err := oh.PushEvent("conv-1240", f); err != nil {
				t.Fatal(err)
			}
		}
		fin := h.waitTaskKind(1240, "finished")
		logs := h.logsOf(1240)
		if len(logs) != 2 {
			t.Fatalf("log events = %d, want 2 (user input is not a log): %v", len(logs), h.kindsFor(1240))
		}
		if logs[0].Body["stream"] != "reasoning" || logs[0].Body["text"] != "I will run the tests first." {
			t.Fatalf("first log = %s", logs[0].Raw)
		}
		if logs[1].Body["stream"] != "work" || logs[1].Body["text"] != "ok  3 passed" {
			t.Fatalf("second log = %s", logs[1].Raw)
		}
		if logs[0].Seq >= logs[1].Seq || logs[1].Seq >= fin.Seq {
			t.Fatalf("order: reasoning %d, work %d, finished %d", logs[0].Seq, logs[1].Seq, fin.Seq)
		}
		for _, l := range logs {
			if l.Subject != "executor.event.7.log.pub" {
				t.Fatalf("log subject %s", l.Subject)
			}
		}
	})

	t.Run("An action and its observation publish the command and its output once", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.runTask(newTask(1243))
		oh := h.waitOpenHands(1243)
		frames := []any{
			map[string]any{"kind": "ActionEvent", "source": "agent", "tool_name": "terminal", "tool_call_id": "call-1",
				"thought": []any{map[string]any{"type": "text", "text": "I'll list the project."}},
				"action":  map[string]any{"kind": "TerminalAction", "command": "pwd && ls -la"}},
			map[string]any{"kind": "ObservationEvent", "source": "environment", "tool_name": "terminal", "tool_call_id": "call-1",
				"observation": map[string]any{"kind": "TerminalObservation", "command": "pwd && ls -la", "exit_code": 0,
					"content": []any{map[string]any{"type": "text", "text": "/workspace/project"}}}},
			map[string]any{"kind": "ActionEvent", "source": "agent", "tool_name": "terminal", "tool_call_id": "call-2",
				"action": map[string]any{"kind": "TerminalAction", "command": "pwd && ls -la"}},
			map[string]any{"kind": "ObservationEvent", "source": "environment", "tool_name": "terminal", "tool_call_id": "call-2",
				"observation": map[string]any{"kind": "TerminalObservation", "command": "pwd && ls -la", "exit_code": 0,
					"content": []any{map[string]any{"type": "text", "text": "/workspace/project"}}}},
			map[string]any{"kind": "ActionEvent", "source": "agent", "tool_name": "finish", "tool_call_id": "call-3",
				"action": map[string]any{"kind": "FinishAction", "message": "Done: marker written."}},
			map[string]any{"kind": "ObservationEvent", "source": "environment", "tool_name": "finish", "tool_call_id": "call-3",
				"observation": map[string]any{"kind": "FinishObservation", "content": []any{map[string]any{"type": "text", "text": "Done: marker written."}}}},
		}
		want := [][2]string{{"reasoning", "I'll list the project."}, {"work", "pwd && ls -la"}, {"work", "/workspace/project"},
			{"work", "pwd && ls -la"}, {"work", "/workspace/project"}, {"work", "Done: marker written."}}
		for _, f := range frames {
			if err := oh.PushEvent("conv-1243", f); err != nil {
				t.Fatal(err)
			}
		}
		h.terminal(1243, "finished")
		h.waitTaskKind(1243, "finished")
		var got [][2]string
		for _, l := range h.logsOf(1243) {
			got = append(got, [2]string{l.Body["stream"].(string), l.Body["text"].(string)})
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("logs = %q, want %q (a repeated command runs again; an echo of the same tool call is not repeated)", got, want)
		}
	})

	t.Run("OpenHands emits only protocol state", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.runTask(newTask(1241))
		oh := h.waitOpenHands(1241)
		_ = oh.PushEvent("conv-1241", map[string]any{"kind": "ConversationStateUpdateEvent", "key": "stats", "value": map[string]any{"tokens": 12}})
		h.terminal(1241, "finished")
		h.waitTaskKind(1241, "finished")
		time.Sleep(200 * time.Millisecond)
		if logs := h.logsOf(1241); len(logs) != 0 {
			t.Fatalf("protocol metadata published as logs: %s", logs[0].Raw)
		}
		n := 0
		for _, k := range h.kindsFor(1241) {
			if k == "finished" {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("finished published %d times", n)
		}
	})

	t.Run("A failed log publish does not change the task lifecycle", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.broker.DropPublishes("executor.event.7.log.pub", 1000)
		h.start()
		h.runTask(newTask(1242))
		oh := h.waitOpenHands(1242)
		_ = oh.PushEvent("conv-1242", map[string]any{"kind": "MessageEvent", "source": "agent", "llm_message": map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": "thinking"}}}})
		h.terminal(1242, "finished")
		h.waitTaskKind(1242, "finished")
		if k := h.kindsFor(1242); len(k) != 3 || k[0] != "created" || k[1] != "running" || k[2] != "finished" {
			t.Fatalf("lifecycle = %v", k)
		}
		eventually(t, "log publish failure in the service log", func() bool {
			return containsAll(h.logs.String(), "log", "publish")
		})
	})
}

func containsAll(s string, parts ...string) bool {
	for _, p := range parts {
		if !strings.Contains(s, p) {
			return false
		}
	}
	return true
}
