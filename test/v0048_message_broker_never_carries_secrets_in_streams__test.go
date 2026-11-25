package test

import (
	"slices"
	"strings"
	"testing"
)

// Requirement: message-broker never carries secrets in streams or logs —
// the Executor's side.
func TestV0048MessageBrokerNeverCarriesSecretsInStreamsOrLogs(t *testing.T) {
	t.Parallel()

	t.Run("Model check carries no claim token", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.runTask(newTask(1280))
		checks := h.requestsOf("check")
		if len(checks) == 0 {
			t.Fatalf("no model check")
		}
		for _, r := range checks {
			if r.Subject != "model-registry.executor.7.check.pub" {
				t.Fatalf("check subject %s", r.Subject)
			}
			b := bodyOf(r)
			if asInt(b["task_id"]) != 1280 || asInt(b["attempt"]) != 1 || asInt(b["claim_seq"]) != 1 {
				t.Fatalf("check body = %v", b)
			}
			if strings.Contains(string(r.Raw), "ct-1280-1-1") || strings.Contains(string(r.Raw), "claim_token") {
				t.Fatalf("check carries the claim token: %s", r.Raw)
			}
			if r.Header.Get("Authorization") != "" {
				t.Fatalf("check carries an Authorization header")
			}
		}
	})

	t.Run("Executor receives values only over HTTP edges", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		tk := newTask(1281)
		h.runTask(tk)
		h.enqueueCancel(tk, 81)
		h.waitTaskKind(1281, "cancelled")
		spec, _ := h.docker.specFor(1281)
		if !slices.Contains(spec.Env, "DEPLOY_TOKEN=s3cr3t-value-1281") {
			t.Fatalf("value from the open edge not in the runtime: %v", spec.Env)
		}
		if len(h.env.callsFor(1281)) != 1 || len(h.model.callsFor(1281)) != 1 {
			t.Fatalf("edge calls env=%d model=%d", len(h.env.callsFor(1281)), len(h.model.callsFor(1281)))
		}
		secrets := []string{"s3cr3t-value-1281", "sk-task-key-1281", "ct-1281-1-1"}
		for _, op := range h.broker.Ops() {
			for _, s := range secrets {
				if strings.Contains(string(op.Data), s) {
					t.Fatalf("broker %s %s carries %q", op.Kind, op.Subject, s)
				}
			}
		}
	})
}
