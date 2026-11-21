package test

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/pandops/flowai-executor-docker-openhands/internal/executor"
)

// openAPIDoc is the part of an OpenAPI document the test reads.
type openAPIDoc struct {
	OpenAPI string                    `yaml:"openapi"`
	Paths   map[string]map[string]any `yaml:"paths"`
}

func readOpenAPI(t *testing.T, name string) openAPIDoc {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(), "openapi", name))
	if err != nil {
		t.Fatalf("OpenAPI document %s: %v", name, err)
	}
	var doc openAPIDoc
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	if !strings.HasPrefix(doc.OpenAPI, "3.1") {
		t.Fatalf("%s is OpenAPI %q, want 3.1", name, doc.OpenAPI)
	}
	return doc
}

// Requirement: Docker Executor API contract is documented as OpenAPI.
func TestV0048DockerExecutorAPIContractIsDocumentedAsOpenAPI(t *testing.T) {
	t.Parallel()

	t.Run("Developer implements Executor service clients", func(t *testing.T) {
		t.Parallel()
		probes := readOpenAPI(t, "executor.openapi.yaml")
		if len(probes.Paths) != 2 || probes.Paths["/v1/livez"]["get"] == nil || probes.Paths["/v1/readyz"]["get"] == nil {
			t.Fatalf("probe paths = %v", probes.Paths)
		}
		env := readOpenAPI(t, "env-registry-open.client.openapi.yaml")
		if env.Paths["/v1/tasks/{task_id}/launch-parameters/open"]["get"] == nil {
			t.Fatalf("env-registry client paths = %v", env.Paths)
		}
		key := readOpenAPI(t, "model-registry-litellm-key.client.openapi.yaml")
		if key.Paths["/v1/tasks/{task_id}/litellm-key"]["get"] == nil {
			t.Fatalf("model-registry client paths = %v", key.Paths)
		}
		for _, doc := range []openAPIDoc{probes, env, key} {
			for p := range doc.Paths {
				if strings.Contains(strings.ToLower(p), "state-registry") || strings.Contains(p, "/executors") {
					t.Fatalf("State Registry contract still required: %s", p)
				}
			}
		}
		if d, err := executor.LoadConfig(""); err == nil && d.ExecutorAPIBind != "127.0.0.1:8020" {
			// LoadConfig("") fails without DOCKER_SOCKET_PATH; a default bind
			// is only checked when it loads.
			t.Fatalf("default executor_api_bind = %q", d.ExecutorAPIBind)
		}

		// One claimed task through every client; published payloads are
		// checked against the Executor's schemas by the harness, consumed
		// payloads against the publishers' schemas here.
		h := newHarness(t, nil)
		h.start()
		tk := newTask(1190)
		h.runTask(tk)
		h.schema.validate(t, tmTaskSchema, mustJSON(tk.body()))
		cancel := map[string]any{"publisher": "task-manager", "publisher_seq": 9, "team_id": 3, "occurred_at": "2026-11-07T10:00:00.000Z",
			"executor_id": 7, "control": "cancel", "control_id": 1, "task_id": 1190, "attempt": 1, "claim_seq": 1}
		h.schema.validate(t, tmCommandSchema, mustJSON(cancel))
		for _, r := range h.requestsOf("claim-token") {
			h.schema.validate(t, tmClaimTokenReqSchema, r.Raw)
		}
		_, ct, _ := h.claimTok(map[string]any{"body": map[string]any{"task_id": 1190.0, "attempt": 1.0, "claim_seq": 1.0}})
		h.schema.validate(t, tmClaimTokenReplySchema, mustJSON(ct))
		for _, r := range h.requestsOf("heartbeat") {
			h.schema.validate(t, erHeartbeatReqSchema, r.Raw)
			_, reply, _ := h.hbReply(r.Body)
			h.schema.validate(t, erHeartbeatReplySchema, mustJSON(reply))
		}
		for _, r := range h.requestsOf("check") {
			h.schema.validate(t, mrCheckReqSchema, r.Raw)
		}
		_, ok, _ := h.check(nil)
		h.schema.validate(t, mrCheckReplySchema, mustJSON(ok))
		h.schema.validate(t, mrCheckReplySchema, mustJSON(map[string]any{"result": "temporary", "reason": "all_selected_models_paused"}))
		h.schema.validate(t, envOpenReplySchema, mustJSON(map[string]any{"values": defaultEnvValues(1190)}))
		h.schema.validate(t, mrKeyEdgeSchema, mustJSON(defaultLLM(1190)))
		e422 := edgeError("configuration_error", "missing_required_parameter")
		e422["request_id"] = 1
		h.schema.validate(t, envErrorReplySchema, mustJSON(e422))
		h.schema.validate(t, mrKeyEdgeSchema, mustJSON(edgeError("configuration_error", "llm_configuration_incomplete")))

		for _, calls := range [][]edgeCall{h.env.callsFor(1190), h.model.callsFor(1190)} {
			if len(calls) != 1 || calls[0].Auth != "Bearer ct-1190-1-1" {
				t.Fatalf("edge calls = %+v; the claim token is the only HTTP credential", calls)
			}
		}
		if h.env.callsFor(1190)[0].Path != "/v1/tasks/1190/launch-parameters/open" || h.model.callsFor(1190)[0].Path != "/v1/tasks/1190/litellm-key" {
			t.Fatalf("edge paths = %v %v", h.env.callsFor(1190), h.model.callsFor(1190))
		}
		_ = http.StatusOK
	})
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
