package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// The Executor's own schemas (rule 15), relative to the repository root.
const (
	executorEventSchema = "schemas/executor-event.schema.json"
	claimTokenReqSchema = "schemas/claim-token-request.schema.json"
	heartbeatReqSchema  = "schemas/heartbeat-request.schema.json"
	checkReqSchema      = "schemas/check-request.schema.json"
)

// requestSchemas maps a request operation to the Executor's schema.
var requestSchemas = map[string]string{
	"claim-token": claimTokenReqSchema,
	"heartbeat":   heartbeatReqSchema,
	"check":       checkReqSchema,
}

// Publishers' schemas the Executor consumes (rule 17 names), declared in
// qa-e2e/scripts/shared-test-inputs.json as test inputs of the Executor.
const (
	tmTaskSchema            = "svc/task-manager/schemas/task.schema.json"
	tmCommandSchema         = "svc/task-manager/schemas/command.schema.json"
	tmClaimTokenReqSchema   = "svc/task-manager/schemas/executor.claim-token.request.schema.json"
	tmClaimTokenReplySchema = "svc/task-manager/schemas/executor.claim-token.reply.schema.json"
	erHeartbeatReqSchema    = "svc/executor-registry/schemas/executor.heartbeat.request.schema.json"
	erHeartbeatReplySchema  = "svc/executor-registry/schemas/executor.heartbeat.reply.schema.json"
	erErrorReplySchema      = "svc/executor-registry/schemas/error.reply.schema.json"
	mrCheckReqSchema        = "svc/model-registry/schemas/executor.check.request.schema.json"
	mrCheckReplySchema      = "svc/model-registry/schemas/executor.check.reply.schema.json"
	mrKeyEdgeSchema         = "svc/model-registry/schemas/edge.litellm-key.reply.schema.json"
	envOpenReplySchema      = "svc/env-registry/schemas/executor.launch-parameters-open.reply.schema.json"
	envErrorReplySchema     = "svc/env-registry/schemas/error.reply.schema.json"
)

// repoRoot returns the repository root from this file's location.
func repoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), ".."))
}

// schemaSet compiles schema files on demand. Every JSON file under the
// Executor's schemas and the declared publisher inputs is registered
// by its $id, so relative $refs between a publisher's files resolve.
type schemaSet struct {
	mu       sync.Mutex
	compiler *jsonschema.Compiler
	idOf     map[string]string
	compiled map[string]*jsonschema.Schema
	loadErr  error
}

var (
	sharedSchemas     *schemaSet
	sharedSchemasOnce sync.Once
)

func loadSchemas(t testing.TB) *schemaSet {
	sharedSchemasOnce.Do(func() {
		s := &schemaSet{compiler: jsonschema.NewCompiler(), idOf: map[string]string{}, compiled: map[string]*jsonschema.Schema{}}
		fixtures := os.Getenv("FLOWAI_CONTRACT_FIXTURE_DIR")
		if fixtures == "" {
			s.loadErr = fmt.Errorf("FLOWAI_CONTRACT_FIXTURE_DIR is required for publisher schemas")
			sharedSchemas = s
			return
		}
		for _, rel := range []string{executorEventSchema, claimTokenReqSchema, heartbeatReqSchema, checkReqSchema} {
			if err := s.register(repoRoot(), rel); err != nil {
				s.loadErr = err
				break
			}
		}
		if s.loadErr == nil {
			for _, rel := range []string{
				tmTaskSchema, tmCommandSchema, tmClaimTokenReqSchema, tmClaimTokenReplySchema,
				erHeartbeatReqSchema, erHeartbeatReplySchema, erErrorReplySchema,
				mrCheckReqSchema, mrCheckReplySchema, mrKeyEdgeSchema,
				envOpenReplySchema, envErrorReplySchema,
				"svc/task-manager/schemas/defs.schema.json",
				"svc/model-registry/schemas/defs.schema.json",
			} {
				if err := s.register(fixtures, rel); err != nil {
					s.loadErr = err
					break
				}
			}
		}
		sharedSchemas = s
	})
	if sharedSchemas.loadErr != nil {
		t.Fatal(sharedSchemas.loadErr)
	}
	return sharedSchemas
}

func (s *schemaSet) register(root, rel string) error {
	path, err := filepath.Abs(filepath.Join(root, rel))
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read schema %s: %w", rel, err)
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("decode schema %s: %w", rel, err)
	}
	id := (&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String()
	if object, ok := doc.(map[string]any); ok {
		if declared, ok := object["$id"].(string); ok && declared != "" {
			id = declared
		}
	}
	if err := s.compiler.AddResource(id, doc); err != nil {
		return fmt.Errorf("register schema %s: %w", rel, err)
	}
	s.idOf[rel] = id
	return nil
}

// schemaFor compiles the schema file at rel (relative to the repo root).
// It fails the test when the file does not exist.
func (s *schemaSet) schemaFor(t testing.TB, rel string) *jsonschema.Schema {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if sch, ok := s.compiled[rel]; ok {
		return sch
	}
	id, ok := s.idOf[rel]
	if !ok {
		t.Fatalf("schema file %s does not exist (or is not valid JSON)", rel)
	}
	sch, err := s.compiler.Compile(id)
	if err != nil {
		t.Fatalf("compile schema %s: %v", rel, err)
	}
	s.compiled[rel] = sch
	return sch
}

// validate checks a JSON document against the schema file rel.
func (s *schemaSet) validate(t testing.TB, rel string, data []byte) {
	t.Helper()
	sch := s.schemaFor(t, rel)
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		t.Errorf("%s: instance is not JSON: %v", rel, err)
		return
	}
	if err := sch.Validate(inst); err != nil {
		t.Errorf("%s: %s does not validate: %v", rel, string(data), err)
	}
}

// valid reports whether data validates against rel, without failing.
func (s *schemaSet) valid(t testing.TB, rel string, v any) error {
	t.Helper()
	data, _ := json.Marshal(v)
	sch := s.schemaFor(t, rel)
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return err
	}
	return sch.Validate(inst)
}

// Each event kind owns its reason values: a top-level enum must not
// override the per-kind branches.
func TestExecutorEventSchemaReasonIsPerKind(t *testing.T) {
	t.Parallel()
	s := loadSchemas(t)
	base := func(kind, reason string) map[string]any {
		return map[string]any{
			"publisher": "executor", "publisher_seq": 12, "team_id": 3, "executor_id": 7, "kind": kind,
			"occurred_at": "2026-11-08T05:20:10.000Z", "task_id": 1, "attempt": 1, "claim_seq": 1, "reason": reason,
		}
	}
	accept := map[string]map[string]any{
		"released executor_shutdown":     base("released", "executor_shutdown"),
		"released executor_restarted":    base("released", "executor_restarted"),
		"released pending_timeout":       base("released", "pending_timeout"),
		"pending no_active_model":        base("pending", "no_active_model"),
		"pending dependency_unavailable": base("pending", "dependency_unavailable"),
	}
	for name, ev := range accept {
		if err := s.valid(t, executorEventSchema, ev); err != nil {
			t.Errorf("%s rejected: %v", name, err)
		}
	}
	reject := map[string]map[string]any{
		"pending pending_timeout":  base("pending", "pending_timeout"),
		"released no_active_model": base("released", "no_active_model"),
	}
	for name, ev := range reject {
		if s.valid(t, executorEventSchema, ev) == nil {
			t.Errorf("%s accepted", name)
		}
	}
}
