package test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/pandops/flowai-executor-docker-openhands/internal/platform"
)

// spellingRE matches every separator and case variant of the identifier.
var spellingRE = regexp.MustCompile(`(?i)executor[-_ ]?docker[-_ ]?open[-_ ]?hands?\b`)

// Requirement: Docker OpenHands concrete service uses the correctly spelled identifier.
func TestV0048DockerOpenHandsConcreteServiceUsesTheCorrectlySpelledIdentifier(t *testing.T) {
	t.Parallel()

	t.Run("Docker OpenHands starts under the corrected wire value", func(t *testing.T) {
		t.Parallel()
		if platform.ExecutorTypeDockerOpenHands != "executor_docker_openhands" {
			t.Fatalf("wire value = %q", platform.ExecutorTypeDockerOpenHands)
		}
		h := newHarness(t, nil)
		h.start()
		st := h.waitSelfKind("executor_started")
		hb := h.waitHeartbeats(1)[0]
		if st.Body["executor_type"] != "executor_docker_openhands" || hb["executor_type"] != "executor_docker_openhands" {
			t.Fatalf("executor_type = %v / %v", st.Body["executor_type"], hb["executor_type"])
		}
		root := repoRoot()
		if _, err := os.Stat(filepath.Join(root, "cmd", "executor_docker_openhands", "main.go")); err != nil {
			t.Fatalf("command path: %v", err)
		}
		cf, _ := os.ReadFile(filepath.Join(root, "Containerfile"))
		if !strings.Contains(string(cf), "/executor_docker_openhands") || !strings.Contains(string(cf), "--config") ||
			!strings.Contains(string(cf), "/etc/flowai/config.yaml") {
			t.Fatalf("Containerfile ENTRYPOINT does not run executor_docker_openhands --config /etc/flowai/config.yaml")
		}
		if _, err := os.Stat(filepath.Join(root, "config.example.yaml")); err != nil {
			t.Fatalf("config.example.yaml: %v", err)
		}

		// slog service value on every log line.
		lines := 0
		for _, line := range strings.Split(strings.TrimSpace(h.logs.String()), "\n") {
			var rec map[string]any
			if json.Unmarshal([]byte(line), &rec) != nil {
				continue
			}
			lines++
			if rec["service"] != "executor_docker_openhands" {
				t.Fatalf("log line service = %v: %s", rec["service"], line)
			}
		}
		if lines == 0 {
			t.Fatalf("no JSON log lines captured")
		}
		// Probe service label.
		for _, p := range []string{"/v1/livez", "/v1/readyz"} {
			if _, body := h.probe(p); body["service"] != "executor_docker_openhands" {
				t.Fatalf("%s service label = %v", p, body["service"])
			}
		}
		// Older spellings are absent from this service's runtime and
		// current-state files.
		self, _ := filepath.Abs("v0048_docker_openhands_concrete_service_uses_the_corre_test.go")
		_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if path == filepath.Join(root, ".git") && err == nil && info.IsDir() {
				return filepath.SkipDir
			}
			if err != nil || info.IsDir() || path == self || info.Size() > 4<<20 {
				return nil //nolint:nilerr // unreadable entries are skipped
			}
			raw, _ := os.ReadFile(path)
			for _, m := range spellingRE.FindAllString(strings.ReplaceAll(string(raw), "github.com/pandops/flowai-executor-docker-openhands", ""), -1) {
				if m != "executor_docker_openhands" {
					rel, _ := filepath.Rel(root, path)
					t.Errorf("%s carries the older spelling %q", rel, m)
				}
			}
			return nil
		})
	})
}
