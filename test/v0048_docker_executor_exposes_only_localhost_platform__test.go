package test

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Requirement: Docker Executor exposes only localhost platform health probes.
func TestV0048DockerExecutorExposesOnlyLocalhostPlatformHealthProbes(t *testing.T) {
	t.Parallel()

	t.Run("Operator hits the liveness endpoint", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.waitFirstPull(t)
		if code, body := h.probe("/v1/livez"); code != http.StatusOK || body["status"] != "ok" {
			t.Fatalf("livez = %d %v", code, body)
		}
		h.docker.BreakEvents(errTest("docker down"))
		h.waitSelfKind("executor_failed")
		if code, _ := h.probe("/v1/livez"); code != http.StatusOK {
			t.Fatalf("livez = %d after a container-runtime failure", code)
		}
		for _, p := range []string{"/v1/tasks", "/v1/tasks/1/interrupt", "/v1/openhands/api", "/v1/messages"} {
			rec := httptest.NewRecorder()
			h.exec.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, p, nil))
			if rec.Code != http.StatusNotFound && rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("%s answered %d", p, rec.Code)
			}
		}
	})

}

type errTest string

func (e errTest) Error() string { return string(e) }
