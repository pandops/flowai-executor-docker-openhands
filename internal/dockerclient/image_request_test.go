package dockerclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	engine "github.com/docker/docker/client"
)

func TestCreateRequestPreservesResolvedImageReference(t *testing.T) {
	for _, repository := range []string{"localhost/agent-openhands-image", "registry.example:5443/team/agent"} {
		t.Run(repository, func(t *testing.T) {
			image := repository + "@sha256:" + strings.Repeat("a", 64)
			var created, started atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1.45/containers/create":
					var request struct{ Image string }
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
					}
					if request.Image != image {
						t.Errorf("Docker request image = %q, want exact %q", request.Image, image)
					}
					created.Store(true)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusCreated)
					_, _ = w.Write([]byte(`{"Id":"fixture-container","Warnings":[]}`))
				case "/v1.45/containers/fixture-container/start":
					started.Store(true)
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected Docker request %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client, err := engine.NewClientWithOpts(
				engine.WithHost(server.URL),
				engine.WithVersion("1.45"),
				engine.WithHTTPClient(server.Client()),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			adapter := &SDKClient{cli: client, publishedHost: "127.0.0.1"}
			container, err := adapter.StartContainer(context.Background(), ContainerSpec{Name: "fixture", Image: image})
			if err != nil {
				t.Fatal(err)
			}
			if !created.Load() || !started.Load() || container.Image != image {
				t.Fatalf("create=%t start=%t container=%+v", created.Load(), started.Load(), container)
			}
		})
	}
}
