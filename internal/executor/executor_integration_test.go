//go:build integration

package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/distribution/reference"
	dockersdk "github.com/docker/docker/client"

	"github.com/pandops/flowai-executor-docker-openhands/internal/cache"
	"github.com/pandops/flowai-executor-docker-openhands/internal/dockerclient"
)

func TestV0002TerminalCleanupDelayAgainstDocker(t *testing.T) {
	image := os.Getenv("FLOWAI_OPENHANDS_TEST_IMAGE")
	ref, err := reference.ParseNamed(image)
	pinned, hasDigest := ref.(reference.Digested)
	_, hasTag := ref.(reference.Tagged)
	if err != nil || !hasDigest || hasTag || pinned.Digest().Algorithm() != "sha256" {
		t.Fatal("set FLOWAI_OPENHANDS_TEST_IMAGE to a fully qualified repository@sha256:digest available on the local runtime")
	}

	socket := dockerSocketPath(t)
	client, err := dockerclient.NewSDKClient(socket)
	if err != nil {
		t.Fatalf("new Docker client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Docker-compatible runtime unavailable: %v", err)
	}

	inspector, err := dockersdk.NewClientWithOpts(dockersdk.WithHost("unix://"+socket), dockersdk.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := inspector.Close(); err != nil {
			t.Errorf("close Docker inspector: %v", err)
		}
	})
	imageInfo, err := inspector.ImageInspect(ctx, image)
	if err != nil {
		t.Fatalf("inspect pinned local test image %s: %v", image, err)
	}
	if !slices.Contains(imageInfo.RepoDigests, image) {
		t.Fatalf("local test image %s is missing the requested repository digest", image)
	}

	tests := []struct {
		name      string
		eventType string
		finished  time.Duration
		failed    time.Duration
	}{
		{name: "finished selects finished delay", eventType: "finished", finished: 300 * time.Millisecond, failed: 2 * time.Second},
		{name: "failed selects failed delay", eventType: "failed", finished: 2 * time.Second, failed: 300 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executorID := "integration-" + strings.NewReplacer(" ", "-", "_", "-").Replace(tt.name)
			ref, err := client.StartContainer(ctx, dockerclient.ContainerSpec{
				Name:    fmt.Sprintf("flowai-v0005-cleanup-%d", time.Now().UnixNano()),
				Image:   image,
				Command: []string{"sh", "-c", "sleep 300"},
				Labels:  map[string]string{dockerclient.LabelExecutorID: executorID},
			})
			if err != nil {
				t.Fatalf("start test container: %v", err)
			}
			t.Cleanup(func() {
				cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancelCleanup()
				if err := client.ForceKill(cleanupCtx, ref.ID); err != nil {
					t.Errorf("cleanup owned Docker container %s: %v", ref.ID, err)
				}
			})

			if ledger := os.Getenv("FLOWAI_RESOURCE_LEDGER"); ledger != "" {
				file, err := os.OpenFile(ledger, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := file.Close(); err != nil {
						t.Errorf("close owned resource ledger: %v", err)
					}
				})
				encoder := json.NewEncoder(file)
				if err := encoder.Encode(map[string]string{"kind": "containers", "id": ref.ID}); err != nil {
					t.Fatal(err)
				}
				inspected, err := inspector.ContainerInspect(ctx, ref.ID)
				if err != nil {
					t.Fatal(err)
				}
				for _, mount := range inspected.Mounts {
					// This fixture requested no named mounts: every volume is
					// an anonymous volume created with this exact container.
					if mount.Type == "volume" && mount.Name != "" {
						if err := encoder.Encode(map[string]string{"kind": "volumes", "id": mount.Name}); err != nil {
							t.Fatal(err)
						}
					}
				}
			}

			cfg := validConfig()
			cfg.FinishedCleanupDelay = tt.finished
			cfg.FailedCleanupDelay = tt.failed
			recorder := &cleanupRecordingClient{Client: client}
			exec := New(cfg, Deps{Docker: recorder, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil))})
			slot := &taskSlot{containerID: ref.ID, doneCh: make(chan struct{}), exitCh: make(chan struct{})}
			exec.addSlot(cache.ClaimKey{TaskID: 1, Attempt: 1, ClaimSeq: 1}, slot)
			slot.recordAcceptedTerminal(tt.eventType)

			cleaned := make(chan struct{})
			_, acceptedAt := slot.acceptedTerminalRecord()
			go func() {
				exec.cleanupContainer(slot)
				close(cleaned)
			}()

			time.Sleep(100 * time.Millisecond)
			if got := exec.RunningChildCount(); got != 1 {
				t.Fatalf("running count during cleanup delay = %d, want 1", got)
			}
			if !dockerContainerExists(t, ctx, client, executorID, ref.ID) {
				t.Fatal("container was removed before selected cleanup delay expired")
			}
			select {
			case <-cleaned:
			case <-time.After(5 * time.Second):
				t.Fatal("cleanup did not finish")
			}
			calls := recorder.recordedCalls()
			if len(calls) == 0 || calls[0].operation != "stop" || calls[0].containerID != ref.ID {
				t.Fatalf("cleanup did not stop the owned container: %+v", calls)
			}
			// Retention ends at the stop request, before Docker stop/remove latency.
			if elapsed := calls[0].at.Sub(acceptedAt); elapsed < 250*time.Millisecond || elapsed >= time.Second {
				t.Fatalf("selected cleanup delay elapsed = %s, want about 300ms", elapsed)
			}
			if dockerContainerExists(t, ctx, client, executorID, ref.ID) {
				t.Fatal("container still exists after cleanup delay")
			}

			// A second cleanup is a no-op and must tolerate the already-removed container.
			exec.cleanupContainer(slot)
			if got := len(recorder.recordedCalls()); got != len(calls) {
				t.Fatalf("second cleanup issued %d additional runtime calls", got-len(calls))
			}
		})
	}
}

func dockerSocketPath(t *testing.T) string {
	t.Helper()
	host := os.Getenv("DOCKER_HOST")
	if strings.HasPrefix(host, "unix://") {
		return strings.TrimPrefix(host, "unix://")
	}
	if runtimeDir := os.Getenv("XDG_RUNTIME_DIR"); runtimeDir != "" {
		candidate := runtimeDir + "/podman/podman.sock"
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return "/var/run/docker.sock"
}

func dockerContainerExists(t *testing.T, ctx context.Context, client dockerclient.Client, executorID, containerID string) bool {
	t.Helper()
	refs, err := client.ListOwned(ctx, executorID)
	if err != nil {
		t.Fatalf("list Docker containers: %v", err)
	}
	for _, ref := range refs {
		if ref.ID == containerID {
			return true
		}
	}
	return false
}

// cleanupRecordingClient observes runtime requests while delegating every call.
type cleanupRecordingClient struct {
	dockerclient.Client
	mu    sync.Mutex
	calls []cleanupRuntimeCall
}

type cleanupRuntimeCall struct {
	operation   string
	containerID string
	at          time.Time
}

func (c *cleanupRecordingClient) record(operation, containerID string) {
	at := time.Now()
	c.mu.Lock()
	c.calls = append(c.calls, cleanupRuntimeCall{operation: operation, containerID: containerID, at: at})
	c.mu.Unlock()
}

func (c *cleanupRecordingClient) recordedCalls() []cleanupRuntimeCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]cleanupRuntimeCall(nil), c.calls...)
}

func (c *cleanupRecordingClient) StopContainer(ctx context.Context, containerID string, timeout time.Duration) error {
	c.record("stop", containerID)
	return c.Client.StopContainer(ctx, containerID, timeout)
}

func (c *cleanupRecordingClient) ForceKill(ctx context.Context, containerID string) error {
	c.record("kill", containerID)
	return c.Client.ForceKill(ctx, containerID)
}
