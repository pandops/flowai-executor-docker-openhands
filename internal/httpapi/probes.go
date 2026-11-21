// Package httpapi provides the Executor's HTTP probe scaffolding.
package httpapi

import (
	"net/http"
	"sync/atomic"

	"github.com/go-chi/chi/v5"

	"github.com/pandops/flowai-executor-docker-openhands/internal/platform"
)

// ReadinessDeps are the readiness conditions. The Executor is ready only
// when it is connected to the broker with a valid JWT, is not in connection
// loss, and can supervise its containers.
type ReadinessDeps struct {
	BrokerConnected bool
	ConnectionLoss  bool
	CanSupervise    bool
}

// Ready reports whether every condition holds.
func (d ReadinessDeps) Ready() bool {
	return d.BrokerConnected && !d.ConnectionLoss && d.CanSupervise
}

// ReadinessChecker returns the current readiness conditions.
type ReadinessChecker func() ReadinessDeps

// ReadinessFunc adapts a func that only knows ready/not-ready.
func ReadinessFunc(fn func() bool) ReadinessChecker {
	return func() ReadinessDeps {
		ok := fn()
		return ReadinessDeps{BrokerConnected: ok, CanSupervise: ok}
	}
}

// RegisterProbes mounts GET /livez and GET /readyz onto the router.
// /livez answers 200 while the process is alive; /readyz answers 200 only
// when ready() reports ready and 503 otherwise.
func RegisterProbes(r chi.Router, executorID func() int64, ready ReadinessChecker) {
	r.Get("/livez", func(w http.ResponseWriter, req *http.Request) {
		JSON(w, req, http.StatusOK, platform.LivenessResponse{Status: "ok", ExecutorID: executorID(), Service: platform.ExecutorTypeDockerOpenHands})
	})
	r.Get("/readyz", func(w http.ResponseWriter, req *http.Request) {
		deps := ready()
		body := platform.ReadinessResponse{
			Status:          "ready",
			ExecutorID:      executorID(),
			Service:         platform.ExecutorTypeDockerOpenHands,
			BrokerConnected: deps.BrokerConnected,
			ConnectionLoss:  deps.ConnectionLoss,
			CanSupervise:    deps.CanSupervise,
		}
		status := http.StatusOK
		if !deps.Ready() {
			body.Status = "not_ready"
			status = http.StatusServiceUnavailable
		}
		JSON(w, req, status, body)
	})
}

// AtomicBool is a thin wrapper around atomic.Bool.
type AtomicBool struct {
	v atomic.Bool
}

// Set updates the value.
func (a *AtomicBool) Set(v bool) { a.v.Store(v) }

// Get returns the value.
func (a *AtomicBool) Get() bool { return a.v.Load() }
