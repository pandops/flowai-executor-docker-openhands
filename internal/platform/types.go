// Package platform holds the Executor's own wire constants and probe types.
package platform

// ExecutorTypeDockerOpenHands is the concrete Executor type identifier.
//
// The wire value follows executor_<runtime>_<tool> documented in AGENTS.md
// and matches the concrete service directory. The Go identifier preserves
// the runtime/tool axis; both refer to the same concrete Executor.
const ExecutorTypeDockerOpenHands = "executor_docker_openhands"

// Task log streams.
const (
	TaskLogStreamWork      = "work"
	TaskLogStreamReasoning = "reasoning"
)

// LivenessResponse is the GET /v1/livez body.
type LivenessResponse struct {
	Status     string `json:"status"`
	ExecutorID int64  `json:"executor_id,omitempty"`
	Service    string `json:"service"`
}

// ReadinessResponse is the GET /v1/readyz body.
type ReadinessResponse struct {
	Status          string `json:"status"`
	ExecutorID      int64  `json:"executor_id,omitempty"`
	Service         string `json:"service"`
	BrokerConnected bool   `json:"broker_connected"`
	ConnectionLoss  bool   `json:"connection_loss"`
	CanSupervise    bool   `json:"can_supervise"`
}
