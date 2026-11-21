package executor

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/jwt/v2"
)

// brokerIdentity is the identity executor-registry pinned in the user
// JWT's name claim: {"team_id":3,"executor_id":7,"tag":"gpu",
// "executor_type":"executor_docker_openhands"}.
type brokerIdentity struct {
	TeamID       int64  `json:"team_id"`
	ExecutorID   int64  `json:"executor_id"`
	Tag          string `json:"tag"`
	ExecutorType string `json:"executor_type"`
}

// parseIdentity decodes the bearer user JWT and reads its name claim. The
// broker verifies the signature; the Executor only needs the identity.
func parseIdentity(token string) (brokerIdentity, error) {
	uc, err := jwt.DecodeUserClaims(token)
	if err != nil {
		return brokerIdentity{}, fmt.Errorf("executor: broker token is not a NATS user JWT: %w", err)
	}
	var id brokerIdentity
	if err := json.Unmarshal([]byte(uc.Name), &id); err != nil {
		return brokerIdentity{}, fmt.Errorf("executor: broker token name claim is not the Executor identity: %w", err)
	}
	if id.TeamID < 1 || id.ExecutorID < 1 || id.Tag == "" {
		return brokerIdentity{}, errors.New("executor: broker token name claim lacks team_id, executor_id or tag")
	}
	return id, nil
}
