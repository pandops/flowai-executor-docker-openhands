package test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"

	"github.com/pandops/flowai-executor-docker-openhands/internal/broker"
)

// validNATSName matches a valid JetStream consumer name.
var validNATSName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,255}$`)

// answerExecutor installs heartbeat answers for an Executor id other than
// the harness default, for tests that start a second identity.
func (h *harness) answerExecutor(id int64) {
	h.broker.Handle(fmt.Sprintf("executor-registry.executor.%d.heartbeat.pub", id), func(m broker.Message) (broker.Reply, error) {
		var body map[string]any
		_ = json.Unmarshal(m.Data, &body)
		data, _ := json.Marshal(heartbeatReplyListing(body))
		hdr := broker.Header{}
		hdr.Set("status", strconv.Itoa(http.StatusOK))
		return broker.Reply{Status: http.StatusOK, Header: hdr, Data: data}, nil
	})
}

func commandCreateSubjectFor(id int64) string {
	return fmt.Sprintf("$JS.API.CONSUMER.CREATE.TASK_MANAGER_COMMAND.commands-%d.task-manager.command.%d.sub", id, id)
}
