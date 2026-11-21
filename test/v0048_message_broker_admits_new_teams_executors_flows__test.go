package test

import (
	"testing"
)

// Requirement: message-broker admits new teams, Executors, Flows and
// Sources without a restart — the Executor's side.
func TestV0048MessageBrokerAdmitsNewTeamsExecutorsFlowsAndSourcesWithoutARestart(t *testing.T) {
	t.Parallel()

	t.Run("Executor is created without a restart", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, func(o *opts) { o.token = makeJWT(t, teamID, 12, tagName, execType) })
		h.answerExecutor(12)
		h.start()
		eventually(t, "commands-12 created through the JetStream API", func() bool {
			for _, op := range h.broker.Requests() {
				if op.Subject == commandCreateSubjectFor(12) {
					return h.broker.ConsumerExists(streamCommand, "commands-12")
				}
			}
			return false
		})
		eventually(t, "pull from the team's tag consumer", func() bool {
			for _, op := range h.broker.Fetches() {
				if op.Stream == streamTask && op.Consumer == taskConsumerName(teamID, tagName) {
					return true
				}
			}
			return false
		})
		dials := h.broker.Dials()
		if len(dials) != 1 || dials[0].Token != h.token || dials[0].InboxPrefix != "_INBOX.executor-12" {
			t.Fatalf("dials = %+v", dials)
		}
	})
}
