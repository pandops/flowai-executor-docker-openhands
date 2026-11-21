package test

import (
	"testing"
)

// Requirement: message-broker defines five JetStream streams with fixed
// retention — the Executor's side of consumer naming.
func TestV0048MessageBrokerDefinesFiveJetStreamStreamsWithFixedRetention(t *testing.T) {
	t.Parallel()

	t.Run("Consumer names are derived from identifiers and the tag hash", func(t *testing.T) {
		t.Parallel()
		const tag = "GPU_large-1"
		want := taskConsumerName(teamID, tag)
		if want != "tasks-3-"+tagHash(tag) || len(tagHash(tag)) != 16 || !validNATSName.MatchString(want) {
			t.Fatalf("fixture consumer name %q", want)
		}
		for _, id := range []int64{7, 8} {
			h := newHarness(t, func(o *opts) { o.token = makeJWT(t, teamID, id, tag, execType) })
			h.broker.MarkConsumer(streamTask, want)
			h.answerExecutor(id)
			h.start()
			eventually(t, "task pull", func() bool {
				for _, op := range h.broker.Fetches() {
					if op.Stream == streamTask && op.Consumer == want {
						return true
					}
				}
				return false
			})
			for _, op := range h.broker.Fetches() {
				if op.Stream == streamTask && op.Consumer != want {
					t.Fatalf("Executor %d pulled %q, want the shared %q", id, op.Consumer, want)
				}
			}
			cmd := commandConsumerName(id)
			if !validNATSName.MatchString(cmd) {
				t.Fatalf("invalid command consumer name %q", cmd)
			}
			eventually(t, "own command consumer created", func() bool { return h.broker.ConsumerExists(streamCommand, cmd) })
			for _, op := range h.broker.Requests() {
				if len(op.Subject) > 24 && op.Subject[:24] == "$JS.API.CONSUMER.CREATE." && op.Subject != commandCreateSubjectFor(id) {
					t.Fatalf("Executor %d created a consumer it does not own: %s", id, op.Subject)
				}
			}
			h.sigterm()
		}
	})
}
