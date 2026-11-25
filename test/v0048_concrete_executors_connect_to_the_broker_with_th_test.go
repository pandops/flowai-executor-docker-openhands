package test

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/pandops/flowai-executor-docker-openhands/internal/executor"
	fakebroker "github.com/pandops/flowai-executor-docker-openhands/internal/mocks/broker"
)

// Requirement: Concrete Executors connect to the broker with their one-time JWT.
func TestV0048ConcreteExecutorsConnectToTheBrokerWithTheirOneTimeJWT(t *testing.T) {
	t.Parallel()

	t.Run("First start stores the token", func(t *testing.T) {
		t.Parallel()
		// The mounted configuration starts world-readable and without a
		// token; the first start runs with --token.
		h := newHarness(t, func(o *opts) { o.tokenInYAML = false; o.cfgMode = 0o644 })
		h.startCLI("--config", h.cfgPath, "--token", h.token)
		info, err := os.Stat(h.cfgPath)
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("config mode = %v, %v; want 0600", info.Mode().Perm(), err)
		}
		var doc struct {
			Broker struct {
				Token string `yaml:"token"`
				URL   string `yaml:"url"`
			} `yaml:"broker"`
			MaxParallel int `yaml:"max_parallel_tasks"`
		}
		raw, _ := os.ReadFile(h.cfgPath)
		if err := yaml.Unmarshal(raw, &doc); err != nil || doc.Broker.Token != h.token {
			t.Fatalf("broker.token not stored: %v", err)
		}
		if doc.Broker.URL == "" || doc.MaxParallel != 2 {
			t.Fatalf("other keys lost: %+v", doc)
		}
		h.waitHeartbeats(1)
		ops := h.broker.Ops()
		dial := opIndex(ops, func(op fakebroker.Op) bool { return op.Kind == fakebroker.OpDial })
		hb := opIndex(ops, func(op fakebroker.Op) bool { return strings.HasSuffix(op.Subject, ".heartbeat.pub") })
		pull := opIndex(ops, func(op fakebroker.Op) bool { return op.Kind == fakebroker.OpFetch && op.Stream == streamTask })
		if dial < 0 || hb < dial {
			t.Fatalf("heartbeat before dial: dial=%d heartbeat=%d", dial, hb)
		}
		eventually(t, "task pull", func() bool { return len(h.taskFetches()) > 0 })
		if pull >= 0 && pull < dial {
			t.Fatalf("pull before dial")
		}
		d := h.broker.Dials()[0]
		if d.Token != h.token || d.InboxPrefix != "_INBOX.executor-7" || d.URL != "tls://nats.example:4222" || d.CAFile != "/run/flowai/stand-ca/ca.pem" {
			t.Fatalf("dial options = %+v", d)
		}
	})

	t.Run("Later start reads the stored token", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.waitHeartbeats(1)
		if d := h.broker.Dials(); len(d) == 0 || d[0].Token != h.token {
			t.Fatalf("dials = %+v", d)
		}
		// No HTTP registration: the only HTTP calls go to the two edges,
		// and none happened without a claim.
		if c := append(h.env.allCalls(), h.model.allCalls()...); len(c) != 0 {
			t.Fatalf("HTTP calls without a claim: %+v", c)
		}
	})

	t.Run("Start without any token", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, func(o *opts) { o.tokenInYAML = false })
		h.start()
		err, exited := h.waitExit(waitLong)
		if !exited || !errIs(err, executor.ErrNoToken) {
			t.Fatalf("Run = %v (exited %v), want ErrNoToken", err, exited)
		}
		if code, _ := h.probe("/v1/readyz"); code != http.StatusServiceUnavailable {
			t.Fatalf("readyz = %d, want 503", code)
		}
		if len(h.broker.Dials()) != 0 || len(h.taskFetches()) != 0 {
			t.Fatalf("broker activity without a token: %+v", h.broker.Ops())
		}
	})

	t.Run("Revoked token closes the connection", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, func(o *opts) { o.wait = "4s" })
		h.start()
		tk := newTask(1010)
		h.runTask(tk)
		oh := h.docker.openHandsFor(tk.TaskID)
		h.broker.Revoke(h.token)
		eventually(t, "unhealthy after revocation", func() bool {
			code, _ := h.probe("/v1/readyz")
			return code == http.StatusServiceUnavailable
		})
		eventually(t, "agent paused", func() bool { return oh.PauseCallCount() > 0 })
		if _, ok := h.docker.containerForTask(tk.TaskID); !ok {
			t.Fatalf("container removed before W")
		}
		dials := len(h.broker.Dials())
		pulls := len(h.taskFetches())
		eventually(t, "container removed at W", func() bool {
			_, ok := h.docker.containerForTask(tk.TaskID)
			return !ok
		})
		if len(h.taskFetches()) != pulls {
			t.Fatalf("pulled after revocation")
		}
		for _, d := range h.broker.Dials()[dials-1:] {
			if d.Token == h.token && len(h.broker.Dials()) > dials {
				t.Fatalf("redialed with the revoked JWT")
			}
		}
	})

	t.Run("Restart with a reissued token", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		tk := newTask(1011)
		h.runTask(tk)
		oh := h.docker.openHandsFor(tk.TaskID)
		h.broker.Revoke(h.token)
		eventually(t, "agent paused", func() bool { return oh.PauseCallCount() > 0 })
		before := len(h.broker.Stored())
		start := time.Now()
		h.sigterm()
		if time.Since(start) > 1500*time.Millisecond {
			t.Fatalf("shutdown waited for a drain: %s", time.Since(start))
		}
		if _, ok := h.docker.containerForTask(tk.TaskID); ok {
			t.Fatalf("container kept after SIGTERM")
		}
		if len(h.broker.Stored()) != before {
			t.Fatalf("published while the connection was revoked")
		}
		newToken := makeJWT(t, teamID, executorID, tagName, execType)
		h.token = newToken
		hbs, pulls := len(h.heartbeats()), len(h.taskFetches())
		h.startCLI("--config", h.cfgPath, "--token", newToken)
		h.waitHeartbeats(hbs + 1)
		eventually(t, "pull after restart", func() bool { return len(h.taskFetches()) > pulls })
		if h.hasTaskKind(tk.TaskID, "released") {
			t.Fatalf("released published for the dropped claim")
		}
		if oh.RunCallCount() != 0 {
			t.Fatalf("continue sent to the old agent")
		}
		last := h.heartbeats()[len(h.heartbeats())-1]
		if len(claimsIn(last)) != 0 {
			t.Fatalf("restarted Executor holds claims: %v", claimsIn(last))
		}
		if d := h.broker.Dials(); d[len(d)-1].Token != newToken {
			t.Fatalf("restart did not use the new JWT")
		}
	})

	t.Run("Team display name changes", func(t *testing.T) {
		t.Parallel()
		// team_name is not part of the identity: the JWT pins only
		// integers and the tag, and the Executor keeps serving its team.
		h := newHarness(t, nil)
		h.start()
		// The display-only team_name changes between two tasks; team_id
		// stays 3.
		named := func(id int64, name string) task {
			tk := newTask(id)
			body := tk.body()
			body["team_name"] = name
			tk.raw = body
			return tk
		}
		h.runTask(named(1012, "Team Three"))
		h.terminal(1012, "finished")
		h.waitTaskKind(1012, "finished")
		fetchesBefore := len(h.broker.Fetches())
		h.runTask(named(1013, "Renamed Team"))
		h.terminal(1013, "finished")
		h.waitTaskKind(1013, "finished")
		for _, ev := range h.events() {
			if _, has := ev.Body["team_name"]; has {
				t.Fatalf("event carries team_name: %s", ev.Raw)
			}
			if asInt(ev.Body["team_id"]) != teamID || ev.Team != "3" {
				t.Fatalf("team changed: header %q %s", ev.Team, ev.Raw)
			}
		}
		if kinds := h.kindsFor(1013); len(kinds) == 0 || kinds[len(kinds)-1] != "finished" {
			t.Fatalf("work after the rename did not continue: %v", kinds)
		}
		for _, op := range h.broker.Ops() {
			if op.Kind != fakebroker.OpPublish && op.Kind != fakebroker.OpRequest {
				continue
			}
			if strings.Contains(string(op.Data), "Team Three") || strings.Contains(string(op.Data), "Renamed Team") {
				t.Fatalf("team_name leaked into %s", op.Subject)
			}
			if team := op.Header.Get("team"); team != "" && team != "3" {
				t.Fatalf("header team = %q on %s", team, op.Subject)
			}
		}
		// Identity, team and tag stay the JWT's: one connection with the
		// same inbox, and pulls from the same (team 3, tag) task consumer.
		dials := h.broker.Dials()
		if len(dials) != 1 || dials[0].InboxPrefix != fmt.Sprintf("_INBOX.executor-%d", executorID) {
			t.Fatalf("dials after the rename: %+v", dials)
		}
		after := h.broker.Fetches()[fetchesBefore:]
		pulledTasks := false
		for _, f := range after {
			if f.Stream == streamTask {
				pulledTasks = true
				if f.Consumer != taskConsumerName(teamID, tagName) {
					t.Fatalf("pulled tasks from %s after the rename", f.Consumer)
				}
			}
		}
		if !pulledTasks {
			t.Fatalf("no task pulls after the rename")
		}
	})

	t.Run("Executor identity comes from the JWT as integers", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.start()
		h.runTask(newTask(1014))
		for _, op := range h.broker.Ops() {
			switch op.Kind {
			case fakebroker.OpPublish:
				if !strings.HasPrefix(op.Subject, "executor.event.7.") || !strings.HasSuffix(op.Subject, ".pub") {
					t.Fatalf("publish on foreign subject %s", op.Subject)
				}
			case fakebroker.OpRequest:
				ok := strings.HasPrefix(op.Subject, "$JS.API.CONSUMER.CREATE.TASK_MANAGER_COMMAND.commands-7.") ||
					(strings.Contains(op.Subject, ".executor.7.") && strings.HasSuffix(op.Subject, ".pub"))
				if !ok {
					t.Fatalf("request on foreign subject %s", op.Subject)
				}
			case fakebroker.OpFetch:
				if (op.Stream != streamTask || op.Consumer != taskConsumerName(3, "gpu")) && (op.Stream != streamCommand || op.Consumer != "commands-7") {
					t.Fatalf("pull from %s/%s", op.Stream, op.Consumer)
				}
			}
		}
		if h.broker.Dials()[0].InboxPrefix != "_INBOX.executor-7" {
			t.Fatalf("inbox prefix = %s", h.broker.Dials()[0].InboxPrefix)
		}
		for _, ev := range h.events() {
			if fmt.Sprintf("%T", ev.Body["team_id"]) != "float64" || asInt(ev.Body["team_id"]) != 3 || asInt(ev.Body["executor_id"]) != 7 {
				t.Fatalf("identity not integer: %s", ev.Raw)
			}
		}
	})

	t.Run("Token never appears in logs", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, nil)
		h.broker.SetDialError(fmt.Errorf("dial tls://nats.example:4222: connection refused"))
		h.start()
		eventually(t, "two dial attempts", func() bool { return len(h.broker.Dials()) >= 2 })
		h.broker.SetDialError(nil)
		h.waitHeartbeats(1)
		h.sigterm()
		logs := h.logs.String()
		parts := strings.Split(h.token, ".")
		for _, p := range append(parts, h.token) {
			if len(p) >= 8 && strings.Contains(logs, p) {
				t.Fatalf("log carries the JWT or part of it")
			}
		}
		if !strings.Contains(logs, "connection refused") {
			t.Fatalf("connection failure was not logged")
		}
	})
}
