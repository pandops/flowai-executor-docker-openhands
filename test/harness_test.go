// Shared harness for the v0048 Docker OpenHands Executor unit tests. The
// Executor runs in-process through its public API (LoadConfig, New, Run,
// Handler) against the fake broker, the in-memory Docker fake, one fake
// OpenHands agent server per started container, httptest servers for the
// env-registry and model-registry HTTP edges, a fake network source and a
// captured logger. Every message the Executor publishes or requests is
// validated against the Executor's own JSON Schemas.
package test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"

	"github.com/pandops/flowai-executor-docker-openhands/internal/broker"
	"github.com/pandops/flowai-executor-docker-openhands/internal/cache"
	"github.com/pandops/flowai-executor-docker-openhands/internal/cli"
	"github.com/pandops/flowai-executor-docker-openhands/internal/dockerclient"
	"github.com/pandops/flowai-executor-docker-openhands/internal/executor"
	fakebroker "github.com/pandops/flowai-executor-docker-openhands/internal/mocks/broker"
	fakedocker "github.com/pandops/flowai-executor-docker-openhands/internal/mocks/docker"
	fakeopenhands "github.com/pandops/flowai-executor-docker-openhands/internal/mocks/openhands"
	"github.com/pandops/flowai-executor-docker-openhands/internal/netaddr"
)

// The identity most tests run under: team 3, Executor 7, tag gpu.
const (
	teamID     int64 = 3
	executorID int64 = 7
	tagName          = "gpu"
	execType         = "executor_docker_openhands"

	streamTask    = "TASK_MANAGER_TASK"
	streamCommand = "TASK_MANAGER_COMMAND"
)

// waitLong bounds every wait for an Executor reaction.
const waitLong = 15 * time.Second

// tagHash is the first 16 lowercase hex characters of SHA-256(tag).
func tagHash(tag string) string {
	sum := sha256.Sum256([]byte(tag))
	return hex.EncodeToString(sum[:])[:16]
}

func taskConsumerName(team int64, tag string) string {
	return fmt.Sprintf("tasks-%d-%s", team, tagHash(tag))
}

func commandConsumerName(exec int64) string { return fmt.Sprintf("commands-%d", exec) }

// makeJWT returns a bearer user JWT signed by a fresh account key whose
// name claim pins the identity (agents.md rule 14).
func makeJWT(t testing.TB, team, exec int64, tag, typ string) string {
	t.Helper()
	akp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatalf("account key: %v", err)
	}
	ukp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatalf("user key: %v", err)
	}
	upub, _ := ukp.PublicKey()
	uc := jwt.NewUserClaims(upub)
	name, _ := json.Marshal(map[string]any{"team_id": team, "executor_id": exec, "tag": tag, "executor_type": typ})
	uc.Name = string(name)
	uc.BearerToken = true
	tok, err := uc.Encode(akp)
	if err != nil {
		t.Fatalf("encode jwt: %v", err)
	}
	return tok
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// fakeNet is a mutable netaddr.Source.
type fakeNet struct {
	mu   sync.Mutex
	snap netaddr.Snapshot
	err  error
}

func (n *fakeNet) Snapshot() (netaddr.Snapshot, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.snap, n.err
}

func (n *fakeNet) set(s netaddr.Snapshot) {
	n.mu.Lock()
	n.snap = s
	n.mu.Unlock()
}

func prefixes(t testing.TB, ps ...string) []netip.Prefix {
	t.Helper()
	out := make([]netip.Prefix, 0, len(ps))
	for _, p := range ps {
		out = append(out, netip.MustParsePrefix(p))
	}
	return out
}

// harnessDocker wraps FakeDocker: it records every ContainerSpec, can fail
// image pulls or starts, and starts one FakeOpenHands per container.
type harnessDocker struct {
	*fakedocker.FakeDocker
	t testing.TB

	mu       sync.Mutex
	specs    []dockerclient.ContainerSpec
	byPort   map[int]*fakeopenhands.FakeOpenHands
	byTask   map[int64]*fakeopenhands.FakeOpenHands
	pullErr  map[string]error
	startErr error
	// onStart configures each new FakeOpenHands before it serves.
	onStart func(taskID int64, oh *fakeopenhands.FakeOpenHands)
}

func newHarnessDocker(t testing.TB) *harnessDocker {
	return &harnessDocker{
		FakeDocker: fakedocker.NewFakeDocker(),
		t:          t,
		byPort:     map[int]*fakeopenhands.FakeOpenHands{},
		byTask:     map[int64]*fakeopenhands.FakeOpenHands{},
		pullErr:    map[string]error{},
	}
}

func (d *harnessDocker) PullImage(ctx context.Context, ref, policy string) error {
	d.mu.Lock()
	err := d.pullErr[ref]
	d.mu.Unlock()
	if err != nil {
		return err
	}
	return d.FakeDocker.PullImage(ctx, ref, policy)
}

func (d *harnessDocker) StartContainer(ctx context.Context, spec dockerclient.ContainerSpec) (*dockerclient.ContainerRef, error) {
	d.mu.Lock()
	d.specs = append(d.specs, spec)
	err := d.startErr
	hook := d.onStart
	d.mu.Unlock()
	if err != nil {
		return nil, err
	}
	taskID, _ := strconv.ParseInt(spec.Labels[dockerclient.LabelTaskID], 10, 64)
	oh := &fakeopenhands.FakeOpenHands{ConversationID: fmt.Sprintf("conv-%d", taskID)}
	if hook != nil {
		hook(taskID, oh)
	}
	oh.Start()
	d.t.Cleanup(oh.Stop)
	ref, err := d.FakeDocker.StartContainer(ctx, spec)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	if len(spec.Ports) > 0 {
		d.byPort[spec.Ports[0].HostPort] = oh
	}
	d.byTask[taskID] = oh
	d.mu.Unlock()
	return ref, nil
}

func (d *harnessDocker) ContainerURL(hostPort int) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if oh, ok := d.byPort[hostPort]; ok {
		return oh.URL()
	}
	return fmt.Sprintf("http://127.0.0.1:%d", hostPort)
}

func (d *harnessDocker) specsSnapshot() []dockerclient.ContainerSpec {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]dockerclient.ContainerSpec(nil), d.specs...)
}

func (d *harnessDocker) specFor(taskID int64) (dockerclient.ContainerSpec, bool) {
	for _, s := range d.specsSnapshot() {
		if s.Labels[dockerclient.LabelTaskID] == strconv.FormatInt(taskID, 10) {
			return s, true
		}
	}
	return dockerclient.ContainerSpec{}, false
}

func (d *harnessDocker) openHandsFor(taskID int64) *fakeopenhands.FakeOpenHands {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.byTask[taskID]
}

// containerForTask returns the live container of a task, if any.
func (d *harnessDocker) containerForTask(taskID int64) (dockerclient.ContainerRef, bool) {
	refs, _ := d.ListOwned(context.Background(), strconv.FormatInt(executorID, 10))
	for _, r := range refs {
		if r.Labels[dockerclient.LabelTaskID] == strconv.FormatInt(taskID, 10) {
			return r, true
		}
	}
	return dockerclient.ContainerRef{}, false
}

// edgeCall is one HTTP edge request.
type edgeCall struct {
	Path   string
	TaskID int64
	Auth   string
	At     time.Time
}

// edgeServer is a fake HTTP edge (env-registry open or model-registry key).
type edgeServer struct {
	srv *httptest.Server

	mu      sync.Mutex
	calls   []edgeCall
	respond func(taskID int64) (int, any)
}

func newEdgeServer(t testing.TB, suffix string, respond func(taskID int64) (int, any)) *edgeServer {
	e := &edgeServer{respond: respond}
	e.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rest, ok := strings.CutPrefix(r.URL.Path, "/v1/tasks/")
		idText, tail, _ := strings.Cut(rest, "/")
		taskID, err := strconv.ParseInt(idText, 10, 64)
		if !ok || err != nil || tail != suffix || r.Method != http.MethodGet {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		e.mu.Lock()
		e.calls = append(e.calls, edgeCall{Path: r.URL.Path, TaskID: taskID, Auth: r.Header.Get("Authorization"), At: time.Now()})
		respond := e.respond
		e.mu.Unlock()
		status, body := respond(taskID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(e.srv.Close)
	return e
}

func (e *edgeServer) set(respond func(taskID int64) (int, any)) {
	e.mu.Lock()
	e.respond = respond
	e.mu.Unlock()
}

func (e *edgeServer) callsFor(taskID int64) []edgeCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []edgeCall
	for _, c := range e.calls {
		if c.TaskID == taskID {
			out = append(out, c)
		}
	}
	return out
}

func (e *edgeServer) allCalls() []edgeCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]edgeCall(nil), e.calls...)
}

// edgeError is an edge error without request_id: the key edge has no
// request_id (D12 owner decision 2026-11-09).
func edgeError(code, failureReason string) map[string]any {
	body := map[string]any{"code": code, "message": code}
	if failureReason != "" {
		body["failure_reason"] = failureReason
	}
	return body
}

// Default edge answers.
func defaultEnvValues(taskID int64) map[string]string {
	return map[string]string{"REPO_URL": "https://git.example/acme/repo", "DEPLOY_TOKEN": fmt.Sprintf("s3cr3t-value-%d", taskID)}
}

func defaultLLM(taskID int64) map[string]any {
	return map[string]any{
		"OPENAI_BASE_URL": "http://litellm-gateway-data:4000/v1",
		"OPENAI_MODEL":    "3/gpt-4o",
		"OPENAI_API_KEY":  fmt.Sprintf("sk-task-key-%d", taskID),
	}
}

// opts configures one harness.
type opts struct {
	maxParallel    int
	heartbeat      string
	missLimit      int
	wait           string
	recheck        string
	requestTimeout string
	publishAck     string
	pullWait       string
	drain          string
	finishedDelay  string
	failedDelay    string
	// tokenInYAML stores the default JWT in broker.token of the YAML; when
	// false the YAML has no token and the test supplies one itself.
	tokenInYAML bool
	// token overrides the default JWT.
	token     string
	extraYAML string
	// omitKeys leaves these top-level keys out of the YAML so their
	// defaults apply.
	omitKeys []string
	// cfgMode is the mode the configuration file is written with.
	cfgMode os.FileMode
}

func defaultOpts() opts {
	return opts{
		maxParallel:    2,
		heartbeat:      "1s",
		missLimit:      3,
		wait:           "600s",
		recheck:        "200ms",
		requestTimeout: "500ms",
		publishAck:     "300ms",
		pullWait:       "100ms",
		drain:          "2s",
		finishedDelay:  "0s",
		failedDelay:    "0s",
		tokenInYAML:    true,
		cfgMode:        0o600,
	}
}

// harness is one Executor under test with its fakes.
type harness struct {
	t                             *testing.T
	o                             opts
	dir                           string
	cfgPath, cacheDir, cleanupDir string
	token                         string

	broker *fakebroker.Fake
	docker *harnessDocker
	net    *fakeNet
	env    *edgeServer
	model  *edgeServer
	logs   *syncBuffer
	schema *schemaSet

	mu     sync.Mutex
	exec   *executor.Executor
	cancel context.CancelFunc
	run    *runState
	runErr error
	// stopping is set by sigterm; exited and exitErr record Run's return.
	stopping, exited bool
	exitErr          error
	claimTok         func(body map[string]any) (int, any, error)
	hbReply          func(body map[string]any) (int, any, error)
	check            func(body map[string]any) (int, any, error)
}

// newHarness builds the fakes and writes the configuration; it does not
// start the Executor.
func newHarness(t *testing.T, mutate func(*opts)) *harness {
	t.Helper()
	o := defaultOpts()
	if mutate != nil {
		mutate(&o)
	}
	dir := t.TempDir()
	h := &harness{
		t:          t,
		o:          o,
		dir:        dir,
		cfgPath:    filepath.Join(dir, "config", "config.yaml"),
		cacheDir:   filepath.Join(dir, "cache"),
		cleanupDir: filepath.Join(dir, "cleanup"),
		broker:     fakebroker.New(),
		docker:     newHarnessDocker(t),
		net:        &fakeNet{},
		logs:       &syncBuffer{},
		schema:     loadSchemas(t),
	}
	h.token = o.token
	if h.token == "" {
		h.token = makeJWT(t, teamID, executorID, tagName, execType)
	}
	h.net.set(netaddr.Snapshot{DefaultIface4: "eth0", Addrs: map[string][]netip.Prefix{"eth0": prefixes(t, "192.0.2.10/24")}})
	h.env = newEdgeServer(t, "launch-parameters/open", func(taskID int64) (int, any) {
		return http.StatusOK, map[string]any{"values": defaultEnvValues(taskID)}
	})
	h.model = newEdgeServer(t, "litellm-key", func(taskID int64) (int, any) {
		return http.StatusOK, defaultLLM(taskID)
	})
	h.claimTok = func(body map[string]any) (int, any, error) {
		b := body["body"].(map[string]any)
		return http.StatusOK, tokenReply(body, fmt.Sprintf("ct-%v-%v-%v", b["task_id"], b["attempt"], b["claim_seq"])), nil
	}
	h.hbReply = func(body map[string]any) (int, any, error) {
		return http.StatusOK, heartbeatReplyListing(body), nil
	}
	h.check = func(body map[string]any) (int, any, error) {
		return http.StatusOK, map[string]any{"result": "ok", "model_alias": "3/gpt-4o"}, nil
	}
	h.installResponders()
	h.broker.MarkConsumer(streamTask, taskConsumerName(teamID, tagName))
	if err := os.MkdirAll(filepath.Dir(h.cfgPath), 0o700); err != nil {
		t.Fatal(err)
	}
	h.writeConfig()
	return h
}

// heartbeatReplyListing answers a heartbeat with claims_known = true and
// every claim the heartbeat carried (task-manager still holds them all).
func heartbeatReplyListing(req map[string]any) map[string]any {
	claims := []any{}
	if b, ok := req["body"].(map[string]any); ok {
		if cs, ok := b["claims"].([]any); ok {
			for _, c := range cs {
				m := c.(map[string]any)
				claims = append(claims, map[string]any{"task_id": m["task_id"], "attempt": m["attempt"], "claim_seq": m["claim_seq"]})
			}
		}
	}
	return map[string]any{"connection_state": "connected", "claims_known": true, "claims": claims}
}

func (h *harness) writeConfig() {
	o := h.o
	var b strings.Builder
	fmt.Fprintf(&b, "broker:\n  url: tls://nats.example:4222\n  ca_file: /run/flowai/stand-ca/ca.pem\n")
	if o.tokenInYAML {
		fmt.Fprintf(&b, "  token: %s\n", h.token)
	}
	fmt.Fprintf(&b, "env_registry_url: %s\nmodel_registry_url: %s\n", h.env.srv.URL, h.model.srv.URL)
	fmt.Fprintf(&b, "max_parallel_tasks: %d\nheartbeat_interval: %s\nheartbeat_miss_limit: %d\nconnection_wait: %s\n", o.maxParallel, o.heartbeat, o.missLimit, o.wait)
	fmt.Fprintf(&b, "pending_recheck_interval: %s\nrequest_timeout: %s\npublish_ack_timeout: %s\npull_wait: %s\n", o.recheck, o.requestTimeout, o.publishAck, o.pullWait)
	fmt.Fprintf(&b, "cache_dir: %s\ncleanup_id_dir: %s\nexecutor_api_bind: 127.0.0.1:0\n", h.cacheDir, h.cleanupDir)
	fmt.Fprintf(&b, "docker_socket_path: /run/flowai/runtime.sock\nopenhands_host_port_start: 19600\nopenhands_host_port_end: 19699\n")
	fmt.Fprintf(&b, "openhands_api_key: test-session-key\nopenhands_startup_timeout: 5s\nopenhands_drain_timeout: %s\n", o.drain)
	fmt.Fprintf(&b, "finished_cleanup_delay: %s\nfailed_cleanup_delay: %s\nwebsocket_dial_timeout: 2s\nlitellm_data_network: flowai-litellm-data\nlog_level: debug\n", o.finishedDelay, o.failedDelay)
	b.WriteString(o.extraYAML)
	var kept []string
	for _, line := range strings.Split(b.String(), "\n") {
		key, _, _ := strings.Cut(line, ":")
		if !slices.Contains(o.omitKeys, key) {
			kept = append(kept, line)
		}
	}
	if err := os.WriteFile(h.cfgPath, []byte(strings.Join(kept, "\n")), o.cfgMode); err != nil {
		h.t.Fatalf("write config: %v", err)
	}
	if err := os.Chmod(h.cfgPath, o.cfgMode); err != nil {
		h.t.Fatal(err)
	}
}

// installResponders answers the Executor's requests on the fake broker.
func (h *harness) installResponders() {
	reply := func(fn func() func(map[string]any) (int, any, error), schemaFile string) fakebroker.Handler {
		return func(m broker.Message) (broker.Reply, error) {
			var body map[string]any
			_ = json.Unmarshal(m.Data, &body)
			status, out, err := fn()(body)
			if err != nil {
				return broker.Reply{}, err
			}
			data, _ := json.Marshal(out)
			hdr := broker.Header{}
			hdr.Set("status", strconv.Itoa(status))
			return broker.Reply{Status: status, Header: hdr, Data: data}, nil
		}
	}
	h.broker.Handle(fmt.Sprintf("task-manager.executor.%d.claim-token.pub", executorID), reply(func() func(map[string]any) (int, any, error) {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.claimTok
	}, ""))
	h.broker.Handle(fmt.Sprintf("executor-registry.executor.%d.heartbeat.pub", executorID), reply(func() func(map[string]any) (int, any, error) {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.hbReply
	}, ""))
	h.broker.Handle(fmt.Sprintf("model-registry.executor.%d.check.pub", executorID), reply(func() func(map[string]any) (int, any, error) {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.check
	}, ""))
}

func (h *harness) setClaimToken(fn func(body map[string]any) (int, any, error)) {
	h.mu.Lock()
	h.claimTok = fn
	h.mu.Unlock()
}

func (h *harness) setHeartbeat(fn func(body map[string]any) (int, any, error)) {
	h.mu.Lock()
	h.hbReply = fn
	h.mu.Unlock()
}

func (h *harness) setCheck(fn func(body map[string]any) (int, any, error)) {
	h.mu.Lock()
	h.check = fn
	h.mu.Unlock()
}

// tokenReply is task-manager's 200 claim-token reply for the requested claim.
func tokenReply(req map[string]any, token string) map[string]any {
	b, _ := req["body"].(map[string]any)
	return map[string]any{"claim_token": token, "task_id": b["task_id"], "attempt": b["attempt"], "claim_seq": b["claim_seq"]}
}

// noReply makes a responder behave as if no reply arrived in time.
func noReply(map[string]any) (int, any, error) { return 0, nil, broker.ErrTimeout }

// start loads the configuration and runs the Executor until stop.
func (h *harness) start() {
	h.t.Helper()
	cfg, err := executor.LoadConfig(h.cfgPath)
	if err != nil {
		h.t.Fatalf("load config: %v", err)
	}
	h.startWith(cfg)
}

// startCLI prepares the configuration through the command's own
// argument handling (--config, --token), as the container entry point does,
// and starts the Executor with it.
func (h *harness) startCLI(args ...string) {
	h.t.Helper()
	cfg, err := cli.Prepare(args)
	if err != nil {
		h.t.Fatalf("prepare %v: %v", args, err)
	}
	h.startWith(cfg)
}

func (h *harness) startWith(cfg *executor.Config) {
	h.t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.MultiWriter(h.logs, testLogWriter{h.t}), &slog.HandlerOptions{Level: slog.LevelDebug}))
	exec := executor.New(cfg, executor.Deps{
		Dialer:     h.broker.Dialer(),
		Docker:     h.docker,
		Network:    h.net,
		Logger:     logger,
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
	})
	ctx, cancel := context.WithCancel(context.Background())
	run := &runState{done: make(chan struct{})}
	h.mu.Lock()
	h.exec, h.cancel, h.run = exec, cancel, run
	h.stopping, h.exited, h.exitErr = false, false, nil
	h.mu.Unlock()
	unexpectedExit.Store(h.t, func() error {
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.exited && !h.stopping {
			return fmt.Errorf("Executor exited on its own: %w", h.exitErr)
		}
		return nil
	})
	go func() {
		err := exec.Run(ctx)
		h.mu.Lock()
		h.exited, h.exitErr = true, err
		h.mu.Unlock()
		run.err = err
		close(run.done)
	}()
	h.t.Cleanup(func() {
		unexpectedExit.Delete(h.t)
		cancel()
		select {
		case <-run.done:
		case <-time.After(waitLong):
		}
	})
}

// runState is one Run of the Executor. done is closed when Run returns, so
// sigterm, waitExit and the cleanup all observe the same exit; err is Run's
// result and is safe to read after done is closed.
type runState struct {
	done chan struct{}
	err  error
}

// testLogWriter mirrors Executor logs into the test log on failure.
type testLogWriter struct{ t *testing.T }

func (w testLogWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// sigterm cancels Run, as SIGTERM does, and waits for it to return.
func (h *harness) sigterm() error {
	h.t.Helper()
	h.mu.Lock()
	cancel, run := h.cancel, h.run
	h.stopping = true
	h.mu.Unlock()
	cancel()
	select {
	case <-run.done:
		h.mu.Lock()
		h.runErr = run.err
		h.mu.Unlock()
		return run.err
	case <-time.After(waitLong):
		h.t.Fatalf("Executor did not exit within %s after SIGTERM", waitLong)
		return nil
	}
}

// waitExit waits for Run to return on its own.
func (h *harness) waitExit(d time.Duration) (error, bool) {
	h.mu.Lock()
	run := h.run
	h.mu.Unlock()
	select {
	case <-run.done:
		return run.err, true
	case <-time.After(d):
		return nil, false
	}
}

// probe calls one of the Executor's probe endpoints.
func (h *harness) probe(path string) (int, map[string]any) {
	h.t.Helper()
	h.mu.Lock()
	exec := h.exec
	h.mu.Unlock()
	rec := httptest.NewRecorder()
	exec.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec.Code, body
}

// openCache opens the Executor's cache while the Executor is not running.
func (h *harness) openCache() *cache.Store {
	h.t.Helper()
	store, err := cache.Open(h.cacheDir)
	if err != nil {
		h.t.Fatalf("open cache: %v", err)
	}
	return store
}

// --- task messages and commands --------------------------------------

// task describes one task message from task-manager (contract A1).
type task struct {
	TaskID, Attempt, ClaimSeq, FlowID, TeamID int64
	Tag                                       string
	Image                                     *string
	ImageSource                               string
	Prompt                                    *string
	RevisionID                                int64
	PendingLimit                              int
	// raw replaces the whole body when set.
	raw map[string]any
}

func strp(s string) *string { return &s }

func newTask(id int64) task {
	return task{TaskID: id, Attempt: 1, ClaimSeq: 1, FlowID: 42, TeamID: teamID, Tag: tagName,
		Image: strp("ghcr.io/acme/agent-runtime@sha256:" + strings.Repeat("a", 64)), ImageSource: "flow_default",
		Prompt: strp(fmt.Sprintf("Fix issue %d", id)), RevisionID: 900 + id, PendingLimit: 600}
}

var taskSeq struct {
	mu sync.Mutex
	n  int64
}

func (tk task) body() map[string]any {
	if tk.raw != nil {
		return tk.raw
	}
	taskSeq.mu.Lock()
	taskSeq.n++
	seq := taskSeq.n
	taskSeq.mu.Unlock()
	exec := map[string]any{
		"execution_revision_id": tk.RevisionID,
		"required_tag":          tk.Tag,
		"execution_parameters":  map[string]any{},
	}
	if tk.Prompt != nil {
		exec["rendered_prompt"] = *tk.Prompt
	}
	var image any
	if tk.Image != nil {
		image = *tk.Image
	}
	return map[string]any{
		"publisher": "task-manager", "publisher_seq": seq, "team_id": tk.TeamID,
		"occurred_at": time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		"task_id":     tk.TaskID, "flow_id": tk.FlowID, "attempt": tk.Attempt, "claim_seq": tk.ClaimSeq,
		"source_id": 31, "external_id": fmt.Sprintf("issue-%d", tk.TaskID), "intake_id": 5000 + tk.TaskID,
		"required_tag": tk.Tag, "project_id": nil, "environment_id": nil,
		"payload": map[string]any{"issue": tk.TaskID}, "execution": exec,
		"resolved_image": image, "image_source": tk.ImageSource, "pending_limit_seconds": tk.PendingLimit,
	}
}

// enqueueTask puts a task message on the Executor's task consumer.
func (h *harness) enqueueTask(tk task) *fakebroker.Delivered {
	h.t.Helper()
	data, _ := json.Marshal(tk.body())
	subject := fmt.Sprintf("task-manager.task.%d.%s.%d.sub", teamID, tagName, tk.TaskID)
	return h.broker.Enqueue(streamTask, taskConsumerName(teamID, tagName), subject, data)
}

// enqueueCancel puts a cancel command (contract A2) on commands-7.
func (h *harness) enqueueCancel(tk task, controlID int64) *fakebroker.Delivered {
	h.t.Helper()
	data, _ := json.Marshal(map[string]any{
		"publisher": "task-manager", "publisher_seq": 5000 + controlID, "team_id": teamID,
		"occurred_at": time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		"executor_id": executorID, "control": "cancel", "control_id": controlID,
		"task_id": tk.TaskID, "attempt": tk.Attempt, "claim_seq": tk.ClaimSeq,
	})
	return h.broker.Enqueue(streamCommand, commandConsumerName(executorID), fmt.Sprintf("task-manager.command.%d.sub", executorID), data)
}

// --- observed events --------------------------------------------------

// event is one Executor event JetStream stored.
type event struct {
	Subject  string
	MsgID    string
	Team     string
	Kind     string
	Seq      int64
	TaskID   int64
	Attempt  int64
	ClaimSeq int64
	Body     map[string]any
	Raw      []byte
}

func asInt(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	}
	return 0
}

// events returns every stored Executor event in stream order, after
// checking its schema, subject, Nats-Msg-Id and team header.
func (h *harness) events() []event {
	h.t.Helper()
	var out []event
	prefix := fmt.Sprintf("executor.event.%d.", executorID)
	for _, m := range h.broker.Stored() {
		if !strings.HasPrefix(m.Subject, prefix) {
			continue
		}
		h.schema.validate(h.t, executorEventSchema, m.Data)
		var body map[string]any
		if err := json.Unmarshal(m.Data, &body); err != nil {
			h.t.Errorf("event %s is not JSON: %v", m.Subject, err)
			continue
		}
		ev := event{Subject: m.Subject, MsgID: m.Header.Get("Nats-Msg-Id"), Team: m.Header.Get("team"), Body: body, Raw: m.Data}
		ev.Kind, _ = body["kind"].(string)
		ev.Seq = asInt(body["publisher_seq"])
		ev.TaskID, ev.Attempt, ev.ClaimSeq = asInt(body["task_id"]), asInt(body["attempt"]), asInt(body["claim_seq"])
		if want := prefix + ev.Kind + ".pub"; m.Subject != want {
			h.t.Errorf("event subject %q, want %q", m.Subject, want)
		}
		if want := fmt.Sprintf("executor-%d-%d-%d", teamID, executorID, ev.Seq); ev.MsgID != want {
			h.t.Errorf("Nats-Msg-Id %q, want %q", ev.MsgID, want)
		}
		if ev.Team != strconv.FormatInt(teamID, 10) {
			h.t.Errorf("team header %q, want %d", ev.Team, teamID)
		}
		out = append(out, ev)
	}
	return out
}

func (h *harness) eventsFor(taskID int64) []event {
	var out []event
	for _, ev := range h.events() {
		if ev.TaskID == taskID {
			out = append(out, ev)
		}
	}
	return out
}

func (h *harness) kindsFor(taskID int64) []string {
	var out []string
	for _, ev := range h.eventsFor(taskID) {
		out = append(out, ev.Kind)
	}
	return out
}

func (h *harness) selfKinds() []string {
	var out []string
	for _, ev := range h.events() {
		if ev.TaskID == 0 {
			out = append(out, ev.Kind)
		}
	}
	return out
}

// eventually polls cond until it holds or waitLong passes.
func eventually(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitLong)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		// Once the Executor has exited on its own nothing changes any more:
		// fail now instead of at the deadline.
		if err := exitedUnexpectedly(t); err != nil {
			if cond() {
				return
			}
			t.Fatalf("waiting for %s: %v", desc, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}

// unexpectedExit maps a test to a probe that reports whether its running
// Executor returned from Run without the test asking it to stop.
var unexpectedExit sync.Map

func exitedUnexpectedly(t testing.TB) error {
	if f, ok := unexpectedExit.Load(t); ok {
		return f.(func() error)()
	}
	return nil
}

// never asserts cond stays false for d.
func never(t *testing.T, desc string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			t.Fatalf("unexpected: %s", desc)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitEvent waits for the first event matching pred.
func (h *harness) waitEvent(desc string, pred func(event) bool) event {
	h.t.Helper()
	var found event
	eventually(h.t, desc, func() bool {
		for _, ev := range h.events() {
			if pred(ev) {
				found = ev
				return true
			}
		}
		return false
	})
	return found
}

func (h *harness) waitTaskKind(taskID int64, kind string) event {
	h.t.Helper()
	return h.waitEvent(fmt.Sprintf("%s for task %d", kind, taskID), func(ev event) bool {
		return ev.TaskID == taskID && ev.Kind == kind
	})
}

func (h *harness) waitSelfKind(kind string) event {
	h.t.Helper()
	return h.waitEvent(kind, func(ev event) bool { return ev.TaskID == 0 && ev.Kind == kind })
}

func (h *harness) hasTaskKind(taskID int64, kind string) bool {
	for _, k := range h.kindsFor(taskID) {
		if k == kind {
			return true
		}
	}
	return false
}

// --- observed requests --------------------------------------------------

// request is one request the Executor sent on the broker.
type request struct {
	Subject string
	Header  broker.Header
	Body    map[string]any
	Raw     []byte
	At      time.Time
}

// requestsOf returns the Executor's requests on `<svc>.executor.7.<op>.pub`,
// each checked against the Executor's own request schema.
func (h *harness) requestsOf(op string) []request {
	h.t.Helper()
	var out []request
	suffix := fmt.Sprintf(".executor.%d.%s.pub", executorID, op)
	for _, r := range h.broker.Requests() {
		if !strings.HasSuffix(r.Subject, suffix) {
			continue
		}
		if file, ok := requestSchemas[op]; ok {
			h.schema.validate(h.t, file, r.Data)
		}
		var body map[string]any
		_ = json.Unmarshal(r.Data, &body)
		out = append(out, request{Subject: r.Subject, Header: r.Header, Body: body, Raw: r.Data, At: r.At})
	}
	return out
}

func bodyOf(r request) map[string]any {
	b, _ := r.Body["body"].(map[string]any)
	return b
}

// heartbeats returns the bodies of every heartbeat sent.
func (h *harness) heartbeats() []map[string]any {
	var out []map[string]any
	for _, r := range h.requestsOf("heartbeat") {
		out = append(out, bodyOf(r))
	}
	return out
}

func (h *harness) waitHeartbeats(n int) []map[string]any {
	h.t.Helper()
	eventually(h.t, fmt.Sprintf("%d heartbeats", n), func() bool { return len(h.heartbeats()) >= n })
	return h.heartbeats()
}

// claimsIn returns "task/attempt/seq=state" for every claim of a heartbeat.
func claimsIn(hb map[string]any) []string {
	var out []string
	cs, _ := hb["claims"].([]any)
	for _, c := range cs {
		m := c.(map[string]any)
		out = append(out, fmt.Sprintf("%d/%d/%d=%s", asInt(m["task_id"]), asInt(m["attempt"]), asInt(m["claim_seq"]), m["state"]))
	}
	sort.Strings(out)
	return out
}

// taskFetches returns the batch sizes of every pull from the task consumer.
func (h *harness) taskFetches() []int {
	var out []int
	for _, op := range h.broker.Fetches() {
		if op.Stream == streamTask {
			out = append(out, op.Batch)
		}
	}
	return out
}

// opIndex returns the index of the first op matching pred, or -1.
func opIndex(ops []fakebroker.Op, pred func(fakebroker.Op) bool) int {
	for i, op := range ops {
		if pred(op) {
			return i
		}
	}
	return -1
}

// --- runtime helpers ----------------------------------------------------

// waitOpenHands waits until the task's agent server is up and its event
// stream is connected, and returns it.
func (h *harness) waitOpenHands(taskID int64) *fakeopenhands.FakeOpenHands {
	h.t.Helper()
	var oh *fakeopenhands.FakeOpenHands
	eventually(h.t, fmt.Sprintf("OpenHands stream of task %d", taskID), func() bool {
		oh = h.docker.openHandsFor(taskID)
		return oh != nil && oh.StreamConnFor(fmt.Sprintf("conv-%d", taskID)) != nil
	})
	return oh
}

// finish makes the task's conversation report a terminal status.
func (h *harness) terminal(taskID int64, status string) {
	h.t.Helper()
	oh := h.waitOpenHands(taskID)
	if err := oh.PushTerminal(fmt.Sprintf("conv-%d", taskID), status); err != nil {
		h.t.Fatalf("push terminal: %v", err)
	}
}

// runTask enqueues a task and waits until it is running.
func (h *harness) runTask(tk task) {
	h.t.Helper()
	h.enqueueTask(tk)
	h.waitTaskKind(tk.TaskID, "running")
	h.waitOpenHands(tk.TaskID)
}

// leaks reports every published or requested message that contains one of
// the secret strings.
func (h *harness) leaks(secrets ...string) []string {
	var out []string
	for _, op := range h.broker.Ops() {
		if op.Kind != fakebroker.OpPublish && op.Kind != fakebroker.OpRequest {
			continue
		}
		for _, s := range secrets {
			if s != "" && bytes.Contains(op.Data, []byte(s)) {
				out = append(out, fmt.Sprintf("%s carries %q", op.Subject, s))
			}
		}
	}
	return out
}

// errIs reports errors.Is for tests that compare Run errors.
func errIs(err, target error) bool { return errors.Is(err, target) }

// netipPrefix shortens snapshot literals in tests.
type netipPrefix = netip.Prefix

func opIndexSubject(ops []fakebroker.Op, subject string) int {
	return opIndex(ops, func(op fakebroker.Op) bool {
		return op.Subject == subject && (op.Kind != fakebroker.OpPublish || op.Acked)
	})
}

func opIndexFetch(ops []fakebroker.Op, stream string) int {
	return opIndex(ops, func(op fakebroker.Op) bool { return op.Kind == fakebroker.OpFetch && op.Stream == stream })
}
