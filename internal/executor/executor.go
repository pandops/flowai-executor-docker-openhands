// Package executor is the Docker OpenHands concrete Executor: it pulls
// work from its (team, tag) JetStream consumer, claims it through
// task-manager, checks it, runs one OpenHands container per claim and
// publishes every lifecycle event to JetStream.
package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/pandops/flowai-executor-docker-openhands/internal/broker"
	"github.com/pandops/flowai-executor-docker-openhands/internal/cache"
	"github.com/pandops/flowai-executor-docker-openhands/internal/dockerclient"
	"github.com/pandops/flowai-executor-docker-openhands/internal/httpapi"
	"github.com/pandops/flowai-executor-docker-openhands/internal/netaddr"
	"github.com/pandops/flowai-executor-docker-openhands/internal/openhands"
	"github.com/pandops/flowai-executor-docker-openhands/internal/platform"
)

// Startup errors returned by Run.
var (
	// ErrNoToken means neither --token nor a stored broker.token exists.
	ErrNoToken = errors.New("executor: no broker token; start once with --token <jwt>")
	// ErrCacheLocked means another process holds the cache volume.
	ErrCacheLocked = errors.New("executor: cache lock is held by another process")
)

// State is the Executor's coarse lifecycle state.
type State string

// Lifecycle states.
const (
	StateStarting       State = "starting"
	StateReady          State = "ready"
	StateConnectionLoss State = "connection_loss"
	StateStopping       State = "stopping"
	StateStopped        State = "stopped"
	StateFailed         State = "failed"
)

// Stream and consumer names.
const (
	taskStream    = "TASK_MANAGER_TASK"
	commandStream = "TASK_MANAGER_COMMAND"
)

// Claim-token repetition (fixed by the contract).
const (
	claimRetryEvery = time.Second
	claimRetryFor   = 10 * time.Second
)

// Deps are the Executor's external collaborators.
type Deps struct {
	Dialer     broker.Dialer
	Docker     dockerclient.Client
	Network    netaddr.Source
	Logger     *slog.Logger
	HTTPClient *http.Client
}

// Executor is one concrete Executor process.
type Executor struct {
	cfg  *Config
	deps Deps
	log  *slog.Logger

	ident     brokerIdentity
	store     *cache.Store
	cleanupID string

	connMu sync.Mutex
	conn   broker.Conn

	seqMu   sync.Mutex
	lastSeq int64

	mu        sync.Mutex
	slots     map[cache.ClaimKey]*taskSlot
	ports     map[int]bool
	lossSince time.Time
	lastReply time.Time
	misses    int

	executorID        atomic.Int64
	running           atomic.Bool
	connected         atomic.Bool
	revoked           atomic.Bool
	inLoss            atomic.Bool
	canSupervise      atomic.Bool
	cmdReady          atomic.Bool
	draining          atomic.Bool
	supervisionFailed atomic.Bool

	// capacity is signalled whenever a slot frees or pulling may resume.
	capacity chan struct{}
	// bg is the publish context: it outlives SIGTERM until Run returns.
	bg context.Context
}

// New constructs an Executor. It does no I/O; Run starts it.
func New(cfg *Config, deps Deps) *Executor {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if deps.HTTPClient == nil {
		deps.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	if deps.Network == nil {
		deps.Network = netaddr.System()
	}
	return &Executor{
		cfg:      cfg,
		deps:     deps,
		log:      logger.With("service", platform.ExecutorTypeDockerOpenHands),
		slots:    map[cache.ClaimKey]*taskSlot{},
		ports:    map[int]bool{},
		capacity: make(chan struct{}, 1),
		bg:       context.Background(),
	}
}

// Handler serves GET /v1/livez and GET /v1/readyz.
func (e *Executor) Handler() http.Handler {
	r := chi.NewRouter()
	r.Route("/v1", func(r chi.Router) {
		httpapi.RegisterProbes(r, e.executorID.Load, func() httpapi.ReadinessDeps {
			return httpapi.ReadinessDeps{
				BrokerConnected: e.running.Load() && e.connected.Load() && !e.revoked.Load() && e.cmdReady.Load(),
				ConnectionLoss:  e.inLoss.Load(),
				CanSupervise:    e.running.Load() && e.canSupervise.Load(),
			}
		})
	})
	return r
}

func (e *Executor) currentConn() broker.Conn {
	e.connMu.Lock()
	defer e.connMu.Unlock()
	return e.conn
}

func (e *Executor) signalCapacity() {
	select {
	case e.capacity <- struct{}{}:
	default:
	}
}

// Run starts the Executor and blocks until ctx is cancelled (SIGTERM) and
// shutdown finished, or until startup fails.
func (e *Executor) Run(ctx context.Context) error {
	lock, err := cache.Lock(e.cfg.CacheDir)
	if err != nil {
		if errors.Is(err, cache.ErrLocked) {
			return fmt.Errorf("%w: %w", ErrCacheLocked, err)
		}
		return err
	}
	defer func() { _ = lock.Release() }()
	store, err := cache.Open(e.cfg.CacheDir)
	if err != nil {
		if errors.Is(err, cache.ErrLocked) {
			return fmt.Errorf("%w: %w", ErrCacheLocked, err)
		}
		return err
	}
	defer func() { _ = store.Close() }()
	e.store = store

	if e.cfg.Broker.Token == "" {
		return ErrNoToken
	}
	ident, err := parseIdentity(e.cfg.Broker.Token)
	if err != nil {
		return err
	}
	e.ident = ident
	e.executorID.Store(ident.ExecutorID)
	e.log = e.log.With("executor_id", ident.ExecutorID, "team_id", ident.TeamID)

	cleanupPath := CleanupIDPath()
	if e.cfg.CleanupIDDir != "" {
		cleanupPath = filepath.Join(e.cfg.CleanupIDDir, CleanupIDFileName)
	}
	e.cleanupID, _ = loadOrCreateCleanupID(cleanupPath)

	if err := e.loadSeq(ctx); err != nil {
		return err
	}

	// Recovery: decide on cached claims before anything else changes.
	claims, err := store.ListClaims(ctx)
	if err != nil {
		return err
	}
	lastHB, _ := store.LastHeartbeat(ctx)
	withinW := !lastHB.IsZero() && time.Since(lastHB) < e.cfg.ConnectionWait

	bg, cancelBG := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelBG()
	e.bg = bg

	e.removeLeftovers(bg)
	if !withinW {
		for _, c := range claims {
			_ = store.DeleteClaim(bg, c.Key())
			_ = store.PruneSent(bg, c.Key())
		}
		claims = nil
		// Unsent events of claims dropped after W are gone with their
		// claims; only Executor-level events remain to be republished.
	}

	if err := e.dial(ctx); err != nil {
		return err
	}
	defer func() {
		if c := e.currentConn(); c != nil {
			c.Close()
		}
	}()
	e.running.Store(true)
	defer e.running.Store(false)
	e.canSupervise.Store(true)

	// Republish what an earlier process stored but never got acknowledged,
	// with the original publisher_seq and Nats-Msg-Id.
	if pending, err := store.PendingOutbox(bg); err == nil {
		for _, rec := range pending {
			if err := e.publishRecord(ctx, rec); err != nil {
				return e.stopEarly(ctx)
			}
		}
	}
	for _, c := range claims {
		key := c.Key()
		kinds, _ := store.SentKinds(bg, key)
		if !hasTerminal(kinds) {
			if err := e.emit(ctx, "released", &key, fields{"reason": "executor_restarted"}); err != nil {
				return e.stopEarly(ctx)
			}
		}
		_ = store.DeleteClaim(bg, key)
		_ = store.PruneSent(bg, key)
	}

	if err := store.PutExecutorID(bg, ident.ExecutorID); err != nil {
		e.log.Error("cache write failed", "err", err)
	}
	if err := e.emit(ctx, "executor_started", nil, fields{"executor_type": platform.ExecutorTypeDockerOpenHands}); err != nil {
		return e.stopEarly(ctx)
	}
	e.log.Info("executor started", "executor_type", platform.ExecutorTypeDockerOpenHands, "tag", ident.Tag)

	loopCtx, stopLoops := context.WithCancel(ctx)
	defer stopLoops()
	var wg sync.WaitGroup
	start := func(fn func(context.Context)) {
		wg.Add(1)
		go func() { defer wg.Done(); fn(loopCtx) }()
	}
	start(e.heartbeatLoop)
	start(e.watchConnection)
	start(e.watchDocker)
	start(e.commandLoop)
	start(e.taskLoop)

	<-ctx.Done()
	stopLoops()
	wg.Wait()
	e.shutdown()
	return ctx.Err()
}

// stopEarly is SIGTERM before startup finished: nothing is drained.
func (e *Executor) stopEarly(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return errors.New("executor: startup publish failed")
}

func hasTerminal(kinds []string) bool {
	for _, k := range kinds {
		switch k {
		case "finished", "failed", "cancelled", "released":
			return true
		}
	}
	return false
}

// removeLeftovers removes every container labelled with this host's
// cleanup_id: a previous process left it behind.
func (e *Executor) removeLeftovers(ctx context.Context) {
	refs, err := e.deps.Docker.ListLeftover(ctx, e.cleanupID)
	if err != nil {
		e.log.Error("listing leftover containers failed", "err", err)
		return
	}
	for _, r := range refs {
		e.removeContainer(ctx, r.ID)
		e.log.Info("removed leftover container", "container", r.Name)
	}
}

func (e *Executor) removeContainer(ctx context.Context, id string) {
	if id == "" {
		return
	}
	if err := e.deps.Docker.StopContainer(ctx, id, 10*time.Second); err != nil && !dockerclient.IsNotFound(err) {
		if kerr := e.deps.Docker.ForceKill(ctx, id); kerr != nil {
			e.log.Warn("container removal failed", "container_id", id, "err", err)
		}
	}
}

// dial connects to the broker, retrying about every second.
func (e *Executor) dial(ctx context.Context) error {
	opts := broker.DialOptions{
		URL:         e.cfg.Broker.URL,
		Token:       e.cfg.Broker.Token,
		CAFile:      e.cfg.Broker.CAFile,
		InboxPrefix: fmt.Sprintf("_INBOX.executor-%d", e.ident.ExecutorID),
		Name:        fmt.Sprintf("executor-%d", e.ident.ExecutorID),
	}
	for {
		conn, err := e.deps.Dialer.Dial(ctx, opts)
		if err == nil {
			e.connMu.Lock()
			e.conn = conn
			e.connMu.Unlock()
			e.connected.Store(true)
			return nil
		}
		e.log.Error("broker connection failed; retrying", "url", e.cfg.Broker.URL, "err", redact(err.Error(), e.cfg.Broker.Token))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// watchConnection follows reconnects and revocation of the connection.
func (e *Executor) watchConnection(ctx context.Context) {
	conn := e.currentConn()
	for {
		select {
		case <-ctx.Done():
			return
		case <-conn.Reconnected():
			e.connected.Store(true)
			e.cmdReady.Store(false)
			e.log.Info("broker reconnected")
			e.signalCapacity()
		case <-conn.Done():
			e.connected.Store(false)
			if errors.Is(conn.Err(), broker.ErrRevoked) {
				e.revoked.Store(true)
				e.log.Error("broker closed the connection: the JWT was revoked; not redialing")
				e.enterLoss(ctx)
			} else {
				e.log.Error("broker connection closed", "err", conn.Err())
			}
			return
		}
	}
}

// watchDocker supervises containers through the Docker events stream.
func (e *Executor) watchDocker(ctx context.Context) {
	filter := dockerclient.NewEventFilter(
		dockerclient.EventFilterEntry{Key: dockerclient.EventFilterKeyType, Value: "container"},
		dockerclient.EventFilterEntry{Key: dockerclient.EventFilterKeyLabel, Value: dockerclient.LabelExecutorID + "=" + strconv.FormatInt(e.ident.ExecutorID, 10)},
	)
	events, errs := e.deps.Docker.SubscribeEvents(ctx, filter)
	for {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			e.supervisionLost(ctx, err)
			return
		case ev, ok := <-events:
			if !ok {
				e.supervisionLost(ctx, errors.New("docker events stream closed"))
				return
			}
			if ev.Action != "die" {
				continue
			}
			if s := e.slotByContainer(ev.ActorID, ev.ActorName); s != nil {
				s.closeExit()
			}
		}
	}
}

func (e *Executor) supervisionLost(ctx context.Context, err error) {
	e.log.Error("container supervision lost; no new tasks are taken", "err", err)
	e.canSupervise.Store(false)
	e.supervisionFailed.Store(true)
	_ = e.emit(ctx, "executor_failed", nil, fields{"phase": "container_supervision"})
}

func (e *Executor) slotByContainer(id, name string) *taskSlot {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, s := range e.slots {
		cid, cname := s.container()
		if (id != "" && (id == cid || id == cname)) || (name != "" && (name == cname || name == cid)) {
			return s
		}
	}
	return nil
}

// --- slots -------------------------------------------------------------

// taskSlot holds the per-claim runtime state.
type taskSlot struct {
	key  cache.ClaimKey
	task *taskMessage

	ctx    context.Context
	cancel context.CancelFunc

	// containerID is the task's container.
	containerID   string
	containerName string
	hostPort      int
	// doneCh closes when the claim is finished; exitCh when the container exited.
	doneCh, exitCh chan struct{}
	exitOnce       sync.Once
	doneOnce       sync.Once

	mu           sync.Mutex
	state        string // created, pending, running, terminal
	claimAckedAt time.Time
	oh           *openhands.Client
	convID       string
	// cancelControl is the control_id of a cancel being carried out.
	cancelControl int64
	// selfPaused means the Executor paused the agent itself.
	selfPaused atomic.Bool
	// terminal is set once a terminal event was chosen.
	terminal bool
	// toolLogs holds the logs each tool call published (frame loop only).
	toolLogs toolCallLogs

	// failureReason is the failure_reason the conversation stream established.
	failureReason atomic.Value
	// errorCode is the code of the latest OpenHands conversation error.
	errorCode atomic.Value

	// acceptedTerminalMu guards the accepted-terminal fields; cleanup
	// needs the acknowledged terminal kind to select
	// finished_cleanup_delay or failed_cleanup_delay.
	acceptedTerminalMu sync.Mutex
	acceptedTerminal   string
	acceptedAt         time.Time
	cleaned            bool
}

func (s *taskSlot) container() (string, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.containerID, s.containerName
}

func (s *taskSlot) getState() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func (s *taskSlot) setState(st string) {
	s.mu.Lock()
	s.state = st
	s.mu.Unlock()
}

func (s *taskSlot) closeExit() {
	s.exitOnce.Do(func() {
		if s.exitCh != nil {
			close(s.exitCh)
		}
	})
}

func (s *taskSlot) closeDone() {
	s.doneOnce.Do(func() {
		if s.doneCh != nil {
			close(s.doneCh)
		}
	})
}

// claimTerminal marks the slot terminal; it reports false when another
// path already chose the claim's end.
func (s *taskSlot) claimTerminal() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminal {
		return false
	}
	s.terminal = true
	return true
}

// recordAcceptedTerminal records the terminal kind JetStream acknowledged
// and when.
func (s *taskSlot) recordAcceptedTerminal(kind string) {
	if s == nil {
		return
	}
	s.acceptedTerminalMu.Lock()
	defer s.acceptedTerminalMu.Unlock()
	s.acceptedTerminal = kind
	s.acceptedAt = time.Now().UTC()
}

// acceptedTerminalRecord returns the acknowledged terminal kind ("",
// "finished" or "failed") and its acknowledgement time.
func (s *taskSlot) acceptedTerminalRecord() (string, time.Time) {
	if s == nil {
		return "", time.Time{}
	}
	s.acceptedTerminalMu.Lock()
	defer s.acceptedTerminalMu.Unlock()
	return s.acceptedTerminal, s.acceptedAt
}

// addSlot registers a claim's slot.
func (e *Executor) addSlot(key cache.ClaimKey, slot *taskSlot) {
	slot.key = key
	if slot.doneCh == nil {
		slot.doneCh = make(chan struct{})
	}
	if slot.exitCh == nil {
		slot.exitCh = make(chan struct{})
	}
	if slot.ctx == nil {
		slot.ctx, slot.cancel = context.WithCancel(e.bg)
	}
	e.mu.Lock()
	e.slots[key] = slot
	e.mu.Unlock()
}

// freeSlot forgets a claim: its slot, host port and cache record.
func (e *Executor) freeSlot(s *taskSlot) {
	e.mu.Lock()
	if cur, ok := e.slots[s.key]; ok && cur == s {
		delete(e.slots, s.key)
	}
	if s.hostPort != 0 {
		delete(e.ports, s.hostPort)
	}
	e.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
	}
	if e.store != nil {
		if err := e.store.DeleteClaim(context.WithoutCancel(e.bg), s.key); err != nil {
			e.log.Error("cache delete failed", "task_id", s.key.TaskID, "err", err)
		}
		_ = e.store.PruneSent(context.WithoutCancel(e.bg), s.key)
	}
	s.closeDone()
	e.signalCapacity()
}

// cleanupContainer removes a slot's container after its terminal cleanup
// delay; a second call is a no-op.
func (e *Executor) cleanupContainer(slot *taskSlot) {
	slot.acceptedTerminalMu.Lock()
	if slot.cleaned {
		slot.acceptedTerminalMu.Unlock()
		return
	}
	slot.cleaned = true
	kind, at := slot.acceptedTerminal, slot.acceptedAt
	slot.acceptedTerminalMu.Unlock()
	delay := time.Duration(0)
	switch kind {
	case "finished":
		delay = e.cfg.FinishedCleanupDelay
	case "failed":
		delay = e.cfg.FailedCleanupDelay
	}
	if wait := time.Until(at.Add(delay)); delay > 0 && wait > 0 {
		time.Sleep(wait)
	}
	id, _ := slot.container()
	e.removeContainer(context.WithoutCancel(e.bg), id)
}

// RunningChildCount is the number of slots in use.
func (e *Executor) RunningChildCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.slots)
}

func (e *Executor) slotsSnapshot() []*taskSlot {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]*taskSlot, 0, len(e.slots))
	for _, s := range e.slots {
		out = append(out, s)
	}
	return out
}

func (e *Executor) freeSlots() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cfg.MaxParallelTasks - len(e.slots)
}

// allocatePort returns the lowest free host port of the configured range.
func (e *Executor) allocatePort() (int, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for p := e.cfg.OpenHandsPortStart; p <= e.cfg.OpenHandsPortEnd; p++ {
		if !e.ports[p] {
			e.ports[p] = true
			return p, true
		}
	}
	return 0, false
}

// dropClaim forgets a claim without any event: its container is removed.
func (e *Executor) dropClaim(s *taskSlot) {
	if s.cancel != nil {
		s.cancel()
	}
	id, _ := s.container()
	e.removeContainer(context.WithoutCancel(e.bg), id)
	e.freeSlot(s)
}

// --- shutdown ------------------------------------------------------------

// shutdown is SIGTERM: stop, release, drain, clean up.
func (e *Executor) shutdown() {
	ctx := e.bg
	e.draining.Store(true)
	if e.inLoss.Load() || e.revoked.Load() {
		// No connection: nothing is drained or published.
		for _, s := range e.slotsSnapshot() {
			e.dropClaim(s)
		}
		e.clearOutbox()
		e.log.Info("executor stopped during connection loss")
		return
	}
	_ = e.emit(ctx, "executor_stopping", nil, nil)
	var wg sync.WaitGroup
	for _, s := range e.slotsSnapshot() {
		if s.getState() == "running" {
			continue
		}
		wg.Add(1)
		go func(s *taskSlot) {
			defer wg.Done()
			e.release(s, "executor_shutdown")
		}(s)
	}
	wg.Wait()
	drain, stopDrain := context.WithTimeout(ctx, e.cfg.OpenHandsDrainTO)
	for _, s := range e.slotsSnapshot() {
		select {
		case <-s.doneCh:
		case <-drain.Done():
		}
	}
	stopDrain()
	for _, s := range e.slotsSnapshot() {
		wg.Add(1)
		go func(s *taskSlot) {
			defer wg.Done()
			e.release(s, "executor_shutdown")
		}(s)
	}
	wg.Wait()
	// Terminal cleanups still waiting on their delay end now.
	for _, s := range e.slotsSnapshot() {
		e.dropClaim(s)
	}
	_ = e.emit(ctx, "executor_stopped", nil, nil)
	e.log.Info("executor stopped")
}

// clearOutbox drops every unsent event.
func (e *Executor) clearOutbox() {
	ctx := context.WithoutCancel(e.bg)
	pending, err := e.store.PendingOutbox(ctx)
	if err != nil {
		return
	}
	for _, r := range pending {
		_ = e.store.MarkSent(ctx, r.PublisherSeq)
	}
}

// release ends a claim with released(reason) unless it already ended.
func (e *Executor) release(s *taskSlot, reason string) {
	if !s.claimTerminal() {
		<-s.doneCh
		return
	}
	s.cancel()
	id, _ := s.container()
	e.removeContainer(context.WithoutCancel(e.bg), id)
	key := s.key
	_ = e.emit(e.bg, "released", &key, fields{"reason": reason})
	e.freeSlot(s)
}
