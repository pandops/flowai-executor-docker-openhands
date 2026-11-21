package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pandops/flowai-executor-docker-openhands/internal/cache"
	"github.com/pandops/flowai-executor-docker-openhands/internal/netaddr"
	"github.com/pandops/flowai-executor-docker-openhands/internal/platform"
)

// heartbeatClaim is one claim of a heartbeat.
type heartbeatClaim struct {
	TaskID   int64  `json:"task_id"`
	Attempt  int64  `json:"attempt"`
	ClaimSeq int64  `json:"claim_seq"`
	State    string `json:"state"`
}

// heartbeatReply is executor-registry's heartbeat answer.
type heartbeatReply struct {
	ConnectionState string           `json:"connection_state"`
	ClaimsKnown     bool             `json:"claims_known"`
	Claims          []cache.ClaimKey `json:"claims"`
}

// heartbeatLoop sends one heartbeat every interval, also during
// connection loss; it stops at SIGTERM.
func (e *Executor) heartbeatLoop(ctx context.Context) {
	e.mu.Lock()
	e.lastReply = time.Now()
	e.mu.Unlock()
	ticker := time.NewTicker(e.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		e.sendHeartbeat(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (e *Executor) primaryIP() any {
	snap, err := e.deps.Network.Snapshot()
	if err == nil {
		if addr, ok := netaddr.Primary(snap); ok {
			return addr.String()
		}
	}
	e.log.Warn("heartbeat_ip_unavailable: no default-route interface address; heartbeat ip is null", "err", err)
	return nil
}

func (e *Executor) sendHeartbeat(ctx context.Context) {
	sentAt := time.Now()
	slots := e.slotsSnapshot()
	claims := []heartbeatClaim{}
	for _, s := range slots {
		switch st := s.getState(); st {
		case "created", "pending", "running":
			claims = append(claims, heartbeatClaim{TaskID: s.key.TaskID, Attempt: s.key.Attempt, ClaimSeq: s.key.ClaimSeq, State: st})
		}
	}
	body, _ := json.Marshal(map[string]any{
		"params": map[string]any{},
		"body": map[string]any{
			"executor_type":      platform.ExecutorTypeDockerOpenHands,
			"interval_seconds":   int64(e.cfg.HeartbeatInterval / time.Second),
			"miss_limit":         e.cfg.HeartbeatMissLimit,
			"wait_seconds":       int64(e.cfg.ConnectionWait / time.Second),
			"max_parallel_tasks": e.cfg.MaxParallelTasks,
			"slots_in_use":       len(slots),
			"ip":                 e.primaryIP(),
			"claims":             claims,
		},
	})
	reply, err := e.request(ctx, fmt.Sprintf("executor-registry.executor.%d.heartbeat.pub", e.ident.ExecutorID), body)
	if ctx.Err() != nil {
		return
	}
	var r heartbeatReply
	if err == nil && reply.Status >= 200 && reply.Status < 300 {
		err = json.Unmarshal(reply.Data, &r)
	} else if err == nil {
		err = fmt.Errorf("heartbeat answered with status %d", reply.Status)
	}
	if err != nil {
		e.heartbeatMissed(ctx, err)
		return
	}
	now := time.Now()
	e.mu.Lock()
	e.misses = 0
	e.lastReply = now
	e.mu.Unlock()
	if err := e.store.PutLastHeartbeat(e.bg, now); err != nil {
		e.log.Error("cache write failed", "err", err)
	}
	if e.inLoss.Load() {
		if r.ClaimsKnown && !e.revoked.Load() {
			e.restore(ctx, r.Claims, sentAt)
		}
		return
	}
	if r.ClaimsKnown {
		e.dropUnlisted(r.Claims, sentAt)
	}
}

// heartbeatMissed counts a failed heartbeat and enforces W.
func (e *Executor) heartbeatMissed(ctx context.Context, err error) {
	e.mu.Lock()
	e.misses++
	misses, last := e.misses, e.lastReply
	e.mu.Unlock()
	e.log.Warn("heartbeat failed", "misses", misses, "err", err)
	if misses >= e.cfg.HeartbeatMissLimit {
		e.enterLoss(ctx)
	}
	if e.inLoss.Load() && time.Since(last) >= e.cfg.ConnectionWait {
		e.expireW()
	}
}

// enterLoss pauses every agent and stops pulling.
func (e *Executor) enterLoss(ctx context.Context) {
	e.mu.Lock()
	if e.inLoss.Load() {
		e.mu.Unlock()
		return
	}
	e.inLoss.Store(true)
	e.lossSince = time.Now()
	e.mu.Unlock()
	e.log.Warn("connection loss: agents paused, no new tasks")
	for _, s := range e.slotsSnapshot() {
		if s.getState() != "running" {
			continue
		}
		if err := e.pauseAgent(context.WithoutCancel(ctx), s); err != nil {
			e.log.Warn("pause at connection loss failed", "task_id", s.key.TaskID, "err", err)
		}
	}
}

// expireW drops every claim silently: no reply came within W.
func (e *Executor) expireW() {
	slots := e.slotsSnapshot()
	if len(slots) > 0 {
		e.log.Warn("no heartbeat reply within W; task containers removed", "claims", len(slots))
	}
	for _, s := range slots {
		// A claim whose end is being published is aborted and forgotten too.
		s.claimTerminal()
		e.dropClaim(s)
	}
}

// restore ends connection loss after a reply that lists the claims
// task-manager still holds.
func (e *Executor) restore(ctx context.Context, listed []cache.ClaimKey, sentAt time.Time) {
	e.mu.Lock()
	lostAt := e.lossSince
	e.mu.Unlock()
	restoredAt := time.Now()
	if err := e.emit(ctx, "executor_connection_lost", nil, fields{"lost_at": stamp(lostAt)}); err != nil {
		return
	}
	if err := e.emit(ctx, "executor_connection_restored", nil, fields{"lost_at": stamp(lostAt), "restored_at": stamp(restoredAt)}); err != nil {
		return
	}
	keep := map[cache.ClaimKey]bool{}
	for _, k := range listed {
		keep[k] = true
	}
	for _, s := range e.slotsSnapshot() {
		if keep[s.key] {
			e.continueAgent(ctx, s)
			continue
		}
		if acked := s.ackedAt(); !acked.IsZero() && acked.Before(sentAt) && s.claimTerminal() {
			e.dropClaim(s)
		}
	}
	e.mu.Lock()
	e.lossSince = time.Time{}
	e.misses = 0
	e.mu.Unlock()
	e.inLoss.Store(false)
	e.log.Info("connection restored")
	e.signalCapacity()
}

// dropUnlisted drops claims a claims_known reply omits, but only those
// whose claim-token reply came before the heartbeat was sent.
func (e *Executor) dropUnlisted(listed []cache.ClaimKey, sentAt time.Time) {
	keep := map[cache.ClaimKey]bool{}
	for _, k := range listed {
		keep[k] = true
	}
	for _, s := range e.slotsSnapshot() {
		if keep[s.key] {
			continue
		}
		acked := s.ackedAt()
		if acked.IsZero() || !acked.Before(sentAt) {
			continue
		}
		if s.claimTerminal() {
			e.log.Info("task-manager no longer holds the claim; dropped", "task_id", s.key.TaskID)
			e.dropClaim(s)
		}
	}
}

func (s *taskSlot) ackedAt() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claimAckedAt
}
