package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pandops/flowai-executor-docker-openhands/internal/broker"
	"github.com/pandops/flowai-executor-docker-openhands/internal/cache"
)

// taskMessage is one task message from task-manager.
type taskMessage struct {
	Publisher    string
	PublisherSeq int64
	TeamID       int64
	TaskID       int64
	FlowID       int64
	Attempt      int64
	ClaimSeq     int64
	RequiredTag  string
	Prompt       *string
	Image        *string
	ImageSource  string
	PendingLimit time.Duration
}

// parseTaskMessage validates a task message against the Executor's
// identity; the reason names the first violation.
func (e *Executor) parseTaskMessage(data []byte) (*taskMessage, string) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var raw map[string]any
	if err := dec.Decode(&raw); err != nil {
		return nil, "malformed_message"
	}
	m := &taskMessage{}
	m.Publisher, _ = raw["publisher"].(string)
	m.PublisherSeq, _ = positiveInt(raw["publisher_seq"])
	var ok bool
	if m.TaskID, ok = positiveInt(raw["task_id"]); !ok {
		return m, "invalid_task_id"
	}
	if m.Attempt, ok = positiveInt(raw["attempt"]); !ok {
		return m, "invalid_attempt"
	}
	if m.ClaimSeq, ok = positiveInt(raw["claim_seq"]); !ok {
		return m, "invalid_claim_seq"
	}
	if m.TeamID, ok = positiveInt(raw["team_id"]); !ok || m.TeamID != e.ident.TeamID {
		return m, "foreign_team"
	}
	m.FlowID, _ = positiveInt(raw["flow_id"])
	exec, _ := raw["execution"].(map[string]any)
	if exec == nil {
		return m, "missing_execution"
	}
	m.RequiredTag, _ = exec["required_tag"].(string)
	if top, ok := raw["required_tag"].(string); m.RequiredTag != e.ident.Tag || (ok && top != e.ident.Tag) {
		return m, "foreign_tag"
	}
	if p, ok := exec["rendered_prompt"].(string); ok && p != "" {
		m.Prompt = &p
	}
	if img, ok := raw["resolved_image"].(string); ok && img != "" {
		m.Image = &img
	}
	m.ImageSource, _ = raw["image_source"].(string)
	if n, ok := positiveInt(raw["pending_limit_seconds"]); ok {
		m.PendingLimit = time.Duration(n) * time.Second
	}
	return m, ""
}

func positiveInt(v any) (int64, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	i, err := n.Int64()
	if err != nil || i < 1 {
		return 0, false
	}
	return i, true
}

// pullAllowed reports whether the Executor may take new work now.
func (e *Executor) pullAllowed() bool {
	return e.cmdReady.Load() && !e.inLoss.Load() && !e.revoked.Load() && !e.draining.Load() &&
		!e.supervisionFailed.Load() && e.connected.Load()
}

// taskLoop pulls task messages up to the free slots, in delivery order.
func (e *Executor) taskLoop(ctx context.Context) {
	consumer := fmt.Sprintf("tasks-%d-%s", e.ident.TeamID, tagHash(e.ident.Tag))
	for ctx.Err() == nil {
		free := e.freeSlots()
		if free < 1 || !e.pullAllowed() {
			select {
			case <-ctx.Done():
				return
			case <-e.capacity:
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		conn := e.currentConn()
		msgs, err := conn.Fetch(ctx, taskStream, consumer, free, e.cfg.PullWait)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			e.log.Debug("task pull failed", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(250 * time.Millisecond):
			}
			continue
		}
		for _, d := range msgs {
			e.takeTask(ctx, d)
		}
	}
}

// takeTask claims one delivered task message: the claim is persisted and
// its created event acknowledged before the message is acknowledged.
func (e *Executor) takeTask(ctx context.Context, d broker.Delivery) {
	m, reason := e.parseTaskMessage(d.Data())
	if reason != "" {
		e.log.Warn("protocol violation on the task consumer", "reason", reason, "subject", d.Subject())
		_ = d.Term(ctx)
		f := fields{"reason": reason}
		if s := d.Subject(); len(s) <= 512 {
			f["subject"] = s
		}
		if m != nil && m.Publisher != "" && len(m.Publisher) <= 128 {
			f["message_publisher"] = m.Publisher
		}
		if m != nil && m.PublisherSeq > 0 {
			f["message_publisher_seq"] = m.PublisherSeq
		}
		_ = e.emit(e.bg, "protocol_violation", nil, f)
		return
	}
	key := cache.ClaimKey{TaskID: m.TaskID, Attempt: m.Attempt, ClaimSeq: m.ClaimSeq}
	slot := &taskSlot{task: m, state: "created"}
	e.addSlot(key, slot)
	rec := cache.ClaimRecord{
		TaskID: m.TaskID, Attempt: m.Attempt, ClaimSeq: m.ClaimSeq, TeamID: e.ident.TeamID,
		ExecutorID: e.ident.ExecutorID, FlowID: m.FlowID, State: cache.ClaimClaiming,
		ResolvedImageSource: m.ImageSource, UpdatedAt: time.Now().UTC(),
	}
	if err := e.store.PutClaim(e.bg, rec); err != nil {
		e.log.Error("cache write failed; claim abandoned", "task_id", m.TaskID, "err", err)
		e.freeSlot(slot)
		return
	}
	createdSeq, err := e.emitSeq(slot.ctx, "created", &key, nil)
	if err != nil {
		e.freeSlot(slot)
		return
	}
	rec.CreatedPublisherSeq = createdSeq
	_ = e.store.PutClaim(e.bg, rec)
	if err := d.Ack(ctx); err != nil {
		e.log.Warn("task message acknowledgement failed", "task_id", m.TaskID, "err", err)
	}
	// The next message waits for this one's first claim-token exchange
	// (queue order): at most the request timeout, inside the 10 s.
	first := make(chan struct{})
	go e.runClaim(slot, rec, first)
	select {
	case <-first:
	case <-ctx.Done():
	}
}

// claimTokenReply is task-manager's claim-token answer.
type claimTokenReply struct {
	ClaimToken string `json:"claim_token"`
	TaskID     int64  `json:"task_id"`
	Attempt    int64  `json:"attempt"`
	ClaimSeq   int64  `json:"claim_seq"`
	Code       string `json:"code"`
}

// requestClaimToken asks task-manager for the claim token, repeating the
// identical body every second for at most ten seconds.
// first is closed once the first request has ended.
func (e *Executor) requestClaimToken(s *taskSlot, createdSeq int64, first chan struct{}) (string, bool) {
	defer func() {
		if first != nil {
			close(first)
		}
	}()
	body, _ := json.Marshal(map[string]any{
		"params": map[string]any{},
		"body": map[string]any{
			"task_id": s.key.TaskID, "attempt": s.key.Attempt, "claim_seq": s.key.ClaimSeq,
			"created_publisher_seq": createdSeq,
		},
	})
	subject := fmt.Sprintf("task-manager.executor.%d.claim-token.pub", e.ident.ExecutorID)
	deadline := time.Now().Add(claimRetryFor)
	for {
		sent := time.Now()
		reply, err := e.request(s.ctx, subject, body)
		if first != nil {
			close(first)
			first = nil
		}
		if s.ctx.Err() != nil {
			return "", false
		}
		if err == nil {
			var r claimTokenReply
			_ = json.Unmarshal(reply.Data, &r)
			switch {
			case reply.Status == 200 && r.ClaimToken != "":
				if r.TaskID != s.key.TaskID || r.Attempt != s.key.Attempt || r.ClaimSeq != s.key.ClaimSeq {
					e.log.Error("claim token reply names another claim; claim dropped", "task_id", s.key.TaskID)
					return "", false
				}
				return r.ClaimToken, true
			case reply.Status == 409 && r.Code == "claim_rejected":
				e.log.Info("claim lost", "task_id", s.key.TaskID)
				return "", false
			}
		}
		next := sent.Add(claimRetryEvery)
		if next.After(deadline) {
			e.log.Info("claim token not issued within 10 s; claim dropped", "task_id", s.key.TaskID)
			return "", false
		}
		select {
		case <-s.ctx.Done():
			return "", false
		case <-time.After(time.Until(next)):
		}
	}
}

// request sends one core request with the request timeout.
func (e *Executor) request(ctx context.Context, subject string, body []byte) (broker.Reply, error) {
	conn := e.currentConn()
	if conn == nil {
		return broker.Reply{}, broker.ErrNotConnected
	}
	rctx, cancel := context.WithTimeout(ctx, e.cfg.RequestTimeout)
	defer cancel()
	return conn.Request(rctx, broker.Message{Subject: subject, Data: body})
}

// runClaim carries one claim from its claim token to its end.
func (e *Executor) runClaim(s *taskSlot, rec cache.ClaimRecord, first chan struct{}) {
	token, ok := e.requestClaimToken(s, rec.CreatedPublisherSeq, first)
	if !ok {
		if s.claimTerminal() {
			e.freeSlot(s)
		}
		return
	}
	s.mu.Lock()
	s.claimAckedAt = time.Now()
	s.mu.Unlock()
	rec.ClaimAckedAt = time.Now().UTC()
	_ = e.store.PutClaim(e.bg, rec)

	m := s.task
	if m.Image == nil {
		e.fail(s, "image_missing", false, "")
		return
	}
	if m.Prompt == nil {
		e.fail(s, "invalid_flow_revision", false, "")
		return
	}
	launch, ok := e.prepare(s, &rec, token)
	if !ok {
		return
	}
	e.runRuntime(s, &rec, launch)
}

// fail ends a claim with failed.
func (e *Executor) fail(s *taskSlot, reason string, retryable bool, code string) {
	if !s.claimTerminal() {
		return
	}
	s.setState("terminal")
	key := s.key
	f := fields{"failure_reason": reason, "retryable": retryable}
	if code != "" {
		f["openhands_error_code"] = code
	}
	id, _ := s.container()
	if err := e.emit(s.ctx, "failed", &key, f); err == nil {
		s.recordAcceptedTerminal("failed")
	}
	if id != "" {
		e.cleanupContainer(s)
	}
	e.freeSlot(s)
}
