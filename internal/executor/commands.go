package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/pandops/flowai-executor-docker-openhands/internal/broker"
	"github.com/pandops/flowai-executor-docker-openhands/internal/cache"
)

// Command consumer settings fixed by the contract.
const (
	commandAckWait           = 30 * time.Second
	commandInactiveThreshold = 24 * time.Hour
)

func (e *Executor) commandConsumer() string { return fmt.Sprintf("commands-%d", e.ident.ExecutorID) }

// createCommandConsumer creates (or reuses) the Executor's own durable
// pull consumer on TASK_MANAGER_COMMAND.
func (e *Executor) createCommandConsumer(ctx context.Context) error {
	name := e.commandConsumer()
	filter := fmt.Sprintf("task-manager.command.%d.sub", e.ident.ExecutorID)
	body, _ := json.Marshal(map[string]any{
		"stream_name": commandStream,
		"action":      "create",
		"config": map[string]any{
			"durable_name":       name,
			"filter_subject":     filter,
			"deliver_policy":     "all",
			"ack_policy":         "explicit",
			"ack_wait":           int64(commandAckWait),
			"inactive_threshold": int64(commandInactiveThreshold),
		},
	})
	reply, err := e.request(ctx, fmt.Sprintf("$JS.API.CONSUMER.CREATE.%s.%s.%s", commandStream, name, filter), body)
	if err != nil {
		return err
	}
	var r struct {
		Error *struct {
			Code        int    `json:"code"`
			ErrCode     int    `json:"err_code"`
			Description string `json:"description"`
		} `json:"error"`
	}
	if err := json.Unmarshal(reply.Data, &r); err != nil {
		return fmt.Errorf("consumer create reply: %w", err)
	}
	if r.Error != nil {
		return fmt.Errorf("consumer create refused: %d %s", r.Error.ErrCode, r.Error.Description)
	}
	return nil
}

// commandLoop keeps the command consumer and handles its commands.
func (e *Executor) commandLoop(ctx context.Context) {
	for ctx.Err() == nil {
		if !e.cmdReady.Load() {
			if e.revoked.Load() {
				sleepCtx(ctx, e.cfg.HeartbeatInterval)
				continue
			}
			if err := e.createCommandConsumer(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				e.log.Error("command consumer creation failed; retrying at the next heartbeat interval", "err", err)
				sleepCtx(ctx, e.cfg.HeartbeatInterval)
				continue
			}
			e.cmdReady.Store(true)
			e.signalCapacity()
		}
		conn := e.currentConn()
		msgs, err := conn.Fetch(ctx, commandStream, e.commandConsumer(), 1, e.cfg.PullWait)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			sleepCtx(ctx, 250*time.Millisecond)
			continue
		}
		for _, d := range msgs {
			e.handleCommand(ctx, d)
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// commandMessage is one command from task-manager.
type commandMessage struct {
	ExecutorID int64  `json:"executor_id"`
	Control    string `json:"control"`
	ControlID  int64  `json:"control_id"`
	TaskID     int64  `json:"task_id"`
	Attempt    int64  `json:"attempt"`
	ClaimSeq   int64  `json:"claim_seq"`
}

func (e *Executor) handleCommand(ctx context.Context, d broker.Delivery) {
	var c commandMessage
	if err := json.Unmarshal(d.Data(), &c); err != nil || c.Control != "cancel" || c.ControlID < 1 || c.TaskID < 1 {
		e.log.Warn("unusable command terminated", "subject", d.Subject())
		_ = d.Term(ctx)
		return
	}
	key := cache.ClaimKey{TaskID: c.TaskID, Attempt: c.Attempt, ClaimSeq: c.ClaimSeq}
	e.mu.Lock()
	s := e.slots[key]
	e.mu.Unlock()
	if s != nil {
		e.cancelClaim(s, c.ControlID)
	}
	if err := d.Ack(ctx); err != nil {
		e.log.Warn("command acknowledgement failed", "control_id", c.ControlID, "err", err)
	}
}

// cancelClaim carries out a cancel of a held claim.
func (e *Executor) cancelClaim(s *taskSlot, controlID int64) {
	if s.getState() == "terminal" {
		return
	}
	key := s.key
	if err := e.emit(s.ctx, "control_acknowledged", &key, fields{"control_id": controlID, "control": "cancel"}); err != nil {
		return
	}
	if s.getState() == "running" {
		if err := e.pauseAgent(s.ctx, s); err != nil {
			s.selfPaused.Store(false)
			reason := "pause_rejected"
			var ne net.Error
			var oe *net.OpError
			if errors.As(err, &oe) || errors.As(err, &ne) {
				reason = "runtime_unreachable"
			}
			e.log.Warn("cancel could not pause the agent", "task_id", key.TaskID, "control_id", controlID, "err", err)
			_ = e.emit(s.ctx, "control_failed", &key, fields{"control_id": controlID, "control": "cancel", "reason": reason})
			return
		}
	}
	if !s.claimTerminal() {
		return
	}
	s.setState("terminal")
	s.cancel()
	id, _ := s.container()
	e.removeContainer(context.WithoutCancel(e.bg), id)
	_ = e.emit(e.bg, "cancelled", &key, fields{"control_id": controlID})
	e.freeSlot(s)
}
