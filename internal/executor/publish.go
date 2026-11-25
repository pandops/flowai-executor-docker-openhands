package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pandops/flowai-executor-docker-openhands/internal/broker"
	"github.com/pandops/flowai-executor-docker-openhands/internal/cache"
)

// timeLayout is the event timestamp format: UTC with milliseconds.
const timeLayout = "2006-01-02T15:04:05.000Z"

func stamp(t time.Time) string { return t.UTC().Format(timeLayout) }

// seqState is the publisher_seq state file next to the configuration.
type seqState struct {
	Reserved int64 `json:"publisher_seq_reserved"`
}

// loadSeq reads the reserved publisher_seq so that new events continue
// above every number an earlier process could have used.
func (e *Executor) loadSeq(ctx context.Context) error {
	var st seqState
	data, err := os.ReadFile(e.cfg.StatePath())
	switch {
	case err == nil:
		if err := json.Unmarshal(data, &st); err != nil {
			return fmt.Errorf("executor: state file %s: %w", e.cfg.StatePath(), err)
		}
	case errors.Is(err, os.ErrNotExist):
	default:
		return fmt.Errorf("executor: read state file: %w", err)
	}
	last := st.Reserved
	pending, err := e.store.PendingOutbox(ctx)
	if err != nil {
		return err
	}
	for _, r := range pending {
		if r.PublisherSeq > last {
			last = r.PublisherSeq
		}
	}
	e.seqMu.Lock()
	e.lastSeq = last
	e.seqMu.Unlock()
	return nil
}

// reserveSeq allocates the next publisher_seq and makes it durable (temp
// file, fsync, rename, directory fsync) before it is used.
func (e *Executor) reserveSeq() (int64, error) {
	e.seqMu.Lock()
	defer e.seqMu.Unlock()
	next := e.lastSeq + 1
	if err := writeStateFile(e.cfg.StatePath(), seqState{Reserved: next}); err != nil {
		return 0, err
	}
	e.lastSeq = next
	return next, nil
}

func writeStateFile(path string, st seqState) error {
	data, _ := json.Marshal(st)
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".executor-state-*")
	if err != nil {
		return fmt.Errorf("executor: state file: %w", err)
	}
	name := tmp.Name()
	defer func() { _ = os.Remove(name) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("executor: state file: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("executor: state file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("executor: state file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("executor: state file: %w", err)
	}
	if err := os.Rename(name, path); err != nil {
		return fmt.Errorf("executor: state file: %w", err)
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// fields are the kind-specific fields of one event.
type fields map[string]any

// emit builds one event, stores it in the outbox and publishes it. It
// returns after the JetStream acknowledgement, or with ctx's error. A log
// event is tried once: its loss never changes the task lifecycle.
func (e *Executor) emit(ctx context.Context, kind string, claim *cache.ClaimKey, f fields) error {
	_, err := e.emitSeq(ctx, kind, claim, f)
	return err
}

// emitSeq is emit that also returns the event's publisher_seq.
func (e *Executor) emitSeq(ctx context.Context, kind string, claim *cache.ClaimKey, f fields) (int64, error) {
	seq, err := e.reserveSeq()
	if err != nil {
		e.log.Error("publisher_seq reservation failed", "kind", kind, "err", err)
		return 0, err
	}
	body := map[string]any{
		"publisher":     "executor",
		"publisher_seq": seq,
		"team_id":       e.ident.TeamID,
		"executor_id":   e.ident.ExecutorID,
		"kind":          kind,
		"occurred_at":   stamp(time.Now()),
	}
	if claim != nil {
		body["task_id"], body["attempt"], body["claim_seq"] = claim.TaskID, claim.Attempt, claim.ClaimSeq
	}
	for k, v := range f {
		body[k] = v
	}
	data, err := json.Marshal(body)
	if err != nil {
		return seq, err
	}
	rec := cache.OutboxRecord{
		PublisherSeq: seq,
		Subject:      fmt.Sprintf("executor.event.%d.%s.pub", e.ident.ExecutorID, kind),
		Kind:         kind,
		Claim:        claim,
		Body:         data,
		State:        cache.OutboxPending,
		UpdatedAt:    time.Now().UTC(),
	}
	if err := e.store.PutOutbox(ctx, rec); err != nil {
		e.log.Error("outbox write failed", "kind", kind, "err", err)
		return seq, err
	}
	if kind == "log" {
		err := e.publishOnce(ctx, rec)
		if err != nil {
			e.log.Error("task log publish failed; the log line is dropped", "task_id", claim.TaskID, "publisher_seq", seq, "err", err)
		}
		_ = e.store.MarkSent(context.WithoutCancel(ctx), seq)
		return seq, nil
	}
	return seq, e.publishRecord(ctx, rec)
}

func (e *Executor) msgID(seq int64) string {
	return fmt.Sprintf("executor-%d-%d-%d", e.ident.TeamID, e.ident.ExecutorID, seq)
}

// publishOnce sends one outbox record and waits for its acknowledgement.
func (e *Executor) publishOnce(ctx context.Context, rec cache.OutboxRecord) error {
	conn := e.currentConn()
	if conn == nil {
		return broker.ErrNotConnected
	}
	hdr := broker.Header{}
	hdr.Set("Nats-Msg-Id", e.msgID(rec.PublisherSeq))
	hdr.Set("team", strconv.FormatInt(e.ident.TeamID, 10))
	pctx, cancel := context.WithTimeout(ctx, e.cfg.PublishAckTimeout)
	defer cancel()
	return conn.Publish(pctx, broker.Message{Subject: rec.Subject, Header: hdr, Data: rec.Body})
}

// publishRecord retries one outbox record with the identical body and
// Nats-Msg-Id until JetStream acknowledges it or ctx ends.
func (e *Executor) publishRecord(ctx context.Context, rec cache.OutboxRecord) error {
	for {
		err := e.publishOnce(ctx, rec)
		if err == nil {
			if err := e.store.MarkSent(context.WithoutCancel(ctx), rec.PublisherSeq); err != nil {
				e.log.Error("outbox update failed", "publisher_seq", rec.PublisherSeq, "err", err)
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		e.log.Warn("event publish not acknowledged; retrying", "kind", rec.Kind, "publisher_seq", rec.PublisherSeq, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}
