package cache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func openTestStore(t *testing.T) (*Store, func()) {
	t.Helper()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return store, func() { _ = store.Close() }
}

// The cache stores the integer executor_id the JWT pins and the time of
// the last successful heartbeat in the metadata bucket.
func TestOpenAndStoreExecutorIDAndLastHeartbeat(t *testing.T) {
	store, cleanup := openTestStore(t)
	defer cleanup()
	ctx := context.Background()
	if got, err := store.ExecutorID(ctx); err != nil || got != 0 {
		t.Fatalf("fresh executor_id = %d, %v; want 0, nil", got, err)
	}
	if err := store.PutExecutorID(ctx, 7); err != nil {
		t.Fatalf("put executor_id: %v", err)
	}
	if got, err := store.ExecutorID(ctx); err != nil || got != 7 {
		t.Fatalf("executor_id = %d, %v; want 7", got, err)
	}
	at := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	if err := store.PutLastHeartbeat(ctx, at); err != nil {
		t.Fatalf("put last heartbeat: %v", err)
	}
	if got, err := store.LastHeartbeat(ctx); err != nil || !got.Equal(at) {
		t.Fatalf("last heartbeat = %s, %v; want %s", got, err, at)
	}
}

func TestOpenRejectsUnsupportedNewerSchema(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_ = store.Close()

	dbPath := filepath.Join(dir, "executor.db")
	if err := forceStoredSchemaVersion(dbPath, SchemaVersion+1); err != nil {
		t.Fatalf("force schema version: %v", err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatalf("expected unsupported-schema rejection, got nil")
	}
}

// Claims are keyed by (task_id, attempt, claim_seq); two attempts of one
// task are two records.
func TestClaimLifecycle(t *testing.T) {
	store, cleanup := openTestStore(t)
	defer cleanup()
	ctx := context.Background()
	first := ClaimRecord{TaskID: 1010, Attempt: 1, ClaimSeq: 1, TeamID: 3, ExecutorID: 7, State: ClaimClaiming, CreatedPublisherSeq: 41}
	second := ClaimRecord{TaskID: 1010, Attempt: 2, ClaimSeq: 1, TeamID: 3, ExecutorID: 7, State: ClaimClaiming}
	for _, rec := range []ClaimRecord{first, second} {
		if err := store.PutClaim(ctx, rec); err != nil {
			t.Fatalf("put claim: %v", err)
		}
	}
	got, ok, err := store.Claim(ctx, first.Key())
	if err != nil || !ok || got.CreatedPublisherSeq != 41 || got.State != ClaimClaiming {
		t.Fatalf("claim = %+v, %v, %v", got, ok, err)
	}
	first.State = ClaimRunning
	first.ContainerID = "cid-1"
	if err := store.PutClaim(ctx, first); err != nil {
		t.Fatalf("update claim: %v", err)
	}
	all, err := store.ListClaims(ctx)
	if err != nil || len(all) != 2 || all[0].State != ClaimRunning || all[1].Attempt != 2 {
		t.Fatalf("claims = %+v, %v", all, err)
	}
	if err := store.DeleteClaim(ctx, first.Key()); err != nil {
		t.Fatalf("delete claim: %v", err)
	}
	if _, ok, _ := store.Claim(ctx, first.Key()); ok {
		t.Fatalf("deleted claim still present")
	}
}

// TestAssignmentLifecycle keeps the pre-v0048 oracle on the v0048 claim
// record (assignments became claims; the record still lives in the
// assignments bucket): put, read back by key, update the state, read the new
// state back by key, delete, and the record is gone.
func TestAssignmentLifecycle(t *testing.T) {
	store, cleanup := openTestStore(t)
	defer cleanup()

	rec := ClaimRecord{
		TaskID:     101,
		Attempt:    2,
		ClaimSeq:   5,
		TeamID:     3,
		ExecutorID: 7,
		State:      ClaimClaiming,
		UpdatedAt:  time.Now().UTC(),
	}
	if err := store.PutClaim(context.Background(), rec); err != nil {
		t.Fatalf("put assignment: %v", err)
	}
	got, ok, err := store.Claim(context.Background(), rec.Key())
	if err != nil || !ok {
		t.Fatalf("assignment: %v, found %v", err, ok)
	}
	if got.TaskID != rec.TaskID || got.Attempt != rec.Attempt || got.ClaimSeq != rec.ClaimSeq {
		t.Fatalf("assignment record mismatch: %+v", got)
	}
	// Distinct key parts: a permuted key must not find the claim.
	if _, ok, _ := store.Claim(context.Background(), ClaimKey{TaskID: rec.Attempt, Attempt: rec.ClaimSeq, ClaimSeq: rec.TaskID}); ok {
		t.Fatalf("a permuted claim key found the claim")
	}

	rec.State = ClaimRunning
	if err := store.PutClaim(context.Background(), rec); err != nil {
		t.Fatalf("update assignment: %v", err)
	}
	got, _, _ = store.Claim(context.Background(), rec.Key())
	if got.State != ClaimRunning {
		t.Fatalf("assignment state = %q, want running", got.State)
	}

	if err := store.DeleteClaim(context.Background(), rec.Key()); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got, ok, _ := store.Claim(context.Background(), rec.Key()); ok || got.TaskID != 0 {
		t.Fatalf("expected task to be gone, got %+v", got)
	}
}

// Outbox events are keyed by publisher_seq, returned in seq order while
// pending, and leave the pending list once sent. Deleting a claim drops
// its unsent events but keeps the sent ones' record.
func TestOutboxLifecycle(t *testing.T) {
	store, cleanup := openTestStore(t)
	defer cleanup()
	ctx := context.Background()
	key := ClaimKey{TaskID: 1010, Attempt: 1, ClaimSeq: 1}
	other := ClaimKey{TaskID: 1011, Attempt: 1, ClaimSeq: 1}
	for _, rec := range []OutboxRecord{
		{PublisherSeq: 43, Subject: "executor.event.7.running.pub", Kind: "running", Claim: &key, Body: []byte(`{}`), State: OutboxPending},
		{PublisherSeq: 42, Subject: "executor.event.7.created.pub", Kind: "created", Claim: &key, Body: []byte(`{}`), State: OutboxPending},
		{PublisherSeq: 44, Subject: "executor.event.7.created.pub", Kind: "created", Claim: &other, Body: []byte(`{}`), State: OutboxPending},
	} {
		if err := store.PutOutbox(ctx, rec); err != nil {
			t.Fatalf("put outbox: %v", err)
		}
	}
	pending, err := store.PendingOutbox(ctx)
	if err != nil || len(pending) != 3 || pending[0].PublisherSeq != 42 || pending[1].PublisherSeq != 43 {
		t.Fatalf("pending = %+v, %v", pending, err)
	}
	if err := store.MarkSent(ctx, 42); err != nil {
		t.Fatalf("mark sent: %v", err)
	}
	kinds, err := store.SentKinds(ctx, key)
	if err != nil || len(kinds) != 1 || kinds[0] != "created" {
		t.Fatalf("sent kinds = %v, %v", kinds, err)
	}
	if err := store.PutClaim(ctx, ClaimRecord{TaskID: 1010, Attempt: 1, ClaimSeq: 1, State: ClaimRunning}); err != nil {
		t.Fatalf("put claim: %v", err)
	}
	if err := store.DeleteClaim(ctx, key); err != nil {
		t.Fatalf("delete claim: %v", err)
	}
	pending, _ = store.PendingOutbox(ctx)
	if len(pending) != 1 || pending[0].PublisherSeq != 44 {
		t.Fatalf("pending after claim delete = %+v, want only 44", pending)
	}
}

func TestLockFileSecondProcessFails(t *testing.T) {
	store, cleanup := openTestStore(t)
	defer cleanup()

	first, err := store.LockFile()
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	defer func() { _ = first.Release() }()

	if _, err := store.LockFile(); err == nil {
		t.Fatalf("second lock attempt should fail with contention")
	}
}

func TestLockReleaseAllowsRelock(t *testing.T) {
	store, cleanup := openTestStore(t)
	defer cleanup()

	first, err := store.LockFile()
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}

	second, err := store.LockFile()
	if err != nil {
		t.Fatalf("re-lock: %v", err)
	}
	defer func() { _ = second.Release() }()
	if second.Path() != first.Path() {
		t.Fatalf("lock path mismatch: first=%s second=%s", first.Path(), second.Path())
	}
}

func TestOpenRejectsSymlinkDirectory(t *testing.T) {
	target := t.TempDir()
	parent := t.TempDir()
	link := filepath.Join(parent, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if _, err := Open(link); err == nil {
		t.Fatalf("expected symlink rejection")
	}
}

// forceStoredSchemaVersion mutates the recorded schema_version to
// force the support-range guard to fire on reopen.
func forceStoredSchemaVersion(dbPath string, version uint32) error {
	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("metadata"))
		if b == nil {
			return fmt.Errorf("metadata bucket missing")
		}
		return b.Put([]byte("schema_version"), []byte(fmt.Sprintf("%d", version)))
	})
}
