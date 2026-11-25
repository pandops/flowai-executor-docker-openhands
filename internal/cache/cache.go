// Package cache is the durable recovery cache of the Docker OpenHands
// Executor and the exclusive lock on its cache volume.
//
// The cache is one bbolt database at <cache_dir>/executor.db with three
// versioned buckets:
//
//   - metadata      the schema version, the executor_id the JWT pins and
//     the time of the last successful heartbeat
//   - assignments   one claim record per (task_id, attempt, claim_seq)
//   - event_outbox  every event before it is published, keyed by
//     publisher_seq, with its delivery state (pending or sent)
//
// <cache_dir>/executor.lock is a non-blocking exclusive file lock the
// process holds for its lifetime; the kernel releases it on exit or crash.
// Claim tokens, launch-parameter values and keys are never stored.
package cache

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
)

// CacheDirDefault is the default cache directory used when an
// Executor is started without an explicit override.
const CacheDirDefault = "/var/lib/flowai/executor"

// SchemaVersion pins the bbolt schema that the Executor recognises.
// An unknown newer version fails the cache open call so operators
// do not silently reset or replace the database.
const SchemaVersion uint32 = 2

// knownBuckets enumerates the buckets the cache opens. Keep the
// order stable; new buckets may only be appended with a new
// SchemaVersion.
var knownBuckets = [][]byte{
	[]byte("metadata"),
	[]byte("assignments"),
	[]byte("event_outbox"),
}

// Store is the bbolt-backed persistent recovery cache. It is safe to
// call any Store method on the zero value once Open has returned a
// successful handle.
type Store struct {
	dir string
	db  *bolt.DB
	mu  sync.Mutex
}

// ErrLocked means another process holds the cache.
var ErrLocked = errors.New("cache: another process holds the cache")

// openTimeout bounds the wait for bbolt's own file lock; a held database
// fails fast as ErrLocked.
const openTimeout = 200 * time.Millisecond

var (
	bucketMetadata    = []byte("metadata")
	bucketAssignments = []byte("assignments")
	bucketOutbox      = []byte("event_outbox")
)

const (
	keyExecutorID    = "executor_id"
	keyLastHeartbeat = "last_heartbeat"
)

// OpenDir resolves the cache directory and ensures the directory
// exists with owner-only permissions. A symlink is rejected because
// the cache lives on a host-backed volume (K8s PVC or Docker
// host-volume); an unsupported copy is the operator's responsibility.
func OpenDir(path string) (string, error) {
	if path == "" {
		path = CacheDirDefault
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return "", fmt.Errorf("cache: ensure dir %s: %w", path, err)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("cache: resolve symlinks: %w", err)
	}
	if resolved != path {
		return "", fmt.Errorf("cache: directory %s is a symlink; expected owner-only directory", path)
	}
	return path, nil
}

// Open returns an opened cache store. The store must be Closed when
// the owning process exits; LockFile() acquires the OS-level advisory
// file lock that protects the cache volume against a second process.
func Open(path string) (*Store, error) {
	dir, err := OpenDir(path)
	if err != nil {
		return nil, err
	}
	dbPath := filepath.Join(dir, "executor.db")
	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: openTimeout})
	if err != nil {
		if errors.Is(err, bolterrors.ErrTimeout) {
			return nil, fmt.Errorf("%w: %s", ErrLocked, dbPath)
		}
		return nil, fmt.Errorf("cache: open bbolt %s: %w", dbPath, err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, b := range knownBuckets {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return fmt.Errorf("create bucket %s: %w", string(b), err)
			}
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("cache: init buckets: %w", err)
	}
	if err := resetV1(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := verifySchemaVersion(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := writeSchemaVersion(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{dir: dir, db: db}, nil
}

// Dir returns the configured cache directory.
func (s *Store) Dir() string { return s.dir }

// Close releases the bbolt file handle. The OS lock acquired via
// LockFile() is owned by the caller and must be released separately
// to keep the documented lifecycle.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

const metadataSchemaVersionKey = "schema_version"

// verifySchemaVersion asserts the recorded schema is recognised.
// A cache stored with an unknown newer schema fails closed.
func verifySchemaVersion(db *bolt.DB) error {
	return db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("metadata"))
		if b == nil {
			return errors.New("cache: metadata bucket missing")
		}
		raw := b.Get([]byte(metadataSchemaVersionKey))
		if raw == nil {
			return nil
		}
		var stored uint32
		if _, err := fmt.Sscanf(string(raw), "%d", &stored); err != nil {
			return fmt.Errorf("cache: parse schema_version: %w", err)
		}
		if stored != SchemaVersion {
			return fmt.Errorf("cache: stored schema_version %d is not the supported %d", stored, SchemaVersion)
		}
		return nil
	})
}

func writeSchemaVersion(db *bolt.DB) error {
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("metadata"))
		if b == nil {
			return errors.New("cache: metadata bucket missing")
		}
		return b.Put([]byte(metadataSchemaVersionKey), []byte(fmt.Sprintf("%d", SchemaVersion)))
	})
}

// resetV1 empties a schema 1 cache: its records belong to the removed
// v0002 HTTP protocol and cannot be recovered over the broker.
func resetV1(db *bolt.DB) error {
	return db.Update(func(tx *bolt.Tx) error {
		if string(tx.Bucket(bucketMetadata).Get([]byte(metadataSchemaVersionKey))) != "1" {
			return nil
		}
		for _, name := range [][]byte{bucketMetadata, bucketAssignments, bucketOutbox} {
			if err := tx.DeleteBucket(name); err != nil {
				return fmt.Errorf("cache: reset schema 1 bucket %s: %w", name, err)
			}
			if _, err := tx.CreateBucket(name); err != nil {
				return fmt.Errorf("cache: reset schema 1 bucket %s: %w", name, err)
			}
		}
		return nil
	})
}

// ClaimKey identifies one claim.
type ClaimKey struct {
	TaskID   int64 `json:"task_id"`
	Attempt  int64 `json:"attempt"`
	ClaimSeq int64 `json:"claim_seq"`
}

// String renders the key as task_id/attempt/claim_seq in decimal.
func (k ClaimKey) String() string { return fmt.Sprintf("%d/%d/%d", k.TaskID, k.Attempt, k.ClaimSeq) }

// Claim states.
const (
	ClaimClaiming = "claiming"
	ClaimPending  = "pending"
	ClaimRunning  = "running"
	ClaimTerminal = "terminal"
)

// ClaimRecord is one claim in the assignments bucket, keyed by ClaimKey.
// It never holds a claim token, launch-parameter value or key.
type ClaimRecord struct {
	TaskID     int64 `json:"task_id"`
	Attempt    int64 `json:"attempt"`
	ClaimSeq   int64 `json:"claim_seq"`
	TeamID     int64 `json:"team_id"`
	ExecutorID int64 `json:"executor_id"`
	FlowID     int64 `json:"flow_id"`
	// State is claiming, pending, running or terminal.
	State string `json:"state"`
	// CreatedPublisherSeq is the publisher_seq of the claim's created event.
	CreatedPublisherSeq int64 `json:"created_publisher_seq"`
	// ClaimAckedAt is when task-manager's claim-token reply acknowledged
	// the claim; zero until then.
	ClaimAckedAt time.Time `json:"claim_acked_at"`
	// PendingSince is when the claim entered pending; zero otherwise.
	PendingSince        time.Time `json:"pending_since"`
	ContainerID         string    `json:"container_id"`
	ContainerName       string    `json:"container_name"`
	ConversationID      string    `json:"conversation_id"`
	ToolState           string    `json:"tool_state"`
	ResolvedImageSource string    `json:"resolved_image_source"`
	UpdatedAt           time.Time `json:"updated_at"`
}

// Key returns the record's ClaimKey.
func (r ClaimRecord) Key() ClaimKey {
	return ClaimKey{TaskID: r.TaskID, Attempt: r.Attempt, ClaimSeq: r.ClaimSeq}
}

// Outbox delivery states.
const (
	OutboxPending = "pending"
	OutboxSent    = "sent"
)

// OutboxRecord is one event in the event_outbox bucket, keyed by its
// publisher_seq.
type OutboxRecord struct {
	PublisherSeq int64     `json:"publisher_seq"`
	Subject      string    `json:"subject"`
	Kind         string    `json:"kind"`
	Claim        *ClaimKey `json:"claim,omitempty"`
	Body         []byte    `json:"body"`
	State        string    `json:"state"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func claimKeyBytes(k ClaimKey) []byte {
	b := make([]byte, 24)
	binary.BigEndian.PutUint64(b[0:], uint64(k.TaskID))
	binary.BigEndian.PutUint64(b[8:], uint64(k.Attempt))
	binary.BigEndian.PutUint64(b[16:], uint64(k.ClaimSeq))
	return b
}

func seqKey(seq int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(seq))
	return b
}

func (s *Store) update(fn func(tx *bolt.Tx) error) error {
	if s == nil || s.db == nil {
		return errors.New("cache: store is not open")
	}
	return s.db.Update(fn)
}

func (s *Store) view(fn func(tx *bolt.Tx) error) error {
	if s == nil || s.db == nil {
		return errors.New("cache: store is not open")
	}
	return s.db.View(fn)
}

// ExecutorID returns the cached executor_id the JWT pinned, or 0.
func (s *Store) ExecutorID(ctx context.Context) (int64, error) {
	var id int64
	err := s.view(func(tx *bolt.Tx) error {
		if raw := tx.Bucket(bucketMetadata).Get([]byte(keyExecutorID)); len(raw) == 8 {
			id = int64(binary.BigEndian.Uint64(raw))
		}
		return nil
	})
	return id, err
}

// PutExecutorID stores the executor_id the JWT pins.
func (s *Store) PutExecutorID(ctx context.Context, executorID int64) error {
	return s.update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMetadata).Put([]byte(keyExecutorID), seqKey(executorID))
	})
}

// LastHeartbeat returns the time of the last successful heartbeat, or zero.
func (s *Store) LastHeartbeat(ctx context.Context) (time.Time, error) {
	var at time.Time
	err := s.view(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketMetadata).Get([]byte(keyLastHeartbeat))
		if raw == nil {
			return nil
		}
		return at.UnmarshalText(raw)
	})
	return at, err
}

// PutLastHeartbeat records the time of the last successful heartbeat.
func (s *Store) PutLastHeartbeat(ctx context.Context, at time.Time) error {
	raw, err := at.UTC().MarshalText()
	if err != nil {
		return err
	}
	return s.update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMetadata).Put([]byte(keyLastHeartbeat), raw)
	})
}

// PutClaim stores rec in one write transaction.
func (s *Store) PutClaim(ctx context.Context, rec ClaimRecord) error {
	rec.UpdatedAt = time.Now().UTC()
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return s.update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAssignments).Put(claimKeyBytes(rec.Key()), raw)
	})
}

// Claim returns the record for key; ok is false when there is none.
func (s *Store) Claim(ctx context.Context, key ClaimKey) (rec ClaimRecord, ok bool, err error) {
	err = s.view(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketAssignments).Get(claimKeyBytes(key))
		if raw == nil {
			return nil
		}
		ok = true
		return json.Unmarshal(raw, &rec)
	})
	return rec, ok, err
}

// ListClaims returns every cached claim ordered by key.
func (s *Store) ListClaims(ctx context.Context) ([]ClaimRecord, error) {
	var out []ClaimRecord
	err := s.view(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAssignments).ForEach(func(_, v []byte) error {
			var rec ClaimRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return fmt.Errorf("cache: corrupt claim record: %w", err)
			}
			out = append(out, rec)
			return nil
		})
	})
	return out, err
}

// DeleteClaim removes the claim and every unsent outbox event of it in
// one write transaction.
func (s *Store) DeleteClaim(ctx context.Context, key ClaimKey) error {
	return s.update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketAssignments).Delete(claimKeyBytes(key)); err != nil {
			return err
		}
		ob := tx.Bucket(bucketOutbox)
		var drop [][]byte
		err := ob.ForEach(func(k, v []byte) error {
			var rec OutboxRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return fmt.Errorf("cache: corrupt outbox record: %w", err)
			}
			if rec.State != OutboxSent && rec.Claim != nil && *rec.Claim == key {
				drop = append(drop, append([]byte(nil), k...))
			}
			return nil
		})
		if err != nil {
			return err
		}
		for _, k := range drop {
			if err := ob.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// PutOutbox stores rec before it is published.
func (s *Store) PutOutbox(ctx context.Context, rec OutboxRecord) error {
	rec.UpdatedAt = time.Now().UTC()
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return s.update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketOutbox).Put(seqKey(rec.PublisherSeq), raw)
	})
}

// MarkSent marks the event with publisherSeq sent after JetStream
// acknowledged it. A sent Executor-level event (no claim) is removed; a
// claim's sent events stay until PruneSent, so a restart can tell which
// claims already ended.
func (s *Store) MarkSent(ctx context.Context, publisherSeq int64) error {
	return s.update(func(tx *bolt.Tx) error {
		ob := tx.Bucket(bucketOutbox)
		raw := ob.Get(seqKey(publisherSeq))
		if raw == nil {
			return nil
		}
		var rec OutboxRecord
		if err := json.Unmarshal(raw, &rec); err != nil {
			return fmt.Errorf("cache: corrupt outbox record: %w", err)
		}
		if rec.Claim == nil {
			return ob.Delete(seqKey(publisherSeq))
		}
		rec.State = OutboxSent
		rec.UpdatedAt = time.Now().UTC()
		out, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		return ob.Put(seqKey(publisherSeq), out)
	})
}

// PendingOutbox returns every unsent event ordered by publisher_seq.
func (s *Store) PendingOutbox(ctx context.Context) ([]OutboxRecord, error) {
	var out []OutboxRecord
	err := s.view(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketOutbox).ForEach(func(_, v []byte) error {
			var rec OutboxRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return fmt.Errorf("cache: corrupt outbox record: %w", err)
			}
			if rec.State != OutboxSent {
				out = append(out, rec)
			}
			return nil
		})
	})
	sort.Slice(out, func(i, j int) bool { return out[i].PublisherSeq < out[j].PublisherSeq })
	return out, err
}

// SentKinds returns the kinds of the sent events of one claim.
func (s *Store) SentKinds(ctx context.Context, key ClaimKey) ([]string, error) {
	var out []string
	err := s.view(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketOutbox).ForEach(func(_, v []byte) error {
			var rec OutboxRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return fmt.Errorf("cache: corrupt outbox record: %w", err)
			}
			if rec.State == OutboxSent && rec.Claim != nil && *rec.Claim == key {
				out = append(out, rec.Kind)
			}
			return nil
		})
	})
	return out, err
}

// PruneSent removes the sent events of a claim the Executor forgot.
func (s *Store) PruneSent(ctx context.Context, key ClaimKey) error {
	return s.update(func(tx *bolt.Tx) error {
		ob := tx.Bucket(bucketOutbox)
		var drop [][]byte
		if err := ob.ForEach(func(k, v []byte) error {
			var rec OutboxRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return fmt.Errorf("cache: corrupt outbox record: %w", err)
			}
			if rec.State == OutboxSent && rec.Claim != nil && *rec.Claim == key {
				drop = append(drop, append([]byte(nil), k...))
			}
			return nil
		}); err != nil {
			return err
		}
		for _, k := range drop {
			if err := ob.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// Lock takes the non-blocking exclusive lock on <dir>/executor.lock
// before the cache is opened; ErrLocked means another process holds it.
func Lock(dir string) (*LockFile, error) {
	resolved, err := OpenDir(dir)
	if err != nil {
		return nil, err
	}
	return lockPath(filepath.Join(resolved, "executor.lock"))
}

// LockPath returns the absolute path to the cache-volume lock file.
// The caller is responsible for taking the OS advisory lock via
// LockFile before any registration, discovery, claim, or runtime
// mutation, and for keeping the file descriptor open for the full
// process lifetime.
func (s *Store) LockPath() string { return filepath.Join(s.dir, "executor.lock") }

// LockFile holds the non-blocking exclusive POSIX file lock
// protecting the cache volume against a second process.
type LockFile struct {
	path string
	fd   *os.File
}

// LockFile attempts the non-blocking POSIX exclusive lock on the
// cache volume. Lock contention fails closed; the caller reports the
// second process as unhealthy with no Registry or runtime mutation.
// The kernel releases the lock after a process crash or normal exit;
// the cache store does not implement a Registry-side lease.
func (s *Store) LockFile() (*LockFile, error) {
	if s == nil {
		return nil, errors.New("cache: store is not open")
	}
	return lockPath(s.LockPath())
}

func lockPath(path string) (*LockFile, error) {
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("cache: open lock file: %w", err)
	}
	if err := syscall.Flock(int(fd.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = fd.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: lock %s", ErrLocked, path)
		}
		return nil, fmt.Errorf("cache: lock %s: %w", path, err)
	}
	return &LockFile{path: path, fd: fd}, nil
}

// Release drops the OS file lock and closes the file descriptor.
// Safe to call on a zero-value LockFile.
func (lf *LockFile) Release() error {
	if lf == nil || lf.fd == nil {
		return nil
	}
	err := syscall.Flock(int(lf.fd.Fd()), syscall.LOCK_UN)
	if cErr := lf.fd.Close(); cErr != nil && err == nil {
		err = cErr
	}
	lf.fd = nil
	return err
}

// Path returns the absolute path of the lock file. Useful for
// diagnostics captured before deletion.
func (lf *LockFile) Path() string {
	if lf == nil {
		return ""
	}
	return lf.path
}
