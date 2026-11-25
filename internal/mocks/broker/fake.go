// Package broker is an in-memory fake of the broker.Conn the Executor uses.
// It stands in for NATS JetStream in unit tests: it records every dial,
// publish attempt, request, pull and acknowledgement in one ordered log,
// stores acknowledged publishes once per Nats-Msg-Id (as JetStream
// deduplication does), answers requests through per-subject handlers, and
// serves pull consumers from per-consumer queues. It never checks the
// broker's own behavior; it only gives tests a broker-shaped boundary.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pandops/flowai-executor-docker-openhands/internal/broker"
)

// Op kinds in the ordered log.
const (
	OpDial    = "dial"
	OpPublish = "publish"
	OpRequest = "request"
	OpFetch   = "fetch"
	OpAck     = "ack"
	OpTerm    = "term"
)

// Op is one recorded operation.
type Op struct {
	Kind     string
	Subject  string
	Stream   string
	Consumer string
	Batch    int
	Header   broker.Header
	Data     []byte
	// Acked is true for a publish that got its acknowledgement.
	Acked bool
	At    time.Time
}

// Handler answers one request.
type Handler func(m broker.Message) (broker.Reply, error)

// Fake is the fake broker. The zero value is not usable; call New.
type Fake struct {
	mu   sync.Mutex
	cond *sync.Cond

	ops       []Op
	stored    []broker.Message
	storedIDs map[string]bool
	handlers  []handlerEntry
	queues    map[string][]*Delivered
	consumers map[string]bool

	conns   []*conn
	dials   []broker.DialOptions
	revoked map[string]bool
	dialErr error

	dropPublish map[string]int // subject pattern -> publishes to leave unacknowledged

	// OnPublish runs before a publish is acknowledged. It runs without
	// the fake's lock held.
	OnPublish func(m broker.Message)
	// OnFetch runs when a pull starts, before messages are handed out.
	OnFetch func(stream, consumer string, batch int)
}

type handlerEntry struct {
	pattern string
	h       Handler
}

// New returns an empty fake with the default JetStream consumer-create
// handler installed.
func New() *Fake {
	f := &Fake{
		storedIDs:   map[string]bool{},
		queues:      map[string][]*Delivered{},
		consumers:   map[string]bool{},
		revoked:     map[string]bool{},
		dropPublish: map[string]int{},
	}
	f.cond = sync.NewCond(&f.mu)
	f.Handle("$JS.API.CONSUMER.CREATE.>", f.DefaultConsumerCreate)
	return f
}

// Dialer returns a broker.Dialer backed by this fake.
func (f *Fake) Dialer() broker.Dialer { return dialer{f} }

type dialer struct{ f *Fake }

func (d dialer) Dial(ctx context.Context, opts broker.DialOptions) (broker.Conn, error) {
	f := d.f
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, Op{Kind: OpDial, Subject: opts.InboxPrefix, Data: []byte(opts.Token), At: time.Now()})
	f.dials = append(f.dials, opts)
	if f.revoked[opts.Token] {
		return nil, broker.ErrRevoked
	}
	if f.dialErr != nil {
		return nil, f.dialErr
	}
	c := &conn{f: f, opts: opts, done: make(chan struct{}), reconnected: make(chan struct{}, 16)}
	f.conns = append(f.conns, c)
	return c, nil
}

// SetDialError makes every later dial fail with err (nil clears it).
func (f *Fake) SetDialError(err error) {
	f.mu.Lock()
	f.dialErr = err
	f.mu.Unlock()
}

// Dials returns the options of every dial attempt.
func (f *Fake) Dials() []broker.DialOptions {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]broker.DialOptions(nil), f.dials...)
}

// Revoke closes every connection that uses token with broker.ErrRevoked
// and refuses later dials with it.
func (f *Fake) Revoke(token string) {
	f.mu.Lock()
	f.revoked[token] = true
	conns := append([]*conn(nil), f.conns...)
	f.cond.Broadcast()
	f.mu.Unlock()
	for _, c := range conns {
		if c.opts.Token == token {
			c.closeWith(broker.ErrRevoked)
		}
	}
}

// Handle installs h for request subjects matching pattern (NATS wildcards
// `*` and `>`). A later Handle for the same pattern replaces the earlier one;
// the most recently added matching pattern wins.
func (f *Fake) Handle(pattern string, h Handler) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, e := range f.handlers {
		if e.pattern == pattern {
			f.handlers = append(f.handlers[:i], f.handlers[i+1:]...)
			break
		}
	}
	f.handlers = append(f.handlers, handlerEntry{pattern: pattern, h: h})
}

// DropPublishes leaves the next n publishes on subjects matching pattern
// without an acknowledgement (the publisher sees a timeout).
func (f *Fake) DropPublishes(pattern string, n int) {
	f.mu.Lock()
	f.dropPublish[pattern] += n
	f.mu.Unlock()
}

// Delivered is one message queued on a pull consumer.
type Delivered struct {
	f       *Fake
	subject string
	header  broker.Header
	data    []byte

	mu     sync.Mutex
	taken  bool
	acked  bool
	termed bool
}

// Acked reports whether the consumer acknowledged the message.
func (d *Delivered) Acked() bool { d.mu.Lock(); defer d.mu.Unlock(); return d.acked }

// Termed reports whether the consumer terminated the message.
func (d *Delivered) Termed() bool { d.mu.Lock(); defer d.mu.Unlock(); return d.termed }

// Taken reports whether a pull handed the message out.
func (d *Delivered) Taken() bool { d.mu.Lock(); defer d.mu.Unlock(); return d.taken }

// Enqueue appends a message to the pull queue of stream/consumer.
func (f *Fake) Enqueue(stream, consumer, subject string, data []byte) *Delivered {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := &Delivered{f: f, subject: subject, header: broker.Header{}, data: data}
	key := stream + "/" + consumer
	f.queues[key] = append(f.queues[key], d)
	f.cond.Broadcast()
	return d
}

// Waiting returns how many messages of stream/consumer were not yet pulled.
func (f *Fake) Waiting(stream, consumer string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, d := range f.queues[stream+"/"+consumer] {
		if !d.Taken() {
			n++
		}
	}
	return n
}

// ConsumerExists reports whether a consumer create for name succeeded.
func (f *Fake) ConsumerExists(stream, name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.consumers[stream+"/"+name]
}

// RemoveConsumer forgets a created consumer, as the server does after its
// inactive threshold.
func (f *Fake) RemoveConsumer(stream, name string) {
	f.mu.Lock()
	delete(f.consumers, stream+"/"+name)
	f.mu.Unlock()
}

// MarkConsumer records stream/name as existing (a consumer created by
// someone else, such as executor-registry's task consumers).
func (f *Fake) MarkConsumer(stream, name string) {
	f.mu.Lock()
	f.consumers[stream+"/"+name] = true
	f.cond.Broadcast()
	f.mu.Unlock()
}

// DefaultConsumerCreate answers `$JS.API.CONSUMER.CREATE.<stream>.<name>[.<filter>]`
// like the JetStream API: it records the consumer and returns its config.
func (f *Fake) DefaultConsumerCreate(m broker.Message) (broker.Reply, error) {
	parts := strings.SplitN(strings.TrimPrefix(m.Subject, "$JS.API.CONSUMER.CREATE."), ".", 3)
	if len(parts) < 2 {
		return broker.Reply{Data: []byte(`{"error":{"code":400,"err_code":10025,"description":"bad request"}}`)}, nil
	}
	var req struct {
		Stream string          `json:"stream_name"`
		Config json.RawMessage `json:"config"`
	}
	_ = json.Unmarshal(m.Data, &req)
	f.MarkConsumer(parts[0], parts[1])
	body, _ := json.Marshal(map[string]any{
		"type":        "io.nats.jetstream.api.v1.consumer_create_response",
		"stream_name": parts[0],
		"name":        parts[1],
		"config":      req.Config,
	})
	return broker.Reply{Data: body}, nil
}

// Ops returns the ordered operation log.
func (f *Fake) Ops() []Op {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Op(nil), f.ops...)
}

// Stored returns the acknowledged JetStream publishes, once per
// Nats-Msg-Id, in acknowledgement order.
func (f *Fake) Stored() []broker.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]broker.Message(nil), f.stored...)
}

// Requests returns every request in order.
func (f *Fake) Requests() []Op { return f.opsOf(OpRequest) }

// Fetches returns every pull in order.
func (f *Fake) Fetches() []Op { return f.opsOf(OpFetch) }

func (f *Fake) opsOf(kind string) []Op {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Op
	for _, op := range f.ops {
		if op.Kind == kind {
			out = append(out, op)
		}
	}
	return out
}

// Match reports whether subject matches a NATS pattern with `*` and `>`.
func Match(pattern, subject string) bool {
	p := strings.Split(pattern, ".")
	s := strings.Split(subject, ".")
	for i, tok := range p {
		if tok == ">" {
			return len(s) > i
		}
		if i >= len(s) {
			return false
		}
		if tok != "*" && tok != s[i] {
			return false
		}
	}
	return len(p) == len(s)
}

type conn struct {
	f    *Fake
	opts broker.DialOptions

	mu          sync.Mutex
	closed      bool
	err         error
	done        chan struct{}
	reconnected chan struct{}
}

func (c *conn) isClosed() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.closed }

func (c *conn) closeWith(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	c.err = err
	close(c.done)
	c.mu.Unlock()
	c.f.mu.Lock()
	c.f.cond.Broadcast()
	c.f.mu.Unlock()
	c.mu.Lock()
}

// usable returns nil when the connection may carry traffic.
func (c *conn) usable() error {
	c.mu.Lock()
	closed, err := c.closed, c.err
	c.mu.Unlock()
	if closed {
		return err
	}
	return nil
}

func cloneHeader(h broker.Header) broker.Header {
	out := broker.Header{}
	for k, v := range h {
		out[k] = append([]string(nil), v...)
	}
	return out
}

func (c *conn) Publish(ctx context.Context, m broker.Message) error {
	f := c.f
	op := Op{Kind: OpPublish, Subject: m.Subject, Header: cloneHeader(m.Header), Data: append([]byte(nil), m.Data...), At: time.Now()}
	if err := c.usable(); err != nil {
		f.mu.Lock()
		f.ops = append(f.ops, op)
		f.mu.Unlock()
		return broker.ErrTimeout
	}
	f.mu.Lock()
	drop := false
	for pattern, n := range f.dropPublish {
		if n > 0 && Match(pattern, m.Subject) {
			f.dropPublish[pattern] = n - 1
			drop = true
			break
		}
	}
	hook := f.OnPublish
	f.mu.Unlock()
	if hook != nil {
		hook(broker.Message{Subject: m.Subject, Header: cloneHeader(m.Header), Data: op.Data})
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if drop {
		f.ops = append(f.ops, op)
		return broker.ErrTimeout
	}
	op.Acked = true
	f.ops = append(f.ops, op)
	id := m.Header.Get("Nats-Msg-Id")
	if id == "" || !f.storedIDs[id] {
		if id != "" {
			f.storedIDs[id] = true
		}
		f.stored = append(f.stored, broker.Message{Subject: m.Subject, Header: op.Header, Data: op.Data})
	}
	return nil
}

func (c *conn) Request(ctx context.Context, m broker.Message) (broker.Reply, error) {
	f := c.f
	op := Op{Kind: OpRequest, Subject: m.Subject, Header: cloneHeader(m.Header), Data: append([]byte(nil), m.Data...), At: time.Now()}
	f.mu.Lock()
	f.ops = append(f.ops, op)
	f.mu.Unlock()
	if err := c.usable(); err != nil {
		if errors.Is(err, broker.ErrNotConnected) {
			return broker.Reply{}, broker.ErrTimeout
		}
		return broker.Reply{}, err
	}
	f.mu.Lock()
	var h Handler
	for i := len(f.handlers) - 1; i >= 0; i-- {
		if Match(f.handlers[i].pattern, m.Subject) {
			h = f.handlers[i].h
			break
		}
	}
	f.mu.Unlock()
	if h == nil {
		return broker.Reply{}, broker.ErrNoResponders
	}
	reply, err := h(broker.Message{Subject: m.Subject, Header: op.Header, Data: op.Data})
	if err != nil {
		return broker.Reply{}, err
	}
	return reply, nil
}

func (c *conn) Fetch(ctx context.Context, stream, consumer string, batch int, expires time.Duration) ([]broker.Delivery, error) {
	f := c.f
	f.mu.Lock()
	f.ops = append(f.ops, Op{Kind: OpFetch, Stream: stream, Consumer: consumer, Batch: batch, At: time.Now()})
	hook := f.OnFetch
	f.mu.Unlock()
	if hook != nil {
		hook(stream, consumer, batch)
	}
	if err := c.usable(); err != nil {
		return nil, err
	}
	key := stream + "/" + consumer
	deadline := time.Now().Add(expires)
	timer := time.AfterFunc(expires, func() {
		f.mu.Lock()
		f.cond.Broadcast()
		f.mu.Unlock()
	})
	defer timer.Stop()
	stop := context.AfterFunc(ctx, func() {
		f.mu.Lock()
		f.cond.Broadcast()
		f.mu.Unlock()
	})
	defer stop()

	f.mu.Lock()
	defer f.mu.Unlock()
	for {
		if !f.consumers[key] {
			return nil, fmt.Errorf("fake broker: consumer %s not found", key)
		}
		var out []broker.Delivery
		for _, d := range f.queues[key] {
			if len(out) >= batch {
				break
			}
			if !d.Taken() {
				d.mu.Lock()
				d.taken = true
				d.mu.Unlock()
				out = append(out, &delivery{c: c, stream: stream, consumer: consumer, d: d})
			}
		}
		if len(out) > 0 {
			return out, nil
		}
		if c.isClosed() {
			return nil, broker.ErrNotConnected
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !time.Now().Before(deadline) {
			return nil, nil
		}
		f.cond.Wait()
	}
}

func (c *conn) Reconnected() <-chan struct{} { return c.reconnected }
func (c *conn) Done() <-chan struct{}        { return c.done }
func (c *conn) Err() error                   { c.mu.Lock(); defer c.mu.Unlock(); return c.err }
func (c *conn) Connected() bool              { return c.usable() == nil }
func (c *conn) Close()                       { c.closeWith(broker.ErrClosed) }

type delivery struct {
	c        *conn
	stream   string
	consumer string
	d        *Delivered
}

func (d *delivery) Subject() string       { return d.d.subject }
func (d *delivery) Header() broker.Header { return cloneHeader(d.d.header) }
func (d *delivery) Data() []byte          { return append([]byte(nil), d.d.data...) }

func (d *delivery) Ack(ctx context.Context) error  { return d.settle(OpAck) }
func (d *delivery) Term(ctx context.Context) error { return d.settle(OpTerm) }

func (d *delivery) settle(kind string) error {
	if err := d.c.usable(); err != nil {
		return err
	}
	f := d.c.f
	f.mu.Lock()
	f.ops = append(f.ops, Op{Kind: kind, Stream: d.stream, Consumer: d.consumer, Subject: d.d.subject, Data: d.d.data, At: time.Now()})
	f.mu.Unlock()
	d.d.mu.Lock()
	if kind == OpAck {
		d.d.acked = true
	} else {
		d.d.termed = true
	}
	d.d.mu.Unlock()
	return nil
}
