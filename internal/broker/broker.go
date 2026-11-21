// Package broker is the Executor's view of NATS JetStream: JetStream
// publishes that wait for the publish acknowledgement, core request/reply,
// and pulls from durable pull consumers. The production implementation in
// nats.go uses raw NATS requests only, because the Executor's user JWT
// allows no JetStream API subject other than its own consumer create, pull
// and acknowledgement subjects.
package broker

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// Header is a NATS message header. It has the same shape as nats.Header.
type Header = http.Header

// Message is one outgoing message: a JetStream publish or a request.
type Message struct {
	Subject string
	Header  Header
	Data    []byte
}

// Reply is the answer to a request. Status is the decimal value of the
// `status` header (agents.md rule 9), or 0 when the header is absent, as on
// JetStream API replies.
type Reply struct {
	Status int
	Header Header
	Data   []byte
}

// Delivery is one message pulled from a durable consumer.
type Delivery interface {
	Subject() string
	Header() Header
	Data() []byte
	// Ack acknowledges the message (`+ACK`).
	Ack(ctx context.Context) error
	// Term terminates the message so it is never redelivered (`+TERM`).
	Term(ctx context.Context) error
}

// Errors returned by Conn operations.
var (
	// ErrTimeout means no publish acknowledgement or reply arrived in time.
	ErrTimeout = errors.New("broker: timeout")
	// ErrNoResponders means nobody listens on the request subject.
	ErrNoResponders = errors.New("broker: no responders")
	// ErrNotConnected means the connection is down at the moment.
	ErrNotConnected = errors.New("broker: not connected")
	// ErrRevoked means the server closed the connection because the
	// user JWT was revoked or is no longer accepted.
	ErrRevoked = errors.New("broker: credentials revoked")
	// ErrClosed means the connection was closed for good.
	ErrClosed = errors.New("broker: connection closed")
)

// Conn is one broker connection. Implementations reconnect on their own
// after a transport failure and signal it on Reconnected; they never
// reconnect after ErrRevoked.
type Conn interface {
	// Publish sends m to JetStream and returns after the publish
	// acknowledgement. It returns ErrTimeout when none arrives in time.
	Publish(ctx context.Context, m Message) error
	// Request sends m as a core request and waits for one reply.
	Request(ctx context.Context, m Message) (Reply, error)
	// Fetch pulls at most batch messages from the durable consumer,
	// waiting at most expires for the first one.
	Fetch(ctx context.Context, stream, consumer string, batch int, expires time.Duration) ([]Delivery, error)
	// Reconnected receives one value after every automatic reconnect.
	Reconnected() <-chan struct{}
	// Done is closed when the connection is closed for good; Err then
	// says why (ErrRevoked or ErrClosed).
	Done() <-chan struct{}
	Err() error
	// Connected reports whether the transport is up right now.
	Connected() bool
	Close()
}

// DialOptions configures a connection.
type DialOptions struct {
	// URL is the NATS server URL, for example tls://host:4222.
	URL string
	// Token is the bearer user JWT.
	Token string
	// CAFile is the PEM file of the CA that signed the server certificate.
	CAFile string
	// InboxPrefix is the reply-subject prefix, `_INBOX.executor-<id>`.
	InboxPrefix string
	// Name is the client connection name.
	Name string
}

// Dialer opens connections.
type Dialer interface {
	Dial(ctx context.Context, opts DialOptions) (Conn, error)
}
