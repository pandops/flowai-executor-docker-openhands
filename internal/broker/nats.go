package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// NATSDialer returns the production Dialer.
func NATSDialer() Dialer { return natsDialer{} }

type natsDialer struct{}

func (natsDialer) Dial(ctx context.Context, opts DialOptions) (Conn, error) {
	c := &natsConn{reconnected: make(chan struct{}, 16), done: make(chan struct{})}
	token := opts.Token
	nopts := []nats.Option{
		nats.Name(opts.Name),
		// A bearer user JWT needs no nonce signature.
		nats.UserJWT(func() (string, error) { return token, nil }, func([]byte) ([]byte, error) { return []byte{}, nil }),
		nats.CustomInboxPrefix(opts.InboxPrefix),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.Timeout(10 * time.Second),
		nats.ReconnectHandler(func(*nats.Conn) {
			select {
			case c.reconnected <- struct{}{}:
			default:
			}
		}),
		nats.DisconnectErrHandler(func(nc *nats.Conn, _ error) { c.checkAuth(nc) }),
		nats.ErrorHandler(func(nc *nats.Conn, _ *nats.Subscription, _ error) { c.checkAuth(nc) }),
		nats.ClosedHandler(func(nc *nats.Conn) {
			if authError(nc.LastError()) {
				c.closeWith(ErrRevoked)
				return
			}
			c.closeWith(ErrClosed)
		}),
	}
	if opts.CAFile != "" {
		nopts = append(nopts, nats.RootCAs(opts.CAFile))
	}
	type result struct {
		nc  *nats.Conn
		err error
	}
	ch := make(chan result, 1)
	go func() {
		nc, err := nats.Connect(opts.URL, nopts...)
		ch <- result{nc, err}
	}()
	select {
	case <-ctx.Done():
		go func() {
			if r := <-ch; r.nc != nil {
				r.nc.Close()
			}
		}()
		return nil, ctx.Err()
	case r := <-ch:
		if r.err != nil {
			if authError(r.err) {
				return nil, fmt.Errorf("%w: %s", ErrRevoked, r.err.Error())
			}
			return nil, fmt.Errorf("broker: connect %s: %s", opts.URL, r.err.Error())
		}
		c.nc = r.nc
		return c, nil
	}
}

func authError(err error) bool {
	return errors.Is(err, nats.ErrAuthorization) || errors.Is(err, nats.ErrAuthRevoked) || errors.Is(err, nats.ErrAuthExpired)
}

type natsConn struct {
	nc          *nats.Conn
	reconnected chan struct{}
	done        chan struct{}

	mu   sync.Mutex
	err  error
	once sync.Once
}

// checkAuth closes the connection for good once the server refused the
// JWT, so it is never redialed.
func (c *natsConn) checkAuth(nc *nats.Conn) {
	if authError(nc.LastError()) {
		c.closeWith(ErrRevoked)
		go nc.Close()
	}
}

func (c *natsConn) closeWith(err error) {
	c.once.Do(func() {
		c.mu.Lock()
		c.err = err
		c.mu.Unlock()
		close(c.done)
	})
}

func mapErr(ctx context.Context, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, nats.ErrNoResponders):
		return ErrNoResponders
	case errors.Is(err, nats.ErrTimeout), errors.Is(err, context.DeadlineExceeded):
		return ErrTimeout
	case errors.Is(err, nats.ErrConnectionClosed):
		return ErrClosed
	case errors.Is(err, nats.ErrConnectionReconnecting), errors.Is(err, nats.ErrDisconnected):
		return ErrNotConnected
	case ctx.Err() != nil:
		return ctx.Err()
	}
	return err
}

func (c *natsConn) usable() error {
	select {
	case <-c.done:
		return c.Err()
	default:
	}
	if !c.nc.IsConnected() {
		return ErrNotConnected
	}
	return nil
}

// Publish sends to JetStream as a request and waits for the PubAck.
func (c *natsConn) Publish(ctx context.Context, m Message) error {
	if err := c.usable(); err != nil {
		if errors.Is(err, ErrNotConnected) {
			return ErrTimeout
		}
		return err
	}
	msg := &nats.Msg{Subject: m.Subject, Header: nats.Header(m.Header), Data: m.Data}
	reply, err := c.nc.RequestMsgWithContext(ctx, msg)
	if err != nil {
		return mapErr(ctx, err)
	}
	var ack struct {
		Stream string `json:"stream"`
		Seq    uint64 `json:"seq"`
		Error  *struct {
			Code        int    `json:"code"`
			ErrCode     int    `json:"err_code"`
			Description string `json:"description"`
		} `json:"error"`
	}
	if err := json.Unmarshal(reply.Data, &ack); err != nil {
		return fmt.Errorf("broker: publish acknowledgement: %w", err)
	}
	if ack.Error != nil {
		return fmt.Errorf("broker: publish refused: %d %s", ack.Error.ErrCode, ack.Error.Description)
	}
	if ack.Stream == "" {
		return errors.New("broker: publish acknowledgement without stream")
	}
	return nil
}

// Request sends a core request and returns the first reply.
func (c *natsConn) Request(ctx context.Context, m Message) (Reply, error) {
	if err := c.usable(); err != nil {
		if errors.Is(err, ErrNotConnected) {
			return Reply{}, ErrTimeout
		}
		return Reply{}, err
	}
	msg := &nats.Msg{Subject: m.Subject, Header: nats.Header(m.Header), Data: m.Data}
	reply, err := c.nc.RequestMsgWithContext(ctx, msg)
	if err != nil {
		return Reply{}, mapErr(ctx, err)
	}
	r := Reply{Header: Header(reply.Header), Data: reply.Data}
	if reply.Header != nil {
		if s := reply.Header.Get("status"); s != "" {
			r.Status, _ = strconv.Atoi(s)
		}
	}
	return r, nil
}

// Fetch pulls from a durable pull consumer through the raw JetStream
// API: one CONSUMER.MSG.NEXT request whose messages arrive on an inbox.
func (c *natsConn) Fetch(ctx context.Context, stream, consumer string, batch int, expires time.Duration) ([]Delivery, error) {
	if err := c.usable(); err != nil {
		return nil, err
	}
	inbox := c.nc.NewInbox()
	sub, err := c.nc.SubscribeSync(inbox)
	if err != nil {
		return nil, mapErr(ctx, err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	body, _ := json.Marshal(map[string]any{"batch": batch, "expires": expires.Nanoseconds()})
	subject := fmt.Sprintf("$JS.API.CONSUMER.MSG.NEXT.%s.%s", stream, consumer)
	if err := c.nc.PublishRequest(subject, inbox, body); err != nil {
		return nil, mapErr(ctx, err)
	}
	deadline := time.Now().Add(expires + time.Second)
	var out []Delivery
	for len(out) < batch {
		wait := time.Until(deadline)
		if wait <= 0 {
			break
		}
		if len(out) > 0 {
			// The first message arrived: take what is already queued.
			wait = 50 * time.Millisecond
		}
		wctx, cancel := context.WithTimeout(ctx, wait)
		msg, err := sub.NextMsgWithContext(wctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return out, ctx.Err()
			}
			break
		}
		if status := msg.Header.Get("Status"); status != "" && len(msg.Data) == 0 {
			// 404 no messages, 408 request expired, 409 and 100 heartbeats.
			if status == "100" {
				continue
			}
			if status == "409" && len(out) == 0 {
				return nil, fmt.Errorf("broker: pull from %s/%s: %s %s", stream, consumer, status, msg.Header.Get("Description"))
			}
			break
		}
		out = append(out, &natsDelivery{c: c, msg: msg})
	}
	return out, nil
}

func (c *natsConn) Reconnected() <-chan struct{} { return c.reconnected }
func (c *natsConn) Done() <-chan struct{}        { return c.done }

func (c *natsConn) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *natsConn) Connected() bool { return c.usable() == nil }

func (c *natsConn) Close() {
	c.nc.Close()
	c.closeWith(ErrClosed)
}

type natsDelivery struct {
	c   *natsConn
	msg *nats.Msg
}

func (d *natsDelivery) Subject() string { return d.msg.Subject }
func (d *natsDelivery) Header() Header  { return Header(d.msg.Header) }
func (d *natsDelivery) Data() []byte    { return d.msg.Data }

func (d *natsDelivery) Ack(ctx context.Context) error  { return d.reply(ctx, "+ACK") }
func (d *natsDelivery) Term(ctx context.Context) error { return d.reply(ctx, "+TERM") }

// reply sends an acknowledgement and waits for the server to confirm it.
func (d *natsDelivery) reply(ctx context.Context, body string) error {
	if d.msg.Reply == "" {
		return errors.New("broker: message has no acknowledgement subject")
	}
	actx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := d.c.nc.RequestWithContext(actx, d.msg.Reply, []byte(body))
	return mapErr(actx, err)
}
