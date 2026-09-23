package stream

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
	"github.com/7K-Inari/inari-api/gen/go/inari/agent/v1/agentv1connect"
)

// handshakeEventType is the reverse-DNS type used for the handshake
// exchange; the HandshakeRequest/Response payloads are the first messages
// on every (re)connect.
const handshakeEventType = "inari.agent.handshake.v1"

// Session termination sentinels, classified into metrics/logs by
// sessionCause (issue #28 instrumentation).
var (
	errDeadman       = errors.New("stream: receive timeout (keepalive dead-man switch)")
	errSendPump      = errors.New("stream: send pump failed")
	errTokenRotation = errors.New("stream: token nearing expiry, rotating session")
)

// handshakeError marks failures before the session is established (token
// fetch, handshake exchange) so they classify separately from mid-stream
// receive errors.
type handshakeError struct{ err error }

func (e *handshakeError) Error() string { return e.err.Error() }
func (e *handshakeError) Unwrap() error { return e.err }

// sessionCause maps a session error to a stable metric label.
func sessionCause(err error) string {
	var hsErr *handshakeError
	switch {
	case err == nil, errors.Is(err, context.Canceled):
		return "shutdown"
	case errors.As(err, &hsErr):
		return "handshake_error"
	case errors.Is(err, errDeadman):
		return "deadman"
	case errors.Is(err, errTokenRotation):
		return "token_rotation"
	case errors.Is(err, errSendPump):
		return "send_error"
	default:
		return "recv_error"
	}
}

// ConnectClient is the production stream.Client backed by the inari-api
// EventStreamService (ConnectRPC; use connect.WithGRPC-compatible servers
// transparently — ConnectRPC speaks gRPC when the server does).
//
// Egress-only environments: the underlying HTTP client honours the standard
// HTTPS_PROXY environment variable (HTTP/2 CONNECT to the proxy), so no
// inbound connectivity is ever required (plan §12.1/5).
type ConnectClient struct {
	// Address is the base URL of the Agent Gateway, e.g. https://gw.example.
	Address string
	// HTTPClient optionally overrides the client (tests, custom transports).
	// Defaults to an HTTP/2 client honouring proxy env vars.
	HTTPClient connect.HTTPClient
	// Token supplies short-lived OIDC JWTs per connect.
	Token TokenSource

	AgentVersion string
	TenantID     string
	// Checksum returns the agent's current full-state checksum, sent in the
	// handshake so the gateway can decide on resync (plan §5.2).
	Checksum func() string

	Backoff        Backoff
	ReceiveTimeout time.Duration // reconnect when no inbound traffic for this long (keepalive dead-man switch)

	// Logger receives session errors (otherwise silent by design: partitions
	// are expected). Defaults to slog.Default().
	Logger *slog.Logger

	events chan *agentv1.Event
	sendCh chan *agentv1.Event
	seq    atomic.Int64

	mu        sync.Mutex
	connected bool
	// OnConnectedChange, if set, is invoked whenever the stream transitions
	// between connected and disconnected (used to gate command handling —
	// commands fail closed while the stream is down, plan §5.3).
	OnConnectedChange func(connected bool)
}

// NewConnectClient builds a production client with an HTTP/2 transport that
// honours HTTPS_PROXY (HTTP/2 CONNECT) for egress-only environments.
func NewConnectClient(address string, token TokenSource, agentVersion, tenantID string, checksum func() string) *ConnectClient {
	return &ConnectClient{
		Address:      address,
		HTTPClient:   DefaultHTTPClient(address),
		Token:        token,
		AgentVersion: agentVersion,
		TenantID:     tenantID,
		Checksum:     checksum,
		Backoff:      DefaultBackoff,
		events:       make(chan *agentv1.Event, 64),
		sendCh:       make(chan *agentv1.Event, 256),
	}
}

// DefaultHTTPClient returns an HTTP/2-capable client for the given gateway
// address. ConnectRPC bidi streaming requires HTTP/2. https:// addresses use
// the stdlib transport with automatic HTTP/2 and HTTPS_PROXY support
// (HTTP/2 CONNECT, plan §12.1/5); http:// addresses (in-cluster/test) use
// h2c prior-knowledge.
//
// HTTP/2 keepalive pings are enabled on both paths so a silently blackholed
// connection (e.g. an LB idle-timeout drop) surfaces as a receive error
// instead of only tripping the receive dead-man switch (issue #28).
func DefaultHTTPClient(address string) connect.HTTPClient {
	if strings.HasPrefix(address, "http://") {
		return &http.Client{Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				d := &net.Dialer{Timeout: 10 * time.Second}
				return d.DialContext(ctx, network, addr)
			},
			ReadIdleTimeout: 30 * time.Second,
			PingTimeout:     15 * time.Second,
		}}
	}
	t1 := &http.Transport{
		Proxy:              http.ProxyFromEnvironment,
		ForceAttemptHTTP2:  true,
		DisableCompression: true,
	}
	// Enable HTTP/2 keepalive pings on the https path too: ConfigureTransports
	// returns the underlying *http2.Transport the stdlib delegates to.
	if h2, err := http2.ConfigureTransports(t1); err == nil {
		h2.ReadIdleTimeout = 30 * time.Second
		h2.PingTimeout = 15 * time.Second
	}
	return &http.Client{Transport: t1}
}

// Events implements Client.
func (c *ConnectClient) Events() <-chan *agentv1.Event {
	c.lazyInit()
	return c.events
}

// Send implements Client. The event's sequence number is stamped here so
// the gateway can detect gaps and trigger resync.
func (c *ConnectClient) Send(ctx context.Context, event *agentv1.Event) error {
	c.lazyInit()
	if event == nil {
		return errors.New("stream: nil event")
	}
	event.Sequence = c.seq.Add(1)
	if event.Time == nil {
		event.Time = timestamppb.Now()
	}
	select {
	case c.sendCh <- event:
		streamSendQueueDepth.Set(float64(len(c.sendCh)))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Connected reports whether the stream is currently up.
func (c *ConnectClient) Connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}

// SetOnConnectedChange registers the connection-transition callback.
func (c *ConnectClient) SetOnConnectedChange(cb func(bool)) {
	c.mu.Lock()
	c.OnConnectedChange = cb
	c.mu.Unlock()
}

func (c *ConnectClient) setConnected(v bool) {
	c.mu.Lock()
	changed := c.connected != v
	c.connected = v
	cb := c.OnConnectedChange
	c.mu.Unlock()
	if changed && cb != nil {
		cb(v)
	}
}

func (c *ConnectClient) lazyInit() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.events == nil {
		c.events = make(chan *agentv1.Event, 64)
	}
	if c.sendCh == nil {
		c.sendCh = make(chan *agentv1.Event, 256)
	}
}

// Run implements Client: supervise connect → session → reconnect with
// exponential backoff until ctx is cancelled.
func (c *ConnectClient) Run(ctx context.Context) error {
	c.lazyInit()
	backoff := c.Backoff
	if backoff.InitialInterval <= 0 {
		backoff = DefaultBackoff
	}
	delay := backoff.InitialInterval
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		start := time.Now()
		err := c.session(ctx)
		c.setConnected(false)
		cause := sessionCause(err)
		streamSessionsTotal.WithLabelValues(cause).Inc()
		streamSessionDuration.Observe(time.Since(start).Seconds())
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, errTokenRotation) {
			// Planned pre-expiry rotation, not a failure: reconnect promptly
			// and don't ratchet the backoff, or the stream spends ever-longer
			// stretches down between healthy sessions (issue #28 QA).
			delay = backoff.InitialInterval
			if log := c.logger(); log != nil {
				log.Info("rotating stream session before token expiry",
					"sessionDuration", time.Since(start).Round(time.Second).String())
			}
			continue
		}
		if err != nil {
			log := c.Logger
			if log == nil {
				log = slog.Default()
			}
			log.Warn("stream session ended, backing off",
				"error", err, "cause", cause,
				"sessionDuration", time.Since(start).Round(time.Second).String(),
				"retryIn", delay.String())
		}
		wait := jitterDelay(delay)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		delay = nextDelay(delay, backoff)
	}
}

// jitterDelay returns 50–100% of d (full jitter) so fleets of agents do not
// reconnect in lockstep after a gateway outage.
func jitterDelay(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (0.5 + rand.Float64()*0.5))
}

func nextDelay(current time.Duration, b Backoff) time.Duration {
	mult := b.Multiplier
	if mult <= 1 {
		mult = 2
	}
	maxInterval := b.MaxInterval
	if maxInterval <= 0 {
		maxInterval = DefaultBackoff.MaxInterval
	}
	next := time.Duration(float64(current) * mult)
	if next > maxInterval {
		return maxInterval
	}
	return next
}

// session runs one connection lifetime: handshake → recv/send pumps until
// the stream fails or the receive dead-man switch fires.
func (c *ConnectClient) session(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	token, expiry, err := c.token(ctx)
	if err != nil {
		return &handshakeError{fmt.Errorf("stream: fetch token: %w", err)}
	}
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	auth := connect.WithInterceptors(connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Set("Authorization", "Bearer "+token)
			return next(ctx, req)
		}
	}))
	streamClient := agentv1connect.NewEventStreamServiceClient(httpClient, c.Address, auth)
	bidi := streamClient.Connect(ctx)
	bidi.RequestHeader().Set("Authorization", "Bearer "+token)

	// Handshake: first message on every (re)connect carries the last known
	// state checksum.
	hsPayload, err := anypb.New(&agentv1.HandshakeRequest{
		AgentVersion:          c.AgentVersion,
		TenantId:              c.TenantID,
		ContractVersion:       "inari.agent.v1",
		LastSeenStateChecksum: checksumOf(c.Checksum),
	})
	if err != nil {
		return &handshakeError{fmt.Errorf("stream: marshal handshake: %w", err)}
	}
	if err := bidi.Send(&agentv1.ConnectRequest{Event: &agentv1.Event{
		EventId: fmt.Sprintf("handshake-%d", time.Now().UnixNano()),
		Type:    handshakeEventType,
		Payload: hsPayload,
		Time:    timestamppb.Now(),
	}}); err != nil {
		return &handshakeError{fmt.Errorf("stream: send handshake: %w", err)}
	}

	// First inbound message must be the handshake response.
	first, err := bidi.Receive()
	if err != nil {
		return &handshakeError{fmt.Errorf("stream: receive handshake response: %w", err)}
	}
	var hsResp agentv1.HandshakeResponse
	if first.Event == nil || first.Event.Payload == nil || !first.Event.Payload.MessageIs(&hsResp) {
		return &handshakeError{errors.New("stream: expected handshake response as first server message")}
	}
	if err := first.Event.Payload.UnmarshalTo(&hsResp); err != nil {
		return &handshakeError{fmt.Errorf("stream: decode handshake response: %w", err)}
	}
	c.setConnected(true)

	// Rotate the session shortly before the JWT expires: gateways that
	// enforce exp on an open stream otherwise terminate it on the token
	// lifespan cadence (Keycloak default: 300s, issue #28). Reconnect is
	// safe — the handshake is idempotent and checksum-resynced.
	var rotate <-chan time.Time
	if !expiry.IsZero() {
		ttl := time.Until(expiry)
		margin := min(30*time.Second, ttl/4)
		if d := ttl - margin; d > 0 {
			rotate = time.After(d)
		} else {
			rotate = time.After(0) // already (nearly) expired: rotate immediately
		}
	}
	if log := c.logger(); log != nil {
		attrs := []any{"sessionID", hsResp.SessionId}
		if !expiry.IsZero() {
			attrs = append(attrs, "tokenTTL", time.Until(expiry).Round(time.Second).String())
		}
		log.Debug("stream session established", attrs...)
	}

	if hsResp.ResyncRequired {
		c.emit(ctx, &agentv1.Event{
			EventId: "resync-required-" + hsResp.SessionId,
			Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_RESYNC_REQUEST),
			Time:    timestamppb.Now(),
		})
	}

	recvTimeout := c.ReceiveTimeout
	if recvTimeout <= 0 {
		recvTimeout = 2 * time.Minute
	}

	activity := make(chan struct{}, 1)
	recvErr := make(chan error, 1)
	go func() {
		for {
			resp, err := bidi.Receive()
			if err != nil {
				recvErr <- err
				return
			}
			select {
			case activity <- struct{}{}:
			default:
			}
			if resp.Event == nil {
				continue
			}
			switch agentv1.EventTypeFromString(resp.Event.Type) {
			case agentv1.EventType_EVENT_TYPE_PING:
				// Keepalive: answer server pings with pongs (plan §5.3).
				// The pong is queued on sendCh so the send pump is the only
				// goroutine calling bidi.Send — connect-go streaming clients
				// are not safe for concurrent Send (issue #28). Pongs are
				// droppable under saturation: the next ping re-pongs.
				pong, perr := anypb.New(&agentv1.Pong{Time: timestamppb.Now()})
				if perr == nil {
					// Pongs carry no Sequence: they are droppable keepalives,
					// and consuming a sequence number for an event that may
					// never reach the wire opens a gap the gateway would read
					// as a resync trigger (issue #28 QA).
					ev := &agentv1.Event{
						EventId: "pong-" + resp.Event.EventId,
						Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_PONG),
						Payload: pong,
						Time:    timestamppb.Now(),
					}
					select {
					case c.sendCh <- ev:
						streamSendQueueDepth.Set(float64(len(c.sendCh)))
					default:
					}
				}
			default:
				c.emit(ctx, resp.Event)
			}
		}
	}()

	// Send pump: drain queued events onto the wire.
	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-c.sendCh:
				streamSendQueueDepth.Set(float64(len(c.sendCh)))
				if err := bidi.Send(&agentv1.ConnectRequest{Event: ev}); err != nil {
					return
				}
			}
		}
	}()

	deadman := time.NewTimer(recvTimeout)
	defer deadman.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-recvErr:
			return fmt.Errorf("stream: receive: %w", err)
		case <-sendDone:
			return errSendPump
		case <-rotate:
			return errTokenRotation
		case <-activity:
			if !deadman.Stop() {
				select {
				case <-deadman.C:
				default:
				}
			}
			deadman.Reset(recvTimeout)
		case <-deadman.C:
			return errDeadman
		}
	}
}

// emit delivers an inbound event to consumers. It blocks when the buffer is
// full (backpressure) rather than silently dropping: inbound events include
// commands, and a dropped command would never be acked or redelivered. If
// the consumer stalls, the receive dead-man switch eventually fires and the
// session reconnects.
func (c *ConnectClient) emit(ctx context.Context, ev *agentv1.Event) {
	select {
	case c.events <- ev:
		streamEventsQueueDepth.Set(float64(len(c.events)))
	case <-ctx.Done():
	}
}

// token fetches the session token, plus its expiry when the TokenSource
// supports it (ExpiringTokenSource). A zero expiry means "unknown".
func (c *ConnectClient) token(ctx context.Context) (string, time.Time, error) {
	if ts, ok := c.Token.(ExpiringTokenSource); ok {
		return ts.TokenWithExpiry(ctx)
	}
	tok, err := c.Token.Token(ctx)
	return tok, time.Time{}, err
}

func (c *ConnectClient) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

func checksumOf(f func() string) string {
	if f == nil {
		return ""
	}
	return f()
}

var _ Client = (*ConnectClient)(nil)
