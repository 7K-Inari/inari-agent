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
func DefaultHTTPClient(address string) connect.HTTPClient {
	if strings.HasPrefix(address, "http://") {
		return &http.Client{Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				d := &net.Dialer{Timeout: 10 * time.Second}
				return d.DialContext(ctx, network, addr)
			},
		}}
	}
	return &http.Client{Transport: &http.Transport{
		Proxy:              http.ProxyFromEnvironment,
		ForceAttemptHTTP2:  true,
		DisableCompression: true,
	}}
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
		err := c.session(ctx)
		c.setConnected(false)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			log := c.Logger
			if log == nil {
				log = slog.Default()
			}
			log.Warn("stream session ended, backing off", "error", err, "retryIn", delay.String())
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

	token, err := c.Token.Token(ctx)
	if err != nil {
		return fmt.Errorf("stream: fetch token: %w", err)
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
		return fmt.Errorf("stream: marshal handshake: %w", err)
	}
	if err := bidi.Send(&agentv1.ConnectRequest{Event: &agentv1.Event{
		EventId: fmt.Sprintf("handshake-%d", time.Now().UnixNano()),
		Type:    handshakeEventType,
		Payload: hsPayload,
		Time:    timestamppb.Now(),
	}}); err != nil {
		return fmt.Errorf("stream: send handshake: %w", err)
	}

	// First inbound message must be the handshake response.
	first, err := bidi.Receive()
	if err != nil {
		return fmt.Errorf("stream: receive handshake response: %w", err)
	}
	var hsResp agentv1.HandshakeResponse
	if first.Event == nil || first.Event.Payload == nil || !first.Event.Payload.MessageIs(&hsResp) {
		return errors.New("stream: expected handshake response as first server message")
	}
	if err := first.Event.Payload.UnmarshalTo(&hsResp); err != nil {
		return fmt.Errorf("stream: decode handshake response: %w", err)
	}
	c.setConnected(true)

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
				pong, perr := anypb.New(&agentv1.Pong{Time: timestamppb.Now()})
				if perr == nil {
					_ = bidi.Send(&agentv1.ConnectRequest{Event: &agentv1.Event{
						EventId: "pong-" + resp.Event.EventId,
						Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_PONG),
						Payload: pong,
						Time:    timestamppb.Now(),
					}})
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
			return errors.New("stream: send pump failed")
		case <-activity:
			if !deadman.Stop() {
				select {
				case <-deadman.C:
				default:
				}
			}
			deadman.Reset(recvTimeout)
		case <-deadman.C:
			return errors.New("stream: receive timeout (keepalive dead-man switch)")
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
	case <-ctx.Done():
	}
}

func checksumOf(f func() string) string {
	if f == nil {
		return ""
	}
	return f()
}

var _ Client = (*ConnectClient)(nil)
