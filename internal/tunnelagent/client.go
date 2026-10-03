package tunnelagent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	tunnelv1 "github.com/7K-Inari/inari-api/gen/go/inari/tunnel/v1"
	"github.com/7K-Inari/inari-api/gen/go/inari/tunnel/v1/tunnelv1connect"

	"github.com/7K-Inari/inari-agent/internal/stream"
)

// errTokenRotation ends a healthy session shortly before the JWT expires so
// the reconnect picks up a fresh token instead of letting the hub terminate
// the stream mid-flight (same cadence hazard as issue #28).
var errTokenRotation = errors.New("tunnelagent: token nearing expiry, rotating session")

// Client owns the outbound TunnelService.Connect bidi stream lifecycle:
// dial (h2c/TLS via stream.DefaultHTTPClient), bearer client-credentials
// auth, keepalive pings, backoff reconnect (plan §8; pull, never push).
type Client struct {
	// Address is the kubeproxy base URL, e.g. https://kubeproxy.example.
	Address string
	// HTTPClient optionally overrides the client (tests). Defaults to
	// stream.DefaultHTTPClient(Address).
	HTTPClient connect.HTTPClient
	// Token supplies short-lived OIDC JWTs per connect.
	Token stream.TokenSource
	// Handler receives inbound tunnel traffic.
	Handler *Handler

	Backoff      stream.Backoff
	PingInterval time.Duration

	Logger *slog.Logger

	connected atomic.Bool
}

// NewClient builds a production tunnel client.
func NewClient(address string, token stream.TokenSource, handler *Handler) *Client {
	return &Client{
		Address:      address,
		HTTPClient:   stream.DefaultHTTPClient(address),
		Token:        token,
		Handler:      handler,
		Backoff:      stream.DefaultBackoff,
		PingInterval: 30 * time.Second,
	}
}

// Connected reports whether the tunnel stream is currently up (/readyz).
func (c *Client) Connected() bool { return c.connected.Load() }

// Run supervises connect → session → reconnect with exponential backoff
// until ctx is cancelled. Partitions are expected; sessions reset all
// per-connection state on teardown (the hub drops its side too).
func (c *Client) Run(ctx context.Context) error {
	backoff := c.Backoff
	if backoff.InitialInterval <= 0 {
		backoff = stream.DefaultBackoff
	}
	delay := backoff.InitialInterval
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		start := time.Now()
		err := c.session(ctx)
		c.connected.Store(false)
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, errTokenRotation) {
			// Planned rotation, not a failure: reconnect promptly without
			// ratcheting the backoff.
			delay = backoff.InitialInterval
			c.logger().Info("rotating tunnel session before token expiry",
				"sessionDuration", time.Since(start).Round(time.Second).String())
			continue
		}
		if err != nil {
			c.logger().Warn("tunnel session ended, backing off",
				"error", err,
				"sessionDuration", time.Since(start).Round(time.Second).String(),
				"retryIn", delay.String())
		}
		wait := time.Duration(float64(delay) * (0.5 + rand.Float64()*0.5))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		delay = nextDelay(delay, backoff)
	}
}

// session runs one stream lifetime: connect → ping/send pumps + receive
// loop until failure or ctx cancel.
func (c *Client) session(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	token, expiry, err := c.token(ctx)
	if err != nil {
		return fmt.Errorf("tunnelagent: fetch token: %w", err)
	}
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = stream.DefaultHTTPClient(c.Address)
	}
	auth := connect.WithInterceptors(connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Set("Authorization", "Bearer "+token)
			return next(ctx, req)
		}
	}))
	svc := tunnelv1connect.NewTunnelServiceClient(httpClient, c.Address, auth)
	bidi := svc.Connect(ctx)
	bidi.RequestHeader().Set("Authorization", "Bearer "+token)

	// Wire the handler's outbound path to this session's send pump; reset
	// all connections when the session ends (the hub's side is gone).
	sendCh := make(chan *tunnelv1.TunnelMessage, 256)
	c.Handler.SetSend(func(msg *tunnelv1.TunnelMessage) {
		select {
		case sendCh <- msg:
		default:
			c.logger().Warn("tunnel send queue full, dropping message", "connID", msg.GetConnectionId())
		}
	})
	defer func() {
		c.Handler.SetSend(nil)
		c.Handler.ResetAll("stream-ended")
	}()

	// Send pump: the only goroutine calling bidi.Send (connect-go streams
	// are not safe for concurrent Send).
	sendDone := make(chan error, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				sendDone <- nil
				return
			case msg := <-sendCh:
				if err := bidi.Send(msg); err != nil {
					sendDone <- err
					return
				}
			}
		}
	}()

	// Session keepalive pings (connection_id empty per contract).
	pingInterval := c.PingInterval
	if pingInterval <= 0 {
		pingInterval = 30 * time.Second
	}
	go func() {
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				ping := &tunnelv1.TunnelMessage{Payload: &tunnelv1.TunnelMessage_Ping{
					Ping: &tunnelv1.TunnelPing{Time: timestamppb.New(t)},
				}}
				select {
				case sendCh <- ping:
				default: // droppable keepalive
				}
			}
		}
	}()

	// Rotate shortly before JWT expiry (see errTokenRotation).
	var rotate <-chan time.Time
	if !expiry.IsZero() {
		ttl := time.Until(expiry)
		margin := min(30*time.Second, ttl/4)
		if d := ttl - margin; d > 0 {
			rotate = time.After(d)
		} else {
			rotate = time.After(0)
		}
	}

	c.connected.Store(true)
	c.logger().Debug("tunnel session established", "address", c.Address)

	recvErr := make(chan error, 1)
	go func() {
		for {
			msg, err := bidi.Receive()
			if err != nil {
				recvErr <- err
				return
			}
			c.Handler.Handle(msg)
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-recvErr:
		return fmt.Errorf("tunnelagent: receive: %w", err)
	case err := <-sendDone:
		if err != nil {
			return fmt.Errorf("tunnelagent: send: %w", err)
		}
		return errors.New("tunnelagent: send pump ended")
	case <-rotate:
		return errTokenRotation
	}
}

// token fetches the session token plus expiry when supported (zero = unknown).
func (c *Client) token(ctx context.Context) (string, time.Time, error) {
	if ts, ok := c.Token.(stream.ExpiringTokenSource); ok {
		return ts.TokenWithExpiry(ctx)
	}
	tok, err := c.Token.Token(ctx)
	return tok, time.Time{}, err
}

func (c *Client) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

func nextDelay(current time.Duration, b stream.Backoff) time.Duration {
	mult := b.Multiplier
	if mult <= 1 {
		mult = 2
	}
	maxInterval := b.MaxInterval
	if maxInterval <= 0 {
		maxInterval = stream.DefaultBackoff.MaxInterval
	}
	next := time.Duration(float64(current) * mult)
	if next > maxInterval {
		return maxInterval
	}
	return next
}
