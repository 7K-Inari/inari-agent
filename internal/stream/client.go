// Package stream implements the outbound-only bidirectional event stream to
// the Inari control plane (plan §5.3). The control plane never connects in.
// Wire types come from the pinned inari-api module (inari.agent.v1).
package stream

import (
	"context"
	"time"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

// Backoff configures reconnect behaviour for the stream.
type Backoff struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
}

// DefaultBackoff is the reconnect policy (1s doubling to 5m).
var DefaultBackoff = Backoff{
	InitialInterval: time.Second,
	MaxInterval:     5 * time.Minute,
	Multiplier:      2.0,
}

// TokenSource supplies short-lived OIDC JWTs (client-credentials grant with
// a hardcoded cluster_id claim, plan §5.3). Tokens are fetched per connect
// and refreshed by the implementation as they expire.
type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

// Client is the agent-side bidirectional stream. Implementations reconnect
// with backoff on partition and re-handshake with the last acknowledged
// state checksum on reconnect; the gateway decides whether a full resync is
// required (at-least-once delivery; consumers must be idempotent).
type Client interface {
	// Run owns the connection lifecycle: dial, handshake, keepalive,
	// backoff reconnect. It blocks until ctx is cancelled.
	Run(ctx context.Context) error
	// Send queues an event for the control plane. Safe for concurrent use.
	Send(ctx context.Context, event *agentv1.Event) error
	// Events returns the channel of inbound control-plane events (commands,
	// pings, resync requests).
	Events() <-chan *agentv1.Event
}
