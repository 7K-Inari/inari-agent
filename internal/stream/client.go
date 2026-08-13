// Package stream defines the outbound-only bidirectional gRPC stream to the
// Inari control plane (plan §5.3). The control plane never connects in.
// Message types here are M0 placeholders; the real EventStream protos come
// from the inari-api repo. TODO: pin github.com/7K-Inari/inari-api once it
// tags v0.1.0 and replace these types with its versioned packages.
package stream

import (
	"context"
	"time"
)

// Backoff configures reconnect behaviour for the stream.
type Backoff struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
}

// DefaultBackoff is the M0 reconnect policy.
var DefaultBackoff = Backoff{
	InitialInterval: time.Second,
	MaxInterval:     5 * time.Minute,
	Multiplier:      2.0,
}

// Event is a placeholder envelope for messages received from the control
// plane (commands, resync requests, pongs). Replaced by inari-api protos in M1.
type Event struct {
	Type    string
	Payload []byte
}

// Update is a placeholder envelope for messages sent to the control plane
// (capability updates, status reports, pings). Replaced by inari-api protos in M1.
type Update struct {
	Type    string
	Payload []byte
}

// Client is the agent-side bidirectional stream. Implementations must
// reconnect with backoff on partition and perform a checksum-based resync
// on reconnect (at-least-once delivery; consumers must be idempotent).
type Client interface {
	// Run owns the connection lifecycle: dial, ping/pong, backoff reconnect.
	// It blocks until ctx is cancelled.
	Run(ctx context.Context) error
	// Send queues an update for the control plane.
	Send(ctx context.Context, update Update) error
	// Events returns the channel of inbound control-plane events.
	Events() <-chan Event
}
