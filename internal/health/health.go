// Package health implements the agent's readiness probe depth (plan §5.3):
// /readyz reflects control-plane stream connectivity, with a grace period
// after disconnect so brief reconnects don't flap readiness. Liveness stays
// a bare ping — a process that can recover must not be restarted.
package health

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

// StreamTracker tracks the control-plane stream's connectivity for the
// readiness probe. It starts unarmed: until the agent lifecycle builds a
// stream client (standalone mode never does), Check reports ready.
type StreamTracker struct {
	grace time.Duration
	// now is injectable for tests.
	now func() time.Time

	armed          atomic.Bool
	connected      atomic.Bool
	lastDisconnect atomic.Int64 // unix nano
}

// NewStreamTracker returns a tracker that tolerates disconnects shorter than
// grace before reporting unready.
func NewStreamTracker(grace time.Duration) *StreamTracker {
	return &StreamTracker{grace: grace, now: time.Now}
}

// SetConnected records a connectivity transition. The first call arms the
// tracker; arming disconnected starts the initial-connect grace window.
func (t *StreamTracker) SetConnected(v bool) {
	t.armed.Store(true)
	t.connected.Store(v)
	if !v {
		t.lastDisconnect.Store(t.now().UnixNano())
	}
}

// Check is a healthz.Checker: ready when unarmed (standalone), connected, or
// disconnected within the grace period.
func (t *StreamTracker) Check(_ *http.Request) error {
	if !t.armed.Load() || t.connected.Load() {
		return nil
	}
	since := t.now().Sub(time.Unix(0, t.lastDisconnect.Load()))
	if since < t.grace {
		return nil
	}
	return fmt.Errorf("control-plane stream disconnected for %s (grace %s)", since.Truncate(time.Second), t.grace)
}
