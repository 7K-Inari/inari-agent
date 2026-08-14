// Package command executes control-plane commands received on the agent
// EventStream (plan §5.3). Delivery is at-least-once: command_id is the
// idempotency key and handlers MUST be idempotent. Mutations are limited
// to Inari-managed namespaces/resources; out-of-band actions fail closed
// when the stream is disconnected.
package command

import (
	"context"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

// Handler executes control-plane commands. HandleEvent must be safe to
// invoke multiple times with the same command_id.
type Handler interface {
	HandleEvent(ctx context.Context, ev *agentv1.Event) (*agentv1.CommandAck, error)
}
