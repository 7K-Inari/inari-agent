// Package command defines the contract for control-plane commands executed
// by the agent (GitOps rendering, ArgoCD command proxy — plan §5.3).
// Delivery is at-least-once: handlers MUST be idempotent. Mutations are
// limited to Inari-managed namespaces/resources. Implementations land in M1.
package command

import "context"

// Command is an idempotent unit of work from the control plane.
type Command struct {
	ID       string
	TenantID string
	Kind     string
	Payload  []byte
}

// Result reports the outcome of handling a Command.
type Result struct {
	CommandID string
	Succeeded bool
	Message   string
}

// Handler executes control-plane commands. Handle must be safe to invoke
// multiple times with the same Command.ID.
type Handler interface {
	Handle(ctx context.Context, cmd Command) (Result, error)
}
