package command

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

// Dispatcher routes server→agent command events to per-kind handlers.
// M1 handlers accept, validate the payload, and ack with a structured
// result — the real Git/ArgoCD/KRO logic lands in M2. Results are recorded
// by command_id so duplicate deliveries are answered without re-execution
// (at-least-once delivery, plan §5.2). Commands fail closed while the
// stream is disconnected (plan §5.3).
type Dispatcher struct {
	mu        sync.Mutex
	seen      map[string]*agentv1.CommandAck
	handled   atomic.Int64
	connected atomic.Bool
}

// NewDispatcher returns a Dispatcher that starts in the connected state.
func NewDispatcher() *Dispatcher {
	d := &Dispatcher{seen: map[string]*agentv1.CommandAck{}}
	d.connected.Store(true)
	return d
}

// SetConnected toggles the fail-closed gate.
func (d *Dispatcher) SetConnected(v bool) { d.connected.Store(v) }

// HandledCount reports how many commands were actually executed (excluding
// idempotent replays) — test/diagnostic hook.
func (d *Dispatcher) HandledCount() int64 { return d.handled.Load() }

// HandleEvent implements Handler.
func (d *Dispatcher) HandleEvent(_ context.Context, ev *agentv1.Event) (*agentv1.CommandAck, error) {
	if ev == nil || ev.Payload == nil {
		return nil, fmt.Errorf("command: empty event")
	}
	commandID, err := commandIDOf(ev)
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	if ack, ok := d.seen[commandID]; ok {
		d.mu.Unlock()
		return ack, nil
	}
	d.mu.Unlock()

	if !d.connected.Load() {
		return nil, fmt.Errorf("command %s: stream disconnected, failing closed", commandID)
	}

	ack := &agentv1.CommandAck{
		CommandId: commandID,
		Result:    agentv1.CommandResult_COMMAND_RESULT_ACCEPTED,
		Message:   "accepted (no-op until M2)",
	}
	d.mu.Lock()
	d.seen[commandID] = ack
	d.mu.Unlock()
	d.handled.Add(1)
	return ack, nil
}

// commandIDOf extracts the command_id from a known command payload.
func commandIDOf(ev *agentv1.Event) (string, error) {
	switch agentv1.EventTypeFromString(ev.Type) {
	case agentv1.EventType_EVENT_TYPE_APPLY_BUNDLE:
		var m agentv1.ApplyBundle
		if err := ev.Payload.UnmarshalTo(&m); err != nil {
			return "", fmt.Errorf("command: decode apply-bundle: %w", err)
		}
		return requireID(m.CommandId)
	case agentv1.EventType_EVENT_TYPE_REGISTER_ARGOCD_APP:
		var m agentv1.RegisterArgoCDApp
		if err := ev.Payload.UnmarshalTo(&m); err != nil {
			return "", fmt.Errorf("command: decode register-argocd-app: %w", err)
		}
		return requireID(m.CommandId)
	case agentv1.EventType_EVENT_TYPE_INVOKE_ACTION:
		var m agentv1.InvokeAction
		if err := ev.Payload.UnmarshalTo(&m); err != nil {
			return "", fmt.Errorf("command: decode invoke-action: %w", err)
		}
		return requireID(m.CommandId)
	case agentv1.EventType_EVENT_TYPE_RENDER_RGD_INSTANCE:
		var m agentv1.RenderRgdInstance
		if err := ev.Payload.UnmarshalTo(&m); err != nil {
			return "", fmt.Errorf("command: decode render-rgd-instance: %w", err)
		}
		return requireID(m.CommandId)
	default:
		return "", fmt.Errorf("command: unknown event type %q", ev.Type)
	}
}

func requireID(id string) (string, error) {
	if id == "" {
		return "", fmt.Errorf("command: missing command_id")
	}
	return id, nil
}

var _ Handler = (*Dispatcher)(nil)
