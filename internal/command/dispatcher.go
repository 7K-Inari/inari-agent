package command

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

// KindHandler executes one command kind. Handlers must be idempotent
// (command_id is the dedupe key, plan §5.2) and return APPLIED/FAILED
// with a human-readable message. A non-nil error means the failure may
// be transient: the ack is not recorded and a redelivery re-executes.
type KindHandler func(ctx context.Context, ev *agentv1.Event) (agentv1.CommandResult, string, error)

// Journal persists command acks so idempotency survives agent restarts.
type Journal interface {
	// Load returns all recorded acks (keyed by command_id).
	Load(ctx context.Context) (map[string]*agentv1.CommandAck, error)
	// Record persists one ack; must be idempotent per command_id.
	Record(ctx context.Context, ack *agentv1.CommandAck) error
}

// Dispatcher routes server→agent command events to per-kind handlers.
// Kinds without a registered handler are accepted as no-ops (M1
// behavior). Results are recorded by command_id so duplicate deliveries
// are answered without re-execution (at-least-once delivery, plan §5.2).
// Commands fail closed while the stream is disconnected (plan §5.3).
type Dispatcher struct {
	mu        sync.Mutex
	seen      map[string]*agentv1.CommandAck
	handlers  map[agentv1.EventType]KindHandler
	journal   Journal
	handled   atomic.Int64
	connected atomic.Bool
}

// NewDispatcher returns a Dispatcher that starts in the connected state.
func NewDispatcher() *Dispatcher {
	d := &Dispatcher{
		seen:     map[string]*agentv1.CommandAck{},
		handlers: map[agentv1.EventType]KindHandler{},
	}
	d.connected.Store(true)
	return d
}

// Register installs the handler for a command kind, replacing any
// previous one.
func (d *Dispatcher) Register(t agentv1.EventType, h KindHandler) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.handlers[t] = h
}

// AttachJournal preloads previously recorded acks and enables
// write-through persistence for new ones.
func (d *Dispatcher) AttachJournal(ctx context.Context, j Journal) error {
	loaded, err := j.Load(ctx)
	if err != nil {
		return fmt.Errorf("command: load journal: %w", err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for id, ack := range loaded {
		if _, ok := d.seen[id]; !ok {
			d.seen[id] = ack
		}
	}
	d.journal = j
	return nil
}

// SetConnected toggles the fail-closed gate.
func (d *Dispatcher) SetConnected(v bool) { d.connected.Store(v) }

// HandledCount reports how many commands were actually executed (excluding
// idempotent replays) — test/diagnostic hook.
func (d *Dispatcher) HandledCount() int64 { return d.handled.Load() }

// HandleEvent implements Handler.
func (d *Dispatcher) HandleEvent(ctx context.Context, ev *agentv1.Event) (*agentv1.CommandAck, error) {
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

	et := agentv1.EventTypeFromString(ev.Type)
	d.mu.Lock()
	handler, ok := d.handlers[et]
	d.mu.Unlock()

	var ack *agentv1.CommandAck
	if !ok {
		ack = &agentv1.CommandAck{
			CommandId: commandID,
			Result:    agentv1.CommandResult_COMMAND_RESULT_ACCEPTED,
			Message:   "accepted (no handler registered)",
		}
	} else {
		result, message, err := handler(ctx, ev)
		if err != nil {
			// Transient failure: not recorded; redelivery re-executes.
			return nil, fmt.Errorf("command %s: %w", commandID, err)
		}
		ack = &agentv1.CommandAck{CommandId: commandID, Result: result, Message: message}
	}

	d.mu.Lock()
	d.seen[commandID] = ack
	journal := d.journal
	d.mu.Unlock()
	if journal != nil {
		if err := journal.Record(ctx, ack); err != nil {
			return nil, fmt.Errorf("command %s: journal record: %w", commandID, err)
		}
	}
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
