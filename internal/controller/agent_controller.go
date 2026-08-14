// Package controller hosts the agent lifecycle: register (one-time token →
// per-cluster OIDC client), connect (outbound EventStream), discover
// (capability watches), and dispatch (command handling) — plan §5.3.
package controller

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/go-logr/logr"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/capability"
	"github.com/7K-Inari/inari-agent/internal/command"
	"github.com/7K-Inari/inari-agent/internal/registration"
	"github.com/7K-Inari/inari-agent/internal/stream"
)

// AgentReconciler is the agent lifecycle Runnable. With no Registrar or no
// bootstrap token it runs standalone (healthy, no upstream connection) so
// the agent can be deployed before a control plane is configured.
type AgentReconciler struct {
	Log logr.Logger

	// BootstrapToken is the one-time TTL'd registration token from the
	// install manifest. It is cleared after the exchange.
	BootstrapToken string

	Registrar    registration.Registrar
	SecretReader registration.SecretReader

	Kube    kubernetes.Interface
	Dynamic dynamic.Interface

	// NewStreamClient builds the stream client for registered credentials.
	// The checksum function is the aggregator's live state checksum.
	NewStreamClient func(creds *registration.Credentials, clientSecret string, checksum func() string) stream.Client
	// NewWatchers builds the capability watchers.
	NewWatchers func(kube kubernetes.Interface, dyn dynamic.Interface) []capability.Watcher
	// Handler defaults to command.NewDispatcher().
	Handler command.Handler

	tokenMu        sync.Mutex
	tokenForgotten atomic.Bool
}

// TokenForgotten reports whether the one-time bootstrap token has been
// cleared (after the registration exchange).
func (r *AgentReconciler) TokenForgotten() bool { return r.tokenForgotten.Load() }

func (r *AgentReconciler) hasBootstrapToken() bool {
	r.tokenMu.Lock()
	defer r.tokenMu.Unlock()
	return r.BootstrapToken != ""
}

// NewAgentReconciler builds the lifecycle Runnable with zero-value
// dependencies (standalone mode).
func NewAgentReconciler(_ manager.Manager) *AgentReconciler {
	return &AgentReconciler{Log: ctrl.Log.WithName("agent")}
}

// Start runs the lifecycle until ctx is cancelled.
func (r *AgentReconciler) Start(ctx context.Context) error {
	log := r.Log
	if r.Registrar == nil || !r.hasBootstrapToken() {
		log.Info("running standalone: no registration token or registrar configured; waiting")
		<-ctx.Done()
		return nil
	}

	creds, err := r.register(ctx)
	if err != nil {
		return fmt.Errorf("agent lifecycle: %w", err)
	}

	clientSecret, err := r.SecretReader.ReadSecret(ctx, creds.SecretRef)
	if err != nil {
		return fmt.Errorf("agent lifecycle: read client secret: %w", err)
	}

	watchers := r.NewWatchers(r.Kube, r.Dynamic)
	aggregator := capability.NewAggregator(creds.TenantID, creds.ClusterID, nil, watchers)
	client := r.NewStreamClient(creds, clientSecret, aggregator.Checksum)
	aggregator.Client = client

	handler := r.Handler
	if handler == nil {
		handler = command.NewDispatcher()
	}
	// Fail closed until the stream is actually up: gate command handling on
	// the connection state (plan §5.3).
	if gate, ok := handler.(interface{ SetConnected(bool) }); ok {
		gate.SetConnected(false)
		if tracker, ok := client.(interface{ SetOnConnectedChange(func(bool)) }); ok {
			tracker.SetOnConnectedChange(gate.SetConnected)
		}
	}

	log.Info("agent registered and connecting",
		"tenant", creds.TenantID, "cluster", creds.ClusterID, "controlPlane", creds.ControlPlane)

	errCh := make(chan error, 2)
	go func() { errCh <- client.Run(ctx) }()
	go func() { errCh <- aggregator.Run(ctx) }()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			return err
		case ev := <-client.Events():
			r.dispatch(ctx, client, aggregator, handler, ev)
		}
	}
}

// register exchanges the bootstrap token and forgets it.
func (r *AgentReconciler) register(ctx context.Context) (*registration.Credentials, error) {
	r.tokenMu.Lock()
	token := r.BootstrapToken
	r.tokenMu.Unlock()
	creds, err := r.Registrar.Register(ctx, token)
	r.tokenMu.Lock()
	r.BootstrapToken = "" // one-time token: forget immediately (plan §5.3)
	r.tokenMu.Unlock()
	r.tokenForgotten.Store(true)
	if err != nil {
		return nil, fmt.Errorf("register: %w", err)
	}
	return creds, nil
}

// dispatch routes one inbound control-plane event.
func (r *AgentReconciler) dispatch(
	ctx context.Context,
	client stream.Client,
	aggregator *capability.Aggregator,
	handler command.Handler,
	ev *agentv1.Event,
) {
	switch agentv1.EventTypeFromString(ev.Type) {
	case agentv1.EventType_EVENT_TYPE_RESYNC_REQUEST:
		if err := aggregator.ReplayFullState(ctx); err != nil {
			r.Log.Error(err, "resync replay failed")
		}
	case agentv1.EventType_EVENT_TYPE_APPLY_BUNDLE,
		agentv1.EventType_EVENT_TYPE_REGISTER_ARGOCD_APP,
		agentv1.EventType_EVENT_TYPE_INVOKE_ACTION,
		agentv1.EventType_EVENT_TYPE_RENDER_RGD_INSTANCE:
		r.handleCommand(ctx, client, handler, ev)
	default:
		// Compatibility contract: unknown event types are dropped-and-logged,
		// never fatal (an N−1 gateway may send newer types).
		r.Log.V(1).Info("dropping unknown event", "type", ev.Type, "eventId", ev.EventId)
	}
}

func (r *AgentReconciler) handleCommand(
	ctx context.Context,
	client stream.Client,
	handler command.Handler,
	ev *agentv1.Event,
) {
	ack, err := handler.HandleEvent(ctx, ev)
	if err != nil {
		r.sendAck(ctx, client, ev.EventId, &agentv1.CommandAck{
			CommandId: ev.EventId,
			Result:    agentv1.CommandResult_COMMAND_RESULT_FAILED,
			Message:   err.Error(),
		}, agentv1.EventType_EVENT_TYPE_COMMAND_NACK)
		return
	}
	ackType := agentv1.EventType_EVENT_TYPE_COMMAND_ACK
	if ack.Result == agentv1.CommandResult_COMMAND_RESULT_FAILED {
		ackType = agentv1.EventType_EVENT_TYPE_COMMAND_NACK
	}
	r.sendAck(ctx, client, ack.CommandId, ack, ackType)
}

func (r *AgentReconciler) sendAck(
	ctx context.Context,
	client stream.Client,
	id string,
	ack *agentv1.CommandAck,
	ackType agentv1.EventType,
) {
	payload, err := anypb.New(ack)
	if err != nil {
		r.Log.Error(err, "marshal command ack")
		return
	}
	if err := client.Send(ctx, &agentv1.Event{
		EventId: "ack-" + id,
		Type:    agentv1.EventTypeString(ackType),
		Payload: payload,
		Time:    timestamppb.Now(),
	}); err != nil {
		r.Log.Error(err, "send command ack", "commandId", id)
	}
}

// SetupWithManager registers the lifecycle as a manager Runnable.
func (r *AgentReconciler) SetupWithManager(mgr manager.Manager) error {
	return mgr.Add(r)
}

var _ manager.Runnable = (*AgentReconciler)(nil)
