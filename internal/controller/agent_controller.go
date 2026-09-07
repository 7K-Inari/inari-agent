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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"
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
	Meta    metadata.Interface

	// NewStreamClient builds the stream client for registered credentials.
	// The checksum function is the aggregator's live state checksum.
	NewStreamClient func(creds *registration.Credentials, clientSecret string, checksum func() string) stream.Client
	// NewWatchers builds the capability watchers.
	NewWatchers func(kube kubernetes.Interface, dyn dynamic.Interface, meta metadata.Interface) []capability.Watcher
	// Handler defaults to command.NewDispatcher().
	Handler command.Handler
	// GitOps, when set, registers the real M2 command handlers on the
	// dispatcher and runs the status streamer (plan §5.3 phases 4-5).
	GitOps *GitOpsConfig

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
	creds, err := r.loadPersistedCredentials(ctx)
	if err != nil {
		return fmt.Errorf("agent lifecycle: load persisted credentials: %w", err)
	}
	if creds == nil {
		if r.Registrar == nil || !r.hasBootstrapToken() {
			log.Info("running standalone: no registration token or registrar configured; waiting")
			<-ctx.Done()
			return nil
		}
		creds, err = r.register(ctx)
		if err != nil {
			return fmt.Errorf("agent lifecycle: %w", err)
		}
		if err := r.persistCredentials(ctx, creds); err != nil {
			return fmt.Errorf("agent lifecycle: persist credentials: %w", err)
		}
	} else {
		log.Info("using persisted registration, skipping exchange",
			"tenant", creds.TenantID, "cluster", creds.ClusterID)
	}

	clientSecret, err := r.SecretReader.ReadSecret(ctx, creds.SecretRef)
	if err != nil {
		return fmt.Errorf("agent lifecycle: read client secret: %w", err)
	}

	watchers := r.NewWatchers(r.Kube, r.Dynamic, r.Meta)
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

	errCh := make(chan error, 3)
	go func() { errCh <- client.Run(ctx) }()
	go func() { errCh <- aggregator.Run(ctx) }()
	if r.GitOps != nil {
		if err := r.GitOps.configure(ctx, handler, creds.TenantID); err != nil {
			return fmt.Errorf("agent lifecycle: gitops setup: %w", err)
		}
		streamer := r.GitOps.streamer(r.Log, r.Dynamic, client)
		go func() { errCh <- streamer.Run(ctx) }()
	}

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

// registrationConfigMap persists the non-secret registration output (cluster
// identity, token URL, delivery reference) so the agent survives restarts
// without re-consuming the one-time bootstrap token.
const registrationConfigMap = "inari-agent-registration"

// loadPersistedCredentials returns previously persisted credentials, or nil
// when the agent has never completed registration.
func (r *AgentReconciler) loadPersistedCredentials(ctx context.Context) (*registration.Credentials, error) {
	if r.Kube == nil {
		return nil, nil
	}
	cm, err := r.Kube.CoreV1().ConfigMaps("inari-system").Get(ctx, registrationConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	d := cm.Data
	creds := &registration.Credentials{
		TenantID:     d["tenant-id"],
		ClusterID:    d["cluster-id"],
		ClientID:     d["client-id"],
		TokenURL:     d["token-url"],
		ControlPlane: d["control-plane"],
		SecretRef: registration.SecretReference{
			Store:     d["secret-store"],
			Name:      d["secret-name"],
			Namespace: d["secret-namespace"],
			Key:       d["secret-key"],
		},
	}
	if creds.ClusterID == "" || creds.ClientID == "" || creds.TokenURL == "" {
		return nil, nil
	}
	return creds, nil
}

// persistCredentials stores the registration output for future restarts.
func (r *AgentReconciler) persistCredentials(ctx context.Context, creds *registration.Credentials) error {
	if r.Kube == nil {
		return nil
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: registrationConfigMap, Namespace: "inari-system"},
		Data: map[string]string{
			"tenant-id":        creds.TenantID,
			"cluster-id":       creds.ClusterID,
			"client-id":        creds.ClientID,
			"token-url":        creds.TokenURL,
			"control-plane":    creds.ControlPlane,
			"secret-store":     creds.SecretRef.Store,
			"secret-name":      creds.SecretRef.Name,
			"secret-namespace": creds.SecretRef.Namespace,
			"secret-key":       creds.SecretRef.Key,
		},
	}
	_, err := r.Kube.CoreV1().ConfigMaps("inari-system").Create(ctx, cm, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
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
