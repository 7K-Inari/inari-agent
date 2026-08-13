// Package controller hosts the M0 no-op reconcile loop: a manager Runnable
// that ticks on an interval and proves the agent runs healthy in-cluster.
// Lifecycle components (Registrar, stream.Client, capability.Watcher,
// command.Handler) are constructor-injected interfaces with no M0
// implementations; M1 wires the real ones (plan §5.3).
package controller

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/7K-Inari/inari-agent/internal/capability"
	"github.com/7K-Inari/inari-agent/internal/command"
	"github.com/7K-Inari/inari-agent/internal/registration"
	"github.com/7K-Inari/inari-agent/internal/stream"
)

const defaultReconcileInterval = 30 * time.Second

// AgentReconciler is the M0 no-op agent loop. Nil lifecycle dependencies are
// tolerated: they are supplied by M1 implementations.
type AgentReconciler struct {
	Log      logr.Logger
	Interval time.Duration

	Registrar registration.Registrar
	Stream    stream.Client
	Watchers  []capability.Watcher
	Handler   command.Handler
}

// NewAgentReconciler builds the no-op reconciler with zero-value lifecycle
// dependencies.
func NewAgentReconciler(_ manager.Manager) *AgentReconciler {
	return &AgentReconciler{
		Log:      ctrl.Log.WithName("agent"),
		Interval: defaultReconcileInterval,
	}
}

// Start ticks until ctx is cancelled. M0 behaviour: log and continue.
func (r *AgentReconciler) Start(ctx context.Context) error {
	interval := r.Interval
	if interval <= 0 {
		interval = defaultReconcileInterval
	}
	r.Log.Info("agent reconcile loop starting", "interval", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		r.reconcileOnce(ctx)
		select {
		case <-ctx.Done():
			r.Log.Info("agent reconcile loop stopping")
			return nil
		case <-ticker.C:
		}
	}
}

func (r *AgentReconciler) reconcileOnce(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	default:
	}
	r.Log.V(1).Info("reconcile tick",
		"registrarConfigured", r.Registrar != nil,
		"streamConfigured", r.Stream != nil,
		"watcherCount", len(r.Watchers),
		"handlerConfigured", r.Handler != nil,
	)
}

// SetupWithManager registers the reconciler as a manager Runnable.
func (r *AgentReconciler) SetupWithManager(mgr manager.Manager) error {
	return mgr.Add(r)
}

var _ manager.Runnable = (*AgentReconciler)(nil)
