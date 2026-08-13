package controller

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
)

func TestAgentReconcilerStartReturnsOnCancel(t *testing.T) {
	r := &AgentReconciler{Log: logr.Discard(), Interval: 10 * time.Millisecond}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after context cancellation")
	}
}

func TestAgentReconcilerDefaultInterval(t *testing.T) {
	r := &AgentReconciler{Log: logr.Discard(), Interval: 0}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start with zero interval returned error: %v", err)
	}
}

func TestNewAgentReconcilerDefaults(t *testing.T) {
	r := NewAgentReconciler(nil)
	if r == nil {
		t.Fatal("NewAgentReconciler returned nil")
	}
	if r.Interval != defaultReconcileInterval {
		t.Errorf("Interval = %v, want %v", r.Interval, defaultReconcileInterval)
	}
	if r.Registrar != nil || r.Stream != nil || r.Handler != nil || len(r.Watchers) != 0 {
		t.Error("expected zero-value lifecycle dependencies")
	}
}

func TestSetupWithManager(t *testing.T) {
	r := &AgentReconciler{Log: logr.Discard(), Interval: time.Hour}

	mgr, err := ctrl.NewManager(&rest.Config{Host: "https://127.0.0.1:1"}, ctrl.Options{})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}
}
