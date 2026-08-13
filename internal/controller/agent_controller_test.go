package controller

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
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
