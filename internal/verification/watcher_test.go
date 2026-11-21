package verification

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type watcherSender struct {
	mu     sync.Mutex
	events []*agentv1.Event
	err    error
}

func (s *watcherSender) Send(_ context.Context, event *agentv1.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.events = append(s.events, event)
	return nil
}
func (s *watcherSender) count() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.events) }

func watcherWorkflow() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "argoproj.io/v1alpha1", "kind": "Workflow",
		"metadata": map[string]any{
			"name": "vr-run-a1", "namespace": "verification", "uid": "wf-1", "resourceVersion": "1",
			"labels": map[string]any{LabelVerification: "true"},
			"annotations": map[string]any{
				AnnotationRunID: "vr-run", AnnotationAttempt: "1", AnnotationTriggerFingerprint: "sha256:" + strings.Repeat("a", 64),
				AnnotationSuiteDigest: "sha256:" + strings.Repeat("c", 64), AnnotationProfile: "prod",
				AnnotationRenderFingerprint: "sha256:" + strings.Repeat("b", 64), AnnotationInstanceUID: "2b6e4424-87a4-4f7c-b6d8-dfe2f6a894f8",
				AnnotationAppInstanceID: "app-1", AnnotationAppRevision: "sha256:" + strings.Repeat("d", 64),
				AnnotationAppDigest: "sha256:" + strings.Repeat("e", 64), AnnotationTargetID: "rt-1", AnnotationTargetRevision: "1",
				AnnotationPolicyRevision: "1", AnnotationGrantRevision: "1",
			},
		},
		"status": map[string]any{"phase": "Running", "startedAt": "2026-11-21T01:00:00Z"},
	}}
}

func TestWatcherDedupeResetAndSendFailure(t *testing.T) {
	sender := &watcherSender{}
	watcher := NewWatcher(logr.Discard(), nil, sender)
	obj := watcherWorkflow()
	watcher.emit(context.Background(), obj, false)
	watcher.emit(context.Background(), obj, false)
	if sender.count() != 1 {
		t.Fatalf("duplicate content sent %d events", sender.count())
	}
	watcher.Reset()
	watcher.emit(context.Background(), obj, false)
	if sender.count() != 2 {
		t.Fatalf("reset did not resend current state: %d", sender.count())
	}
	if sender.events[0].Type != agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_VERIFICATION_REPORT) {
		t.Fatalf("event type = %q", sender.events[0].Type)
	}
	watcher.emit(context.Background(), obj, true)
	var report agentv1.VerificationReport
	if err := sender.events[sender.count()-1].Payload.UnmarshalTo(&report); err != nil || !report.Deleted {
		t.Fatalf("delete report = %#v, %v", &report, err)
	}

	failed := &watcherSender{err: errors.New("disconnected")}
	retryWatcher := NewWatcher(logr.Discard(), nil, failed)
	retryWatcher.emit(context.Background(), obj, false)
	retryWatcher.emit(context.Background(), obj, false)
	failed.mu.Lock()
	failed.err = nil
	failed.mu.Unlock()
	retryWatcher.emit(context.Background(), obj, false)
	if failed.count() != 1 {
		t.Fatalf("failed Send incorrectly advanced dedupe: %d", failed.count())
	}
}

func TestWatcherRunStopsOnCancel(t *testing.T) {
	watcher := NewWatcher(logr.Discard(), nil, &watcherSender{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- watcher.Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("watcher did not stop")
	}
}
