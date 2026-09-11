package status

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/argocd"
)

type captureSender struct {
	mu     chan struct{}
	events []*agentv1.Event
}

func (c *captureSender) Send(_ context.Context, ev *agentv1.Event) error {
	c.events = append(c.events, ev)
	select {
	case c.mu <- struct{}{}:
	default:
	}
	return nil
}

func app(name string, health, sync string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "argoproj.io/v1alpha1",
		"kind":       "Application",
		"metadata":   map[string]interface{}{"name": name, "namespace": "argocd", "uid": "uid-" + name},
		"status": map[string]interface{}{
			"health": map[string]interface{}{"status": health, "message": "all good"},
			"sync":   map[string]interface{}{"status": sync},
		},
	}}
}

func kroInstance(name string, ready string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "inari.dev/v1alpha1",
		"kind":       "WebService",
		"metadata":   map[string]interface{}{"name": name, "namespace": "apps", "uid": "uid-" + name},
		"status": map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": ready},
			},
		},
	}}
}

var webServiceGVR = schema.GroupVersionResource{Group: "inari.dev", Version: "v1alpha1", Resource: "webservices"}

func keycloakRealm(name string, conditions ...interface{}) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "platform.inari.io/v1alpha1",
		"kind":       "KeycloakRealm",
		"metadata":   map[string]interface{}{"name": name, "namespace": "tenant-acme", "uid": "uid-" + name},
		"status":     map[string]interface{}{"conditions": conditions},
	}}
}

var keycloakRealmGVR = schema.GroupVersionResource{Group: "platform.inari.io", Version: "v1alpha1", Resource: "keycloakrealms"}

func newDyn(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	listKinds := map[schema.GroupVersionResource]string{
		argocd.ApplicationGVR: "ApplicationList",
		webServiceGVR:         "WebServiceList",
	}
	// The fake client only needs a unique registered list kind per GVR.
	for _, gvr := range PlatformGVRs {
		listKinds[gvr] = gvr.Resource + "List"
	}
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, objs...)
}

func startStreamer(t *testing.T, dyn *dynamicfake.FakeDynamicClient, sender *captureSender) context.CancelFunc {
	t.Helper()
	s := NewStreamer(logr.Discard(), dyn, sender, "argocd", []schema.GroupVersionResource{webServiceGVR})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-errCh; err != nil {
			t.Errorf("run: %v", err)
		}
	})
	return cancel
}

func lastStatus(t *testing.T, ev *agentv1.Event) *agentv1.StatusUpdate {
	t.Helper()
	if ev.Type != agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_STATUS_UPDATE) {
		t.Fatalf("event type %q", ev.Type)
	}
	var upd agentv1.StatusUpdate
	if err := ev.Payload.UnmarshalTo(&upd); err != nil {
		t.Fatal(err)
	}
	return &upd
}

func TestStreamerEmitsApplicationStatus(t *testing.T) {
	sender := &captureSender{mu: make(chan struct{}, 8)}
	dyn := newDyn(app("my-web", "Healthy", "Synced"))
	startStreamer(t, dyn, sender)

	select {
	case <-sender.mu:
	case <-time.After(5 * time.Second):
		t.Fatal("no status-update emitted")
	}
	upd := lastStatus(t, sender.events[0])
	if upd.Health != agentv1.HealthStatus_HEALTH_STATUS_HEALTHY || upd.Sync != agentv1.SyncState_SYNC_STATE_SYNCED {
		t.Fatalf("update %+v", upd)
	}
	if upd.Resource.Name != "my-web" || upd.Resource.Kind != "Application" || upd.Resource.Uid != "uid-my-web" {
		t.Fatalf("ref %+v", upd.Resource)
	}
}

func TestStreamerDedupesAndEmitsOnChange(t *testing.T) {
	sender := &captureSender{mu: make(chan struct{}, 8)}
	dyn := newDyn(app("my-web", "Healthy", "Synced"))
	startStreamer(t, dyn, sender)
	<-sender.mu

	// Health change triggers a new event.
	changed := app("my-web", "Degraded", "OutOfSync")
	if _, err := dyn.Resource(argocd.ApplicationGVR).Namespace("argocd").UpdateStatus(
		context.Background(), changed, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-sender.mu:
			last := lastStatus(t, sender.events[len(sender.events)-1])
			if last.Health == agentv1.HealthStatus_HEALTH_STATUS_DEGRADED {
				if len(sender.events) != 2 {
					t.Fatalf("expected exactly 2 events (dedupe), got %d", len(sender.events))
				}
				return
			}
		case <-deadline:
			t.Fatalf("no degraded event; events=%d", len(sender.events))
		}
	}
}

func TestStreamerKROInstanceConditions(t *testing.T) {
	sender := &captureSender{mu: make(chan struct{}, 8)}
	dyn := newDyn(kroInstance("my-web", "True"))
	startStreamer(t, dyn, sender)

	select {
	case <-sender.mu:
	case <-time.After(5 * time.Second):
		t.Fatal("no event")
	}
	upd := lastStatus(t, sender.events[0])
	if upd.Health != agentv1.HealthStatus_HEALTH_STATUS_HEALTHY || upd.Sync != agentv1.SyncState_SYNC_STATE_SYNCED {
		t.Fatalf("kro instance %+v", upd)
	}
}

func TestStreamerPlatformFailedCondition(t *testing.T) {
	sender := &captureSender{mu: make(chan struct{}, 8)}
	dyn := newDyn(keycloakRealm("acme", map[string]interface{}{"type": "Failed", "status": "True", "message": "realm error"}))
	startStreamer(t, dyn, sender)

	select {
	case <-sender.mu:
	case <-time.After(5 * time.Second):
		t.Fatal("no event")
	}
	upd := lastStatus(t, sender.events[0])
	if upd.Health != agentv1.HealthStatus_HEALTH_STATUS_DEGRADED {
		t.Fatalf("platform update %+v", upd)
	}
	if upd.Resource.Kind != "KeycloakRealm.platform.inari.io" {
		t.Fatalf("kind %q", upd.Resource.Kind)
	}
	if upd.Resource.Name != "acme" || upd.Resource.Namespace != "tenant-acme" || upd.Resource.Uid != "uid-acme" {
		t.Fatalf("ref %+v", upd.Resource)
	}
	if upd.Message != "realm error" {
		t.Fatalf("message %q", upd.Message)
	}
}

func TestEmitDeletePlatformKindSuffix(t *testing.T) {
	sender := &captureSender{mu: make(chan struct{}, 8)}
	s := NewStreamer(logr.Discard(), newDyn(), sender, "argocd", nil)
	obj := keycloakRealm("acme", map[string]interface{}{"type": "Ready", "status": "True"})

	s.emit(context.Background(), obj)
	s.emitDelete(context.Background(), obj)
	if len(sender.events) != 2 {
		t.Fatalf("expected create + delete events, got %d", len(sender.events))
	}
	upd := lastStatus(t, sender.events[1])
	if upd.Health != agentv1.HealthStatus_HEALTH_STATUS_MISSING {
		t.Fatalf("delete update %+v", upd)
	}
	if upd.Resource.Kind != "KeycloakRealm.platform.inari.io" {
		t.Fatalf("delete kind %q", upd.Resource.Kind)
	}
}

type failOnceSender struct {
	failed bool
	inner  *captureSender
}

func (f *failOnceSender) Send(ctx context.Context, ev *agentv1.Event) error {
	if !f.failed {
		f.failed = true
		return fmt.Errorf("transient send failure")
	}
	return f.inner.Send(ctx, ev)
}

func TestEmitRetriesAfterSendFailure(t *testing.T) {
	sender := &failOnceSender{inner: &captureSender{mu: make(chan struct{}, 8)}}
	s := NewStreamer(logr.Discard(), newDyn(), sender, "argocd", nil)
	obj := app("my-web", "Healthy", "Synced")

	s.emit(context.Background(), obj)
	if len(sender.inner.events) != 0 {
		t.Fatalf("failed send must not be recorded as emitted, got %d events", len(sender.inner.events))
	}
	s.emit(context.Background(), obj)
	if len(sender.inner.events) != 1 {
		t.Fatalf("expected retry to emit, got %d events", len(sender.inner.events))
	}
}

func TestEmitDeleteStreamsMissingAndPrunes(t *testing.T) {
	sender := &captureSender{mu: make(chan struct{}, 8)}
	s := NewStreamer(logr.Discard(), newDyn(), sender, "argocd", nil)
	obj := app("my-web", "Healthy", "Synced")

	s.emit(context.Background(), obj)
	s.emitDelete(context.Background(), obj)
	if len(sender.events) != 2 {
		t.Fatalf("expected create + delete events, got %d", len(sender.events))
	}
	upd := lastStatus(t, sender.events[1])
	if upd.Health != agentv1.HealthStatus_HEALTH_STATUS_MISSING {
		t.Fatalf("delete update %+v", upd)
	}
	// Dedupe state pruned: re-adding the same content re-emits.
	s.emit(context.Background(), obj)
	if len(sender.events) != 3 {
		t.Fatalf("expected re-add to re-emit, got %d events", len(sender.events))
	}
}
