package status

import (
	"context"
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

func newDyn(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	listKinds := map[schema.GroupVersionResource]string{
		argocd.ApplicationGVR: "ApplicationList",
		webServiceGVR:         "WebServiceList",
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
