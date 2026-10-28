package status

import (
	"context"
	"fmt"
	"strings"
	"sync"
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

// captureSender records sent events. Send is called from multiple
// informer processor goroutines concurrently, so events is mutex-guarded;
// signal notifies waiters non-blockingly.
type captureSender struct {
	signal chan struct{}

	mu     sync.Mutex
	events []*agentv1.Event
}

func (c *captureSender) Send(_ context.Context, ev *agentv1.Event) error {
	c.mu.Lock()
	c.events = append(c.events, ev)
	c.mu.Unlock()
	select {
	case c.signal <- struct{}{}:
	default:
	}
	return nil
}

func (c *captureSender) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.events)
}

func (c *captureSender) at(i int) *agentv1.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.events[i]
}

func (c *captureSender) snapshot() []*agentv1.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*agentv1.Event(nil), c.events...)
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

func newDyn(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	listKinds := map[schema.GroupVersionResource]string{
		argocd.ApplicationGVR: "ApplicationList",
		webServiceGVR:         "WebServiceList",
		clusterIssuerGVR:      "ClusterIssuerList",
		dnsEndpointGVR:        "DNSEndpointList",
	}
	// The fake client only needs a unique registered list kind per GVR.
	for _, w := range DefaultWatchResources() {
		listKinds[w.GVR] = w.GVR.Resource + "List"
	}
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, objs...)
}

func startStreamer(t *testing.T, dyn *dynamicfake.FakeDynamicClient, sender *captureSender) context.CancelFunc {
	t.Helper()
	s := NewStreamer(logr.Discard(), dyn, sender, "argocd", []schema.GroupVersionResource{webServiceGVR}, DefaultWatchResources())
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
	sender := &captureSender{signal: make(chan struct{}, 8)}
	dyn := newDyn(app("my-web", "Healthy", "Synced"))
	startStreamer(t, dyn, sender)

	select {
	case <-sender.signal:
	case <-time.After(5 * time.Second):
		t.Fatal("no status-update emitted")
	}
	upd := lastStatus(t, sender.at(0))
	if upd.Health != agentv1.HealthStatus_HEALTH_STATUS_HEALTHY || upd.Sync != agentv1.SyncState_SYNC_STATE_SYNCED {
		t.Fatalf("update %+v", upd)
	}
	if upd.Resource.Name != "my-web" || upd.Resource.Kind != "Application" || upd.Resource.Uid != "uid-my-web" {
		t.Fatalf("ref %+v", upd.Resource)
	}
}

func TestStreamerDedupesAndEmitsOnChange(t *testing.T) {
	sender := &captureSender{signal: make(chan struct{}, 8)}
	dyn := newDyn(app("my-web", "Healthy", "Synced"))
	startStreamer(t, dyn, sender)
	<-sender.signal

	// Health change triggers a new event.
	changed := app("my-web", "Degraded", "OutOfSync")
	if _, err := dyn.Resource(argocd.ApplicationGVR).Namespace("argocd").UpdateStatus(
		context.Background(), changed, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-sender.signal:
			last := lastStatus(t, sender.at(sender.count()-1))
			if last.Health == agentv1.HealthStatus_HEALTH_STATUS_DEGRADED {
				if sender.count() != 2 {
					t.Fatalf("expected exactly 2 events (dedupe), got %d", sender.count())
				}
				return
			}
		case <-deadline:
			t.Fatalf("no degraded event; events=%d", sender.count())
		}
	}
}

func TestStreamerKROInstanceConditions(t *testing.T) {
	sender := &captureSender{signal: make(chan struct{}, 8)}
	dyn := newDyn(kroInstance("my-web", "True"))
	startStreamer(t, dyn, sender)

	select {
	case <-sender.signal:
	case <-time.After(5 * time.Second):
		t.Fatal("no event")
	}
	upd := lastStatus(t, sender.at(0))
	if upd.Health != agentv1.HealthStatus_HEALTH_STATUS_HEALTHY || upd.Sync != agentv1.SyncState_SYNC_STATE_SYNCED {
		t.Fatalf("kro instance %+v", upd)
	}
}

func TestStreamerPlatformFailedCondition(t *testing.T) {
	sender := &captureSender{signal: make(chan struct{}, 8)}
	dyn := newDyn(keycloakRealm("acme", map[string]interface{}{"type": "Failed", "status": "True", "message": "realm error"}))
	startStreamer(t, dyn, sender)

	select {
	case <-sender.signal:
	case <-time.After(5 * time.Second):
		t.Fatal("no event")
	}
	upd := lastStatus(t, sender.at(0))
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
	sender := &captureSender{signal: make(chan struct{}, 8)}
	s := NewStreamer(logr.Discard(), newDyn(), sender, "argocd", nil, nil)
	obj := keycloakRealm("acme", map[string]interface{}{"type": "Ready", "status": "True"})

	s.emit(context.Background(), obj, false)
	s.emitDelete(context.Background(), obj, false)
	if sender.count() != 2 {
		t.Fatalf("expected create + delete events, got %d", sender.count())
	}
	upd := lastStatus(t, sender.at(1))
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
	sender := &failOnceSender{inner: &captureSender{signal: make(chan struct{}, 8)}}
	s := NewStreamer(logr.Discard(), newDyn(), sender, "argocd", nil, nil)
	obj := app("my-web", "Healthy", "Synced")

	s.emit(context.Background(), obj, false)
	if sender.inner.count() != 0 {
		t.Fatalf("failed send must not be recorded as emitted, got %d events", sender.inner.count())
	}
	s.emit(context.Background(), obj, false)
	if sender.inner.count() != 1 {
		t.Fatalf("expected retry to emit, got %d events", sender.inner.count())
	}
}

func TestEmitDeleteStreamsMissingAndPrunes(t *testing.T) {
	sender := &captureSender{signal: make(chan struct{}, 8)}
	s := NewStreamer(logr.Discard(), newDyn(), sender, "argocd", nil, nil)
	obj := app("my-web", "Healthy", "Synced")

	s.emit(context.Background(), obj, false)
	s.emitDelete(context.Background(), obj, false)
	if sender.count() != 2 {
		t.Fatalf("expected create + delete events, got %d", sender.count())
	}
	upd := lastStatus(t, sender.at(1))
	if upd.Health != agentv1.HealthStatus_HEALTH_STATUS_MISSING {
		t.Fatalf("delete update %+v", upd)
	}
	// Dedupe state pruned: re-adding the same content re-emits.
	s.emit(context.Background(), obj, false)
	if sender.count() != 3 {
		t.Fatalf("expected re-add to re-emit, got %d events", sender.count())
	}
}

func appWithCondition(name, health, sync, condType, condMsg string) *unstructured.Unstructured {
	u := app(name, health, sync)
	u.Object["status"].(map[string]interface{})["conditions"] = []interface{}{
		map[string]interface{}{"type": condType, "message": condMsg},
	}
	return u
}

func TestComparisonErrorSurfacedInMessage(t *testing.T) {
	// ArgoCD render failures (missing CRD, unreadable repo) live in
	// .status.conditions, not health/sync: the message must carry them so the
	// control plane shows why delivery is stuck.
	upd, _ := mapStatus(appWithCondition("broken-app", "Healthy", "Unknown",
		"ComparisonError", "failed to list refs: repository not found"), "Application")
	if upd.Health != agentv1.HealthStatus_HEALTH_STATUS_HEALTHY {
		t.Fatalf("health = %v", upd.Health)
	}
	if upd.Sync != agentv1.SyncState_SYNC_STATE_ERROR {
		t.Fatalf("sync = %v", upd.Sync)
	}
	if upd.Message == "" || !contains(upd.Message, "repository not found") {
		t.Fatalf("message = %q", upd.Message)
	}
	// Clean apps: no condition noise.
	upd2, _ := mapStatus(app("clean-app", "Healthy", "Synced"), "Application")
	if upd2.Message != "all good" {
		t.Fatalf("message = %q", upd2.Message)
	}
}

func TestResetReEmitsSnapshot(t *testing.T) {
	sender := &captureSender{signal: make(chan struct{}, 16)}
	dyn := newDyn(app("my-web", "Healthy", "Synced"))
	s := NewStreamer(logr.Discard(), dyn, sender, "argocd", nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()
	defer func() {
		cancel()
		<-errCh
	}()

	waitEvents := func(n int) {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for sender.count() < n {
			select {
			case <-sender.signal:
			case <-deadline:
				t.Fatalf("want %d events, got %d", n, sender.count())
			}
		}
	}
	waitEvents(1) // initial snapshot

	// Content-unchanged re-emit is deduped...
	s.emit(ctx, app("my-web", "Healthy", "Synced"), false)
	if sender.count() != 1 {
		t.Fatalf("dedupe broken: %d events", sender.count())
	}
	// ...but after Reset (stream reconnect) the same object re-emits.
	s.Reset()
	s.emit(ctx, app("my-web", "Healthy", "Synced"), false)
	if sender.count() != 2 {
		t.Fatalf("no re-emit after Reset: %d events", sender.count())
	}
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }

var clusterIssuerGVR = schema.GroupVersionResource{Group: "cert-manager.io", Version: "v1", Resource: "clusterissuers"}
var dnsEndpointGVR = schema.GroupVersionResource{Group: "externaldns.k8s.io", Version: "v1alpha1", Resource: "dnsendpoints"}

func clusterIssuer(name string, ready string, reason string) *unstructured.Unstructured {
	cond := map[string]interface{}{"type": "Ready", "status": ready}
	if reason != "" {
		cond["reason"] = reason
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "cert-manager.io/v1",
		"kind":       "ClusterIssuer",
		"metadata":   map[string]interface{}{"name": name, "uid": "uid-" + name},
		"status":     map[string]interface{}{"conditions": []interface{}{cond}},
	}}
}

// Native replacement resources from configured watches emit the
// group-qualified kind "<Kind>.<group>" (decision D-A) so the server can
// route them; condition mapping reuses the generic Ready fallback.
func TestStreamerNativeClusterIssuerQualifiedKind(t *testing.T) {
	sender := &captureSender{signal: make(chan struct{}, 8)}
	dyn := newDyn(clusterIssuer("letsencrypt-prod", "True", ""))
	s := NewStreamer(logr.Discard(), dyn, sender, "argocd", nil,
		[]WatchResource{{GVR: clusterIssuerGVR}})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-errCh })

	select {
	case <-sender.signal:
	case <-time.After(5 * time.Second):
		t.Fatal("no event")
	}
	upd := lastStatus(t, sender.at(0))
	if upd.Resource.Kind != "ClusterIssuer.cert-manager.io" {
		t.Fatalf("kind %q", upd.Resource.Kind)
	}
	if upd.Health != agentv1.HealthStatus_HEALTH_STATUS_HEALTHY || upd.Sync != agentv1.SyncState_SYNC_STATE_SYNCED {
		t.Fatalf("update %+v", upd)
	}
	if upd.Resource.Name != "letsencrypt-prod" || upd.Resource.Namespace != "" {
		t.Fatalf("ref %+v", upd.Resource)
	}
}

func TestStreamerClusterIssuerNotReadyProgressing(t *testing.T) {
	upd, _ := mapStatus(clusterIssuer("staging", "False", "ACMEAccountRegistrationFailed"), "ClusterIssuer.cert-manager.io")
	if upd.Health != agentv1.HealthStatus_HEALTH_STATUS_PROGRESSING {
		t.Fatalf("health = %v", upd.Health)
	}
}

// Delete events for watched native resources carry the qualified kind too.
func TestEmitDeleteNativeQualifiedKind(t *testing.T) {
	sender := &captureSender{signal: make(chan struct{}, 8)}
	s := NewStreamer(logr.Discard(), newDyn(), sender, "argocd", nil,
		[]WatchResource{{GVR: clusterIssuerGVR}})
	obj := clusterIssuer("letsencrypt-prod", "True", "")

	s.emit(context.Background(), obj, true)
	s.emitDelete(context.Background(), obj, true)
	if sender.count() != 2 {
		t.Fatalf("expected create + delete events, got %d", sender.count())
	}
	upd := lastStatus(t, sender.at(1))
	if upd.Health != agentv1.HealthStatus_HEALTH_STATUS_MISSING {
		t.Fatalf("delete update %+v", upd)
	}
	if upd.Resource.Kind != "ClusterIssuer.cert-manager.io" {
		t.Fatalf("delete kind %q", upd.Resource.Kind)
	}
}

// KRO instance GVRs (InstanceGVRs) keep bare kinds: the server matches
// them unqualified today, and qualifying would break that contract
// (risk #2).
func TestKROInstanceBareKindPreserved(t *testing.T) {
	sender := &captureSender{signal: make(chan struct{}, 8)}
	s := NewStreamer(logr.Discard(), newDyn(), sender, "argocd",
		[]schema.GroupVersionResource{webServiceGVR}, DefaultWatchResources())
	s.emit(context.Background(), kroInstance("my-web", "True"), false)
	if sender.count() != 1 {
		t.Fatalf("expected 1 event, got %d", sender.count())
	}
	upd := lastStatus(t, sender.at(0))
	if upd.Resource.Kind != "WebService" {
		t.Fatalf("KRO instance kind must stay bare, got %q", upd.Resource.Kind)
	}
}

// A GVR present in both InstanceGVRs and Watches must not double-register
// a handler on the shared informer — that would emit the same object
// twice with conflicting kinds (bare and group-qualified).
func TestDuplicateGVRAcrossInstanceGVRsAndWatches(t *testing.T) {
	sender := &captureSender{signal: make(chan struct{}, 8)}
	dyn := newDyn(kroInstance("my-web", "True"))
	s := NewStreamer(logr.Discard(), dyn, sender, "argocd",
		[]schema.GroupVersionResource{webServiceGVR},
		[]WatchResource{{GVR: webServiceGVR}})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-errCh })

	select {
	case <-sender.signal:
	case <-time.After(5 * time.Second):
		t.Fatal("no event")
	}
	if got := sender.count(); got != 1 {
		t.Fatalf("duplicate GVR must emit exactly one event, got %d", got)
	}
	if upd := lastStatus(t, sender.at(0)); upd.Resource.Kind != "WebService" {
		t.Fatalf("InstanceGVRs entry wins with bare kind, got %q", upd.Resource.Kind)
	}
}

func dnsEndpointObj(name, ns string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "externaldns.k8s.io/v1alpha1",
		"kind":       "DNSEndpoint",
		"metadata":   map[string]interface{}{"name": name, "namespace": ns, "uid": "uid-" + name},
		"spec":       map[string]interface{}{"endpoints": []interface{}{}},
	}}
}

// DNSEndpoint (and AppProject) carry no standard status conditions:
// they must still stream an existence signal with UNKNOWN health so the
// inventory sees the resource (and its later deletion), rather than
// silently skipping it.
func TestStatuslessNativeResourceEmitsUnknown(t *testing.T) {
	sender := &captureSender{signal: make(chan struct{}, 8)}
	s := NewStreamer(logr.Discard(), newDyn(), sender, "argocd", nil,
		[]WatchResource{{GVR: dnsEndpointGVR}})
	s.emit(context.Background(), dnsEndpointObj("acme-zone", "tenant-acme"), true)
	if sender.count() != 1 {
		t.Fatalf("statusless resource must still emit existence, got %d events", sender.count())
	}
	upd := lastStatus(t, sender.at(0))
	if upd.Resource.Kind != "DNSEndpoint.externaldns.k8s.io" {
		t.Fatalf("kind %q", upd.Resource.Kind)
	}
	if upd.Health != agentv1.HealthStatus_HEALTH_STATUS_UNKNOWN ||
		upd.Sync != agentv1.SyncState_SYNC_STATE_UNSPECIFIED {
		t.Fatalf("update %+v", upd)
	}
}
