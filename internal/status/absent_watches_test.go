package status

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	discoveryfake "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/argocd"
)

// absentGroup is a guaranteed-uninstalled API group standing in for any
// optional watch target the cluster does not serve.
const absentGroup = "absent.example.io"

func absentWatches() []WatchResource {
	return []WatchResource{
		{GVR: schema.GroupVersionResource{Group: absentGroup, Version: "v1alpha1", Resource: "widgets"}},
		{GVR: schema.GroupVersionResource{Group: absentGroup, Version: "v1alpha1", Resource: "gadgets"}},
	}
}

func absentCRDFake(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	listKinds := map[schema.GroupVersionResource]string{
		argocd.ApplicationGVR: "ApplicationList",
	}
	for _, w := range absentWatches() {
		listKinds[w.GVR] = w.GVR.Resource + "List"
	}
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, objs...)
}

// When a configured watch's API is not installed (optional CRDs absent)
// and no discovery filter is configured, its lists fail with NotFound.
// Application and KRO status emission must continue: each shared informer
// delivers events once its own cache syncs, independently of the failing
// reflectors.
func TestAbsentWatchedAPIsDoNotBlockApplicationEmission(t *testing.T) {
	dyn := absentCRDFake(app("my-web", "Healthy", "Synced"))
	// Simulate a cluster without the watched APIs: lists return NotFound.
	dyn.PrependReactor("list", "*", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetResource().Group == absentGroup {
			return true, nil, apierrors.NewNotFound(action.GetResource().GroupResource(), "")
		}
		return false, nil, nil
	})
	sender := &captureSender{signal: make(chan struct{}, 8)}
	s := NewStreamer(logr.Discard(), dyn, sender, "argocd", nil, absentWatches())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	select {
	case <-sender.signal:
	case <-time.After(5 * time.Second):
		t.Fatal("no Application status emitted when watched APIs are absent")
	}
	upd := lastStatus(t, sender.at(0))
	if upd.Resource.Kind != "Application" || upd.Health != agentv1.HealthStatus_HEALTH_STATUS_HEALTHY {
		t.Fatalf("unexpected update %+v", upd)
	}
}

// With a discovery client configured, watches whose API the cluster does
// not serve are skipped up front — no reflector is ever started for them
// (zero list calls against the absent groups) and the surviving watches
// emit normally.
func TestDiscoveryFilterSkipsAbsentWatches(t *testing.T) {
	dyn := absentCRDFake(app("my-web", "Healthy", "Synced"))
	var absentLists atomic.Int32
	dyn.PrependReactor("list", "*", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetResource().Group == absentGroup {
			absentLists.Add(1)
		}
		return false, nil, nil
	})
	// Discovery serves only argoproj.io — the watched group is absent.
	disc := &discoveryfake.FakeDiscovery{Fake: &k8stesting.Fake{Resources: []*metav1.APIResourceList{
		{
			GroupVersion: "argoproj.io/v1alpha1",
			APIResources: []metav1.APIResource{{Name: "applications"}},
		},
	}}}
	sender := &captureSender{signal: make(chan struct{}, 8)}
	s := NewStreamer(logr.Discard(), dyn, sender, "argocd", nil, absentWatches())
	s.Discovery = disc

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	select {
	case <-sender.signal:
	case <-time.After(5 * time.Second):
		t.Fatal("no Application status emitted with discovery filtering")
	}
	if got := absentLists.Load(); got != 0 {
		t.Fatalf("skipped watch must never list the absent API, got %d list calls", got)
	}
}

// errDiscovery fails every call with a transient error.
type errDiscovery struct {
	discovery.DiscoveryInterface
	err error
}

func (d errDiscovery) ServerResourcesForGroupVersion(string) (*metav1.APIResourceList, error) {
	return nil, d.err
}

// Transient discovery errors fail open (same semantics as
// capability.FilterAvailable): the API may be served later, so the watch
// is kept and its reflector retries on its own.
func TestDiscoveryFilterFailsOpenOnError(t *testing.T) {
	s := NewStreamer(logr.Discard(), absentCRDFake(), &captureSender{}, "argocd", nil, absentWatches())
	s.Discovery = errDiscovery{err: apierrors.NewServiceUnavailable("apiserver restarting")}

	if got := s.filteredWatches(); len(got) != len(absentWatches()) {
		t.Fatalf("fail-open must keep all watches, kept %d of %d", len(got), len(absentWatches()))
	}
}
