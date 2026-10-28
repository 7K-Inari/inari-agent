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

func legacyPlatformWatches() []WatchResource {
	out := make([]WatchResource, 0, len(PlatformGVRs))
	for _, gvr := range PlatformGVRs {
		out = append(out, WatchResource{GVR: gvr})
	}
	return out
}

func absentCRDFake(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	listKinds := map[schema.GroupVersionResource]string{
		argocd.ApplicationGVR: "ApplicationList",
	}
	for _, gvr := range PlatformGVRs {
		listKinds[gvr] = gvr.Resource + "List"
	}
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, objs...)
}

// When the platform.inari.io CRDs are not installed (tenant clusters — only
// the platform cluster runs inari-operator) and no discovery filter is
// configured, platform lists fail with NotFound. Application and KRO status
// emission must continue: each shared informer delivers events once its own
// cache syncs, independently of the failing platform reflectors.
func TestAbsentPlatformCRDsDoNotBlockApplicationEmission(t *testing.T) {
	dyn := absentCRDFake(app("my-web", "Healthy", "Synced"))
	// Simulate a cluster without the CRDs: platform lists return NotFound.
	dyn.PrependReactor("list", "*", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetResource().Group == PlatformGroup {
			return true, nil, apierrors.NewNotFound(action.GetResource().GroupResource(), "")
		}
		return false, nil, nil
	})
	sender := &captureSender{mu: make(chan struct{}, 8)}
	s := NewStreamer(logr.Discard(), dyn, sender, "argocd", nil, legacyPlatformWatches())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	select {
	case <-sender.mu:
	case <-time.After(5 * time.Second):
		t.Fatal("no Application status emitted when platform CRDs are absent")
	}
	upd := lastStatus(t, sender.events[0])
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
	var platformLists atomic.Int32
	dyn.PrependReactor("list", "*", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetResource().Group == PlatformGroup {
			platformLists.Add(1)
		}
		return false, nil, nil
	})
	// Discovery serves only argoproj.io — the platform group is absent.
	disc := &discoveryfake.FakeDiscovery{Fake: &k8stesting.Fake{Resources: []*metav1.APIResourceList{
		{
			GroupVersion: "argoproj.io/v1alpha1",
			APIResources: []metav1.APIResource{{Name: "applications"}},
		},
	}}}
	sender := &captureSender{mu: make(chan struct{}, 8)}
	s := NewStreamer(logr.Discard(), dyn, sender, "argocd", nil, legacyPlatformWatches())
	s.Discovery = disc

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	select {
	case <-sender.mu:
	case <-time.After(5 * time.Second):
		t.Fatal("no Application status emitted with discovery filtering")
	}
	if got := platformLists.Load(); got != 0 {
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
	dyn := absentCRDFake()
	var platformLists atomic.Int32
	dyn.PrependReactor("list", "*", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetResource().Group == PlatformGroup {
			platformLists.Add(1)
			return true, nil, apierrors.NewNotFound(action.GetResource().GroupResource(), "")
		}
		return false, nil, nil
	})
	s := NewStreamer(logr.Discard(), dyn, &captureSender{}, "argocd", nil, legacyPlatformWatches())
	s.Discovery = errDiscovery{err: apierrors.NewServiceUnavailable("apiserver restarting")}

	if got := s.filteredWatches(); len(got) != len(PlatformGVRs) {
		t.Fatalf("fail-open must keep all watches, kept %d of %d", len(got), len(PlatformGVRs))
	}
}
