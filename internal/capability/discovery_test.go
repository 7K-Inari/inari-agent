package capability

import (
	"context"
	"errors"
	"testing"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	discoveryfake "k8s.io/client-go/discovery/fake"
	clienttesting "k8s.io/client-go/testing"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

type stubGVRWatcher struct {
	source Source
	gvr    schema.GroupVersionResource
}

func (w stubGVRWatcher) Source() Source                   { return w.source }
func (w stubGVRWatcher) GVR() schema.GroupVersionResource { return w.gvr }
func (w stubGVRWatcher) Start(ctx context.Context) (<-chan *agentv1.Capability, error) {
	return nil, nil
}

func discoveryWith(resources ...*metav1.APIResourceList) *discoveryfake.FakeDiscovery {
	return &discoveryfake.FakeDiscovery{Fake: &clienttesting.Fake{Resources: resources}}
}

func TestFilterAvailableSkipsAbsentAPI(t *testing.T) {
	disc := discoveryWith(&metav1.APIResourceList{
		GroupVersion: "apiextensions.k8s.io/v1",
		APIResources: []metav1.APIResource{{Name: "customresourcedefinitions"}},
	})
	watchers := []Watcher{
		stubGVRWatcher{source: SourceCRD, gvr: crdGVR},
		stubGVRWatcher{source: SourceOLM, gvr: csvGVR},
		stubGVRWatcher{source: SourceKRORGD, gvr: kroRGDGVR},
		&MetadataWatcher{}, // no GVR: never filtered
	}
	got := FilterAvailable(disc, logr.Discard(), watchers)
	if len(got) != 2 {
		t.Fatalf("expected 2 watchers kept, got %d", len(got))
	}
	if got[0].Source() != SourceCRD || got[1].Source() != SourceClusterMetadata {
		t.Errorf("kept sources = %s, %s", got[0].Source(), got[1].Source())
	}
}

func TestFilterAvailableKeepsPresentAPI(t *testing.T) {
	disc := discoveryWith(&metav1.APIResourceList{
		GroupVersion: "operators.coreos.com/v1alpha1",
		APIResources: []metav1.APIResource{{Name: "clusterserviceversions"}},
	})
	got := FilterAvailable(disc, logr.Discard(), []Watcher{stubGVRWatcher{source: SourceOLM, gvr: csvGVR}})
	if len(got) != 1 {
		t.Fatalf("expected OLM watcher kept, got %d", len(got))
	}
}

func TestFilterAvailableCachesPerGroupVersion(t *testing.T) {
	// Both Crossplane XRD watchers share one group/version: a single
	// discovery entry must cover both.
	disc := discoveryWith(&metav1.APIResourceList{
		GroupVersion: "apiextensions.crossplane.io/v1",
		APIResources: []metav1.APIResource{
			{Name: "compositedresourcedefinitions"},
			{Name: "compositions"},
		},
	})
	watchers := []Watcher{
		stubGVRWatcher{source: SourceCrossplaneXRD, gvr: xrdGVR},
		stubGVRWatcher{source: SourceCrossplaneXRD, gvr: compositionGVR},
	}
	if got := FilterAvailable(disc, logr.Discard(), watchers); len(got) != 2 {
		t.Fatalf("expected both crossplane watchers kept, got %d", len(got))
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

func TestFilterAvailableFailsOpenOnDiscoveryError(t *testing.T) {
	disc := errDiscovery{err: errors.New("api server unreachable")}
	got := FilterAvailable(disc, logr.Discard(), []Watcher{stubGVRWatcher{source: SourceOLM, gvr: csvGVR}})
	if len(got) != 1 {
		t.Fatalf("transient discovery errors must keep the watcher, got %d", len(got))
	}
}

func TestResourceServedNotFound(t *testing.T) {
	disc := discoveryWith()
	if resourceServed(disc, csvGVR) {
		t.Error("absent group/version must report not served")
	}
	if !apierrors.IsNotFound(func() error {
		_, err := disc.ServerResourcesForGroupVersion(csvGVR.GroupVersion().String())
		return err
	}()) {
		t.Error("fake discovery must surface NotFound for absent group/version")
	}
}
