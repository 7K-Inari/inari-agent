package capability

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

func TestCRDWatcherFetchFullDeleteEmitsNameOnly(t *testing.T) {
	crd := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]interface{}{"name": "widgets.example.com"},
		"spec": map[string]interface{}{
			"group": "example.com",
			"versions": []interface{}{
				map[string]interface{}{"name": "v1", "served": true, "storage": true},
			},
		},
	}}
	dyn := fakeDynamic(t, map[schema.GroupVersionResource]string{crdGVR: "CustomResourceDefinitionList"}, crd)
	meta := fakeMetadata(t, crd)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := NewCRDWatcher(dyn, meta)
	ch, err := w.Start(ctx)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if cap := recvOne(t, ch); cap.Action != agentv1.CapabilityAction_CAPABILITY_ACTION_UPSERT {
		t.Fatalf("expected initial UPSERT, got %v", cap.Action)
	}
}

func TestMetadataWatcherShutdownClosesChannel(t *testing.T) {
	secret := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]interface{}{
			"name":      "sh.helm.release.v1.nginx.v3",
			"namespace": "apps",
			"labels":    map[string]interface{}{"owner": "helm", "name": "nginx", "version": "3"},
		},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	w := NewHelmReleaseWatcher(fakeMetadata(t, secret), "")
	ch, err := w.Start(ctx)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	cancel()
	select {
	case _, ok := <-ch:
		for ok {
			_, ok = <-ch
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watcher channel did not close after context cancel")
	}
}

func TestAggregatorDeletesByKindAndNameWhenGroupEmpty(t *testing.T) {
	a := NewAggregator("t", "c", nil, nil)
	a.apply(&agentv1.Capability{
		Kind:    agentv1.CapabilityKind_CAPABILITY_KIND_CRD,
		Name:    "widgets.example.com",
		Group:   "example.com",
		Version: "v1",
		Action:  agentv1.CapabilityAction_CAPABILITY_ACTION_UPSERT,
	})
	a.apply(&agentv1.Capability{
		Kind:   agentv1.CapabilityKind_CAPABILITY_KIND_CRD,
		Name:   "widgets.example.com",
		Action: agentv1.CapabilityAction_CAPABILITY_ACTION_DELETE,
	})
	if got := a.Checksum(); got != StateChecksum(nil) {
		t.Errorf("snapshot not empty after name-only delete (checksum %s)", got)
	}
}
