package capability

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/metadata"
	metadatafake "k8s.io/client-go/metadata/fake"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

func fakeDynamic(t *testing.T, listKinds map[schema.GroupVersionResource]string, objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	t.Helper()
	scheme := runtime.NewScheme()
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, objs...)
}

func fakeMetadata(t *testing.T, objs ...runtime.Object) metadata.Interface {
	t.Helper()
	// The metadata fake serves PartialObjectMetadata lists; convert
	// unstructured fixtures to their metadata projection.
	scheme := runtime.NewScheme()
	pms := make([]runtime.Object, 0, len(objs))
	for _, o := range objs {
		u, ok := o.(*unstructured.Unstructured)
		if !ok {
			t.Fatalf("fakeMetadata: unsupported object %T", o)
		}
		gvk := u.GroupVersionKind()
		scheme.AddKnownTypeWithName(gvk, &metav1.PartialObjectMetadata{})
		scheme.AddKnownTypeWithName(gvk.GroupVersion().WithKind(gvk.Kind+"List"), &metav1.PartialObjectMetadataList{})
		pm := &metav1.PartialObjectMetadata{
			TypeMeta: metav1.TypeMeta{APIVersion: u.GetAPIVersion(), Kind: u.GetKind()},
		}
		pm.SetName(u.GetName())
		pm.SetNamespace(u.GetNamespace())
		pm.SetLabels(u.GetLabels())
		pm.SetAnnotations(u.GetAnnotations())
		pms = append(pms, pm)
	}
	return metadatafake.NewSimpleMetadataClient(scheme, pms...)
}

func recvOne(t *testing.T, ch <-chan *agentv1.Capability) *agentv1.Capability {
	t.Helper()
	select {
	case c := <-ch:
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for capability")
		return nil
	}
}

func TestCRDWatcherEmitsSchemaWithCELValidations(t *testing.T) {
	crd := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]interface{}{"name": "widgets.example.com"},
		"spec": map[string]interface{}{
			"group": "example.com",
			"versions": []interface{}{
				map[string]interface{}{
					"name": "v1", "served": true, "storage": true,
					"schema": map[string]interface{}{
						"openAPIV3Schema": map[string]interface{}{
							"type": "object",
							"properties": map[string]interface{}{
								"spec": map[string]interface{}{
									"type": "object",
									"x-kubernetes-validations": []interface{}{
										map[string]interface{}{"rule": "self.replicas <= 10", "message": "too many"},
									},
								},
							},
						},
					},
				},
			},
		},
	}}
	client := fakeDynamic(t, map[schema.GroupVersionResource]string{crdGVR: "CustomResourceDefinitionList"}, crd)

	w := NewCRDWatcher(client, fakeMetadata(t, crd))
	ch, err := w.Start(context.Background())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	cap := recvOne(t, ch)
	if cap.Kind != agentv1.CapabilityKind_CAPABILITY_KIND_CRD {
		t.Errorf("kind = %v", cap.Kind)
	}
	if cap.Name != "widgets.example.com" || cap.Group != "example.com" || cap.Version != "v1" {
		t.Errorf("identity = %s/%s/%s", cap.Group, cap.Name, cap.Version)
	}
	if cap.Action != agentv1.CapabilityAction_CAPABILITY_ACTION_UPSERT {
		t.Errorf("action = %v", cap.Action)
	}
	if cap.ManagementMode != agentv1.ManagementMode_MANAGEMENT_MODE_OBSERVE_ONLY {
		t.Errorf("pre-existing CRD must default to observe-only, got %v", cap.ManagementMode)
	}
	validations := cap.Schema.GetFields()["properties"].GetStructValue().
		GetFields()["spec"].GetStructValue().
		GetFields()["x-kubernetes-validations"].GetListValue()
	if len(validations.GetValues()) != 1 {
		t.Errorf("CEL validations not preserved in schema: %v", cap.Schema)
	}
}

func TestHelmWatcherReadsReleaseLabelsOnly(t *testing.T) {
	secret := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]interface{}{
			"name":      "sh.helm.release.v1.nginx.v3",
			"namespace": "apps",
			"labels": map[string]interface{}{
				"owner": "helm", "name": "nginx", "version": "3", "status": "deployed",
			},
		},
		"data": map[string]interface{}{"release": "aGVscG1lZA=="},
	}}
	other := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata":   map[string]interface{}{"name": "plain", "namespace": "apps"},
	}}
	w := NewHelmReleaseWatcher(fakeMetadata(t, secret, other), "")
	ch, err := w.Start(context.Background())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	cap := recvOne(t, ch)
	if cap.Name != "apps/nginx" {
		t.Errorf("name = %q", cap.Name)
	}
	if cap.Version != "3" {
		t.Errorf("version = %q", cap.Version)
	}
	if cap.UiHints.GetFields()["status"].GetStringValue() != "deployed" {
		t.Errorf("status hint = %v", cap.UiHints)
	}
}

func TestCrossplaneProviderWatcher(t *testing.T) {
	prov := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "pkg.crossplane.io/v1",
		"kind":       "Provider",
		"metadata": map[string]interface{}{
			"name":        "provider-aws",
			"annotations": map[string]interface{}{ManagementAnnotation: "adopt"},
		},
		"spec":   map[string]interface{}{"package": "xpkg.upbound.io/upbound/provider-aws:v1.14.0"},
		"status": map[string]interface{}{"currentRevision": "provider-aws-abc123"},
	}}
	client := fakeDynamic(t, map[schema.GroupVersionResource]string{providerGVR: "ProviderList"}, prov)

	w := NewCrossplaneProviderWatcher(client)
	ch, err := w.Start(context.Background())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	cap := recvOne(t, ch)
	if cap.Kind != agentv1.CapabilityKind_CAPABILITY_KIND_CROSSPLANE_PROVIDER {
		t.Errorf("kind = %v", cap.Kind)
	}
	if cap.ManagementMode != agentv1.ManagementMode_MANAGEMENT_MODE_ADOPT {
		t.Errorf("explicit adopt annotation must be honoured, got %v", cap.ManagementMode)
	}
}

func TestIgnoreAnnotatedResourceExcludedFromInventory(t *testing.T) {
	crd := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata": map[string]interface{}{
			"name":        "secret-widgets.example.com",
			"annotations": map[string]interface{}{ManagementAnnotation: "ignore"},
		},
		"spec": map[string]interface{}{
			"group": "example.com",
			"versions": []interface{}{
				map[string]interface{}{"name": "v1", "served": true, "storage": true},
			},
		},
	}}
	client := fakeDynamic(t, map[schema.GroupVersionResource]string{crdGVR: "CustomResourceDefinitionList"}, crd)

	w := NewCRDWatcher(client, fakeMetadata(t, crd))
	ch, err := w.Start(context.Background())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	cap := recvOne(t, ch)
	// ignore = excluded from inventory (plan §12.1/3): surfaced only as a
	// DELETE so a resource transitioning into ignore is removed upstream.
	if cap.Action != agentv1.CapabilityAction_CAPABILITY_ACTION_DELETE {
		t.Errorf("ignore-classified resource must not be upserted, got action %v", cap.Action)
	}
}
