package argocd

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

const testAppSetManifest = `apiVersion: argoproj.io/v1alpha1
kind: ApplicationSet
metadata:
  name: inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1
spec:
  goTemplate: false
  generators:
    - list:
        elements:
          - cluster: prod-1
          - cluster: prod-2
  template:
    metadata:
      name: inari-addon-01j5z8k2n4xqj4tvdmx7a3b2c1-{{cluster}}
    spec:
      project: default
      source:
        repoURL: https://charts.jetstack.io
        chart: cert-manager
      destination:
        server: https://kubernetes.default.svc
        namespace: cert-manager
      syncPolicy:
        automated:
          prune: false
          selfHeal: true
        syncOptions:
          - TakeOwnership=true
`

const testAppManifest = `apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: inari-addon-01j5z8k2n4xqj4tvdmx7a3b2c1
spec:
  project: default
  sources:
    - repoURL: https://charts.jetstack.io
      chart: cert-manager
      targetRevision: v1.16.2
      helm:
        valueFiles:
          - $values/addons/cert-manager/values.yaml
    - repoURL: https://git/org/acme-inari-state
      targetRevision: main
      ref: values
  destination:
    server: https://kubernetes.default.svc
    namespace: cert-manager
  syncPolicy:
    automated:
      prune: false
      selfHeal: true
    syncOptions:
      - TakeOwnership=true
`

func TestEnsureApplicationSetUpsertsManifestVerbatim(t *testing.T) {
	ctx := context.Background()
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	c := NewClient(dyn, "argocd")

	if err := c.EnsureApplicationSet(ctx, "inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1", testAppSetManifest); err != nil {
		t.Fatal(err)
	}
	obj, err := dyn.Resource(ApplicationSetGVR).Namespace("argocd").Get(ctx, "inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if obj.GetLabels()[ManagedByLabel] != ManagedByValue {
		t.Errorf("missing ownership label: %v", obj.GetLabels())
	}
	// The manifest is authoritative: multi-element list generator and
	// take-ownership sync options survive verbatim.
	elements, _, _ := unstructured.NestedSlice(obj.Object, "spec", "generators")
	if len(elements) != 1 {
		t.Fatalf("generators = %v", elements)
	}
	opts, _, _ := unstructured.NestedSlice(obj.Object, "spec", "template", "spec", "syncPolicy", "syncOptions")
	if len(opts) != 1 || opts[0] != "TakeOwnership=true" {
		t.Errorf("syncOptions = %v, want [TakeOwnership=true]", opts)
	}
	prune, _, _ := unstructured.NestedBool(obj.Object, "spec", "template", "spec", "syncPolicy", "automated", "prune")
	if prune {
		t.Error("prune must stay off for adopted addons")
	}
	// Idempotent: second call is a no-op (no update action).
	if err := c.EnsureApplicationSet(ctx, "inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1", testAppSetManifest); err != nil {
		t.Fatal(err)
	}
	for _, a := range dyn.Actions() {
		if a.GetVerb() == "update" {
			t.Fatalf("idempotent re-ensure must not update; actions %v", dyn.Actions())
		}
	}
}

func TestEnsureApplicationSetDriftUpdate(t *testing.T) {
	ctx := context.Background()
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	c := NewClient(dyn, "argocd")
	if err := c.EnsureApplicationSet(ctx, "inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1", testAppSetManifest); err != nil {
		t.Fatal(err)
	}
	updated := strings.Replace(testAppSetManifest, "prod-2", "prod-3", 1)
	if err := c.EnsureApplicationSet(ctx, "inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1", updated); err != nil {
		t.Fatal(err)
	}
	obj, _ := dyn.Resource(ApplicationSetGVR).Namespace("argocd").Get(ctx, "inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1", metav1.GetOptions{})
	raw, _ := obj.MarshalJSON()
	if !strings.Contains(string(raw), "prod-3") {
		t.Error("drifted spec was not updated")
	}
}

func TestEnsureApplicationSetRefusesForeignObject(t *testing.T) {
	ctx := context.Background()
	foreign := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "argoproj.io/v1alpha1",
		"kind":       "ApplicationSet",
		"metadata":   map[string]interface{}{"name": "theirs", "namespace": "argocd"},
		"spec":       map[string]interface{}{"goTemplate": false},
	}}
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), foreign)
	c := NewClient(dyn, "argocd")
	m := strings.Replace(testAppSetManifest, "inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1", "theirs", 1)
	if err := c.EnsureApplicationSet(ctx, "theirs", m); err == nil || !strings.Contains(err.Error(), "observe-only") {
		t.Fatalf("foreign ApplicationSet must be refused, got %v", err)
	}
}

func TestEnsureApplicationManifestMultiSource(t *testing.T) {
	ctx := context.Background()
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	c := NewClient(dyn, "argocd")

	if err := c.EnsureApplicationManifest(ctx, "inari-addon-01j5z8k2n4xqj4tvdmx7a3b2c1", testAppManifest); err != nil {
		t.Fatal(err)
	}
	obj, err := dyn.Resource(ApplicationGVR).Namespace("argocd").Get(ctx, "inari-addon-01j5z8k2n4xqj4tvdmx7a3b2c1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Multi-source helm app (chart + git $values) survives verbatim —
	// RegisterArgoCDApp fields cannot express this.
	sources, _, _ := unstructured.NestedSlice(obj.Object, "spec", "sources")
	if len(sources) != 2 {
		t.Fatalf("sources = %v, want 2 (chart + $values)", sources)
	}
	opts, _, _ := unstructured.NestedSlice(obj.Object, "spec", "syncPolicy", "syncOptions")
	if len(opts) != 1 || opts[0] != "TakeOwnership=true" {
		t.Errorf("syncOptions = %v, want [TakeOwnership=true]", opts)
	}
	if obj.GetLabels()[ManagedByLabel] != ManagedByValue {
		t.Errorf("missing ownership label: %v", obj.GetLabels())
	}
}

func TestEnsureManifestRejectsKindMismatch(t *testing.T) {
	ctx := context.Background()
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	c := NewClient(dyn, "argocd")
	if err := c.EnsureApplicationSet(ctx, "x", testAppManifest); err == nil {
		t.Error("Application manifest via EnsureApplicationSet must fail")
	}
	if err := c.EnsureApplicationManifest(ctx, "x", testAppSetManifest); err == nil {
		t.Error("ApplicationSet manifest via EnsureApplicationManifest must fail")
	}
	if err := c.EnsureApplicationSet(ctx, "x", "not yaml: ["); err == nil {
		t.Error("invalid YAML must fail")
	}
}
