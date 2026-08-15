package argocd

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

func argocdServerDeployment(ns, image string, helm bool) *appsv1.Deployment {
	labels := map[string]string{}
	if helm {
		labels["app.kubernetes.io/managed-by"] = "Helm"
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "argocd-server", Namespace: ns, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{
					{Name: "argocd-server", Image: image},
				}},
			},
		},
	}
}

func TestParseVersion(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"quay.io/argoproj/argocd:v2.14.11", "2.14.11"},
		{"ghcr.io/x/argocd:3.0.6", "3.0.6"},
	} {
		got, err := ParseVersion(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("ParseVersion(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
		}
	}
	if _, err := ParseVersion("argocd:latest"); err == nil {
		t.Error("expected error for untagged image")
	}
}

func TestInSkewPolicy(t *testing.T) {
	cases := []struct {
		version string
		want    bool
	}{
		{"3.0.6", true},   // bundle line N
		{"2.14.11", true}, // N-1
		{"2.14.0", true},  // floor
		{"2.13.9", false}, // below floor
		{"3.1.0", false},  // newer than bundle
		{"v2.14.5", true}, // leading v tolerated
	}
	for _, tc := range cases {
		if got := InSkew(tc.version); got != tc.want {
			t.Errorf("InSkew(%s) = %v, want %v", tc.version, got, tc.want)
		}
	}
}

func TestDetectClassifies(t *testing.T) {
	ctx := context.Background()

	kube := k8sfake.NewSimpleClientset()
	inst, err := Detect(ctx, kube)
	if err != nil || inst != nil {
		t.Fatalf("empty cluster: %+v, %v", inst, err)
	}

	kube = k8sfake.NewSimpleClientset(argocdServerDeployment("argocd", "quay.io/argoproj/argocd:v2.14.11", true))
	inst, err = Detect(ctx, kube)
	if err != nil {
		t.Fatal(err)
	}
	if inst.Classification != ClassAdoptable || inst.Version != "2.14.11" || !inst.HelmManaged || inst.Namespace != "argocd" {
		t.Fatalf("adoptable: %+v", inst)
	}

	kube = k8sfake.NewSimpleClientset(argocdServerDeployment("custom-ns", "quay.io/argoproj/argocd:v2.10.0", false))
	inst, err = Detect(ctx, kube)
	if err != nil {
		t.Fatal(err)
	}
	if inst.Classification != ClassOutOfSkew {
		t.Fatalf("out-of-skew: %+v", inst)
	}
}

func TestLifecycleEnsureReady(t *testing.T) {
	ctx := context.Background()

	t.Run("byo refused without install", func(t *testing.T) {
		l := &Lifecycle{Kube: k8sfake.NewSimpleClientset(), Mode: ModeBYO}
		if _, err := l.EnsureReady(ctx); err == nil {
			t.Fatal("expected refusal")
		}
	})
	t.Run("byo refuses out-of-skew, never upgrades", func(t *testing.T) {
		kube := k8sfake.NewSimpleClientset(argocdServerDeployment("argocd", "argocd:v2.10.0", false))
		l := &Lifecycle{Kube: kube, Mode: ModeBYO}
		_, err := l.EnsureReady(ctx)
		if err == nil || !strings.Contains(err.Error(), "out of supported skew") {
			t.Fatalf("expected skew refusal, got %v", err)
		}
	})
	t.Run("byo adopts in-skew", func(t *testing.T) {
		kube := k8sfake.NewSimpleClientset(argocdServerDeployment("gitops", "argocd:v3.0.6", false))
		l := &Lifecycle{Kube: kube, Mode: ModeBYO}
		ns, err := l.EnsureReady(ctx)
		if err != nil || ns != "gitops" {
			t.Fatalf("ns=%s err=%v", ns, err)
		}
	})
	t.Run("bundle installs when missing", func(t *testing.T) {
		scheme := runtime.NewScheme()
		dyn := dynamicfake.NewSimpleDynamicClient(scheme)
		l := &Lifecycle{
			Kube: k8sfake.NewSimpleClientset(),
			Dyn:  dyn,
			Mode: ModeBundle,
			Manifests: []byte(`apiVersion: v1
kind: ConfigMap
metadata:
  name: argocd-cm
`),
		}
		ns, err := l.EnsureReady(ctx)
		if err != nil || ns != "argocd" {
			t.Fatalf("ns=%s err=%v", ns, err)
		}
		got, err := dyn.Resource(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}).Namespace("argocd").Get(ctx, "argocd-cm", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got.GetName() != "argocd-cm" {
			t.Fatalf("got %v", got.GetName())
		}
	})
	t.Run("bundle ready on managed in-skew install", func(t *testing.T) {
		kube := k8sfake.NewSimpleClientset(argocdServerDeployment("argocd", "argocd:v3.0.0", false))
		l := &Lifecycle{Kube: kube, Mode: ModeBundle}
		if _, err := l.EnsureReady(ctx); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("bundle conflicts with foreign install", func(t *testing.T) {
		kube := k8sfake.NewSimpleClientset(argocdServerDeployment("other", "argocd:v3.0.0", false))
		l := &Lifecycle{Kube: kube, Mode: ModeBundle}
		if _, err := l.EnsureReady(ctx); err == nil {
			t.Fatal("expected conflict error")
		}
	})
}

func TestEnsureApplicationIdempotent(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	dyn := dynamicfake.NewSimpleDynamicClient(scheme)
	c := NewClient(dyn, "argocd")

	m := &agentv1.RegisterArgoCDApp{
		CommandId:            "cmd-a1",
		Name:                 "my-web",
		Project:              "team-a",
		Source:               &agentv1.ApplicationSource{RepoUrl: "https://git/org/acme-inari-state", Path: "my-web"},
		DestinationNamespace: "apps",
		SyncPolicy:           &agentv1.SyncPolicy{Automated: true, Prune: true},
	}
	if err := c.EnsureAppProject(ctx, "team-a"); err != nil {
		t.Fatal(err)
	}
	if err := c.EnsureApplication(ctx, m); err != nil {
		t.Fatal(err)
	}
	// Second call must be a no-op.
	if err := c.EnsureApplication(ctx, m); err != nil {
		t.Fatal(err)
	}
	apps := dyn.Resource(ApplicationGVR).Namespace("argocd")
	app, err := apps.Get(ctx, "my-web", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if app.GetLabels()[ManagedByLabel] != ManagedByValue {
		t.Fatalf("missing ownership label: %v", app.GetLabels())
	}
	project, _, _ := unstructured.NestedString(app.Object, "spec", "project")
	if project != "team-a" {
		t.Fatalf("project %q", project)
	}
	if rev, _, _ := unstructured.NestedString(app.Object, "spec", "source", "targetRevision"); rev != "main" {
		t.Fatalf("targetRevision %q", rev)
	}
	actions := dyn.Actions()
	updates := 0
	for _, a := range actions {
		if a.GetVerb() == "update" {
			updates++
		}
	}
	if updates != 0 {
		t.Fatalf("idempotent second EnsureApplication must not update; actions %v", actions)
	}
}

func TestEnsureApplicationRefusesForeignObject(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	foreign := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "argoproj.io/v1alpha1",
		"kind":       "Application",
		"metadata":   map[string]interface{}{"name": "theirs", "namespace": "argocd"},
		"spec":       map[string]interface{}{"project": "default"},
	}}
	dyn := dynamicfake.NewSimpleDynamicClient(scheme, foreign)
	c := NewClient(dyn, "argocd")
	err := c.EnsureApplication(ctx, &agentv1.RegisterArgoCDApp{
		Name:   "theirs",
		Source: &agentv1.ApplicationSource{RepoUrl: "https://git/org/repo"},
	})
	if err == nil || !strings.Contains(err.Error(), "observe-only") {
		t.Fatalf("pre-existing unmanaged object must be refused, got %v", err)
	}
}
