// Agent-side end-to-end addon adoption test (Feature C, plan §6, risk
// item 2) against a real apiserver via envtest: a pre-existing ("helm
// installed") cert-manager release is discovered by the scanner, the
// registration bridge commands upsert the rendered manifests verbatim,
// and the pre-existing resources are never touched. The live ArgoCD
// in-place sync validation is the kind e2e layer.
//
// Requires envtest binaries (setup-envtest; KUBEBUILDER_ASSETS or the
// default ~/.local/share/kubebuilder-envtest path). Skips when absent.
package controller

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/addonscan"
	"github.com/7K-Inari/inari-agent/internal/argocd"
	"github.com/7K-Inari/inari-agent/internal/command"
)

// startEnvtest boots a local control plane with the ArgoCD CRDs and
// returns typed clients; it skips when the envtest binaries are absent.
// Binary discovery mirrors setup-envtest: KUBEBUILDER_ASSETS wins, then
// the newest version under ~/.local/share/kubebuilder-envtest/k8s.
func startEnvtest(t *testing.T) (*rest.Config, kubernetes.Interface, dynamic.Interface, metadata.Interface) {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		if dir := latestEnvtestAssets(); dir != "" {
			_ = os.Setenv("KUBEBUILDER_ASSETS", dir)
		}
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("testdata", "argocd-crds.yaml")}}
	cfg, err := env.Start()
	if err != nil {
		t.Skipf("envtest unavailable (install via setup-envtest or set KUBEBUILDER_ASSETS): %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := metadata.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return cfg, kube, dyn, meta
}

// seedCertManagerRelease simulates "helm install cert-manager": the
// release namespace, a pre-existing Deployment (as the chart would have
// created), and the deployed helm storage Secret the scanner matches.
func seedCertManagerRelease(t *testing.T, kube kubernetes.Interface) (deployUID, secretUID string) {
	t.Helper()
	ctx := context.Background()
	if _, err := kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "cert-manager"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	dep, err := kube.AppsV1().Deployments("cert-manager").Create(ctx, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cert-manager",
			Namespace: "cert-manager",
			Labels:    map[string]string{"app.kubernetes.io/managed-by": "Helm"},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "cert-manager"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "cert-manager"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "cm", Image: "cert-manager:v1.16.2"}}},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sec, err := kube.CoreV1().Secrets("cert-manager").Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sh.helm.release.v1.cert-manager.v3",
			Namespace: "cert-manager",
			Labels: map[string]string{
				"owner": "helm", "name": "cert-manager", "version": "3", "status": "deployed",
			},
		},
		Type: "helm.sh/release.v1",
		Data: map[string][]byte{"release": []byte("not-decoded-by-the-agent")},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return string(dep.UID), string(sec.UID)
}

// TestAdoptionAgentSideEndToEnd drives the agent half of the adoption
// flow against envtest: scan → report → register (manifest bridge,
// take-ownership, prune OFF) → pre-existing resources untouched.
func TestAdoptionAgentSideEndToEnd(t *testing.T) {
	_, kube, dyn, meta := startEnvtest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	deployUID, secretUID := seedCertManagerRelease(t, kube)
	if _, err := kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "argocd"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	// --- 1. Scanner: the pre-existing release is reported as adoptable.
	sink := &reportSink{}
	scanner := addonscan.NewScanner(meta, sink, addonscan.DefaultKnownAddons, []string{"cert-manager"})
	go func() { _ = scanner.Run(ctx) }()

	var report map[string]interface{}
	waitFor(t, "scanner reports cert-manager", func() bool {
		for _, ev := range sink.events() {
			if ev.Type != addonscan.ReportAddonsEventType {
				continue
			}
			items := decodeReport(t, ev)
			if r, ok := items["platform:cert-manager"]; ok {
				report = r
				return true
			}
		}
		return false
	})
	if report["namespace"] != "cert-manager" || report["releaseName"] != "cert-manager" {
		t.Errorf("report = %v", report)
	}
	if report["chart"] != "cert-manager" || report["repoUrl"] != "https://charts.jetstack.io" {
		t.Errorf("source coordinates = %v", report)
	}

	// --- 2. Registration: the manifest bridge command upserts the
	// committed application.yaml verbatim — prune OFF + take-ownership.
	d := command.NewDispatcher()
	d.RegisterBridge(command.RegisterArgoCDAppManifestEventType,
		command.RegisterArgoCDAppManifestHandler(envtestGate{ns: "argocd"}, func(ns string) *argocd.Client {
			return argocd.NewClient(dyn, ns)
		}))
	d.RegisterBridge(command.RegisterArgoCDAppSetEventType,
		command.RegisterArgoCDAppSetHandler(envtestGate{ns: "argocd"}, func(ns string) *argocd.Client {
			return argocd.NewClient(dyn, ns)
		}))

	ack, err := d.HandleEvent(ctx, bridgeCmd(command.RegisterArgoCDAppManifestEventType, "inari-addon-01j5z8k2n4xqj4tvdmx7a3b2c1", adoptionAppManifest))
	if err != nil {
		t.Fatal(err)
	}
	if ack.Result.String() != "COMMAND_RESULT_APPLIED" {
		t.Fatalf("ack = %+v", ack)
	}
	app, err := dyn.Resource(argocd.ApplicationGVR).Namespace("argocd").Get(ctx, "inari-addon-01j5z8k2n4xqj4tvdmx7a3b2c1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	opts, _, _ := unstructured.NestedSlice(app.Object, "spec", "syncPolicy", "syncOptions")
	if len(opts) != 1 || opts[0] != "TakeOwnership=true" {
		t.Errorf("syncOptions = %v, want take-ownership in place", opts)
	}
	prune, _, _ := unstructured.NestedBool(app.Object, "spec", "syncPolicy", "automated", "prune")
	if prune {
		t.Error("prune must stay OFF at adoption (risk item 2)")
	}
	sources, _, _ := unstructured.NestedSlice(app.Object, "spec", "sources")
	if len(sources) != 2 {
		t.Errorf("multi-source helm app must survive verbatim, sources = %v", sources)
	}

	// --- 3. Fleet mode: the ApplicationSet manifest upserts verbatim.
	ack, err = d.HandleEvent(ctx, bridgeCmd(command.RegisterArgoCDAppSetEventType, "inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1", adoptionAppSetManifest))
	if err != nil {
		t.Fatal(err)
	}
	if ack.Result.String() != "COMMAND_RESULT_APPLIED" {
		t.Fatalf("appset ack = %+v", ack)
	}
	appset, err := dyn.Resource(argocd.ApplicationSetGVR).Namespace("argocd").Get(ctx, "inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	gens, _, _ := unstructured.NestedSlice(appset.Object, "spec", "generators")
	if len(gens) != 1 {
		t.Fatalf("generators = %v", gens)
	}
	elements, _, _ := unstructured.NestedSlice(gens[0].(map[string]interface{}), "list", "elements")
	if len(elements) != 2 {
		t.Errorf("list generator elements = %v, want 2 clusters", elements)
	}

	// --- 4. In-place adoption: the pre-existing release was never
	// deleted or modified (registration only writes ArgoCD objects).
	dep, err := kube.AppsV1().Deployments("cert-manager").Get(ctx, "cert-manager", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("pre-existing deployment GONE: %v", err)
	}
	if string(dep.UID) != deployUID {
		t.Error("pre-existing deployment was recreated (UID changed)")
	}
	sec, err := kube.CoreV1().Secrets("cert-manager").Get(ctx, "sh.helm.release.v1.cert-manager.v3", metav1.GetOptions{})
	if err != nil || string(sec.UID) != secretUID {
		t.Errorf("pre-existing helm secret touched: %v", err)
	}
}

// latestEnvtestAssets returns the newest setup-envtest-managed binary dir
// ("" when none), matching setup-envtest's default store layout.
func latestEnvtestAssets() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	entries, err := os.ReadDir(filepath.Join(home, ".local", "share", "kubebuilder-envtest", "k8s"))
	if err != nil {
		return ""
	}
	best := ""
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(home, ".local", "share", "kubebuilder-envtest", "k8s", e.Name())
		if _, err := os.Stat(filepath.Join(dir, "kube-apiserver")); err != nil {
			continue
		}
		if e.Name() > best { // version-arch names sort lexically
			best = e.Name()
		}
	}
	if best == "" {
		return ""
	}
	return filepath.Join(home, ".local", "share", "kubebuilder-envtest", "k8s", best)
}

// envtestGate is the ArgoCDGate seam: the ArgoCD namespace is created by
// the test before registration commands run.
type envtestGate struct{ ns string }

func (g envtestGate) EnsureReady(context.Context) (string, error) { return g.ns, nil }

// reportSink captures emitted stream events.
type reportSink struct {
	mu  sync.Mutex
	evt []*agentv1.Event
}

func (s *reportSink) Send(_ context.Context, ev *agentv1.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evt = append(s.evt, ev)
	return nil
}

func (s *reportSink) events() []*agentv1.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*agentv1.Event{}, s.evt...)
}

// decodeReport decodes a ReportAddons bridge event into items keyed by
// catalog item.
func decodeReport(t *testing.T, ev *agentv1.Event) map[string]map[string]interface{} {
	t.Helper()
	var doc structpb.Struct
	if err := ev.Payload.UnmarshalTo(&doc); err != nil {
		t.Fatalf("report payload: %v", err)
	}
	out := map[string]map[string]interface{}{}
	for _, v := range doc.GetFields()["addons"].GetListValue().GetValues() {
		item := v.GetStructValue().AsMap()
		out[item["catalogItem"].(string)] = item
	}
	return out
}

// bridgeCmd builds a structpb bridge command event the way the server's
// addons service enqueues it.
func bridgeCmd(typeStr, name, manifest string) *agentv1.Event {
	doc, err := structpb.NewStruct(map[string]interface{}{
		"commandId": name, "name": name, "namespace": "argocd", "manifest": manifest,
	})
	if err != nil {
		panic(err)
	}
	payload, err := anypb.New(doc)
	if err != nil {
		panic(err)
	}
	return &agentv1.Event{EventId: "evt-" + name, Type: typeStr, Payload: payload}
}

const adoptionAppManifest = `apiVersion: argoproj.io/v1alpha1
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

const adoptionAppSetManifest = `apiVersion: argoproj.io/v1alpha1
kind: ApplicationSet
metadata:
  name: inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1
spec:
  goTemplate: false
  generators:
    - list:
        elements:
          - cluster: prod-eu
          - cluster: prod-us
  template:
    metadata:
      name: inari-addon-01j5z8k2n4xqj4tvdmx7a3b2c1-{{cluster}}
    spec:
      project: default
      source:
        repoURL: https://charts.jetstack.io
        chart: cert-manager
        targetRevision: v1.16.2
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
