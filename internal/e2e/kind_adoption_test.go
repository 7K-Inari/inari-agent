//go:build e2e

// Live in-place adoption validation (Feature C, risk item 2): against a
// real cluster with real ArgoCD and a real helm-installed cert-manager —
// driven by hack/e2e-addon-adoption.sh, which provisions kind, installs
// ArgoCD, and helm-installs cert-manager before running this test.
//
// Assertions that fakes/envtest cannot make: ArgoCD actually syncs the
// registered Application with TakeOwnership + prune OFF over the existing
// release IN PLACE — cert-manager pods, CRDs, and the helm release Secret
// survive with their UIDs unchanged — and the app reaches Healthy.
//
// Environment: KUBECONFIG must point at the prepared cluster.
package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/7K-Inari/inari-agent/internal/addonscan"
	"github.com/7K-Inari/inari-agent/internal/argocd"
	"github.com/7K-Inari/inari-agent/internal/command"
)

var appsGVR = schema.GroupVersionResource{Group: "argoproj.io", Version: "v1alpha1", Resource: "applications"}

func kubeClients(t *testing.T) (*rest.Config, kubernetes.Interface, dynamic.Interface, metadata.Interface) {
	t.Helper()
	kc := os.Getenv("KUBECONFIG")
	if kc == "" {
		t.Skip("KUBECONFIG not set — run via hack/e2e-addon-adoption.sh")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kc)
	if err != nil {
		t.Fatal(err)
	}
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

func podUIDs(t *testing.T, kube kubernetes.Interface, ns string) map[string]string {
	t.Helper()
	pods, err := kube.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, p := range pods.Items {
		out[p.Name] = string(p.UID)
	}
	return out
}

func crdNames(t *testing.T, dyn dynamic.Interface) map[string]string {
	t.Helper()
	crdGVR := schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	list, err := dyn.Resource(crdGVR).List(context.Background(), metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/managed-by=Helm",
	})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, c := range list.Items {
		out[c.GetName()] = string(c.GetUID())
	}
	return out
}

type e2eGate struct{ ns string }

func (g e2eGate) EnsureReady(context.Context) (string, error) { return g.ns, nil }

// TestLiveAddonAdoption assumes the driver script has prepared: kind
// cluster, ArgoCD in namespace argocd, helm release cert-manager in
// namespace cert-manager (crds.enabled=true).
func TestLiveAddonAdoption(t *testing.T) {
	_, kube, dyn, meta := kubeClients(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Baseline: the running release's pods, CRDs, and helm Secret.
	podsBefore := podUIDs(t, kube, "cert-manager")
	if len(podsBefore) == 0 {
		t.Fatal("no cert-manager pods — driver script must helm-install first")
	}
	crdsBefore := crdNames(t, dyn)
	if len(crdsBefore) == 0 {
		t.Fatal("no cert-manager CRDs found")
	}

	// Scanner discovers the live release.
	sink := newReportSink()
	scanner := addonscan.NewScanner(meta, sink, addonscan.DefaultKnownAddons, []string{"cert-manager"})
	go func() { _ = scanner.Run(ctx) }()
	waitFor(t, 60*time.Second, "scanner discovers cert-manager", func() bool {
		for _, ev := range sink.events() {
			if ev.Type == addonscan.ReportAddonsEventType {
				return true
			}
		}
		return false
	})

	// Register the adoption Application exactly as the server's bridge
	// command delivers it (prune OFF + take-ownership; helm source with
	// inline values so the test needs no git host).
	d := command.NewDispatcher()
	d.RegisterBridge(command.RegisterArgoCDAppManifestEventType,
		command.RegisterArgoCDAppManifestHandler(e2eGate{ns: "argocd"}, func(ns string) *argocd.Client {
			return argocd.NewClient(dyn, ns)
		}))
	ack, err := d.HandleEvent(ctx, bridgeCmd(command.RegisterArgoCDAppManifestEventType,
		"inari-addon-01j5z8k2n4xqj4tvdmx7a3b2c1", liveAdoptionManifest))
	if err != nil {
		t.Fatal(err)
	}
	if ack.Result != agentv1.CommandResult_COMMAND_RESULT_APPLIED {
		t.Fatalf("ack = %+v", ack)
	}

	// ArgoCD syncs in place: the app reaches Healthy without deleting the
	// pre-existing release.
	app := dyn.Resource(appsGVR).Namespace("argocd")
	waitFor(t, 5*time.Minute, "application Healthy", func() bool {
		obj, err := app.Get(ctx, "inari-addon-01j5z8k2n4xqj4tvdmx7a3b2c1", metav1.GetOptions{})
		if err != nil {
			return false
		}
		health, _, _ := unstructured.NestedString(obj.Object, "status", "health", "status")
		return health == "Healthy"
	})

	// The pre-existing release survived: same pod UIDs (no delete/recreate),
	// CRDs intact, helm Secret untouched.
	podsAfter := podUIDs(t, kube, "cert-manager")
	for name, uid := range podsBefore {
		if podsAfter[name] != uid {
			t.Errorf("pod %s was recreated (uid %s -> %s) — in-place adoption violated", name, uid, podsAfter[name])
		}
	}
	for name, uid := range crdsBefore {
		got := crdNames(t, dyn)
		if got[name] != uid {
			t.Errorf("CRD %s changed (uid %s -> %s)", name, uid, got[name])
		}
	}
	if _, err := kube.CoreV1().Secrets("cert-manager").Get(ctx,
		"sh.helm.release.v1.cert-manager.v1", metav1.GetOptions{}); err != nil {
		t.Errorf("helm release secret missing after adoption: %v", err)
	}

	// Prune stays OFF: deleting the Application (non-cascading) leaves the
	// workload running — the adoption never claimed lifecycle ownership of
	// destruction.
	if err := app.Delete(ctx, "inari-addon-01j5z8k2n4xqj4tvdmx7a3b2c1", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Second)
	if len(podUIDs(t, kube, "cert-manager")) == 0 {
		t.Error("cert-manager pods vanished after non-cascading app delete")
	}
}

// reportSink captures scanner emissions. The channel is created up front:
// a lazy init here would race the scanner goroutine and drop reports sent
// before the first events() call.
type reportSink struct {
	ch chan *agentv1.Event
}

func newReportSink() *reportSink { return &reportSink{ch: make(chan *agentv1.Event, 64)} }

func (s *reportSink) Send(_ context.Context, ev *agentv1.Event) error {
	select {
	case s.ch <- ev:
	default:
	}
	return nil
}

func (s *reportSink) events() []*agentv1.Event {
	out := []*agentv1.Event{}
	for {
		select {
		case ev := <-s.ch:
			out = append(out, ev)
		default:
			return out
		}
	}
}

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

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for %s", what)
}

const liveAdoptionManifest = `apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: inari-addon-01j5z8k2n4xqj4tvdmx7a3b2c1
spec:
  project: default
  source:
    repoURL: https://charts.jetstack.io
    chart: cert-manager
    targetRevision: v1.16.2
    helm:
      releaseName: cert-manager
      values: |
        crds:
          enabled: true
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
