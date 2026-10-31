// Status streamer envtest (M5W1, operator-sunset risk #4): configured
// GVR/label-selector watches run against a real apiserver — a native
// cert-manager ClusterIssuer streams HEALTHY with the group-qualified
// kind, a label selector bounds the DNSEndpoint watch, absent CRDs are
// skipped by the discovery filter without blocking the surviving watches,
// and deletes stream MISSING with the qualified kind.
//
// Requires envtest binaries (setup-envtest; KUBEBUILDER_ASSETS or the
// default ~/.local/share/kubebuilder-envtest path). Skips when absent.
package status

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

// startStatusEnvtest boots a local control plane with the status-watch
// CRD fixtures; it skips when the envtest binaries are absent. Binary
// discovery mirrors setup-envtest: KUBEBUILDER_ASSETS wins, then the
// newest version under ~/.local/share/kubebuilder-envtest/k8s.
func startStatusEnvtest(t *testing.T) (*rest.Config, kubernetes.Interface, dynamic.Interface) {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		if dir := latestEnvtestAssetsDir(); dir != "" {
			_ = os.Setenv("KUBEBUILDER_ASSETS", dir)
		}
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("testdata", "status-crds.yaml")}}
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
	return cfg, kube, dyn
}

// latestEnvtestAssetsDir returns the newest setup-envtest-managed binary
// dir ("" when none), matching setup-envtest's default store layout.
// Mirrors the helper in internal/controller (unexported there).
func latestEnvtestAssetsDir() string {
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

func TestStreamerConfiguredWatchesEnvtest(t *testing.T) {
	_, kube, dyn := startStatusEnvtest(t)
	ctx := context.Background()

	// Namespaces for the namespaced watches.
	for _, ns := range []string{"argocd", "tenant-acme"} {
		if _, err := kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	// Seed: a Ready ClusterIssuer, a labeled DNSEndpoint, and an unlabeled
	// DNSEndpoint the selector must exclude.
	issuer := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "cert-manager.io/v1",
		"kind":       "ClusterIssuer",
		"metadata":   map[string]interface{}{"name": "letsencrypt-prod"},
		"spec":       map[string]interface{}{"acme": map[string]interface{}{"server": "https://acme.example"}},
	}}
	created, err := dyn.Resource(clusterIssuerGVR).Create(ctx, issuer, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	issuerStatus := created.DeepCopy()
	issuerStatus.Object["status"] = map[string]interface{}{"conditions": []interface{}{
		map[string]interface{}{"type": "Ready", "status": "True", "message": "issuer ready"},
	}}
	if _, err := dyn.Resource(clusterIssuerGVR).UpdateStatus(ctx, issuerStatus, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	dnsEndpoint := func(name string, labels map[string]interface{}) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "externaldns.k8s.io/v1alpha1",
			"kind":       "DNSEndpoint",
			"metadata":   map[string]interface{}{"name": name, "namespace": "tenant-acme", "labels": labels},
			"spec":       map[string]interface{}{"endpoints": []interface{}{}},
		}}
	}
	if _, err := dyn.Resource(dnsEndpointGVR).Namespace("tenant-acme").Create(ctx,
		dnsEndpoint("acme-zone", map[string]interface{}{"inari.io/managed-by": "inari"}), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := dyn.Resource(dnsEndpointGVR).Namespace("tenant-acme").Create(ctx,
		dnsEndpoint("foreign-zone", nil), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	// A statusless AppProject: existence signal with UNKNOWN health.
	appProjectGVR := schema.GroupVersionResource{Group: "argoproj.io", Version: "v1alpha1", Resource: "appprojects"}
	project := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "argoproj.io/v1alpha1",
		"kind":       "AppProject",
		"metadata":   map[string]interface{}{"name": "team-a", "namespace": "argocd"},
		"spec":       map[string]interface{}{"sourceRepos": []interface{}{"*"}},
	}}
	if _, err := dyn.Resource(appProjectGVR).Namespace("argocd").Create(ctx, project, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	// Watch config: native replacements with a DNSEndpoint label selector,
	// plus watches for APIs NOT installed here — the discovery filter must
	// skip them without blocking the rest.
	watches := []WatchResource{
		{GVR: clusterIssuerGVR},
		{GVR: dnsEndpointGVR, LabelSelector: "inari.io/managed-by=inari"},
		{GVR: appProjectGVR},
	}
	watches = append(watches, absentWatches()...)

	sender := &captureSender{signal: make(chan struct{}, 16)}
	s := NewStreamer(logr.Discard(), dyn, sender, "argocd", nil, watches)
	s.Discovery = kube.Discovery()
	sctx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(sctx) }()
	t.Cleanup(func() { cancel(); <-errCh })

	collect := func(n int, d time.Duration) []*agentv1.StatusUpdate {
		t.Helper()
		deadline := time.After(d)
		for sender.count() < n {
			select {
			case <-sender.signal:
			case <-deadline:
				t.Fatalf("want %d events, got %d", n, sender.count())
			}
		}
		evs := sender.snapshot()
		out := make([]*agentv1.StatusUpdate, len(evs))
		for i, ev := range evs {
			out[i] = lastStatus(t, ev)
		}
		return out
	}

	// Exactly three resources are watched: the ClusterIssuer, the labeled
	// DNSEndpoint, and the AppProject. The unlabeled DNSEndpoint and the
	// absent watched APIs must not produce events.
	events := collect(3, 15*time.Second)
	byKind := map[string]*agentv1.StatusUpdate{}
	for _, upd := range events {
		byKind[upd.Resource.Kind+"/"+upd.Resource.Name] = upd
	}
	iss := byKind["ClusterIssuer.cert-manager.io/letsencrypt-prod"]
	if iss == nil {
		t.Fatalf("no ClusterIssuer update; got %v", keys(byKind))
	}
	if iss.Health != agentv1.HealthStatus_HEALTH_STATUS_HEALTHY || iss.Sync != agentv1.SyncState_SYNC_STATE_SYNCED {
		t.Fatalf("issuer update %+v", iss)
	}
	dns := byKind["DNSEndpoint.externaldns.k8s.io/acme-zone"]
	if dns == nil {
		t.Fatalf("no labeled DNSEndpoint update; got %v", keys(byKind))
	}
	if _, ok := byKind["DNSEndpoint.externaldns.k8s.io/foreign-zone"]; ok {
		t.Fatal("label selector must exclude the unlabeled DNSEndpoint")
	}
	proj := byKind["AppProject.argoproj.io/team-a"]
	if proj == nil {
		t.Fatalf("no AppProject update; got %v", keys(byKind))
	}
	if proj.Health != agentv1.HealthStatus_HEALTH_STATUS_UNKNOWN {
		t.Fatalf("statusless AppProject must report UNKNOWN, got %v", proj.Health)
	}

	// Delete the issuer: a MISSING update with the qualified kind streams.
	if err := dyn.Resource(clusterIssuerGVR).Delete(ctx, "letsencrypt-prod", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	events = collect(4, 15*time.Second)
	last := events[len(events)-1]
	if last.Resource.Kind != "ClusterIssuer.cert-manager.io" ||
		last.Health != agentv1.HealthStatus_HEALTH_STATUS_MISSING {
		t.Fatalf("delete update %+v", last)
	}
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
