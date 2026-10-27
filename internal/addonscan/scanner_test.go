package addonscan

import (
	"context"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/metadata"
	metadatafake "k8s.io/client-go/metadata/fake"
	"google.golang.org/protobuf/types/known/structpb"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

// helmSecret projects a helm storage Secret to the metadata the
// metadata-only informer serves (labels only; blobs never decoded).
func helmSecret(ns, release, revision, status string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("v1")
	u.SetKind("Secret")
	u.SetName("sh.helm.release.v1." + release + ".v" + revision)
	u.SetNamespace(ns)
	u.SetLabels(map[string]string{
		"owner":   "helm",
		"name":    release,
		"version": revision,
		"status":  status,
	})
	return u
}

type eventSink struct {
	mu     sync.Mutex
	events []*agentv1.Event
}

func (s *eventSink) Send(_ context.Context, ev *agentv1.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
	return nil
}

func (s *eventSink) reports() []*agentv1.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []*agentv1.Event{}
	for _, ev := range s.events {
		if ev.Type == ReportAddonsEventType {
			out = append(out, ev)
		}
	}
	return out
}

func fakeMeta(objs ...runtime.Object) metadata.Interface {
	scheme := runtime.NewScheme()
	pms := make([]runtime.Object, 0, len(objs))
	for _, o := range objs {
		u := o.(*unstructured.Unstructured)
		gvk := u.GroupVersionKind()
		scheme.AddKnownTypeWithName(gvk, &metav1.PartialObjectMetadata{})
		scheme.AddKnownTypeWithName(gvk.GroupVersion().WithKind(gvk.Kind+"List"), &metav1.PartialObjectMetadataList{})
		pm := &metav1.PartialObjectMetadata{
			TypeMeta: metav1.TypeMeta{APIVersion: u.GetAPIVersion(), Kind: u.GetKind()},
		}
		pm.SetName(u.GetName())
		pm.SetNamespace(u.GetNamespace())
		pm.SetLabels(u.GetLabels())
		pms = append(pms, pm)
	}
	return metadatafake.NewSimpleMetadataClient(scheme, pms...)
}

func lastReportItems(t *testing.T, ev *agentv1.Event) map[string]map[string]interface{} {
	t.Helper()
	var doc structpb.Struct
	if err := ev.Payload.UnmarshalTo(&doc); err != nil {
		t.Fatalf("payload not struct: %v", err)
	}
	addons := doc.GetFields()["addons"].GetListValue().GetValues()
	out := map[string]map[string]interface{}{}
	for _, a := range addons {
		item := a.GetStructValue().AsMap()
		out[item["catalogItem"].(string)] = item
	}
	return out
}

func runScanner(t *testing.T, meta metadata.Interface, sink *eventSink) context.CancelFunc {
	t.Helper()
	s := NewScanner(meta, sink, DefaultKnownAddons)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	})
	return cancel
}

func waitForReport(t *testing.T, sink *eventSink, want int) *agentv1.Event {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		reps := sink.reports()
		if len(reps) >= want {
			return reps[len(reps)-1]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d report(s), got %d", want, len(sink.reports()))
	return nil
}

func TestScannerReportsKnownAddons(t *testing.T) {
	sink := &eventSink{}
	meta := fakeMeta(
		helmSecret("cert-manager", "cert-manager", "3", "deployed"),
		helmSecret("cert-manager", "cert-manager", "2", "superseded"),
		helmSecret("kube-system", "random-thing", "1", "deployed"),
		helmSecret("eso", "external-secrets", "1", "deployed"),
	)
	runScanner(t, meta, sink)

	ev := waitForReport(t, sink, 1)
	items := lastReportItems(t, ev)
	if len(items) != 2 {
		t.Fatalf("reported %v, want cert-manager + external-secrets", items)
	}
	cm := items["platform:cert-manager"]
	if cm["releaseName"] != "cert-manager" || cm["namespace"] != "cert-manager" {
		t.Errorf("cert-manager item = %v", cm)
	}
	if cm["chart"] != "cert-manager" || cm["repoUrl"] != "https://charts.jetstack.io" {
		t.Errorf("chart/repoUrl = %v", cm)
	}
	if items["platform:external-secrets"]["namespace"] != "eso" {
		t.Errorf("external-secrets item = %v", items["platform:external-secrets"])
	}
}

func TestScannerIgnoresNonDeployedRevisions(t *testing.T) {
	sink := &eventSink{}
	meta := fakeMeta(helmSecret("cert-manager", "cert-manager", "1", "uninstalling"))
	runScanner(t, meta, sink)

	// No deployed revision → no report at all (empty reports are not sent).
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(sink.reports()) != 0 {
			t.Fatalf("unexpected report for non-deployed release: %v", sink.reports())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestScannerDedupesAndResets(t *testing.T) {
	sink := &eventSink{}
	meta := fakeMeta(helmSecret("cert-manager", "cert-manager", "1", "deployed"))
	s := NewScanner(meta, sink, DefaultKnownAddons)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	waitForReport(t, sink, 1)
	// No change → no second emission.
	time.Sleep(300 * time.Millisecond)
	if n := len(sink.reports()); n != 1 {
		t.Fatalf("emissions = %d, want 1 (deduped)", n)
	}
	// Reset (stream reconnect) forces a fresh snapshot.
	s.Reset(ctx)
	waitForReport(t, sink, 2)
}
