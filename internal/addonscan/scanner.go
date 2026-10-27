// Package addonscan discovers platform addons already running on the
// tenant cluster (Feature C, plan §6 "Discovery"): helm storage Secrets
// (owner=helm) are matched against the known-addon table and reported to
// the control plane as unmanaged, adoptable addons.
//
// The scan is metadata-only — release blobs are never decoded (footprint
// budget, plan §12.1/4) — so the detected chart version is not available
// from labels (the "version" label is the release revision); the user
// picks the version at adopt time from the catalog.
package addonscan

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/metadata/metadatainformer"
	"k8s.io/client-go/tools/cache"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

// ReportAddonsEventType is the agent→server status-stream event carrying
// discovered addons. The pinned agent contract has no ReportAddons
// message yet (proto mod lands with the agent milestone); the payload is
// a structpb.Struct bridge the server decodes via addons.DecodeReportAddons:
//
//	{"addons": [{"catalogItem": "platform:cert-manager", "releaseName": "cert-manager",
//	             "namespace": "cert-manager", "chart": "cert-manager",
//	             "repoUrl": "https://charts.jetstack.io"}]}
const ReportAddonsEventType = "EVENT_TYPE_REPORT_ADDONS"

var secretsGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}

// KnownAddon maps a helm release name to its platform-app catalog item.
type KnownAddon struct {
	// ReleaseName is the helm release name to match (any namespace).
	ReleaseName string
	// CatalogItem is the platform-app catalog ID, e.g. "platform:cert-manager".
	CatalogItem string
	// Chart and RepoURL seed the adopt form's source descriptor.
	Chart   string
	RepoURL string
}

// DefaultKnownAddons is the built-in known-addon table (plan §6: the
// platform-app catalog's helm addons). Server-side ingest skips catalog
// items it does not know, so drift here is safe.
var DefaultKnownAddons = []KnownAddon{
	{ReleaseName: "cert-manager", CatalogItem: "platform:cert-manager", Chart: "cert-manager", RepoURL: "https://charts.jetstack.io"},
	{ReleaseName: "external-secrets", CatalogItem: "platform:external-secrets", Chart: "external-secrets", RepoURL: "https://charts.external-secrets.io"},
	{ReleaseName: "istiod", CatalogItem: "platform:istio", Chart: "istiod", RepoURL: "https://istio-release.storage.googleapis.com/charts"},
}

// Sender is the stream-client send seam (status.Sender shape).
type Sender interface {
	Send(ctx context.Context, event *agentv1.Event) error
}

// Scanner watches helm release Secrets and reports known addons. Reports
// are deduped by content; Run emits a full snapshot once its cache syncs,
// and Reset forces a fresh snapshot after a stream reconnect.
type Scanner struct {
	meta   metadata.Interface
	sender Sender
	known  []KnownAddon

	mu      sync.Mutex
	current map[string]map[string]interface{} // key: namespace/release
	lastSum string
}

// NewScanner returns a Scanner reporting matches of known via sender.
func NewScanner(meta metadata.Interface, sender Sender, known []KnownAddon) *Scanner {
	return &Scanner{meta: meta, sender: sender, known: known, current: map[string]map[string]interface{}{}}
}

// Reset clears the dedupe state and re-emits the current snapshot — used
// on stream reconnect, where the control plane may have restarted and
// lost previously reported discoveries.
func (s *Scanner) Reset(ctx context.Context) {
	s.mu.Lock()
	s.lastSum = ""
	s.mu.Unlock()
	s.report(ctx)
}

// Run starts the watch and blocks until ctx is cancelled.
func (s *Scanner) Run(ctx context.Context) error {
	tweak := func(opts *metav1.ListOptions) { opts.LabelSelector = "owner=helm" }
	factory := metadatainformer.NewFilteredSharedInformerFactory(s.meta, 0, metav1.NamespaceAll, tweak)
	informer := factory.ForResource(secretsGVR).Informer()

	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { s.onEvent(ctx, obj) },
		UpdateFunc: func(_, obj interface{}) { s.onEvent(ctx, obj) },
		DeleteFunc: func(obj interface{}) { s.onDelete(ctx, obj) },
	}); err != nil {
		return fmt.Errorf("addonscan: register handler: %w", err)
	}
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("addonscan: cache sync aborted")
	}
	// Full snapshot after the initial list.
	s.report(ctx)
	<-ctx.Done()
	return nil
}

// onEvent folds one helm Secret event into the current snapshot.
func (s *Scanner) onEvent(ctx context.Context, obj interface{}) {
	pm, ok := obj.(*metav1.PartialObjectMetadata)
	if !ok {
		if u, ok := obj.(*unstructured.Unstructured); ok {
			s.applyLabels(ctx, u.GetNamespace(), u.GetLabels())
		}
		return
	}
	s.applyLabels(ctx, pm.GetNamespace(), pm.GetLabels())
}

func (s *Scanner) applyLabels(ctx context.Context, namespace string, labels map[string]string) {
	if labels["owner"] != "helm" || labels["status"] != "deployed" {
		return
	}
	release := labels["name"]
	if release == "" {
		return
	}
	known, ok := s.match(release)
	if !ok {
		return
	}
	// Only the highest deployed revision counts; older deployed secrets
	// linger during upgrades and must not flip the report back.
	rev, _ := strconv.Atoi(labels["version"])
	key := namespace + "/" + release

	s.mu.Lock()
	if cur, ok := s.current[key]; ok {
		if prev, _ := cur["revision"].(int); prev >= rev {
			s.mu.Unlock()
			return
		}
	}
	s.current[key] = map[string]interface{}{
		"catalogItem": known.CatalogItem,
		"releaseName": release,
		"namespace":   namespace,
		"chart":       known.Chart,
		"repoUrl":     known.RepoURL,
		"revision":    rev,
	}
	s.mu.Unlock()
	s.report(ctx)
}

// onDelete drops the release from the snapshot when its deployed storage
// Secret disappears.
func (s *Scanner) onDelete(ctx context.Context, obj interface{}) {
	var namespace string
	var labels map[string]string
	switch o := obj.(type) {
	case *metav1.PartialObjectMetadata:
		namespace, labels = o.GetNamespace(), o.GetLabels()
	case *unstructured.Unstructured:
		namespace, labels = o.GetNamespace(), o.GetLabels()
	case cache.DeletedFinalStateUnknown:
		if u, ok := o.Obj.(*metav1.PartialObjectMetadata); ok {
			namespace, labels = u.GetNamespace(), u.GetLabels()
		}
	}
	if labels["owner"] != "helm" || labels["status"] != "deployed" || labels["name"] == "" {
		return
	}
	key := namespace + "/" + labels["name"]
	s.mu.Lock()
	_, existed := s.current[key]
	delete(s.current, key)
	s.mu.Unlock()
	if existed {
		s.report(ctx)
	}
}

func (s *Scanner) match(release string) (KnownAddon, bool) {
	for _, k := range s.known {
		if k.ReleaseName == release {
			return k, true
		}
	}
	return KnownAddon{}, false
}

// report sends the current snapshot when its content changed. Empty
// snapshots are never sent (nothing discovered is not news; the server
// keeps prior discovered rows until they are adopted or the release
// reappears — deletes of the last addon emit an empty report is avoided
// to keep the audit trail append-only per discovery event).
func (s *Scanner) report(ctx context.Context) {
	s.mu.Lock()
	addons := make([]map[string]interface{}, 0, len(s.current))
	for _, item := range s.current {
		clean := map[string]interface{}{}
		for k, v := range item {
			if k != "revision" {
				clean[k] = v
			}
		}
		addons = append(addons, clean)
	}
	s.mu.Unlock()
	if len(addons) == 0 {
		return
	}
	sort.Slice(addons, func(i, j int) bool {
		return addons[i]["catalogItem"].(string) < addons[j]["catalogItem"].(string)
	})
	sum := checksumOf(addons)
	s.mu.Lock()
	if sum == s.lastSum {
		s.mu.Unlock()
		return
	}
	s.lastSum = sum
	s.mu.Unlock()

	items := make([]interface{}, 0, len(addons))
	for _, a := range addons {
		items = append(items, a)
	}
	doc, err := structpb.NewStruct(map[string]interface{}{"addons": items})
	if err != nil {
		return
	}
	payload, err := anypb.New(doc)
	if err != nil {
		return
	}
	_ = s.sender.Send(ctx, &agentv1.Event{
		EventId: "addonscan-" + uuid.NewString(),
		Type:    ReportAddonsEventType,
		Payload: payload,
		Time:    timestamppb.Now(),
	})
}

func checksumOf(addons []map[string]interface{}) string {
	raw, _ := json.Marshal(addons)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
