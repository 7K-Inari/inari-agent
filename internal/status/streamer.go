// Package status streams Application health/sync, KRO instance status, and
// configured platform-resource status upstream as status-update events
// (plan §5.3 phase 5): near-real-time inventory without the control plane
// ever polling tenant APIs. Watches are scoped to the ArgoCD namespace,
// Inari-managed KRO instance GVRs, and the configured WatchResource list
// (the server-rendered platform resources — operator-sunset plan risk #4)
// to respect the agent's footprint budget (§12.1/4).
package status

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/argocd"
	"github.com/7K-Inari/inari-agent/internal/capability"
)

// Sender accepts outbound events (stream.Client).
type Sender interface {
	Send(ctx context.Context, event *agentv1.Event) error
}

// Streamer watches ArgoCD Applications, KRO instance resources, and the
// configured platform-resource watches, and emits status-update events.
// Emissions are deduped by content; Run emits a full snapshot on start so
// reconnects resync without control-plane polling.
type Streamer struct {
	Log    logr.Logger
	Dyn    dynamic.Interface
	Sender Sender
	// Namespace scopes the Application watch (ArgoCD install namespace).
	Namespace string
	// InstanceGVRs are KRO instance GVRs to watch (cluster- or
	// namespace-scoped, per the generated CRD). They stream bare kinds —
	// the server's inventory contract for KRO instances is unqualified.
	InstanceGVRs []schema.GroupVersionResource
	// Watches are the configured cluster-wide platform-resource watches
	// (the server-rendered platform resources: provider-keycloak claims,
	// the KRO tenantnamespace instance, native cert-manager/external-dns/
	// argoproj resources). Objects from these watches stream the
	// group-qualified kind "<Kind>.<group>".
	Watches []WatchResource
	// Discovery, when set, filters Watches to APIs actually served by the
	// cluster (one discovery call per group/version, fail-open on
	// transient errors) so absent optional CRDs don't spin reflector
	// error loops. Nil disables filtering.
	Discovery discovery.DiscoveryInterface
	// TweakListOptions narrows the Application and InstanceGVR watches
	// further (label selectors). WatchResource entries carry their own
	// per-entry selector instead.
	TweakListOptions func(*metav1.ListOptions)

	mu      sync.Mutex
	lastSum map[string]string
}

// NewStreamer returns a Streamer.
func NewStreamer(log logr.Logger, dyn dynamic.Interface, sender Sender, namespace string, instanceGVRs []schema.GroupVersionResource, watches []WatchResource) *Streamer {
	return &Streamer{
		Log:          log,
		Dyn:          dyn,
		Sender:       sender,
		Namespace:    namespace,
		InstanceGVRs: instanceGVRs,
		Watches:      watches,
		lastSum:      map[string]string{},
	}
}

// Reset clears the dedupe state so every watched object re-emits on the
// next informer event — used on stream reconnect, where the control plane
// may have restarted and lost the previously streamed statuses.
func (s *Streamer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSum = map[string]string{}
}

// Run starts the watches and blocks until ctx is cancelled.
func (s *Streamer) Run(ctx context.Context) error {
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(s.Dyn, 0, s.Namespace, s.TweakListOptions)
	appInformer := factory.ForResource(argocd.ApplicationGVR).Informer()
	stops := []qualifiedInformer{{appInformer, false}}
	factories := []dynamicinformer.DynamicSharedInformerFactory{factory}

	clusterFactory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(s.Dyn, 0, metav1.NamespaceAll, s.TweakListOptions)
	factories = append(factories, clusterFactory)
	instanceGVRs := map[schema.GroupVersionResource]bool{}
	for _, gvr := range s.InstanceGVRs {
		instanceGVRs[gvr] = true
		inf := clusterFactory.ForResource(gvr).Informer()
		stops = append(stops, qualifiedInformer{inf, false})
	}
	for _, w := range s.filteredWatches() {
		// A GVR already watched via InstanceGVRs shares this factory's
		// informer: a second handler would emit the same object twice
		// with conflicting kinds (bare and group-qualified).
		if instanceGVRs[w.GVR] {
			s.Log.Info("status watch already covered by InstanceGVRs, skipping duplicate", "gvr", w.GVR.String())
			continue
		}
		// Entries with a label selector need their own factory: the
		// shared clusterFactory's TweakListOptions is global, not
		// per-GVR. Selector-less entries share the cluster factory.
		f := clusterFactory
		if w.LabelSelector != "" {
			selector := w.LabelSelector
			f = dynamicinformer.NewFilteredDynamicSharedInformerFactory(s.Dyn, 0, metav1.NamespaceAll,
				func(opts *metav1.ListOptions) {
					if s.TweakListOptions != nil {
						s.TweakListOptions(opts)
					}
					opts.LabelSelector = selector
				})
			factories = append(factories, f)
		}
		inf := f.ForResource(w.GVR).Informer()
		stops = append(stops, qualifiedInformer{inf, true})
	}

	for _, qi := range stops {
		if _, err := qi.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj any) { s.emit(ctx, obj, qi.qualify) },
			UpdateFunc: func(_, obj any) { s.emit(ctx, obj, qi.qualify) },
			DeleteFunc: func(obj any) { s.emitDelete(ctx, obj, qi.qualify) },
		}); err != nil {
			return fmt.Errorf("status: register handler: %w", err)
		}
	}

	for _, f := range factories {
		f.Start(ctx.Done())
	}
	syncs := []cache.InformerSynced{}
	for _, qi := range stops {
		syncs = append(syncs, qi.HasSynced)
	}
	if !cache.WaitForCacheSync(ctx.Done(), syncs...) {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("status: cache sync aborted")
	}
	<-ctx.Done()
	return nil
}

// filteredWatches applies the optional discovery filter: watches whose API
// the cluster does not serve are skipped with one log line each (same
// semantics as capability.FilterAvailable) so absent optional CRDs —
// e.g. the identity.inari.io claims or cert-manager where not installed —
// don't spin reflector error loops. Without a Discovery client the watches
// pass through unfiltered.
func (s *Streamer) filteredWatches() []WatchResource {
	if s.Discovery == nil {
		return s.Watches
	}
	// Cache per GVR, not per group/version: a group/version may be
	// served while an individual resource in it is not (partial CRD
	// install). Startup discovery calls stay bounded by the watch count.
	checked := map[schema.GroupVersionResource]bool{}
	out := make([]WatchResource, 0, len(s.Watches))
	for _, w := range s.Watches {
		avail, known := checked[w.GVR]
		if !known {
			avail = capability.ResourceServed(s.Discovery, w.GVR)
			checked[w.GVR] = avail
		}
		if !avail {
			s.Log.Info("status watch skipped: API not installed on this cluster", "gvr", w.GVR.String())
			continue
		}
		out = append(out, w)
	}
	return out
}

// qualifiedInformer pairs an informer with whether its objects stream the
// group-qualified ResourceRef kind ("<Kind>.<group>"). Configured watch
// resources qualify (decision D-A); Applications and KRO InstanceGVRs keep
// bare kinds — the server's existing contract for those is unqualified.
type qualifiedInformer struct {
	cache.SharedIndexInformer
	qualify bool
}

// emit maps one object to a StatusUpdate and sends it when changed. The
// dedupe entry is updated only after a successful send so a transient
// failure is retried on the next informer event.
func (s *Streamer) emit(ctx context.Context, obj any, qualify bool) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	upd, key := s.mapFor(u, qualify)
	sum := checksum(upd)
	s.mu.Lock()
	if s.lastSum[key] == sum {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	if err := s.send(ctx, key, upd); err != nil {
		return
	}
	s.mu.Lock()
	s.lastSum[key] = sum
	s.mu.Unlock()
}

// emitDelete streams a terminal missing-status update for a removed
// resource and prunes its dedupe entry so a later re-add re-emits.
func (s *Streamer) emitDelete(ctx context.Context, obj any, qualify bool) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	kind := u.GetKind()
	if qualify {
		kind = qualifiedKind(u)
	}
	key := fmt.Sprintf("%s/%s/%s/%s", kind, u.GetNamespace(), u.GetName(), u.GetUID())
	upd := &agentv1.StatusUpdate{
		Resource: &agentv1.ResourceRef{
			Kind:      kind,
			Name:      u.GetName(),
			Namespace: u.GetNamespace(),
			Uid:       string(u.GetUID()),
		},
		Health:     agentv1.HealthStatus_HEALTH_STATUS_MISSING,
		Sync:       agentv1.SyncState_SYNC_STATE_UNSPECIFIED,
		Message:    "resource deleted",
		ObservedAt: timestamppb.Now(),
	}
	s.mu.Lock()
	delete(s.lastSum, key)
	s.mu.Unlock()
	_ = s.send(ctx, key, upd)
}

// send marshals and sends one StatusUpdate, logging failures.
func (s *Streamer) send(ctx context.Context, key string, upd *agentv1.StatusUpdate) error {
	payload, err := anypb.New(upd)
	if err != nil {
		s.Log.Error(err, "marshal status update")
		return err
	}
	ev := &agentv1.Event{
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_STATUS_UPDATE),
		Payload: payload,
	}
	if err := s.Sender.Send(ctx, ev); err != nil {
		s.Log.Error(err, "send status update", "resource", key)
		return err
	}
	return nil
}

// mapFor converts a watched object into a StatusUpdate via the generic
// mapper (Ready-condition and health/sync fallbacks), which covers the
// server-rendered platform resources (claims, KRO instances, native
// manifests all carry a Ready condition or ArgoCD health/sync).
func (s *Streamer) mapFor(u *unstructured.Unstructured, qualify bool) (*agentv1.StatusUpdate, string) {
	kind := u.GetKind()
	if qualify {
		kind = qualifiedKind(u)
	}
	return mapStatus(u, kind)
}

// mapStatus converts an Application, KRO instance, or configured native
// watch resource into a StatusUpdate. kind is the ResourceRef kind the
// server routes on (bare for Applications/KRO instances, group-qualified
// for configured watch resources).
func mapStatus(u *unstructured.Unstructured, kind string) (*agentv1.StatusUpdate, string) {
	ref := &agentv1.ResourceRef{
		Kind:      kind,
		Name:      u.GetName(),
		Namespace: u.GetNamespace(),
		Uid:       string(u.GetUID()),
	}
	health, hmsg := healthOf(u)
	sync, smsg := syncOf(u)
	msg := strings.TrimSpace(strings.Join([]string{hmsg, smsg}, " "))
	// ArgoCD comparison/render errors (missing CRD, unreadable repo, bad
	// manifest) live in .status.conditions, not health/sync: surface them so
	// the control plane shows why delivery is stuck.
	if u.GetKind() == "Application" {
		if condMsg := comparisonErrorOf(u); condMsg != "" {
			msg = strings.TrimSpace(msg + " " + condMsg)
		}
	}
	key := fmt.Sprintf("%s/%s/%s/%s", kind, u.GetNamespace(), u.GetName(), u.GetUID())
	return &agentv1.StatusUpdate{
		Resource:      ref,
		Health:        health,
		Sync:          sync,
		Message:       msg,
		ObservedAt:    timestamppb.Now(),
		StateChecksum: "",
	}, key
}

// comparisonErrorOf extracts the first error-type condition message
// (ComparisonError / SharedResourceWarning with error text) from an ArgoCD
// Application; empty when the app renders fine.
func comparisonErrorOf(u *unstructured.Unstructured) string {
	conditions, found, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	if !found {
		return ""
	}
	for _, c := range conditions {
		cm, _ := c.(map[string]interface{})
		typ, _ := cm["type"].(string)
		if typ != "ComparisonError" && typ != "SyncError" {
			continue
		}
		msg, _ := cm["message"].(string)
		if msg != "" {
			return typ + ": " + msg
		}
	}
	return ""
}

// healthOf reads .status.health.status (Applications) or falls back to
// KRO-style Ready conditions for instance resources.
func healthOf(u *unstructured.Unstructured) (agentv1.HealthStatus, string) {
	if hs, found, _ := unstructured.NestedString(u.Object, "status", "health", "status"); found {
		msg, _, _ := unstructured.NestedString(u.Object, "status", "health", "message")
		switch strings.ToLower(hs) {
		case "healthy":
			return agentv1.HealthStatus_HEALTH_STATUS_HEALTHY, msg
		case "progressing":
			return agentv1.HealthStatus_HEALTH_STATUS_PROGRESSING, msg
		case "degraded":
			return agentv1.HealthStatus_HEALTH_STATUS_DEGRADED, msg
		case "suspended":
			return agentv1.HealthStatus_HEALTH_STATUS_SUSPENDED, msg
		case "missing":
			return agentv1.HealthStatus_HEALTH_STATUS_MISSING, msg
		default:
			return agentv1.HealthStatus_HEALTH_STATUS_UNKNOWN, msg
		}
	}
	conditions, found, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	if !found {
		return agentv1.HealthStatus_HEALTH_STATUS_UNKNOWN, ""
	}
	for _, c := range conditions {
		cm, _ := c.(map[string]interface{})
		if cm["type"] == "Ready" {
			msg, _ := cm["message"].(string)
			if cm["status"] == "True" {
				return agentv1.HealthStatus_HEALTH_STATUS_HEALTHY, msg
			}
			reason, _ := cm["reason"].(string)
			if reason != "" {
				return agentv1.HealthStatus_HEALTH_STATUS_PROGRESSING, msg
			}
			return agentv1.HealthStatus_HEALTH_STATUS_DEGRADED, msg
		}
	}
	return agentv1.HealthStatus_HEALTH_STATUS_UNKNOWN, ""
}

// syncOf reads .status.sync.status (Applications); other resources map
// Ready=True to Synced.
func syncOf(u *unstructured.Unstructured) (agentv1.SyncState, string) {
	if ss, found, _ := unstructured.NestedString(u.Object, "status", "sync", "status"); found {
		switch strings.ToLower(ss) {
		case "synced":
			return agentv1.SyncState_SYNC_STATE_SYNCED, ""
		case "outofsync":
			return agentv1.SyncState_SYNC_STATE_OUT_OF_SYNC, ""
		default:
			return agentv1.SyncState_SYNC_STATE_ERROR, ss
		}
	}
	conditions, found, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	if !found {
		return agentv1.SyncState_SYNC_STATE_UNSPECIFIED, ""
	}
	for _, c := range conditions {
		cm, _ := c.(map[string]interface{})
		if cm["type"] == "Ready" {
			if cm["status"] == "True" {
				return agentv1.SyncState_SYNC_STATE_SYNCED, ""
			}
			return agentv1.SyncState_SYNC_STATE_OUT_OF_SYNC, ""
		}
	}
	return agentv1.SyncState_SYNC_STATE_UNSPECIFIED, ""
}

// checksum is the dedupe key content hash.
func checksum(u *agentv1.StatusUpdate) string {
	h := sha256.New()
	h.Write([]byte(u.Resource.Kind + u.Resource.Namespace + u.Resource.Name))
	h.Write([]byte{0})
	h.Write([]byte(u.Health.String() + u.Sync.String() + u.Message))
	return hex.EncodeToString(h.Sum(nil))
}
