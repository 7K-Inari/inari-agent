// Package status streams Application health/sync and KRO instance status
// upstream as status-update events (plan §5.3 phase 5): near-real-time
// inventory without the control plane ever polling tenant APIs. Watches
// are scoped to the ArgoCD namespace and Inari-managed KRO instance GVRs
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
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/argocd"
)

// Sender accepts outbound events (stream.Client).
type Sender interface {
	Send(ctx context.Context, event *agentv1.Event) error
}

// Streamer watches ArgoCD Applications and KRO instance resources and
// emits status-update events. Emissions are deduped by content; Run emits
// a full snapshot on start so reconnects resync without control-plane
// polling.
type Streamer struct {
	Log    logr.Logger
	Dyn    dynamic.Interface
	Sender Sender
	// Namespace scopes the Application watch (ArgoCD install namespace).
	Namespace string
	// InstanceGVRs are KRO instance GVRs to watch (cluster- or
	// namespace-scoped, per the generated CRD).
	InstanceGVRs []schema.GroupVersionResource
	// TweakListOptions narrows watches further (label selectors).
	TweakListOptions func(*metav1.ListOptions)

	mu      sync.Mutex
	lastSum map[string]string
}

// NewStreamer returns a Streamer.
func NewStreamer(log logr.Logger, dyn dynamic.Interface, sender Sender, namespace string, instanceGVRs []schema.GroupVersionResource) *Streamer {
	return &Streamer{
		Log:          log,
		Dyn:          dyn,
		Sender:       sender,
		Namespace:    namespace,
		InstanceGVRs: instanceGVRs,
		lastSum:      map[string]string{},
	}
}

// Run starts the watches and blocks until ctx is cancelled.
func (s *Streamer) Run(ctx context.Context) error {
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(s.Dyn, 0, s.Namespace, s.TweakListOptions)
	appInformer := factory.ForResource(argocd.ApplicationGVR).Informer()
	stops := []cache.SharedIndexInformer{appInformer}

	clusterFactory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(s.Dyn, 0, metav1.NamespaceAll, s.TweakListOptions)
	for _, gvr := range s.InstanceGVRs {
		inf := clusterFactory.ForResource(gvr).Informer()
		stops = append(stops, inf)
	}

	for _, inf := range stops {
		if _, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj any) { s.emit(ctx, obj) },
			UpdateFunc: func(_, obj any) { s.emit(ctx, obj) },
		}); err != nil {
			return fmt.Errorf("status: register handler: %w", err)
		}
	}

	factory.Start(ctx.Done())
	clusterFactory.Start(ctx.Done())
	syncs := []cache.InformerSynced{}
	for _, inf := range stops {
		syncs = append(syncs, inf.HasSynced)
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

// emit maps one object to a StatusUpdate and sends it when changed.
func (s *Streamer) emit(ctx context.Context, obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	upd, key := mapStatus(u)
	sum := checksum(upd)
	s.mu.Lock()
	if s.lastSum[key] == sum {
		s.mu.Unlock()
		return
	}
	s.lastSum[key] = sum
	s.mu.Unlock()

	payload, err := anypb.New(upd)
	if err != nil {
		s.Log.Error(err, "marshal status update")
		return
	}
	ev := &agentv1.Event{
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_STATUS_UPDATE),
		Payload: payload,
	}
	if err := s.Sender.Send(ctx, ev); err != nil {
		s.Log.Error(err, "send status update", "resource", key)
	}
}

// mapStatus converts an Application or KRO instance into a StatusUpdate.
func mapStatus(u *unstructured.Unstructured) (*agentv1.StatusUpdate, string) {
	ref := &agentv1.ResourceRef{
		Kind:      u.GetKind(),
		Name:      u.GetName(),
		Namespace: u.GetNamespace(),
		Uid:       string(u.GetUID()),
	}
	health, hmsg := healthOf(u)
	sync, smsg := syncOf(u)
	msg := strings.TrimSpace(strings.Join([]string{hmsg, smsg}, " "))
	key := fmt.Sprintf("%s/%s/%s/%s", u.GetKind(), u.GetNamespace(), u.GetName(), u.GetUID())
	return &agentv1.StatusUpdate{
		Resource:      ref,
		Health:        health,
		Sync:          sync,
		Message:       msg,
		ObservedAt:    timestamppb.Now(),
		StateChecksum: "",
	}, key
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
