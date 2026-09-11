// Platform-CRD status reporting (M3/M7): the five platform.inari.io/v1alpha1
// kinds reconciled by inari-operator on the platform cluster are watched
// cluster-wide and mapped from their Ready/Reconciling/Failed conditions
// onto the generic StatusUpdate. The server routes these updates by the
// ".platform.inari.io" kind suffix on ResourceRef.
package status

import (
	"fmt"

	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

// PlatformGroup is the API group of the inari-operator platform CRDs.
const PlatformGroup = "platform.inari.io"

// PlatformGVRs are the platform-cluster CRDs the status streamer watches.
// Unlike KRO instance GVRs these are platform-fixed (not tenant-discovered),
// so they are a package-level list rather than streamer configuration.
var PlatformGVRs = []schema.GroupVersionResource{
	{Group: PlatformGroup, Version: "v1alpha1", Resource: "keycloakrealms"},
	{Group: PlatformGroup, Version: "v1alpha1", Resource: "keycloakclients"},
	{Group: PlatformGroup, Version: "v1alpha1", Resource: "dnszones"},
	{Group: PlatformGroup, Version: "v1alpha1", Resource: "dnsrecords"},
	{Group: PlatformGroup, Version: "v1alpha1", Resource: "tenantnamespaces"},
}

// isPlatformGroup reports whether u belongs to the platform API group.
func isPlatformGroup(u *unstructured.Unstructured) bool {
	return u.GroupVersionKind().Group == PlatformGroup
}

// platformKind is the ResourceRef kind the server routes on:
// "<Kind>.platform.inari.io".
func platformKind(u *unstructured.Unstructured) string {
	return u.GetKind() + "." + PlatformGroup
}

// mapPlatformStatus converts a platform.inari.io object into a StatusUpdate
// from its status conditions (api/v1alpha1/shared_types.go in
// inari-operator): Ready=True → HEALTHY, Reconciling=True → PROGRESSING,
// Failed=True → DEGRADED; the mapped condition's message is passed through.
// The operator keeps exactly one of the three True at a time; precedence
// Ready > Reconciling > Failed guards against transient overlaps.
func mapPlatformStatus(u *unstructured.Unstructured) (*agentv1.StatusUpdate, string) {
	health, sync, msg := platformConditionOf(u)
	kind := platformKind(u)
	key := fmt.Sprintf("%s/%s/%s/%s", kind, u.GetNamespace(), u.GetName(), u.GetUID())
	return &agentv1.StatusUpdate{
		Resource: &agentv1.ResourceRef{
			Kind:      kind,
			Name:      u.GetName(),
			Namespace: u.GetNamespace(),
			Uid:       string(u.GetUID()),
		},
		Health:        health,
		Sync:          sync,
		Message:       msg,
		ObservedAt:    timestamppb.Now(),
		StateChecksum: "",
	}, key
}

// platformConditionOf reads .status.conditions for the platform condition
// triplet. Platform conditions carry no sync semantics, so Sync is SYNCED
// only while Ready (consistent with the existing Ready mapping) and
// UNSPECIFIED otherwise.
func platformConditionOf(u *unstructured.Unstructured) (agentv1.HealthStatus, agentv1.SyncState, string) {
	conditions, found, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	if !found {
		return agentv1.HealthStatus_HEALTH_STATUS_UNKNOWN, agentv1.SyncState_SYNC_STATE_UNSPECIFIED, ""
	}
	trueCondition := func(typ string) (string, bool) {
		for _, c := range conditions {
			cm, _ := c.(map[string]interface{})
			if cm["type"] == typ && cm["status"] == "True" {
				msg, _ := cm["message"].(string)
				return msg, true
			}
		}
		return "", false
	}
	if msg, ok := trueCondition("Ready"); ok {
		return agentv1.HealthStatus_HEALTH_STATUS_HEALTHY, agentv1.SyncState_SYNC_STATE_SYNCED, msg
	}
	if msg, ok := trueCondition("Reconciling"); ok {
		return agentv1.HealthStatus_HEALTH_STATUS_PROGRESSING, agentv1.SyncState_SYNC_STATE_UNSPECIFIED, msg
	}
	if msg, ok := trueCondition("Failed"); ok {
		return agentv1.HealthStatus_HEALTH_STATUS_DEGRADED, agentv1.SyncState_SYNC_STATE_UNSPECIFIED, msg
	}
	return agentv1.HealthStatus_HEALTH_STATUS_UNKNOWN, agentv1.SyncState_SYNC_STATE_UNSPECIFIED, ""
}
