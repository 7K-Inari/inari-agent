package status

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

func platformObj(kind, name, ns string, conditions ...interface{}) *unstructured.Unstructured {
	obj := map[string]interface{}{
		"apiVersion": "platform.inari.io/v1alpha1",
		"kind":       kind,
		"metadata":   map[string]interface{}{"name": name, "namespace": ns, "uid": "uid-" + name},
	}
	if conditions != nil {
		obj["status"] = map[string]interface{}{"conditions": conditions}
	}
	return &unstructured.Unstructured{Object: obj}
}

func cond(typ, status, message string) interface{} {
	return map[string]interface{}{"type": typ, "status": status, "message": message}
}

func TestMapPlatformStatus(t *testing.T) {
	tests := []struct {
		name        string
		obj         *unstructured.Unstructured
		wantHealth  agentv1.HealthStatus
		wantSync    agentv1.SyncState
		wantMessage string
	}{
		{
			name:        "ready",
			obj:         platformObj("KeycloakRealm", "acme", "tenant-acme", cond("Ready", "True", "realm provisioned")),
			wantHealth:  agentv1.HealthStatus_HEALTH_STATUS_HEALTHY,
			wantSync:    agentv1.SyncState_SYNC_STATE_SYNCED,
			wantMessage: "realm provisioned",
		},
		{
			name:        "reconciling",
			obj:         platformObj("KeycloakClient", "acme-app", "tenant-acme", cond("Reconciling", "True", "creating client")),
			wantHealth:  agentv1.HealthStatus_HEALTH_STATUS_PROGRESSING,
			wantSync:    agentv1.SyncState_SYNC_STATE_UNSPECIFIED,
			wantMessage: "creating client",
		},
		{
			name:        "failed",
			obj:         platformObj("DNSZone", "acme-zone", "tenant-acme", cond("Failed", "True", "provider rejected zone")),
			wantHealth:  agentv1.HealthStatus_HEALTH_STATUS_DEGRADED,
			wantSync:    agentv1.SyncState_SYNC_STATE_UNSPECIFIED,
			wantMessage: "provider rejected zone",
		},
		{
			name:        "ready wins over stale reconciling",
			obj:         platformObj("DNSRecord", "www", "tenant-acme", cond("Reconciling", "False", "stale"), cond("Ready", "True", "synced")),
			wantHealth:  agentv1.HealthStatus_HEALTH_STATUS_HEALTHY,
			wantSync:    agentv1.SyncState_SYNC_STATE_SYNCED,
			wantMessage: "synced",
		},
		{
			name:        "no conditions",
			obj:         platformObj("TenantNamespace", "acme", "tenant-acme"),
			wantHealth:  agentv1.HealthStatus_HEALTH_STATUS_UNKNOWN,
			wantSync:    agentv1.SyncState_SYNC_STATE_UNSPECIFIED,
			wantMessage: "",
		},
		{
			name:        "ready false without failed",
			obj:         platformObj("KeycloakRealm", "acme", "tenant-acme", cond("Ready", "False", "waiting")),
			wantHealth:  agentv1.HealthStatus_HEALTH_STATUS_UNKNOWN,
			wantSync:    agentv1.SyncState_SYNC_STATE_UNSPECIFIED,
			wantMessage: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upd, key := mapPlatformStatus(tt.obj)
			if upd.Health != tt.wantHealth {
				t.Errorf("health = %v, want %v", upd.Health, tt.wantHealth)
			}
			if upd.Sync != tt.wantSync {
				t.Errorf("sync = %v, want %v", upd.Sync, tt.wantSync)
			}
			if upd.Message != tt.wantMessage {
				t.Errorf("message = %q, want %q", upd.Message, tt.wantMessage)
			}
			wantKind := tt.obj.GetKind() + ".platform.inari.io"
			if upd.Resource.Kind != wantKind {
				t.Errorf("kind = %q, want %q", upd.Resource.Kind, wantKind)
			}
			if upd.Resource.Name != tt.obj.GetName() || upd.Resource.Namespace != tt.obj.GetNamespace() || upd.Resource.Uid != "uid-"+tt.obj.GetName() {
				t.Errorf("ref %+v", upd.Resource)
			}
			if upd.ObservedAt == nil {
				t.Error("ObservedAt must be set")
			}
			wantKey := wantKind + "/" + tt.obj.GetNamespace() + "/" + tt.obj.GetName() + "/uid-" + tt.obj.GetName()
			if key != wantKey {
				t.Errorf("key = %q, want %q", key, wantKey)
			}
		})
	}
}

func TestIsPlatformGroup(t *testing.T) {
	if !isPlatformGroup(platformObj("DNSZone", "z", "ns")) {
		t.Error("platform.inari.io object must be detected")
	}
	if isPlatformGroup(app("my-web", "Healthy", "Synced")) {
		t.Error("Application must not be detected as platform")
	}
}
