package status

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestMapStatusIncludesObservedRevisionAndOperation(t *testing.T) {
	u := app("my-web", "Healthy", "Synced")
	_ = unstructured.SetNestedField(u.Object, "0123456789abcdef0123456789abcdef01234567", "status", "sync", "revision")
	_ = unstructured.SetNestedField(u.Object, "Succeeded", "status", "operationState", "phase")
	_ = unstructured.SetNestedField(u.Object, "2026-11-21T01:00:00Z", "status", "operationState", "startedAt")
	_ = unstructured.SetNestedField(u.Object, "2026-11-21T01:01:00Z", "status", "operationState", "finishedAt")
	_ = unstructured.SetNestedField(u.Object, "0123456789abcdef0123456789abcdef01234567", "status", "operationState", "operation", "sync", "revision")
	update, _ := mapStatus(u, "Application")
	if update.ObservedRevision != "0123456789abcdef0123456789abcdef01234567" || update.SyncOperationPhase != "Succeeded" || update.SyncOperationId == "" {
		t.Fatalf("revision/operation identity missing: %#v", update)
	}
	if update.OperationFinishedAt == nil || update.OperationFinishedAt.AsTime().UTC() != time.Date(2026, 11, 21, 1, 1, 0, 0, time.UTC) {
		t.Fatalf("operation finishedAt mismatch: %#v", update.OperationFinishedAt)
	}
	again, _ := mapStatus(u, "Application")
	if again.SyncOperationId != update.SyncOperationId {
		t.Fatalf("operation ID is not deterministic: %q vs %q", update.SyncOperationId, again.SyncOperationId)
	}
}
