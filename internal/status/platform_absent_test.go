package status

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/argocd"
)

// When the platform.inari.io CRDs are not installed (tenant clusters — only
// the platform cluster runs inari-operator), platform lists fail with
// NotFound. Application and KRO status emission must continue: each shared
// informer delivers events once its own cache syncs, independently of the
// failing platform reflectors.
func TestAbsentPlatformCRDsDoNotBlockApplicationEmission(t *testing.T) {
	listKinds := map[schema.GroupVersionResource]string{
		argocd.ApplicationGVR: "ApplicationList",
	}
	for _, gvr := range PlatformGVRs {
		listKinds[gvr] = gvr.Resource + "List"
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(), listKinds, app("my-web", "Healthy", "Synced"))
	// Simulate a cluster without the CRDs: platform lists return NotFound.
	dyn.PrependReactor("list", "*", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetResource().Group == PlatformGroup {
			return true, nil, apierrors.NewNotFound(action.GetResource().GroupResource(), "")
		}
		return false, nil, nil
	})
	sender := &captureSender{mu: make(chan struct{}, 8)}
	s := NewStreamer(logr.Discard(), dyn, sender, "argocd", nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Run(ctx) }()

	select {
	case <-sender.mu:
	case <-time.After(5 * time.Second):
		t.Fatal("no Application status emitted when platform CRDs are absent")
	}
	upd := lastStatus(t, sender.events[0])
	if upd.Resource.Kind != "Application" || upd.Health != agentv1.HealthStatus_HEALTH_STATUS_HEALTHY {
		t.Fatalf("unexpected update %+v", upd)
	}
}
