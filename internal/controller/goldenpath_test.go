package controller

// Golden-path integration test (M2 exit contribution): a deploy request
// lands as manifests in the tenant state repo, the ArgoCD app registers,
// and health streams back upstream — all over fakes, no cluster needed.

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/argocd"
	"github.com/7K-Inari/inari-agent/internal/command"
	"github.com/7K-Inari/inari-agent/internal/git"
	"github.com/7K-Inari/inari-agent/internal/status"

	"github.com/go-logr/logr"
)

type goldenGate struct{}

func (goldenGate) EnsureReady(context.Context) (string, error) { return "argocd", nil }

type goldenSender struct {
	events chan *agentv1.Event
}

func (s *goldenSender) Send(_ context.Context, ev *agentv1.Event) error {
	s.events <- ev
	return nil
}

func TestGoldenPath(t *testing.T) {
	ctx := context.Background()

	// Cluster fixtures: the RGD the render resolves, and (later) the
	// registered Application reporting Healthy.
	rgd := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "kro.run/v1alpha1",
		"kind":       "ResourceGraphDefinition",
		"metadata":   map[string]interface{}{"name": "web-service"},
		"spec": map[string]interface{}{
			"schema": map[string]interface{}{
				"apiVersion": "v1alpha1",
				"kind":       "WebService",
				"spec":       map[string]interface{}{"required": []interface{}{"image"}},
			},
		},
	}}
	listKinds := map[schema.GroupVersionResource]string{
		argocd.ApplicationGVR: "ApplicationList",
		{Group: "kro.run", Version: "v1alpha1", Resource: "resourcegraphdefinitions"}: "ResourceGraphDefinitionList",
	}
	// The status streamer watches the platform CRDs; the fake client needs
	// a registered list kind per GVR.
	for _, gvr := range status.PlatformGVRs {
		listKinds[gvr] = gvr.Resource + "List"
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, rgd)
	kube := k8sfake.NewSimpleClientset()
	gp := git.NewMemProvider()

	// Dispatcher with the real handlers.
	cfg := &GitOpsConfig{
		Kube:             kube,
		Dyn:              dyn,
		Git:              gp,
		StateRepo:        "org/acme-inari-state",
		JournalNamespace: "inari-system",
	}
	d := command.NewDispatcher()
	if err := cfg.configure(ctx, d, "acme"); err != nil {
		t.Fatal(err)
	}
	d.Register(agentv1.EventType_EVENT_TYPE_REGISTER_ARGOCD_APP,
		command.RegisterArgoCDAppHandler(goldenGate{}, func(ns string) *argocd.Client {
			return argocd.NewClient(dyn, ns)
		}))

	// 1. Deploy request: render-rgd-instance → manifests in state repo.
	params, _ := structpb.NewStruct(map[string]interface{}{"image": "nginx:1.27"})
	renderPayload, _ := anypb.New(&agentv1.RenderRgdInstance{
		CommandId:       "deploy-1",
		RgdRef:          "web-service",
		InstanceName:    "my-web",
		TargetNamespace: "apps",
		Parameters:      params,
		Policy:          agentv1.CommitPolicy_COMMIT_POLICY_DIRECT_COMMIT,
	})
	ack, err := d.HandleEvent(ctx, &agentv1.Event{
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_RENDER_RGD_INSTANCE),
		Payload: renderPayload,
	})
	if err != nil || ack.Result != agentv1.CommandResult_COMMAND_RESULT_APPLIED {
		t.Fatalf("render: %+v %v", ack, err)
	}
	if len(gp.Commits) != 1 || gp.Commits[0].Target.Repo != "org/acme-inari-state" {
		t.Fatalf("state repo commit missing: %+v", gp.Commits)
	}

	// 2. register-argocd-app pointing at the rendered path.
	regPayload, _ := anypb.New(&agentv1.RegisterArgoCDApp{
		CommandId: "deploy-1-reg",
		Name:      "my-web",
		Project:   "team-a",
		Source: &agentv1.ApplicationSource{
			RepoUrl: "https://github.com/org/acme-inari-state",
			Path:    "my-web",
		},
		DestinationNamespace: "apps",
		SyncPolicy:           &agentv1.SyncPolicy{Automated: true},
	})
	ack, err = d.HandleEvent(ctx, &agentv1.Event{
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_REGISTER_ARGOCD_APP),
		Payload: regPayload,
	})
	if err != nil || ack.Result != agentv1.CommandResult_COMMAND_RESULT_APPLIED {
		t.Fatalf("register: %+v %v", ack, err)
	}
	if _, err := dyn.Resource(argocd.ApplicationGVR).Namespace("argocd").Get(ctx, "my-web", metav1.GetOptions{}); err != nil {
		t.Fatalf("application not registered: %v", err)
	}

	// 3. Convergence: Application reports Healthy+Synced; the streamer
	// emits it upstream.
	app := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "argoproj.io/v1alpha1",
		"kind":       "Application",
		"metadata": map[string]interface{}{
			"name": "my-web", "namespace": "argocd", "uid": "u1",
			"labels": map[string]interface{}{argocd.ManagedByLabel: argocd.ManagedByValue},
		},
		"spec": map[string]interface{}{"project": "team-a"},
		"status": map[string]interface{}{
			"health": map[string]interface{}{"status": "Healthy"},
			"sync":   map[string]interface{}{"status": "Synced"},
		},
	}}
	if _, err := dyn.Resource(argocd.ApplicationGVR).Namespace("argocd").UpdateStatus(ctx, app, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	sender := &goldenSender{events: make(chan *agentv1.Event, 4)}
	streamer := status.NewStreamer(logr.Discard(), dyn, sender, "argocd", nil)
	sctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	errCh := make(chan error, 1)
	go func() { errCh <- streamer.Run(sctx) }()

	select {
	case ev := <-sender.events:
		var upd agentv1.StatusUpdate
		if err := ev.Payload.UnmarshalTo(&upd); err != nil {
			t.Fatal(err)
		}
		if upd.Resource.Name != "my-web" ||
			upd.Health != agentv1.HealthStatus_HEALTH_STATUS_HEALTHY ||
			upd.Sync != agentv1.SyncState_SYNC_STATE_SYNCED {
			t.Fatalf("status update name=%s health=%v sync=%v", upd.Resource.Name, upd.Health, upd.Sync)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no status-update streamed upstream")
	}
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("streamer: %v", err)
	}

	// Journal durability: a fresh dispatcher over the same journal
	// replays without re-executing.
	d2 := command.NewDispatcher()
	if err := d2.AttachJournal(ctx, command.NewConfigMapJournal(kube, "inari-system", "inari-agent-command-journal")); err != nil {
		t.Fatal(err)
	}
	ack, err = d2.HandleEvent(ctx, &agentv1.Event{
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_RENDER_RGD_INSTANCE),
		Payload: renderPayload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ack.Result != agentv1.CommandResult_COMMAND_RESULT_APPLIED || d2.HandledCount() != 0 {
		t.Fatalf("replay: %+v handled=%d", ack, d2.HandledCount())
	}
}
