package command

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/argocd"
)

type fakeGate struct {
	ns  string
	err error
}

func (f fakeGate) EnsureReady(context.Context) (string, error) { return f.ns, f.err }

func registerEvent(t *testing.T, m *agentv1.RegisterArgoCDApp) *agentv1.Event {
	t.Helper()
	a, err := anypb.New(m)
	if err != nil {
		t.Fatal(err)
	}
	return &agentv1.Event{Type: agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_REGISTER_ARGOCD_APP), Payload: a}
}

func TestRegisterArgoCDAppHandler(t *testing.T) {
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	h := RegisterArgoCDAppHandler(fakeGate{ns: "argocd"}, func(ns string) *argocd.Client {
		return argocd.NewClient(dyn, ns)
	})
	d := NewDispatcher()
	d.Register(agentv1.EventType_EVENT_TYPE_REGISTER_ARGOCD_APP, h)

	m := &agentv1.RegisterArgoCDApp{
		CommandId:            "cmd-reg1",
		Name:                 "my-web",
		Project:              "team-a",
		Source:               &agentv1.ApplicationSource{RepoUrl: "https://git/org/acme-inari-state", Path: "my-web"},
		DestinationNamespace: "apps",
		SyncPolicy:           &agentv1.SyncPolicy{Automated: true},
	}
	ack, err := d.HandleEvent(context.Background(), registerEvent(t, m))
	if err != nil {
		t.Fatal(err)
	}
	if ack.Result != agentv1.CommandResult_COMMAND_RESULT_APPLIED {
		t.Fatalf("ack %+v", ack)
	}
	app, err := dyn.Resource(argocd.ApplicationGVR).Namespace("argocd").Get(context.Background(), "my-web", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if app.GetLabels()[argocd.ManagedByLabel] != argocd.ManagedByValue {
		t.Fatalf("labels %v", app.GetLabels())
	}
	if _, err := dyn.Resource(argocd.AppProjectGVR).Namespace("argocd").Get(context.Background(), "team-a", metav1.GetOptions{}); err != nil {
		t.Fatalf("project not created: %v", err)
	}

	// Replay: recorded ack, no re-execution.
	ack2, err := d.HandleEvent(context.Background(), registerEvent(t, m))
	if err != nil {
		t.Fatal(err)
	}
	if ack2.Message != ack.Message || d.HandledCount() != 1 {
		t.Fatalf("replay %+v handled=%d", ack2, d.HandledCount())
	}
}

func TestRegisterArgoCDAppHandlerGateRefusal(t *testing.T) {
	h := RegisterArgoCDAppHandler(fakeGate{err: errors.New("out of supported skew")}, func(ns string) *argocd.Client {
		return argocd.NewClient(dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), ns)
	})
	result, msg, err := h(context.Background(), registerEvent(t, &agentv1.RegisterArgoCDApp{
		CommandId: "cmd-reg2", Name: "x", Source: &agentv1.ApplicationSource{RepoUrl: "https://git/o/r"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if result != agentv1.CommandResult_COMMAND_RESULT_FAILED || msg == "" {
		t.Fatalf("gate refusal must surface as FAILED with reason: %v %q", result, msg)
	}
}
