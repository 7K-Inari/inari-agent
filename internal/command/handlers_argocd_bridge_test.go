package command

import (
	"context"
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/argocd"
)

const testBridgeAppSetManifest = `apiVersion: argoproj.io/v1alpha1
kind: ApplicationSet
metadata:
  name: inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1
spec:
  goTemplate: false
  generators:
    - list:
        elements:
          - cluster: prod-1
  template:
    metadata:
      name: inari-addon-01j5z8k2n4xqj4tvdmx7a3b2c1-{{cluster}}
    spec:
      project: default
      source:
        repoURL: https://charts.jetstack.io
        chart: cert-manager
      destination:
        server: https://kubernetes.default.svc
        namespace: cert-manager
      syncPolicy:
        automated:
          prune: false
          selfHeal: true
        syncOptions:
          - TakeOwnership=true
`

const testBridgeAppManifest = `apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: inari-addon-01j5z8k2n4xqj4tvdmx7a3b2c1
spec:
  project: default
  source:
    repoURL: https://charts.jetstack.io
    chart: cert-manager
    targetRevision: v1.16.2
  destination:
    server: https://kubernetes.default.svc
    namespace: cert-manager
  syncPolicy:
    automated:
      prune: false
      selfHeal: true
    syncOptions:
      - TakeOwnership=true
`

func bridgeEvent(t *testing.T, typeStr, commandID, name, manifest string) *agentv1.Event {
	t.Helper()
	return commandEvent(t, commandID, bridgePayload(t, map[string]interface{}{
		"commandId": commandID,
		"name":      name,
		"namespace": "argocd",
		"manifest":  manifest,
	}), typeStr)
}

func TestRegisterArgoCDAppSetHandler(t *testing.T) {
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	h := RegisterArgoCDAppSetHandler(fakeGate{ns: "argocd"}, func(ns string) *argocd.Client {
		return argocd.NewClient(dyn, ns)
	})
	d := NewDispatcher()
	d.RegisterBridge(RegisterArgoCDAppSetEventType, h)

	ev := bridgeEvent(t, RegisterArgoCDAppSetEventType, "inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1", "inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1", testBridgeAppSetManifest)
	ack, err := d.HandleEvent(context.Background(), ev)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Result != agentv1.CommandResult_COMMAND_RESULT_APPLIED {
		t.Fatalf("ack %+v", ack)
	}
	if ack.CommandId != "inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1" {
		t.Errorf("ack command id = %q", ack.CommandId)
	}
	obj, err := dyn.Resource(argocd.ApplicationSetGVR).Namespace("argocd").Get(context.Background(), "inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if obj.GetLabels()[argocd.ManagedByLabel] != argocd.ManagedByValue {
		t.Errorf("labels %v", obj.GetLabels())
	}

	// Replay: recorded ack, no re-execution.
	if _, err := d.HandleEvent(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	if d.HandledCount() != 1 {
		t.Fatalf("replay re-executed: handled=%d", d.HandledCount())
	}
}

func TestRegisterArgoCDAppManifestHandler(t *testing.T) {
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	h := RegisterArgoCDAppManifestHandler(fakeGate{ns: "argocd"}, func(ns string) *argocd.Client {
		return argocd.NewClient(dyn, ns)
	})
	d := NewDispatcher()
	d.RegisterBridge(RegisterArgoCDAppManifestEventType, h)

	ack, err := d.HandleEvent(context.Background(),
		bridgeEvent(t, RegisterArgoCDAppManifestEventType, "inari-addon-01j5z8k2n4xqj4tvdmx7a3b2c1", "inari-addon-01j5z8k2n4xqj4tvdmx7a3b2c1", testBridgeAppManifest))
	if err != nil {
		t.Fatal(err)
	}
	if ack.Result != agentv1.CommandResult_COMMAND_RESULT_APPLIED {
		t.Fatalf("ack %+v", ack)
	}
	obj, err := dyn.Resource(argocd.ApplicationGVR).Namespace("argocd").Get(context.Background(), "inari-addon-01j5z8k2n4xqj4tvdmx7a3b2c1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := obj.MarshalJSON()
	if !strings.Contains(string(raw), "TakeOwnership=true") {
		t.Error("take-ownership sync option lost")
	}
}

func TestRegisterBridgeHandlerGateRefusal(t *testing.T) {
	h := RegisterArgoCDAppSetHandler(fakeGate{err: errors.New("out of supported skew")}, func(ns string) *argocd.Client {
		return argocd.NewClient(dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), ns)
	})
	d := NewDispatcher()
	d.RegisterBridge(RegisterArgoCDAppSetEventType, h)
	ack, err := d.HandleEvent(context.Background(),
		bridgeEvent(t, RegisterArgoCDAppSetEventType, "cmd-gate", "inari-addonset-x", testBridgeAppSetManifest))
	if err != nil {
		t.Fatal(err)
	}
	if ack.Result != agentv1.CommandResult_COMMAND_RESULT_FAILED || !strings.Contains(ack.Message, "out of supported skew") {
		t.Fatalf("gate refusal must NACK, got %+v", ack)
	}
}
