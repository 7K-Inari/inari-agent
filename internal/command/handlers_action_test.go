package command

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/argocd"
)

func managedApp(name string, managed bool) *unstructured.Unstructured {
	labels := map[string]interface{}{}
	if managed {
		labels[argocd.ManagedByLabel] = argocd.ManagedByValue
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "argoproj.io/v1alpha1",
		"kind":       "Application",
		"metadata":   map[string]interface{}{"name": name, "namespace": "argocd", "labels": labels},
	}}
}

func invokeEvent(t *testing.T, m *agentv1.InvokeAction) *agentv1.Event {
	t.Helper()
	a, err := anypb.New(m)
	if err != nil {
		t.Fatal(err)
	}
	return &agentv1.Event{Type: agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_INVOKE_ACTION), Payload: a}
}

func TestInvokeActionSync(t *testing.T) {
	var gotAuth, gotPath, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		gotMethod = r.Method
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), managedApp("my-web", true))
	h := InvokeActionHandler(InvokeActionDeps{
		API:       &argocd.APIClient{BaseURL: srv.URL, Token: "tok", Timeout: 5 * time.Second},
		Dyn:       dyn,
		Namespace: "argocd",
	})
	d := NewDispatcher()
	d.Register(agentv1.EventType_EVENT_TYPE_INVOKE_ACTION, h)

	ack, err := d.HandleEvent(context.Background(), invokeEvent(t, &agentv1.InvokeAction{
		CommandId: "cmd-i1",
		Action:    "sync",
		Resource:  &agentv1.ResourceRef{Kind: "Application", Name: "my-web"},
		Timeout:   durationpb.New(30 * time.Second),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if ack.Result != agentv1.CommandResult_COMMAND_RESULT_APPLIED {
		t.Fatalf("ack %+v", ack)
	}
	if gotMethod != http.MethodPost || gotPath != "/api/v1/applications/my-web/sync" {
		t.Fatalf("call %s %s", gotMethod, gotPath)
	}
	if gotAuth != "Bearer tok" {
		t.Fatalf("auth header %q", gotAuth)
	}
}

func TestInvokeActionAllowListAndScoping(t *testing.T) {
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(),
		managedApp("managed", true), managedApp("foreign", false))
	h := InvokeActionHandler(InvokeActionDeps{
		API:       &argocd.APIClient{BaseURL: "http://127.0.0.1:1", Timeout: time.Second},
		Dyn:       dyn,
		Namespace: "argocd",
	})

	// Unknown action.
	res, msg, err := h(context.Background(), invokeEvent(t, &agentv1.InvokeAction{
		CommandId: "c1", Action: "delete-everything",
		Resource: &agentv1.ResourceRef{Name: "managed"},
	}))
	if err != nil || res != agentv1.CommandResult_COMMAND_RESULT_FAILED || !strings.Contains(msg, "allow-list") {
		t.Fatalf("unknown action: %v %q %v", res, msg, err)
	}

	// Unmanaged application.
	res, msg, err = h(context.Background(), invokeEvent(t, &agentv1.InvokeAction{
		CommandId: "c2", Action: "sync",
		Resource: &agentv1.ResourceRef{Name: "foreign"},
	}))
	if err != nil || res != agentv1.CommandResult_COMMAND_RESULT_FAILED || !strings.Contains(msg, "not Inari-managed") {
		t.Fatalf("foreign app: %v %q %v", res, msg, err)
	}

	// Missing app.
	res, _, err = h(context.Background(), invokeEvent(t, &agentv1.InvokeAction{
		CommandId: "c3", Action: "sync",
		Resource: &agentv1.ResourceRef{Name: "ghost"},
	}))
	if err != nil || res != agentv1.CommandResult_COMMAND_RESULT_FAILED {
		t.Fatalf("missing app: %v %v", res, err)
	}
}

func TestInvokeActionRollbackParams(t *testing.T) {
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), managedApp("my-web", true))
	h := InvokeActionHandler(InvokeActionDeps{
		API: &argocd.APIClient{BaseURL: srv.URL, Timeout: 5 * time.Second}, Dyn: dyn, Namespace: "argocd",
	})
	res, msg, err := h(context.Background(), invokeEvent(t, &agentv1.InvokeAction{
		CommandId: "c4", Action: "rollback",
		Resource:   &agentv1.ResourceRef{Name: "my-web"},
		Parameters: mustStruct(t, map[string]interface{}{"id": float64(7)}),
	}))
	if err != nil || res != agentv1.CommandResult_COMMAND_RESULT_APPLIED {
		t.Fatalf("rollback: %v %q %v", res, msg, err)
	}
	if body["id"] != float64(7) {
		t.Fatalf("rollback body %v", body)
	}

	// Missing id.
	res, msg, err = h(context.Background(), invokeEvent(t, &agentv1.InvokeAction{
		CommandId: "c5", Action: "rollback", Resource: &agentv1.ResourceRef{Name: "my-web"},
	}))
	if err != nil || res != agentv1.CommandResult_COMMAND_RESULT_FAILED || !strings.Contains(msg, "id") {
		t.Fatalf("rollback without id: %v %q %v", res, msg, err)
	}
}

func TestInvokeActionFailsClosedViaDispatcher(t *testing.T) {
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), managedApp("my-web", true))
	d := NewDispatcher()
	d.Register(agentv1.EventType_EVENT_TYPE_INVOKE_ACTION, InvokeActionHandler(InvokeActionDeps{
		API: &argocd.APIClient{BaseURL: "http://127.0.0.1:1", Timeout: time.Second}, Dyn: dyn, Namespace: "argocd",
	}))
	d.SetConnected(false)
	_, err := d.HandleEvent(context.Background(), invokeEvent(t, &agentv1.InvokeAction{
		CommandId: "c6", Action: "sync", Resource: &agentv1.ResourceRef{Name: "my-web"},
	}))
	if err == nil {
		t.Fatal("must fail closed while disconnected")
	}
}
