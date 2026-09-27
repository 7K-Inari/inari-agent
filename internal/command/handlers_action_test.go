package command

import (
	"context"
	"encoding/json"
	"errors"
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

type fakeRedeemer struct {
	token  string
	err    error
	calls  int
	gotRef string
}

func (f *fakeRedeemer) Redeem(_ context.Context, ref string) (string, time.Time, error) {
	f.calls++
	f.gotRef = ref
	if f.err != nil {
		return "", time.Time{}, f.err
	}
	return f.token, time.Now().Add(time.Minute), nil
}

func TestInvokeActionRedeemsUserCredential(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	rd := &fakeRedeemer{token: "user-bearer-token"}
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), managedApp("my-web", true))
	h := InvokeActionHandler(InvokeActionDeps{
		API:       &argocd.APIClient{BaseURL: srv.URL, Token: "static-tok", Timeout: 5 * time.Second},
		Dyn:       dyn,
		Namespace: "argocd",
		Redeemer:  rd,
	})
	res, msg, err := h(context.Background(), invokeEvent(t, &agentv1.InvokeAction{
		CommandId: "c-r1", Action: "sync",
		Resource:          &agentv1.ResourceRef{Name: "my-web"},
		UserCredentialRef: "ref-abc",
	}))
	if err != nil || res != agentv1.CommandResult_COMMAND_RESULT_APPLIED {
		t.Fatalf("redeem path: %v %q %v", res, msg, err)
	}
	if rd.calls != 1 || rd.gotRef != "ref-abc" {
		t.Fatalf("redeemer calls=%d ref=%q", rd.calls, rd.gotRef)
	}
	if gotAuth != "Bearer user-bearer-token" {
		t.Fatalf("auth header %q", gotAuth)
	}
	if strings.Contains(msg, "user-bearer-token") {
		t.Fatalf("ack message leaks token: %q", msg)
	}
}

func TestInvokeActionNoRefUsesStaticToken(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	rd := &fakeRedeemer{token: "user-bearer-token"}
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), managedApp("my-web", true))
	h := InvokeActionHandler(InvokeActionDeps{
		API:       &argocd.APIClient{BaseURL: srv.URL, Token: "static-tok", Timeout: 5 * time.Second},
		Dyn:       dyn,
		Namespace: "argocd",
		Redeemer:  rd,
	})
	res, _, err := h(context.Background(), invokeEvent(t, &agentv1.InvokeAction{
		CommandId: "c-r2", Action: "refresh",
		Resource: &agentv1.ResourceRef{Name: "my-web"},
	}))
	if err != nil || res != agentv1.CommandResult_COMMAND_RESULT_APPLIED {
		t.Fatalf("static path: %v %v", res, err)
	}
	if rd.calls != 0 {
		t.Fatal("redeemer must not be called without a ref")
	}
	if gotAuth != "Bearer static-tok" {
		t.Fatalf("auth header %q", gotAuth)
	}
}

func TestInvokeActionTerminalRedeemFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"missing ref", &RedeemError{Code: RedeemNotFound}},
		{"expired ref", &RedeemError{Code: RedeemExpired}},
		{"already redeemed", &RedeemError{Code: RedeemAlreadyRedeemed}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), managedApp("my-web", true))
			d := NewDispatcher()
			d.Register(agentv1.EventType_EVENT_TYPE_INVOKE_ACTION, InvokeActionHandler(InvokeActionDeps{
				API:       &argocd.APIClient{BaseURL: "http://127.0.0.1:1", Timeout: time.Second},
				Dyn:       dyn,
				Namespace: "argocd",
				Redeemer:  &fakeRedeemer{err: tc.err},
			}))
			ev := invokeEvent(t, &agentv1.InvokeAction{
				CommandId: "c-r3", Action: "sync",
				Resource:          &agentv1.ResourceRef{Name: "my-web"},
				UserCredentialRef: "ref-bad",
			})
			ack, err := d.HandleEvent(context.Background(), ev)
			if err != nil {
				t.Fatalf("terminal failure must not surface as error (no redelivery): %v", err)
			}
			if ack.Result != agentv1.CommandResult_COMMAND_RESULT_FAILED {
				t.Fatalf("ack %+v", ack)
			}
			if strings.Contains(ack.Message, "ref-bad-secret") || strings.Contains(ack.Message, "bearer") {
				t.Fatalf("ack message leaks material: %q", ack.Message)
			}
			// Recorded: replay returns the same terminal ack without re-executing.
			again, err := d.HandleEvent(context.Background(), ev)
			if err != nil || again != ack {
				t.Fatalf("replay: %v %v", again, err)
			}
		})
	}
}

func TestInvokeActionRefWithoutRedeemerFailsClosed(t *testing.T) {
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), managedApp("my-web", true))
	h := InvokeActionHandler(InvokeActionDeps{
		API:       &argocd.APIClient{BaseURL: "http://127.0.0.1:1", Token: "static-tok", Timeout: time.Second},
		Dyn:       dyn,
		Namespace: "argocd",
	})
	res, msg, err := h(context.Background(), invokeEvent(t, &agentv1.InvokeAction{
		CommandId: "c-r4", Action: "sync",
		Resource:          &agentv1.ResourceRef{Name: "my-web"},
		UserCredentialRef: "ref-x",
	}))
	if err != nil || res != agentv1.CommandResult_COMMAND_RESULT_FAILED {
		t.Fatalf("nil redeemer: %v %q %v", res, msg, err)
	}
	if !strings.Contains(msg, "not configured") {
		t.Fatalf("message %q", msg)
	}
}

func TestInvokeActionTransientRedeemErrorRedelivers(t *testing.T) {
	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), managedApp("my-web", true))
	h := InvokeActionHandler(InvokeActionDeps{
		API:       &argocd.APIClient{BaseURL: "http://127.0.0.1:1", Timeout: time.Second},
		Dyn:       dyn,
		Namespace: "argocd",
		Redeemer:  &fakeRedeemer{err: errors.New("connection refused")},
	})
	_, _, err := h(context.Background(), invokeEvent(t, &agentv1.InvokeAction{
		CommandId: "c-r5", Action: "sync",
		Resource:          &agentv1.ResourceRef{Name: "my-web"},
		UserCredentialRef: "ref-t",
	}))
	if err == nil {
		t.Fatal("transient redeem failure must surface as handler error for redelivery")
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
