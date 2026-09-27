package command

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/argocd"
)

// Server redeems successfully but returns an EMPTY bearer: the command must
// fail closed, never silently fall back to the static break-glass token.
func TestInvokeActionEmptyRedeemedBearerFailsClosed(t *testing.T) {
	srv, _ := redeemServer(t, func(context.Context, *connect.Request[agentv1.RedeemUserCredentialRequest]) (*connect.Response[agentv1.RedeemUserCredentialResponse], error) {
		return connect.NewResponse(&agentv1.RedeemUserCredentialResponse{BearerToken: ""}), nil
	})
	r := NewConnectRedeemer(srv.URL, staticTokenSource{"agent-jwt"}, srv.Client())
	tok, _, err := r.Redeem(context.Background(), "ref-1")
	t.Logf("redeem returned token=%q err=%v", tok, err)

	var gotAuth string
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		gotAuth = req.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(apiSrv.Close)

	dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), managedApp("my-web", true))
	h := InvokeActionHandler(InvokeActionDeps{
		API:       &argocd.APIClient{BaseURL: apiSrv.URL, Token: "static-breakglass", Timeout: 5 * time.Second},
		Dyn:       dyn,
		Namespace: "argocd",
		Redeemer:  r,
	})
	res, msg, err := h(context.Background(), invokeEvent(t, &agentv1.InvokeAction{
		CommandId: "qa-1", Action: "sync",
		Resource:          &agentv1.ResourceRef{Name: "my-web"},
		UserCredentialRef: "ref-1",
	}))
	t.Logf("result=%v msg=%q err=%v authHeader=%q", res, msg, err, gotAuth)
	if err != nil || res != agentv1.CommandResult_COMMAND_RESULT_FAILED {
		t.Fatalf("empty bearer: %v %q %v", res, msg, err)
	}
	if gotAuth != "" {
		t.Fatalf("no ArgoCD call must be made, got auth header %q", gotAuth)
	}
}
