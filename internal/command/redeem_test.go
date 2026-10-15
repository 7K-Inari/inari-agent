package command

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
	"github.com/7K-Inari/inari-api/gen/go/inari/agent/v1/agentv1connect"
)

type redeemHandlerFunc func(context.Context, *agentv1.RedeemUserCredentialRequest) (*agentv1.RedeemUserCredentialResponse, error)

func (f redeemHandlerFunc) RedeemUserCredential(ctx context.Context, req *agentv1.RedeemUserCredentialRequest) (*agentv1.RedeemUserCredentialResponse, error) {
	return f(ctx, req)
}

type staticTokenSource struct{ token string }

func (s staticTokenSource) Token(context.Context) (string, error) { return s.token, nil }

func redeemServer(t *testing.T, fn func(context.Context, *agentv1.RedeemUserCredentialRequest) (*agentv1.RedeemUserCredentialResponse, error)) (*httptest.Server, *string) {
	t.Helper()
	var gotAuth string
	server := connect.NewServer()
	agentv1connect.RegisterAgentCredentialsServiceHandler(server, redeemHandlerFunc(fn))
	rpcMux := http.NewServeMux()
	connecthttp.Mount(rpcMux, server)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		rpcMux.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, &gotAuth
}

func newTestRedeemer(srv *httptest.Server) *ConnectRedeemer {
	return NewConnectRedeemer(srv.URL, staticTokenSource{"agent-jwt"}, srv.Client())
}

func TestRedeemSuccess(t *testing.T) {
	exp := time.Now().Add(time.Minute).Truncate(time.Second)
	srv, gotAuth := redeemServer(t, func(_ context.Context, req *agentv1.RedeemUserCredentialRequest) (*agentv1.RedeemUserCredentialResponse, error) {
		if req.CredentialRef != "ref-1" {
			t.Errorf("credential_ref %q", req.CredentialRef)
		}
		return &agentv1.RedeemUserCredentialResponse{
			BearerToken: "user-bearer",
			ExpiresAt:   timestamppb.New(exp),
		}, nil
	})
	r := newTestRedeemer(srv)
	tok, gotExp, err := r.Redeem(context.Background(), "ref-1")
	if err != nil {
		t.Fatal(err)
	}
	if tok != "user-bearer" {
		t.Fatalf("token %q", tok)
	}
	if !gotExp.Equal(exp) {
		t.Fatalf("expiry %v, want %v", gotExp, exp)
	}
	if *gotAuth != "Bearer agent-jwt" {
		t.Fatalf("agent auth header %q", *gotAuth)
	}
}

func TestRedeemTerminalFailures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		code     connect.Code
		wantCode RedeemCode
	}{
		{"missing ref", connect.CodeNotFound, RedeemNotFound},
		{"expired ref", connect.CodeFailedPrecondition, RedeemExpired},
		{"already redeemed", connect.CodeAlreadyExists, RedeemAlreadyRedeemed},
		{"empty ref", connect.CodeInvalidArgument, RedeemInvalid},
		{"revoked agent credential", connect.CodePermissionDenied, RedeemDenied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Server error message deliberately contains secret-looking
			// material; the client must never echo it.
			srv, _ := redeemServer(t, func(context.Context, *agentv1.RedeemUserCredentialRequest) (*agentv1.RedeemUserCredentialResponse, error) {
				return nil, connect.NewError(tc.code, "detail user-bearer-secret")
			})
			r := newTestRedeemer(srv)
			_, _, err := r.Redeem(context.Background(), "ref-1")
			var re *RedeemError
			if !errors.As(err, &re) {
				t.Fatalf("expected RedeemError, got %v", err)
			}
			if re.Code != tc.wantCode {
				t.Fatalf("code %v, want %v", re.Code, tc.wantCode)
			}
			if !re.Terminal() {
				t.Fatal("must be terminal")
			}
			if strings.Contains(err.Error(), "user-bearer-secret") || strings.Contains(err.Error(), "detail") {
				t.Fatalf("error echoes server detail: %v", err)
			}
		})
	}
}

func TestRedeemTransientFailure(t *testing.T) {
	srv, _ := redeemServer(t, func(context.Context, *agentv1.RedeemUserCredentialRequest) (*agentv1.RedeemUserCredentialResponse, error) {
		return nil, connect.NewError(connect.CodeUnavailable, "backend down")
	})
	r := newTestRedeemer(srv)
	_, _, err := r.Redeem(context.Background(), "ref-1")
	var re *RedeemError
	if errors.As(err, &re) && re.Terminal() {
		t.Fatalf("unavailable must be transient, got %v", err)
	}
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRedeemRefNotLeaked(t *testing.T) {
	srv, _ := redeemServer(t, func(context.Context, *agentv1.RedeemUserCredentialRequest) (*agentv1.RedeemUserCredentialResponse, error) {
		return &agentv1.RedeemUserCredentialResponse{BearerToken: "tok"}, nil
	})
	r := newTestRedeemer(srv)
	// The ref is a non-secret opaque id, but ensure the agent JWT is not
	// exposed via errors on a cancelled context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := r.Redeem(ctx, "ref-1")
	if err != nil && strings.Contains(err.Error(), "agent-jwt") {
		t.Fatalf("error leaks agent token: %v", err)
	}
}
