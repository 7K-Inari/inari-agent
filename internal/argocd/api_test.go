package argocd

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func captureAuth(t *testing.T, status int, body string) (*httptest.Server, *string) {
	t.Helper()
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, &gotAuth
}

func TestAPIClientStaticTokenFallback(t *testing.T) {
	srv, gotAuth := captureAuth(t, http.StatusOK, `{}`)
	c := &APIClient{BaseURL: srv.URL, Token: "static-tok", Timeout: 5 * time.Second}
	if err := c.Refresh(context.Background(), "app", false); err != nil {
		t.Fatal(err)
	}
	if *gotAuth != "Bearer static-tok" {
		t.Fatalf("auth header %q", *gotAuth)
	}
}

func TestAPIClientContextBearerOverridesStatic(t *testing.T) {
	srv, gotAuth := captureAuth(t, http.StatusOK, `{}`)
	c := &APIClient{BaseURL: srv.URL, Token: "static-tok", Timeout: 5 * time.Second}
	ctx := WithBearer(context.Background(), "user-tok")
	if err := c.Refresh(ctx, "app", false); err != nil {
		t.Fatal(err)
	}
	if *gotAuth != "Bearer user-tok" {
		t.Fatalf("auth header %q", *gotAuth)
	}
}

func TestAPIClientEmptyContextBearerFallsBackToStatic(t *testing.T) {
	srv, gotAuth := captureAuth(t, http.StatusOK, `{}`)
	c := &APIClient{BaseURL: srv.URL, Token: "static-tok", Timeout: 5 * time.Second}
	ctx := WithBearer(context.Background(), "")
	if err := c.Refresh(ctx, "app", false); err != nil {
		t.Fatal(err)
	}
	if *gotAuth != "Bearer static-tok" {
		t.Fatalf("auth header %q", *gotAuth)
	}
}

func TestAPIClientErrorDoesNotLeakBearer(t *testing.T) {
	srv, _ := captureAuth(t, http.StatusForbidden, `{"error":"denied"}`)
	c := &APIClient{BaseURL: srv.URL, Token: "static-tok", Timeout: 5 * time.Second}
	ctx := WithBearer(context.Background(), "user-tok-secret")
	err := c.Refresh(ctx, "app", false)
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "user-tok-secret") || strings.Contains(err.Error(), "static-tok") {
		t.Fatalf("error leaks token: %v", err)
	}
}
