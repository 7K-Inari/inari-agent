package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConfigFromEnvDefaults(t *testing.T) {
	t.Setenv("INARI_KUBEPROXY_URL", "https://kubeproxy.example.com")
	t.Setenv("INARI_CLUSTER_ID", "cluster-1")
	t.Setenv("INARI_OIDC_ISSUER", "https://keycloak.example.com/realms/tenant")
	t.Setenv("INARI_CLIENT_SECRET", "s3cret")

	cfg, err := configFromEnv()
	if err != nil {
		t.Fatalf("configFromEnv: %v", err)
	}
	if cfg.clientID != "tunnel-cluster-1" {
		t.Errorf("clientID = %q, want derived tunnel-<cluster-id>", cfg.clientID)
	}
	if cfg.apiserverURL != defaultAPIServerURL {
		t.Errorf("apiserverURL = %q", cfg.apiserverURL)
	}
	if cfg.pingInterval != 30*time.Second {
		t.Errorf("pingInterval = %v", cfg.pingInterval)
	}
	if cfg.maxConnLifetime != 60*time.Minute {
		t.Errorf("maxConnLifetime = %v", cfg.maxConnLifetime)
	}
	if cfg.healthAddr != ":8082" {
		t.Errorf("healthAddr = %q", cfg.healthAddr)
	}
}

func TestConfigFromEnvRequired(t *testing.T) {
	t.Setenv("INARI_CLUSTER_ID", "cluster-1")
	if _, err := configFromEnv(); err == nil {
		t.Error("expected error without INARI_KUBEPROXY_URL")
	}
}

func TestConfigFromEnvSecretFile(t *testing.T) {
	dir := t.TempDir()
	secretFile := filepath.Join(dir, "secret")
	if err := os.WriteFile(secretFile, []byte("file-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("INARI_KUBEPROXY_URL", "http://kubeproxy:8080")
	t.Setenv("INARI_CLUSTER_ID", "cluster-1")
	t.Setenv("INARI_OIDC_TOKEN_URL", "https://keycloak/token")
	t.Setenv("INARI_CLIENT_SECRET_FILE", secretFile)

	cfg, err := configFromEnv()
	if err != nil {
		t.Fatalf("configFromEnv: %v", err)
	}
	if cfg.clientSecret != "file-secret" {
		t.Errorf("clientSecret = %q", cfg.clientSecret)
	}
}

func TestFileTokenSourceCaching(t *testing.T) {
	dir := t.TempDir()
	tokFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokFile, []byte("tok-1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ts, err := newFileTokenSource(tokFile)
	if err != nil {
		t.Fatalf("newFileTokenSource: %v", err)
	}
	if got := ts.Get(); got != "tok-1" {
		t.Errorf("Get = %q", got)
	}

	// Rotate the file: the cache serves the old token within the window...
	if err := os.WriteFile(tokFile, []byte("tok-2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := ts.Get(); got != "tok-1" {
		t.Errorf("cached Get = %q", got)
	}
	// ...and an expired cache picks the new token up.
	ts.at = ts.at.Add(-2 * time.Minute)
	if got := ts.Get(); got != "tok-2" {
		t.Errorf("refreshed Get = %q", got)
	}
}

func TestFileTokenSourceEmpty(t *testing.T) {
	dir := t.TempDir()
	tokFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokFile, []byte(""), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newFileTokenSource(tokFile); err == nil {
		t.Error("expected error for empty token file")
	}
}
