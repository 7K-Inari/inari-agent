package main

import (
	"testing"
	"time"

	"github.com/7K-Inari/inari-agent/internal/status"
)

func TestReadyzDisconnectGraceDefaultsWhenUnset(t *testing.T) {
	t.Setenv("INARI_READYZ_DISCONNECT_GRACE", "")
	if got := readyzDisconnectGrace(); got != defaultReadyzDisconnectGrace {
		t.Fatalf("unset env must use default, got %s", got)
	}
}

func TestReadyzDisconnectGraceParsesEnv(t *testing.T) {
	t.Setenv("INARI_READYZ_DISCONNECT_GRACE", "45s")
	if got := readyzDisconnectGrace(); got != 45*time.Second {
		t.Fatalf("valid env must be parsed, got %s", got)
	}
}

func TestReadyzDisconnectGraceFallsBackOnNegative(t *testing.T) {
	t.Setenv("INARI_READYZ_DISCONNECT_GRACE", "-5s")
	if got := readyzDisconnectGrace(); got != defaultReadyzDisconnectGrace {
		t.Fatalf("negative env must fall back to default, got %s", got)
	}
}

func TestReadyzDisconnectGraceFallsBackOnInvalid(t *testing.T) {
	t.Setenv("INARI_READYZ_DISCONNECT_GRACE", "not-a-duration")
	if got := readyzDisconnectGrace(); got != defaultReadyzDisconnectGrace {
		t.Fatalf("invalid env must fall back to default, got %s", got)
	}
}

func TestStatusWatchesDefaultsWhenUnset(t *testing.T) {
	t.Setenv("INARI_STATUS_WATCHES", "")
	got, err := statusWatches()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(status.DefaultWatchResources()) {
		t.Fatalf("expected defaults, got %+v", got)
	}
}

func TestStatusWatchesParsesEnvOverride(t *testing.T) {
	t.Setenv("INARI_STATUS_WATCHES", "cert-manager.io/v1/clusterissuers,kro.run/v1alpha1/tenantnamespaces@inari.io/org=acme")
	got, err := statusWatches()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 watches, got %+v", got)
	}
	if got[1].LabelSelector != "inari.io/org=acme" {
		t.Fatalf("selector %q", got[1].LabelSelector)
	}
}

func TestStatusWatchesFailsFastOnInvalid(t *testing.T) {
	t.Setenv("INARI_STATUS_WATCHES", "cert-manager.io/v1")
	if _, err := statusWatches(); err == nil {
		t.Fatal("invalid override must fail at boot, not silently watch nothing")
	}
}

func TestStatusWatchesWhitespaceOnlyEnvUsesDefaults(t *testing.T) {
	t.Setenv("INARI_STATUS_WATCHES", "   ")
	got, err := statusWatches()
	if err != nil {
		t.Fatal(err)
	}
	// Whitespace parses to zero entries; the streamer layer treats nil as
	// "apply built-in defaults" (GitOpsConfig.streamer).
	if got != nil {
		t.Fatalf("whitespace-only override must parse to nil (defaults downstream), got %+v", got)
	}
}
