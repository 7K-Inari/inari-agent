package main

import (
	"testing"
	"time"
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
