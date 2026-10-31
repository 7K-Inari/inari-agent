package status

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestParseWatchResources(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    []WatchResource
		wantErr bool
	}{
		{
			name: "empty string yields no entries (caller applies defaults)",
			in:   "",
			want: nil,
		},
		{
			name: "single GVR without selector",
			in:   "cert-manager.io/v1/clusterissuers",
			want: []WatchResource{
				{GVR: schema.GroupVersionResource{Group: "cert-manager.io", Version: "v1", Resource: "clusterissuers"}},
			},
		},
		{
			name: "multiple entries with whitespace tolerated",
			in:   "cert-manager.io/v1/clusterissuers; externaldns.k8s.io/v1alpha1/dnsendpoints",
			want: []WatchResource{
				{GVR: schema.GroupVersionResource{Group: "cert-manager.io", Version: "v1", Resource: "clusterissuers"}},
				{GVR: schema.GroupVersionResource{Group: "externaldns.k8s.io", Version: "v1alpha1", Resource: "dnsendpoints"}},
			},
		},
		{
			name: "multi-label selector (comma is selector syntax, not the entry separator)",
			in:   "externaldns.k8s.io/v1alpha1/dnsendpoints@inari.io/managed-by=inari,inari.io/org=acme",
			want: []WatchResource{
				{
					GVR:           schema.GroupVersionResource{Group: "externaldns.k8s.io", Version: "v1alpha1", Resource: "dnsendpoints"},
					LabelSelector: "inari.io/managed-by=inari,inari.io/org=acme",
				},
			},
		},
		{
			name: "set-based selector",
			in:   "cert-manager.io/v1/clusterissuers@inari.io/org in (acme, globex)",
			want: []WatchResource{
				{
					GVR:           schema.GroupVersionResource{Group: "cert-manager.io", Version: "v1", Resource: "clusterissuers"},
					LabelSelector: "inari.io/org in (acme, globex)",
				},
			},
		},
		{
			name: "label selector suffix",
			in:   "externaldns.k8s.io/v1alpha1/dnsendpoints@inari.io/managed-by=inari",
			want: []WatchResource{
				{
					GVR:           schema.GroupVersionResource{Group: "externaldns.k8s.io", Version: "v1alpha1", Resource: "dnsendpoints"},
					LabelSelector: "inari.io/managed-by=inari",
				},
			},
		},
		{
			name: "duplicate entries deduped",
			in:   "cert-manager.io/v1/clusterissuers;cert-manager.io/v1/clusterissuers",
			want: []WatchResource{
				{GVR: schema.GroupVersionResource{Group: "cert-manager.io", Version: "v1", Resource: "clusterissuers"}},
			},
		},
		{
			name:    "missing resource segment",
			in:      "cert-manager.io/v1",
			wantErr: true,
		},
		{
			name:    "too many segments",
			in:      "cert-manager.io/v1/clusterissuers/extra",
			wantErr: true,
		},
		{
			name:    "empty version",
			in:      "cert-manager.io//clusterissuers",
			wantErr: true,
		},
		{
			name:    "invalid label selector",
			in:      "cert-manager.io/v1/clusterissuers@not a selector!",
			wantErr: true,
		},
		{
			name:    "trailing @ with empty selector is a typo, not watch-everything",
			in:      "cert-manager.io/v1/clusterissuers@",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseWatchResources(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got %+v", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("entry %d: got %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestDefaultWatchResources(t *testing.T) {
	defaults := DefaultWatchResources()
	if len(defaults) == 0 {
		t.Fatal("defaults must not be empty")
	}
	byResource := map[schema.GroupVersionResource]bool{}
	for _, w := range defaults {
		if w.LabelSelector != "" {
			t.Errorf("default watch %s must be unfiltered (label selectors are a deployment choice)", w.GVR)
		}
		byResource[w.GVR] = true
	}
	// Server-rendered platform resources (operator sunset, ADR-0017):
	// provider-keycloak claims, the KRO tenantnamespace instance, and the
	// native cert-manager/external-dns/argoproj resources.
	for _, gvr := range []schema.GroupVersionResource{
		{Group: "identity.inari.io", Version: "v1alpha1", Resource: "tenantrealms"},
		{Group: "identity.inari.io", Version: "v1alpha1", Resource: "tenantclients"},
		{Group: "stacks.inari.io", Version: "v1alpha1", Resource: "tenantnamespaces"},
		{Group: "cert-manager.io", Version: "v1", Resource: "clusterissuers"},
		{Group: "externaldns.k8s.io", Version: "v1alpha1", Resource: "dnsendpoints"},
		{Group: "argoproj.io", Version: "v1alpha1", Resource: "appprojects"},
		{Group: "argoproj.io", Version: "v1alpha1", Resource: "applicationsets"},
	} {
		if !byResource[gvr] {
			t.Errorf("platform resource GVR %s missing from defaults", gvr)
		}
	}
	// The retired operator CRDs must not linger in the defaults (D7).
	for _, w := range defaults {
		if w.GVR.Group == "platform.inari.io" {
			t.Errorf("legacy platform.inari.io GVR %s leaked into defaults", w.GVR)
		}
	}
}

func TestQualifiedKind(t *testing.T) {
	obj := func(apiVersion, kind string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": apiVersion,
			"kind":       kind,
		}}
	}
	// Group-qualified form: the server routes on the ".<group>" suffix
	// (decision D-A).
	if got := qualifiedKind(obj("identity.inari.io/v1alpha1", "TenantRealm")); got != "TenantRealm.identity.inari.io" {
		t.Errorf("claim kind = %q", got)
	}
	if got := qualifiedKind(obj("cert-manager.io/v1", "ClusterIssuer")); got != "ClusterIssuer.cert-manager.io" {
		t.Errorf("native kind = %q", got)
	}
	// Core-group objects have no group suffix.
	if got := qualifiedKind(obj("v1", "ConfigMap")); got != "ConfigMap" {
		t.Errorf("core kind = %q", got)
	}
}
