// Configurable status watches (M5, operator-sunset risk #4): the set of
// cluster-scoped resources whose conditions/health are streamed upstream
// is configuration, not a hardcoded platform.inari.io list, so native
// third-party resources (cert-manager ClusterIssuer, external-dns
// DNSEndpoint, argoproj AppProject/ApplicationSet), KRO instances, and
// addon apps keep feeding the Resources Inventory when the operator's
// facade CRDs disappear.
package status

import (
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// WatchResource is one configured status watch: a GVR plus an optional
// label selector bounding the informer (footprint budget §12.1/4).
type WatchResource struct {
	GVR schema.GroupVersionResource
	// LabelSelector narrows the watch (metav1 label selector syntax);
	// empty watches every object of the GVR.
	LabelSelector string
}

// DefaultWatchResources returns the built-in watch list applied when
// INARI_STATUS_WATCHES is unset: the legacy platform.inari.io CRDs
// (dual-run until Phase 4 removes them) plus the native replacements
// from the operator-sunset plan (§4.1). Addon apps surface through the
// namespaced Application watch and the fleet ApplicationSet entry here.
//
// The KRO tenantnamespace instance GVR is deliberately absent: its
// group/version/kind are generated from the RGD's spec.schema, so it is
// supplied via INARI_STATUS_WATCHES once the RGD lands.
func DefaultWatchResources() []WatchResource {
	out := make([]WatchResource, 0, len(PlatformGVRs)+4)
	for _, gvr := range PlatformGVRs {
		out = append(out, WatchResource{GVR: gvr})
	}
	return append(out,
		WatchResource{GVR: schema.GroupVersionResource{Group: "cert-manager.io", Version: "v1", Resource: "clusterissuers"}},
		WatchResource{GVR: schema.GroupVersionResource{Group: "externaldns.k8s.io", Version: "v1alpha1", Resource: "dnsendpoints"}},
		WatchResource{GVR: schema.GroupVersionResource{Group: "argoproj.io", Version: "v1alpha1", Resource: "appprojects"}},
		WatchResource{GVR: schema.GroupVersionResource{Group: "argoproj.io", Version: "v1alpha1", Resource: "applicationsets"}},
	)
}

// ParseWatchResources parses the INARI_STATUS_WATCHES value: a
// comma-separated list of "group/version/resource[@selector]" entries.
// An empty input yields no entries (the caller then applies
// DefaultWatchResources). Label selectors are validated with
// metav1.ParseToLabelSelector so a typo fails fast at boot instead of
// silently watching everything.
func ParseWatchResources(in string) ([]WatchResource, error) {
	in = strings.TrimSpace(in)
	if in == "" {
		return nil, nil
	}
	seen := map[WatchResource]bool{}
	var out []WatchResource
	for _, entry := range strings.Split(in, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		gvrPart, selector, hasSelector := strings.Cut(entry, "@")
		parts := strings.Split(gvrPart, "/")
		if len(parts) != 3 || parts[1] == "" || parts[2] == "" {
			return nil, fmt.Errorf("status watch %q: want group/version/resource[@selector]", entry)
		}
		// A trailing "@" with an empty selector is a typo: silently
		// watching everything is worse than failing boot.
		if hasSelector && selector == "" {
			return nil, fmt.Errorf("status watch %q: empty label selector after '@'", entry)
		}
		if selector != "" {
			if _, err := metav1.ParseToLabelSelector(selector); err != nil {
				return nil, fmt.Errorf("status watch %q: invalid label selector: %w", entry, err)
			}
		}
		w := WatchResource{
			GVR:           schema.GroupVersionResource{Group: parts[0], Version: parts[1], Resource: parts[2]},
			LabelSelector: selector,
		}
		if seen[w] {
			continue
		}
		seen[w] = true
		out = append(out, w)
	}
	return out, nil
}

// qualifiedKind is the ResourceRef kind for configured watch resources:
// "<Kind>.<group>" (decision D-A), extending the platform.inari.io suffix
// convention so the server can route native resources unambiguously.
// Core-group kinds stay bare.
func qualifiedKind(u *unstructured.Unstructured) string {
	if group := u.GroupVersionKind().Group; group != "" {
		return u.GetKind() + "." + group
	}
	return u.GetKind()
}
