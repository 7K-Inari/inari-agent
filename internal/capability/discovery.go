package capability

import (
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

// gvrWatcher is implemented by watchers bound to a single API resource.
type gvrWatcher interface {
	GVR() schema.GroupVersionResource
}

// FilterAvailable drops watchers whose API is not served by the cluster
// (discovery), logging exactly one line per skipped watcher, so absent
// optional platforms (OLM, Crossplane, KRO) don't spin reflector error
// loops forever. Non-NotFound discovery errors keep the watcher (fail
// open): the API may be served later and the informer retries anyway.
func FilterAvailable(disc discovery.DiscoveryInterface, log logr.Logger, watchers []Watcher) []Watcher {
	checked := map[string]bool{}
	out := make([]Watcher, 0, len(watchers))
	for _, w := range watchers {
		gw, ok := w.(gvrWatcher)
		if !ok {
			out = append(out, w)
			continue
		}
		gvr := gw.GVR()
		gvKey := gvr.GroupVersion().String()
		avail, known := checked[gvKey]
		if !known {
			avail = resourceServed(disc, gvr)
			checked[gvKey] = avail
		}
		if !avail {
			log.Info("capability watcher skipped: API not installed on this cluster",
				"source", string(w.Source()), "gvr", gvr.String())
			continue
		}
		out = append(out, w)
	}
	return out
}

// resourceServed reports whether the cluster serves the given resource,
// using a single discovery call per group/version.
func resourceServed(disc discovery.DiscoveryInterface, gvr schema.GroupVersionResource) bool {
	rl, err := disc.ServerResourcesForGroupVersion(gvr.GroupVersion().String())
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false
		}
		if !discovery.IsGroupDiscoveryFailedError(err) {
			// Transient discovery failure: fail open.
			return true
		}
		// Partial discovery: inspect whatever list came back.
	}
	if rl == nil {
		return true
	}
	for _, r := range rl.APIResources {
		if r.Name == gvr.Resource {
			return true
		}
	}
	return false
}
