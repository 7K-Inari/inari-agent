package capability

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/metadata/metadatainformer"
	"k8s.io/client-go/tools/cache"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

// metadataWatcher watches a resource through a metadata-only informer
// (PartialObjectMetadata): no object bodies, blobs, or schemas are kept in
// the informer cache, which keeps the agent flat on CRD/Secret-heavy
// clusters (footprint budget, plan §12.1/4). When fetchFull is set the full
// object is GET on demand per event (never cached) so the mapper still sees
// spec/schema; otherwise the mapper receives metadata only.
type metadataWatcher struct {
	source    Source
	kind      agentv1.CapabilityKind
	meta      metadata.Interface
	dyn       dynamic.Interface // required when fetchFull is set
	gvr       schema.GroupVersionResource
	namespace string
	selector  string
	fetchFull bool
	mapFn     mapFunc
}

func (w *metadataWatcher) Source() Source { return w.source }

// GVR exposes the watched resource for discovery-based availability
// filtering (see FilterAvailable).
func (w *metadataWatcher) GVR() schema.GroupVersionResource { return w.gvr }

func (w *metadataWatcher) Start(ctx context.Context) (<-chan *agentv1.Capability, error) {
	out := make(chan *agentv1.Capability, 32)
	tweak := func(opts *metav1.ListOptions) {
		if w.selector != "" {
			opts.LabelSelector = w.selector
		}
	}
	factory := metadatainformer.NewFilteredSharedInformerFactory(w.meta, 0, w.namespace, tweak)
	informer := factory.ForResource(w.gvr).Informer()

	// resolve turns a PartialObjectMetadata event into the object the
	// mapper consumes: metadata-only, or a one-shot full GET per event.
	resolve := func(obj interface{}) *unstructured.Unstructured {
		pm, ok := obj.(*metav1.PartialObjectMetadata)
		if !ok {
			return nil
		}
		if !w.fetchFull {
			u := &unstructured.Unstructured{}
			u.SetAPIVersion(pm.APIVersion)
			u.SetKind(pm.Kind)
			u.SetName(pm.GetName())
			u.SetNamespace(pm.GetNamespace())
			u.SetLabels(pm.GetLabels())
			u.SetAnnotations(pm.GetAnnotations())
			return u
		}
		full, err := w.dyn.Resource(w.gvr).Namespace(pm.GetNamespace()).Get(ctx, pm.GetName(), metav1.GetOptions{})
		if err != nil {
			// Object vanished between event and GET, or transient API
			// error; the next event/resync converges state.
			return nil
		}
		return full
	}

	emit := func(obj interface{}, action agentv1.CapabilityAction) {
		if u := resolve(obj); u != nil {
			emitCapability(ctx, out, w.kind, w.mapFn, u, action)
		}
	}

	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			emit(obj, agentv1.CapabilityAction_CAPABILITY_ACTION_UPSERT)
		},
		UpdateFunc: func(_, newObj interface{}) {
			emit(newObj, agentv1.CapabilityAction_CAPABILITY_ACTION_UPSERT)
		},
		DeleteFunc: func(obj interface{}) {
			if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tomb.Obj
			}
			// Deletes must not re-GET (the object is gone); emit from
			// metadata so the catalog entry is removed.
			pm, ok := obj.(*metav1.PartialObjectMetadata)
			if !ok {
				return
			}
			if w.fetchFull {
				// The mapper needs the (now deleted) spec; emit a
				// name-only DELETE, matched by kind+name in the
				// aggregator. CRD-style names embed the group, so
				// kind+name is unambiguous.
				cap := &agentv1.Capability{
					Kind:   w.kind,
					Name:   pm.GetName(),
					Action: agentv1.CapabilityAction_CAPABILITY_ACTION_DELETE,
				}
				select {
				case out <- cap:
				case <-ctx.Done():
				}
				return
			}
			u := &unstructured.Unstructured{}
			u.SetAPIVersion(pm.APIVersion)
			u.SetKind(pm.Kind)
			u.SetName(pm.GetName())
			u.SetNamespace(pm.GetNamespace())
			u.SetLabels(pm.GetLabels())
			u.SetAnnotations(pm.GetAnnotations())
			emitCapability(ctx, out, w.kind, w.mapFn, u, agentv1.CapabilityAction_CAPABILITY_ACTION_DELETE)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("capability %s: register informer handler: %w", w.source, err)
	}

	go func() {
		defer close(out)
		informer.Run(ctx.Done())
	}()
	return out, nil
}
