package capability

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

// mapFunc converts a watched object into a Capability. group/version/action
// and the brownfield management mode are filled in by the helper.
type mapFunc func(obj *unstructured.Unstructured) (*agentv1.Capability, error)

// dynamicWatcher is the shared read-only informer loop used by all
// per-source watchers (keeps the agent inside its 100m/128Mi footprint:
// one lightweight informer per GVR, no per-API generated clients).
type dynamicWatcher struct {
	source    Source
	kind      agentv1.CapabilityKind
	client    dynamic.Interface
	gvr       schema.GroupVersionResource
	namespace string
	selector  string
	mapFn     mapFunc
}

func (w *dynamicWatcher) Source() Source { return w.source }

func (w *dynamicWatcher) Start(ctx context.Context) (<-chan *agentv1.Capability, error) {
	out := make(chan *agentv1.Capability, 32)
	tweak := func(opts *metav1.ListOptions) {
		if w.selector != "" {
			opts.LabelSelector = w.selector
		}
	}
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(w.client, 0, w.namespace, tweak)
	informer := factory.ForResource(w.gvr).Informer()

	emit := func(obj *unstructured.Unstructured, action agentv1.CapabilityAction) {
		cap, err := w.mapFn(obj)
		if err != nil || cap == nil {
			return
		}
		cap.Kind = w.kind
		cap.Action = action
		if cap.ManagementMode == agentv1.ManagementMode_MANAGEMENT_MODE_UNSPECIFIED {
			cap.ManagementMode = Classify(obj)
		}
		select {
		case out <- cap:
		case <-ctx.Done():
		}
	}

	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if u, ok := obj.(*unstructured.Unstructured); ok {
				emit(u, agentv1.CapabilityAction_CAPABILITY_ACTION_UPSERT)
			}
		},
		UpdateFunc: func(_, newObj interface{}) {
			if u, ok := newObj.(*unstructured.Unstructured); ok {
				emit(u, agentv1.CapabilityAction_CAPABILITY_ACTION_UPSERT)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tomb.Obj
			}
			if u, ok := obj.(*unstructured.Unstructured); ok {
				emit(u, agentv1.CapabilityAction_CAPABILITY_ACTION_DELETE)
			}
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
