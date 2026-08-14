package capability

import (
	"google.golang.org/protobuf/types/known/structpb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

var (
	xrdGVR = schema.GroupVersionResource{
		Group: "apiextensions.crossplane.io", Version: "v1", Resource: "compositedresourcedefinitions",
	}
	compositionGVR = schema.GroupVersionResource{
		Group: "apiextensions.crossplane.io", Version: "v1", Resource: "compositions",
	}
	providerGVR = schema.GroupVersionResource{
		Group: "pkg.crossplane.io", Version: "v1", Resource: "providers",
	}
)

// NewCrossplaneXRDWatcher watches Crossplane XRDs and Compositions
// (apiextensions.crossplane.io) and exposes their claim/composite schemas.
func NewCrossplaneXRDWatcher(client dynamic.Interface) []Watcher {
	return []Watcher{
		&dynamicWatcher{
			source: SourceCrossplaneXRD,
			kind:   agentv1.CapabilityKind_CAPABILITY_KIND_CROSSPLANE_XRD,
			client: client,
			gvr:    xrdGVR,
			mapFn:  mapXRD,
		},
		&dynamicWatcher{
			source: SourceCrossplaneXRD,
			kind:   agentv1.CapabilityKind_CAPABILITY_KIND_CROSSPLANE_XRD,
			client: client,
			gvr:    compositionGVR,
			mapFn:  mapComposition,
		},
	}
}

// NewCrossplaneProviderWatcher watches installed Crossplane providers.
func NewCrossplaneProviderWatcher(client dynamic.Interface) Watcher {
	return &dynamicWatcher{
		source: SourceCrossplaneProv,
		kind:   agentv1.CapabilityKind_CAPABILITY_KIND_CROSSPLANE_PROVIDER,
		client: client,
		gvr:    providerGVR,
		mapFn:  mapProvider,
	}
}

func mapXRD(obj *unstructured.Unstructured) (*agentv1.Capability, error) {
	group, _, _ := unstructured.NestedString(obj.Object, "spec", "group")
	versions, _, _ := unstructured.NestedSlice(obj.Object, "spec", "versions")
	version := ""
	var schemaProto *structpb.Struct
	if len(versions) > 0 {
		if vm, ok := versions[0].(map[string]interface{}); ok {
			version, _, _ = unstructured.NestedString(vm, "name")
			if schemaMap, found, _ := unstructured.NestedMap(vm, "schema", "openAPIV3Schema"); found {
				if s, err := structpb.NewStruct(schemaMap); err == nil {
					schemaProto = s
				}
			}
		}
	}
	return &agentv1.Capability{
		Name:    obj.GetName(),
		Group:   group,
		Version: version,
		Schema:  schemaProto,
	}, nil
}

func mapComposition(obj *unstructured.Unstructured) (*agentv1.Capability, error) {
	compositeType, _, _ := unstructured.NestedString(obj.Object, "spec", "compositeTypeRef", "apiVersion")
	return &agentv1.Capability{
		Name:    obj.GetName(),
		Group:   "apiextensions.crossplane.io",
		Version: compositeType,
	}, nil
}

func mapProvider(obj *unstructured.Unstructured) (*agentv1.Capability, error) {
	pkg, _, _ := unstructured.NestedString(obj.Object, "spec", "package")
	revision, _, _ := unstructured.NestedString(obj.Object, "status", "currentRevision")
	return &agentv1.Capability{
		Name:    obj.GetName(),
		Group:   "pkg.crossplane.io",
		Version: revision,
		UiHints: mustStruct(map[string]interface{}{"package": pkg}),
	}, nil
}

func mustStruct(m map[string]interface{}) *structpb.Struct {
	s, err := structpb.NewStruct(m)
	if err != nil {
		return nil
	}
	return s
}
