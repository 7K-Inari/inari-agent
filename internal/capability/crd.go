package capability

import (
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

var crdGVR = schema.GroupVersionResource{
	Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions",
}

// NewCRDWatcher watches apiextensions.k8s.io/v1 CRDs and extracts their
// OpenAPI v3 schemas (including CEL x-kubernetes-validations, which live
// inside the schema tree) for catalog form rendering (plan §5.3, §5.5).
//
// The informer cache is metadata-only (CRD schemas are the dominant
// informer memory cost on CRD-heavy clusters); the full object is GET on
// demand per event and never cached.
func NewCRDWatcher(dyn dynamic.Interface, meta metadata.Interface) Watcher {
	return &metadataWatcher{
		source:    SourceCRD,
		kind:      agentv1.CapabilityKind_CAPABILITY_KIND_CRD,
		meta:      meta,
		dyn:       dyn,
		gvr:       crdGVR,
		fetchFull: true,
		mapFn:     mapCRD,
	}
}

func mapCRD(obj *unstructured.Unstructured) (*agentv1.Capability, error) {
	group, _, _ := unstructured.NestedString(obj.Object, "spec", "group")
	versions, found, _ := unstructured.NestedSlice(obj.Object, "spec", "versions")
	if !found || len(versions) == 0 {
		return nil, fmt.Errorf("crd %s has no versions", obj.GetName())
	}
	// Prefer the storage version; fall back to the first listed version.
	chosen, _ := versions[0].(map[string]interface{})
	for _, v := range versions {
		vm, ok := v.(map[string]interface{})
		if !ok {
			continue
		}
		if storage, _, _ := unstructured.NestedBool(vm, "storage"); storage {
			chosen = vm
			break
		}
	}
	version, _, _ := unstructured.NestedString(chosen, "name")
	schemaMap, _, _ := unstructured.NestedMap(chosen, "schema", "openAPIV3Schema")
	var schemaProto *structpb.Struct
	if len(schemaMap) > 0 {
		if s, err := structpb.NewStruct(schemaMap); err == nil {
			schemaProto = s
		}
	}
	return &agentv1.Capability{
		Name:    obj.GetName(),
		Group:   group,
		Version: version,
		Schema:  schemaProto,
	}, nil
}
