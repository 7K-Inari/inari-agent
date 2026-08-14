package capability

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

var kroRGDGVR = schema.GroupVersionResource{
	Group: "kro.run", Version: "v1alpha1", Resource: "resourcegraphdefinitions",
}

// NewKROWatcher watches KRO ResourceGraphDefinitions and exposes their
// instance schemas for the render-rgd-instance flow (plan §5.3, §5.5).
func NewKROWatcher(client dynamic.Interface) Watcher {
	return &dynamicWatcher{
		source: SourceKRORGD,
		kind:   agentv1.CapabilityKind_CAPABILITY_KIND_KRO_RGD,
		client: client,
		gvr:    kroRGDGVR,
		mapFn:  mapRGD,
	}
}

func mapRGD(obj *unstructured.Unstructured) (*agentv1.Capability, error) {
	kind, _, _ := unstructured.NestedString(obj.Object, "spec", "schema", "kind")
	version, _, _ := unstructured.NestedString(obj.Object, "spec", "schema", "apiVersion")
	var schemaProto = mustStruct(nil)
	if schemaMap, found, _ := unstructured.NestedMap(obj.Object, "spec", "schema"); found {
		schemaProto = mustStruct(schemaMap)
	}
	return &agentv1.Capability{
		Name:    obj.GetName(),
		Group:   "kro.run",
		Version: version,
		Schema:  schemaProto,
		UiHints: mustStruct(map[string]interface{}{"instanceKind": kind}),
	}, nil
}
