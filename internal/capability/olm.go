package capability

import (
	"google.golang.org/protobuf/types/known/structpb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

var csvGVR = schema.GroupVersionResource{
	Group: "operators.coreos.com", Version: "v1alpha1", Resource: "clusterserviceversions",
}

// NewOLMWatcher watches OLM ClusterServiceVersions and extracts
// specDescriptors / x-descriptors as UI hints for the catalog (plan §5.5).
func NewOLMWatcher(client dynamic.Interface, namespace string) Watcher {
	return &dynamicWatcher{
		source:    SourceOLM,
		kind:      agentv1.CapabilityKind_CAPABILITY_KIND_OLM_CSV,
		client:    client,
		gvr:       csvGVR,
		namespace: namespace,
		mapFn:     mapCSV,
	}
}

func mapCSV(obj *unstructured.Unstructured) (*agentv1.Capability, error) {
	version, _, _ := unstructured.NestedString(obj.Object, "spec", "version")
	owned, _, _ := unstructured.NestedSlice(obj.Object, "spec", "customresourcedefinitions", "owned")
	hints := map[string]interface{}{}
	var descriptorNames []interface{}
	for _, o := range owned {
		om, ok := o.(map[string]interface{})
		if !ok {
			continue
		}
		if name, _, _ := unstructured.NestedString(om, "name"); name != "" {
			descriptorNames = append(descriptorNames, name)
		}
		if descs, found, _ := unstructured.NestedSlice(om, "specDescriptors"); found && len(descs) > 0 {
			if _, exists := hints["specDescriptors"]; !exists {
				hints["specDescriptors"] = descs
			}
		}
	}
	if len(descriptorNames) > 0 {
		hints["ownedCRDs"] = descriptorNames
	}
	var uiHints *structpb.Struct
	if len(hints) > 0 {
		if s, err := structpb.NewStruct(hints); err == nil {
			uiHints = s
		}
	}
	return &agentv1.Capability{
		Name:    obj.GetName(),
		Group:   "operators.coreos.com",
		Version: version,
		UiHints: uiHints,
	}, nil
}
