package capability

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

var secretsGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}

// NewHelmReleaseWatcher discovers Helm releases via their storage Secrets
// (label owner=helm). Only release metadata from labels is read — chart
// blobs are never decoded (footprint budget, plan §12.1/4).
func NewHelmReleaseWatcher(client dynamic.Interface, namespace string) Watcher {
	return &dynamicWatcher{
		source:    SourceHelmRelease,
		kind:      agentv1.CapabilityKind_CAPABILITY_KIND_HELM_RELEASE,
		client:    client,
		gvr:       secretsGVR,
		namespace: namespace,
		selector:  "owner=helm",
		mapFn:     mapHelmSecret,
	}
}

func mapHelmSecret(obj *unstructured.Unstructured) (*agentv1.Capability, error) {
	labels := obj.GetLabels()
	name := labels["name"]
	if name == "" {
		return nil, fmt.Errorf("helm secret %s/%s has no name label", obj.GetNamespace(), obj.GetName())
	}
	return &agentv1.Capability{
		Name:    fmt.Sprintf("%s/%s", obj.GetNamespace(), name),
		Group:   "helm.sh",
		Version: labels["version"],
		UiHints: mustStruct(map[string]interface{}{
			"namespace": obj.GetNamespace(),
			"status":    labels["status"],
		}),
	}, nil
}
