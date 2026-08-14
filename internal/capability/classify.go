package capability

import (
	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// ManagementAnnotation explicitly selects the brownfield management mode for
// a pre-existing resource: "adopt" (bring under Inari management — explicit,
// audited), "observe-only" (inventory without mutation), or "ignore"
// (excluded from inventory). Absent or unknown values default to
// observe-only (plan §5.3, §12.1/3): nothing is mutated on first connect.
const ManagementAnnotation = "inari.dev/management"

// Classify returns the brownfield management mode for a discovered object.
// The default is always observe-only; adoption must be explicit per
// resource.
func Classify(obj *unstructured.Unstructured) agentv1.ManagementMode {
	switch obj.GetAnnotations()[ManagementAnnotation] {
	case "adopt":
		return agentv1.ManagementMode_MANAGEMENT_MODE_ADOPT
	case "ignore":
		return agentv1.ManagementMode_MANAGEMENT_MODE_IGNORE
	default:
		return agentv1.ManagementMode_MANAGEMENT_MODE_OBSERVE_ONLY
	}
}
