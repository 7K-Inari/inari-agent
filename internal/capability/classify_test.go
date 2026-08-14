package capability

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

func obj(annotations map[string]string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetName("thing")
	u.SetAnnotations(annotations)
	return u
}

func TestClassifyDefaultsToObserveOnly(t *testing.T) {
	if got := Classify(obj(nil)); got != agentv1.ManagementMode_MANAGEMENT_MODE_OBSERVE_ONLY {
		t.Errorf("Classify(pre-existing, no annotations) = %v, want OBSERVE_ONLY", got)
	}
	if got := Classify(obj(map[string]string{"unrelated": "x"})); got != agentv1.ManagementMode_MANAGEMENT_MODE_OBSERVE_ONLY {
		t.Errorf("Classify(unrelated annotations) = %v, want OBSERVE_ONLY", got)
	}
}

func TestClassifyExplicitOverrides(t *testing.T) {
	cases := map[string]agentv1.ManagementMode{
		"adopt":        agentv1.ManagementMode_MANAGEMENT_MODE_ADOPT,
		"observe-only": agentv1.ManagementMode_MANAGEMENT_MODE_OBSERVE_ONLY,
		"ignore":       agentv1.ManagementMode_MANAGEMENT_MODE_IGNORE,
	}
	for value, want := range cases {
		got := Classify(obj(map[string]string{ManagementAnnotation: value}))
		if got != want {
			t.Errorf("Classify(%q) = %v, want %v", value, got, want)
		}
	}
}

func TestClassifyUnknownValueFallsBackToObserveOnly(t *testing.T) {
	got := Classify(obj(map[string]string{ManagementAnnotation: "bogus"}))
	if got != agentv1.ManagementMode_MANAGEMENT_MODE_OBSERVE_ONLY {
		t.Errorf("Classify(bogus) = %v, want OBSERVE_ONLY", got)
	}
}

func TestChecksumDeterministicAndOrderIndependent(t *testing.T) {
	a := &agentv1.Capability{Kind: agentv1.CapabilityKind_CAPABILITY_KIND_CRD, Name: "a", Group: "g", Version: "v1"}
	b := &agentv1.Capability{Kind: agentv1.CapabilityKind_CAPABILITY_KIND_OLM_CSV, Name: "b"}
	if StateChecksum([]*agentv1.Capability{a, b}) != StateChecksum([]*agentv1.Capability{b, a}) {
		t.Error("state checksum must not depend on slice order")
	}
	if StateChecksum([]*agentv1.Capability{a}) == StateChecksum([]*agentv1.Capability{a, b}) {
		t.Error("state checksum must change with membership")
	}
	c := &agentv1.Capability{Kind: agentv1.CapabilityKind_CAPABILITY_KIND_CRD, Name: "a", Group: "g", Version: "v2"}
	if StateChecksum([]*agentv1.Capability{a}) == StateChecksum([]*agentv1.Capability{c}) {
		t.Error("state checksum must change with content")
	}
}
