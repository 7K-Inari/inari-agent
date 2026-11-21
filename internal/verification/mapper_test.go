package verification_test

import (
	"strings"
	"testing"
	"time"

	verification "github.com/7K-Inari/inari-agent/internal/verification"
	contract "github.com/7K-Inari/inari-api/contract/verification/v1"
	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	instanceUID = "2b6e4424-87a4-4f7c-b6d8-dfe2f6a894f8"
	appDigest   = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	suiteDigest = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	appRevision = "sha256:99595485ac9f27595465be7873826681a9a19732e39a8588ffcf827c536308f9"
)

func workflow() *metav1.Unstructured {
	return &metav1.Unstructured{Object: map[string]any{
		"apiVersion": "argoproj.io/v1alpha1", "kind": "Workflow",
		"metadata": map[string]any{
			"name": "vr-run-a1", "namespace": "inari-verification", "uid": "wf-uid-1", "resourceVersion": "42",
			"labels": map[string]any{verification.LabelVerification: "true"},
			"annotations": map[string]any{
				verification.AnnotationRunID: "vr-run", verification.AnnotationAttempt: "1",
				verification.AnnotationTriggerFingerprint: "sha256:" + strings.Repeat("a", 64),
				verification.AnnotationSuiteDigest:        suiteDigest, verification.AnnotationProfile: "prod",
				verification.AnnotationRenderFingerprint: "sha256:" + strings.Repeat("b", 64),
				verification.AnnotationInstanceUID:       instanceUID, verification.AnnotationAppInstanceID: "app-1",
				verification.AnnotationAppRevision: appRevision, verification.AnnotationAppDigest: appDigest,
				verification.AnnotationTargetID: "rt-1", verification.AnnotationTargetRevision: "4",
				verification.AnnotationPolicyRevision: "7", verification.AnnotationGrantRevision: "3",
			},
		},
		"status": map[string]any{
			"phase": "Succeeded", "startedAt": "2026-11-21T01:00:00Z", "finishedAt": "2026-11-21T01:01:00Z",
			"outputs": map[string]any{"parameters": []any{map[string]any{
				"name": verification.OutcomeParameterName, "value": `{"schema":"inari.verification.outcome/v1","result":"passed","expiresAt":"2026-11-22T01:00:00Z"}`,
			}}},
		},
	}}
}

func TestMapWorkflow(t *testing.T) {
	got, err := verification.MapWorkflow(workflow(), time.Date(2026, 11, 21, 1, 2, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("MapWorkflow: %v", err)
	}
	if got == nil || got.RunId != "vr-run" || got.Result != agentv1.VerificationResult_VERIFICATION_RESULT_PASSED ||
		got.Workflow.GetUid() != "wf-uid-1" || got.ResourceVersion != "42" || got.OutcomeJson == "" || got.OutcomeDigest == "" {
		t.Fatalf("unexpected report: %#v", got)
	}
	if got.StartedAt.GetSeconds() != got.FinishedAt.GetSeconds()-60 || got.ExpiresAt == nil || got.ObservedAt.GetSeconds() != got.FinishedAt.GetSeconds()+60 {
		t.Fatalf("timestamp mapping mismatch: %#v", got)
	}
	if got.SubjectKind != "application" || got.PolicyRevision != 7 || got.GrantRevision != 3 {
		t.Fatalf("binding defaults mismatch: %#v", got)
	}
}

func TestMapWorkflowFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*metav1.Unstructured)
	}{
		{name: "unlabeled", mutate: func(u *metav1.Unstructured) { u.SetLabels(nil) }},
		{name: "wrong kind", mutate: func(u *metav1.Unstructured) { u.SetKind("CronWorkflow") }},
		{name: "missing suite", mutate: func(u *metav1.Unstructured) {
			a := u.GetAnnotations()
			delete(a, verification.AnnotationSuiteDigest)
			u.SetAnnotations(a)
		}},
		{name: "oversized outcome", mutate: func(u *metav1.Unstructured) {
			params, _, _ := metav1.NestedSlice(u.Object, "status", "outputs", "parameters")
			params[0].(map[string]any)["value"] = strings.Repeat("x", 65537)
			_ = metav1.SetNestedSlice(u.Object, params, "status", "outputs", "parameters")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := workflow()
			tt.mutate(u)
			if _, err := verification.MapWorkflow(u, time.Now()); err == nil {
				t.Fatal("invalid Workflow was accepted")
			}
		})
	}
	u := workflow()
	u.Object["status"].(map[string]any)["phase"] = "Failed"
	got, err := verification.MapWorkflow(u, time.Now())
	if err != nil || got.Result != agentv1.VerificationResult_VERIFICATION_RESULT_FAILED {
		t.Fatalf("failed phase = %#v, %v", got, err)
	}
}

func TestMapCronWorkflowDerivesSharedIdentity(t *testing.T) {
	u := workflow()
	u.SetName("vc-nightly-1767223200")
	a := u.GetAnnotations()
	delete(a, verification.AnnotationRunID)
	delete(a, verification.AnnotationTriggerFingerprint)
	a[verification.AnnotationCronName] = "vc-nightly"
	a[verification.AnnotationScheduleSetID] = "nightly"
	u.SetAnnotations(a)
	got, err := verification.MapWorkflow(u, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	trigger, err := contract.CronTriggerFromChildName("vc-nightly", u.GetName(), "prod", "nightly")
	if err != nil {
		t.Fatal(err)
	}
	identity, err := contract.ComputeRunIdentity(instanceUID, appRevision, suiteDigest, trigger)
	if err != nil {
		t.Fatal(err)
	}
	if got.RunId != identity.RunID || got.TriggerFingerprint != identity.TriggerFingerprint {
		t.Fatalf("cron identity = %#v, want %#v", got, identity)
	}
}
