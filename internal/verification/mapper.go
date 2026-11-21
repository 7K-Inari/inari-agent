// Package verification observes only Inari-labeled Argo Workflow execution
// objects. It never creates, updates, deletes, or fetches suite artifacts.
package verification

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	contract "github.com/7K-Inari/inari-api/contract/verification/v1"
	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	LabelVerification             = "inari.dev/verification"
	AnnotationRunID               = "inari.dev/run-id"
	AnnotationAttempt             = "inari.dev/attempt"
	AnnotationTriggerFingerprint  = "inari.dev/trigger-fingerprint"
	AnnotationSuiteDigest         = "inari.dev/suite-digest"
	AnnotationProfile             = "inari.dev/profile"
	AnnotationRenderFingerprint   = "inari.dev/render-fingerprint"
	AnnotationInstanceUID         = "inari.dev/instance-uid"
	AnnotationAppInstanceID       = "inari.dev/app-instance-id"
	AnnotationDeploymentPlanID    = "inari.dev/deployment-plan-id"
	AnnotationAppRevision         = "inari.dev/app-revision"
	AnnotationAppDigest           = "inari.dev/app-digest"
	AnnotationTargetID            = "inari.dev/target-id"
	AnnotationTargetRevision      = "inari.dev/target-revision"
	AnnotationCompanionID         = "inari.dev/companion-id"
	AnnotationCompanionGeneration = "inari.dev/companion-generation"
	AnnotationPackDigest          = "inari.dev/pack-digest"
	AnnotationCapabilityDigest    = "inari.dev/capability-digest"
	AnnotationSubjectKind         = "inari.dev/subject-kind"
	AnnotationPolicyRevision      = "inari.dev/policy-revision"
	AnnotationGrantRevision       = "inari.dev/grant-revision"
	AnnotationCronName            = "inari.dev/cron-name"
	AnnotationScheduleSetID       = "inari.dev/schedule-set-id"
	OutcomeParameterName          = "inari-verification-outcome"
	maxOutcomeBytes               = 65536
)

var reportDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// MapWorkflow maps one owned Workflow without interpreting application checks.
func MapWorkflow(obj *unstructured.Unstructured, now time.Time) (*agentv1.VerificationReport, error) {
	if obj == nil || obj.GetKind() != "Workflow" || obj.GetAPIVersion() != "argoproj.io/v1alpha1" || obj.GetLabels()[LabelVerification] != "true" {
		return nil, errors.New("Workflow is not an Inari verification execution")
	}
	if obj.GetName() == "" || obj.GetNamespace() == "" || obj.GetUID() == "" || obj.GetResourceVersion() == "" {
		return nil, errors.New("Workflow identity or resourceVersion is incomplete")
	}
	a := obj.GetAnnotations()
	required := []string{AnnotationAttempt, AnnotationSuiteDigest, AnnotationProfile, AnnotationRenderFingerprint,
		AnnotationInstanceUID, AnnotationAppInstanceID, AnnotationAppRevision, AnnotationAppDigest,
		AnnotationTargetID, AnnotationTargetRevision, AnnotationPolicyRevision, AnnotationGrantRevision}
	for _, key := range required {
		if strings.TrimSpace(a[key]) == "" {
			return nil, fmt.Errorf("required verification annotation %s is missing", key)
		}
	}
	for _, key := range []string{AnnotationSuiteDigest, AnnotationAppRevision, AnnotationAppDigest} {
		if !reportDigestPattern.MatchString(a[key]) {
			return nil, fmt.Errorf("annotation %s must be a sha256 digest", key)
		}
	}
	attempt, err := positiveUint(a[AnnotationAttempt])
	if err != nil {
		return nil, fmt.Errorf("attempt: %w", err)
	}
	targetRevision, err := positiveInt(a[AnnotationTargetRevision])
	if err != nil {
		return nil, fmt.Errorf("targetRevision: %w", err)
	}
	policyRevision, err := positiveInt(a[AnnotationPolicyRevision])
	if err != nil {
		return nil, fmt.Errorf("policyRevision: %w", err)
	}
	grantRevision, err := positiveInt(a[AnnotationGrantRevision])
	if err != nil {
		return nil, fmt.Errorf("grantRevision: %w", err)
	}
	runID := a[AnnotationRunID]
	triggerFingerprint := a[AnnotationTriggerFingerprint]
	if runID == "" {
		trigger, err := contract.CronTriggerFromChildName(a[AnnotationCronName], obj.GetName(), a[AnnotationProfile], a[AnnotationScheduleSetID])
		if err != nil {
			return nil, fmt.Errorf("derive cron identity: %w", err)
		}
		identity, err := contract.ComputeRunIdentity(a[AnnotationInstanceUID], a[AnnotationAppRevision], a[AnnotationSuiteDigest], trigger)
		if err != nil {
			return nil, err
		}
		runID, triggerFingerprint = identity.RunID, identity.TriggerFingerprint
	}
	if runID == "" || len(runID) > 63 || !reportDigestPattern.MatchString(triggerFingerprint) {
		return nil, errors.New("run identity or trigger fingerprint is invalid")
	}
	subjectKind := a[AnnotationSubjectKind]
	if subjectKind == "" {
		subjectKind = "application"
	}
	if subjectKind != "application" && subjectKind != "observability.companion" {
		return nil, errors.New("unsupported verification subjectKind")
	}
	phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
	result := agentv1.VerificationResult_VERIFICATION_RESULT_PENDING
	switch phase {
	case "Succeeded":
		result = agentv1.VerificationResult_VERIFICATION_RESULT_PASSED
	case "Failed", "Error":
		result = agentv1.VerificationResult_VERIFICATION_RESULT_FAILED
	case "", "Pending", "Running":
	default:
		return nil, fmt.Errorf("unsupported Workflow phase %q", phase)
	}
	startedAt, err := workflowTimestamp(obj, "startedAt")
	if err != nil {
		return nil, err
	}
	finishedAt, err := workflowTimestamp(obj, "finishedAt")
	if err != nil {
		return nil, err
	}
	if (result == agentv1.VerificationResult_VERIFICATION_RESULT_PASSED || result == agentv1.VerificationResult_VERIFICATION_RESULT_FAILED) && finishedAt == nil {
		return nil, errors.New("terminal Workflow has no finishedAt timestamp")
	}
	outcomeJSON, err := workflowOutcome(obj)
	if err != nil {
		return nil, err
	}
	var expiresAt *timestamppb.Timestamp
	if outcomeJSON != "" {
		var outcome struct {
			ExpiresAt string `json:"expiresAt"`
		}
		if json.Unmarshal([]byte(outcomeJSON), &outcome) == nil && outcome.ExpiresAt != "" {
			if parsed, err := time.Parse(time.RFC3339, outcome.ExpiresAt); err == nil {
				expiresAt = timestamppb.New(parsed.UTC())
			}
		}
	}
	if now.IsZero() {
		now = time.Now()
	}
	message, _, _ := unstructured.NestedString(obj.Object, "status", "message")
	message = boundedMessage(message, 1024)
	outcomeDigest := ""
	if outcomeJSON != "" {
		sum := sha256.Sum256([]byte(outcomeJSON))
		outcomeDigest = "sha256:" + hex.EncodeToString(sum[:])
	}
	companionGeneration := int64(0)
	if value := a[AnnotationCompanionGeneration]; value != "" {
		companionGeneration, err = positiveInt(value)
		if err != nil {
			return nil, fmt.Errorf("companionGeneration: %w", err)
		}
	}
	checksumInput := strings.Join([]string{runID, a[AnnotationAttempt], obj.GetResourceVersion(), phase, outcomeDigest}, "\x00")
	checksum := sha256.Sum256([]byte(checksumInput))
	return &agentv1.VerificationReport{
		RunId: runID, Attempt: uint32(attempt), TriggerFingerprint: triggerFingerprint,
		SuiteDigest: a[AnnotationSuiteDigest], Profile: a[AnnotationProfile], RenderFingerprint: a[AnnotationRenderFingerprint],
		Workflow:        &agentv1.ResourceRef{Kind: "Workflow", Name: obj.GetName(), Namespace: obj.GetNamespace(), Uid: string(obj.GetUID())},
		ResourceVersion: obj.GetResourceVersion(), ObservedAt: timestamppb.New(now.UTC()), StartedAt: startedAt,
		FinishedAt: finishedAt, ExpiresAt: expiresAt, Result: result, Message: message,
		SubjectKind: subjectKind, InstanceUid: a[AnnotationInstanceUID], AppInstanceId: a[AnnotationAppInstanceID],
		DeploymentPlanId: a[AnnotationDeploymentPlanID], AppRevision: a[AnnotationAppRevision], AppDigest: a[AnnotationAppDigest],
		TargetId: a[AnnotationTargetID], TargetRevision: targetRevision, CompanionId: a[AnnotationCompanionID],
		CompanionGeneration: companionGeneration, PackDigest: a[AnnotationPackDigest], CapabilityDigest: a[AnnotationCapabilityDigest],
		OutcomeJson: outcomeJSON, OutcomeDigest: outcomeDigest, StateChecksum: "sha256:" + hex.EncodeToString(checksum[:]),
		PolicyRevision: policyRevision, GrantRevision: grantRevision,
	}, nil
}

func positiveInt(value string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 1 {
		return 0, errors.New("must be a positive integer")
	}
	return parsed, nil
}

func positiveUint(value string) (uint64, error) {
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil || parsed < 1 {
		return 0, errors.New("must be a positive integer")
	}
	return parsed, nil
}

func workflowTimestamp(obj *unstructured.Unstructured, field string) (*timestamppb.Timestamp, error) {
	value, found, err := unstructured.NestedString(obj.Object, "status", field)
	if err != nil {
		return nil, err
	}
	if !found || value == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, fmt.Errorf("Workflow %s: %w", field, err)
	}
	return timestamppb.New(parsed.UTC()), nil
}

func workflowOutcome(obj *unstructured.Unstructured) (string, error) {
	parameters, found, err := unstructured.NestedSlice(obj.Object, "status", "outputs", "parameters")
	if err != nil || !found {
		return "", err
	}
	for _, item := range parameters {
		parameter, _ := item.(map[string]any)
		if parameter["name"] != OutcomeParameterName {
			continue
		}
		value, _ := parameter["value"].(string)
		if len(value) > maxOutcomeBytes {
			return "", errors.New("verification outcome exceeds 65536 bytes")
		}
		return value, nil
	}
	return "", nil
}

func boundedMessage(value string, maxRunes int) string {
	runes := []rune(value)
	if len(runes) > maxRunes {
		return string(runes[:maxRunes])
	}
	return value
}
