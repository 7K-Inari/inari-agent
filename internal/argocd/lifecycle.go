package argocd

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Skew policy (M1 spike deliverable, §5.11 mirror): the agent supports
// bundle-managed ArgoCD minor lines N and N−1; installs older than
// SupportedMin are refused adoption — never silently upgraded.
const (
	// BundleLine is the ArgoCD minor line the agent's bundle installs (N).
	BundleLine = "3.0"
	// PreviousLine is the N−1 supported minor line (last 2.x, per spike).
	PreviousLine = "2.14"
	// SupportedMin is the oldest BYO version eligible for adoption.
	SupportedMin = "v2.14.0"
)

// Classification of a detected installation.
type Classification string

const (
	// ClassNone: nothing found → bundle-managed default.
	ClassNone Classification = "none"
	// ClassAdoptable: found, in-skew → adoption candidate.
	ClassAdoptable Classification = "adoptable"
	// ClassOutOfSkew: found, out of the supported window → refuse
	// adoption; the agent runs observe-only and never upgrades it.
	ClassOutOfSkew Classification = "out-of-skew"
)

// Installation describes a detected tenant-local ArgoCD.
type Installation struct {
	Namespace      string
	Version        string
	HelmManaged    bool
	Classification Classification
}

var imageVersionRe = regexp.MustCompile(`:(v?\d+\.\d+\.\d+)`)

// ParseVersion extracts "2.14.11" from a container image reference.
func ParseVersion(image string) (string, error) {
	m := imageVersionRe.FindStringSubmatch(image)
	if m == nil {
		return "", fmt.Errorf("argocd: no version tag in image %q", image)
	}
	return strings.TrimPrefix(m[1], "v"), nil
}

// minor returns "3.0" from "3.0.6".
func minor(v string) string {
	parts := strings.Split(v, ".")
	if len(parts) < 2 {
		return v
	}
	return parts[0] + "." + parts[1]
}

// versionGE compares dotted numeric versions.
func versionGE(a, b string) bool {
	pa := strings.Split(strings.TrimPrefix(a, "v"), ".")
	pb := strings.Split(strings.TrimPrefix(b, "v"), ".")
	for i := 0; i < len(pa) && i < len(pb); i++ {
		na, _ := strconv.Atoi(pa[i])
		nb, _ := strconv.Atoi(pb[i])
		if na != nb {
			return na > nb
		}
	}
	return len(pa) >= len(pb)
}

// InSkew reports whether version v is within the supported window of the
// bundle line: minor line N or N−1 and >= SupportedMin. Newer-than-bundle
// installs are out of skew (observe-only until the bundle catches up).
func InSkew(v string) bool {
	v = strings.TrimPrefix(v, "v")
	if !versionGE(v, SupportedMin) {
		return false
	}
	m := minor(v)
	return m == BundleLine || m == PreviousLine
}

// Detect locates a tenant-local ArgoCD across namespaces (port of the
// spike detect-byo.sh probe, read-only): finds the argocd-server
// Deployment, reads its image version, and infers Helm management from
// labels. Returns nil when nothing is found.
func Detect(ctx context.Context, kube kubernetes.Interface) (*Installation, error) {
	deploys, err := kube.AppsV1().Deployments("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("argocd: detect: list deployments: %w", err)
	}
	for i := range deploys.Items {
		d := &deploys.Items[i]
		if d.Name != "argocd-server" {
			continue
		}
		inst := &Installation{Namespace: d.Namespace}
		for _, c := range d.Spec.Template.Spec.Containers {
			if c.Name == "argocd-server" || strings.Contains(c.Image, "argocd") {
				v, err := ParseVersion(c.Image)
				if err != nil {
					return nil, err
				}
				inst.Version = v
				break
			}
		}
		if inst.Version == "" {
			return nil, fmt.Errorf("argocd: detect: argocd-server in %s has no image version", d.Namespace)
		}
		inst.HelmManaged = d.Labels["app.kubernetes.io/managed-by"] == "Helm"
		if InSkew(inst.Version) {
			inst.Classification = ClassAdoptable
		} else {
			inst.Classification = ClassOutOfSkew
		}
		return inst, nil
	}
	return nil, nil
}
