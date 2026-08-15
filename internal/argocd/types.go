// Package argocd integrates the tenant-local GitOps engine (plan §5.3
// phase 4, §11 decision 1). ArgoCD is bundle-managed by default — the
// agent installs and lifecycle-manages it — with a BYO flag to adopt an
// existing installation under the documented version-skew policy (M1
// spike docs/spikes/m1-argocd-bundle-lifecycle.md).
//
// Hard constraints from the spike: argoproj.io/v1alpha1 only; per-team
// AppProjects; restrictive BYO installs surface explicit deploy-time
// errors; ApplicationSet is never required.
package argocd

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ManagedByLabel / ManagedByValue mark Inari-owned ArgoCD objects.
const (
	ManagedByLabel = "inari.dev/managed-by"
	ManagedByValue = "inari-agent"
	// InstanceLabel links an Application to its resource instance.
	InstanceLabel = "inari.dev/instance"
)

var (
	// ApplicationGVR is argoproj.io/v1alpha1 applications.
	ApplicationGVR = schema.GroupVersionResource{Group: "argoproj.io", Version: "v1alpha1", Resource: "applications"}
	// AppProjectGVR is argoproj.io/v1alpha1 appprojects.
	AppProjectGVR = schema.GroupVersionResource{Group: "argoproj.io", Version: "v1alpha1", Resource: "appprojects"}
)

// Mode is how the agent relates to the tenant-local ArgoCD (§11/1).
type Mode string

const (
	// ModeBundle: the agent installs and lifecycle-manages ArgoCD.
	ModeBundle Mode = "bundle"
	// ModeBYO: adopt an existing installation (observe-only default).
	ModeBYO Mode = "byo"
)
