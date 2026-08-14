// Package capability implements discovery watches that project cluster
// reality into the Inari catalog (plan §5.3, design principle 5: the
// catalog is a projection of reality — capabilities are discovered, not
// declared). All watches are read-only by design; pre-existing resources
// are observe-only unless explicitly adopted (brownfield default,
// plan §12.1/3).
package capability

import (
	"context"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

// Source identifies a discovery source.
type Source string

const (
	SourceCRD             Source = "crd" // CRDs + OpenAPI v3 schemas
	SourceOLM             Source = "olm" // OLM ClusterServiceVersions
	SourceCrossplaneXRD   Source = "crossplane-xrd"
	SourceCrossplaneProv  Source = "crossplane-provider"
	SourceHelmRelease     Source = "helm-release"
	SourceKRORGD          Source = "kro-rgd" // KRO ResourceGraphDefinitions
	SourceClusterMetadata Source = "cluster-metadata"
)

// Watcher watches a single discovery source and emits discovered
// capabilities (upserts and deletes).
type Watcher interface {
	// Source returns the discovery source this watcher covers.
	Source() Source
	// Start begins watching and emits capabilities until ctx is cancelled.
	// The returned channel is closed on shutdown.
	Start(ctx context.Context) (<-chan *agentv1.Capability, error)
}
