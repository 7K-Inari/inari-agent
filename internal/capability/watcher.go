// Package capability defines discovery watches that project cluster reality
// into the Inari catalog (plan §5.3, design principle 5: the catalog is a
// projection of reality — capabilities are discovered, not declared).
// Per-source watcher implementations land in M1.
package capability

import "context"

// Source identifies a discovery source.
type Source string

const (
	SourceCRD             Source = "crd"              // CRDs + OpenAPI v3 schemas
	SourceOLM             Source = "olm"              // OLM ClusterServiceVersions
	SourceCrossplaneXRD   Source = "crossplane-xrd"   // XRDs and Compositions
	SourceCrossplaneProv  Source = "crossplane-provider"
	SourceHelmRelease     Source = "helm-release"
	SourceKRORGD          Source = "kro-rgd" // KRO ResourceGraphDefinitions
	SourceClusterMetadata Source = "cluster-metadata"
)

// Event is a capability-update event emitted by a watcher. Checksum enables
// resync-on-reconnect against the control plane's projection.
type Event struct {
	TenantID  string
	ClusterID string
	Source    Source
	Name      string
	Checksum  string
	Payload   []byte
}

// Watcher watches a single discovery source. Watches are read-only by
// design; pre-existing resources are observe-only unless explicitly adopted
// (brownfield default, plan §12.1/3).
type Watcher interface {
	// Source returns the discovery source this watcher covers.
	Source() Source
	// Start begins watching and emits capability-update events until ctx is
	// cancelled. The returned channel is closed on shutdown.
	Start(ctx context.Context) (<-chan Event, error)
}
