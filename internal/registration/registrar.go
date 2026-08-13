// Package registration defines the agent bootstrap contract (plan §5.3).
// A one-time TTL'd bootstrap token is exchanged for a per-cluster Keycloak
// OIDC client (client-credentials, short-lived JWT with a hardcoded
// cluster_id claim). The bootstrap token is forgotten after the exchange.
// Implementation lands in M1.
package registration

import "context"

// Credentials are the per-cluster OIDC client credentials obtained from
// registration. The client secret is delivered via ESO and never stored
// in git.
type Credentials struct {
	TenantID     string
	ClusterID    string
	ClientID     string
	TokenURL     string
	ControlPlane string
}

// Registrar exchanges a one-time bootstrap token for cluster credentials.
type Registrar interface {
	Register(ctx context.Context, bootstrapToken string) (*Credentials, error)
}
