// Package registration defines the agent bootstrap contract (plan §5.3).
// A one-time TTL'd bootstrap token is exchanged for a per-cluster Keycloak
// OIDC client (client-credentials, short-lived JWT with a hardcoded
// cluster_id claim). The bootstrap token is forgotten after the exchange.
package registration

import (
	"context"
	"time"
)

// ContractVersion is the inari-api contract package this agent implements.
const ContractVersion = "inari.agent.v1"

// SecretReference points at the in-cluster location where the OIDC client
// secret is delivered via the External Secrets Operator. The secret VALUE
// never crosses the registration API and is never committed to git (plan
// §5.3, §5.10).
type SecretReference struct {
	Store     string
	Name      string
	Namespace string
	Key       string
}

// Credentials are the per-cluster OIDC client credentials obtained from
// registration. The client secret itself is delivered via ESO; Credentials
// carries only the reference to it (see SecretReference).
type Credentials struct {
	TenantID     string
	ClusterID    string
	ClientID     string
	TokenURL     string
	ControlPlane string
	SecretRef    SecretReference
	ExpiresHint  time.Time
}

// Registrar exchanges a one-time bootstrap token for cluster credentials.
// Implementations must not retain the bootstrap token after Register
// returns (success or failure).
type Registrar interface {
	Register(ctx context.Context, bootstrapToken string) (*Credentials, error)
}

// SecretReader resolves the ESO-delivered client secret for registered
// credentials. It blocks until the referenced Secret materialises or ctx is
// cancelled (ESO sync is asynchronous).
type SecretReader interface {
	ReadSecret(ctx context.Context, ref SecretReference) (string, error)
}
