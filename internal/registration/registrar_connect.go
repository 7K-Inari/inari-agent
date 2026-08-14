package registration

import (
	"context"
	"fmt"
	"strings"

	"connectrpc.com/connect"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
	"github.com/7K-Inari/inari-api/gen/go/inari/agent/v1/agentv1connect"
)

// ConnectRegistrar exchanges the one-time bootstrap token for per-cluster
// OIDC client credentials via the inari-api RegistrationService (plan §5.3
// lifecycle step 1). The client secret VALUE is never returned by the API;
// the response carries only the ESO delivery reference.
type ConnectRegistrar struct {
	Client agentv1connect.RegistrationServiceClient

	// AgentVersion is reported to the control plane for compatibility
	// tracking (plan §5.11 N/N−1 contract).
	AgentVersion string
	// TenantID comes from the install manifest (the registration token is
	// opaque to the agent; the tenant is operator-supplied).
	TenantID string
	// ControlPlane is the base URL of the Agent Gateway.
	ControlPlane string
	// ClusterLabels feed ClusterSet targeting (plan §5.11).
	ClusterLabels map[string]string
	// KubernetesVersion is the tenant cluster's server version.
	KubernetesVersion string
}

// Register implements Registrar. The bootstrap token is sent exactly once
// and never stored, logged, or written to the cluster; replay is rejected
// server-side.
func (r *ConnectRegistrar) Register(ctx context.Context, bootstrapToken string) (*Credentials, error) {
	if bootstrapToken == "" {
		return nil, fmt.Errorf("registration: bootstrap token is empty")
	}
	resp, err := r.Client.RegisterCluster(ctx, connect.NewRequest(&agentv1.RegisterClusterRequest{
		RegistrationToken: bootstrapToken,
		AgentVersion:      r.AgentVersion,
		ContractVersion:   ContractVersion,
		ClusterLabels:     r.ClusterLabels,
		KubernetesVersion: r.KubernetesVersion,
	}))
	if err != nil {
		return nil, fmt.Errorf("registration: exchange bootstrap token: %w", err)
	}
	msg := resp.Msg
	if msg.GetClusterId() == "" || msg.GetClientId() == "" || msg.GetOidcIssuerUrl() == "" {
		return nil, fmt.Errorf("registration: incomplete response from control plane")
	}
	delivery := msg.GetClientSecretDelivery()
	creds := &Credentials{
		TenantID:     r.TenantID,
		ClusterID:    msg.GetClusterId(),
		ClientID:     msg.GetClientId(),
		TokenURL:     strings.TrimSuffix(msg.GetOidcIssuerUrl(), "/") + "/protocol/openid-connect/token",
		ControlPlane: r.ControlPlane,
		SecretRef: SecretReference{
			Store:     delivery.GetEsoSecretStore(),
			Name:      delivery.GetSecretName(),
			Namespace: delivery.GetSecretNamespace(),
			Key:       delivery.GetSecretKey(),
		},
	}
	if ts := msg.GetCredentialsExpireHint(); ts != nil {
		creds.ExpiresHint = ts.AsTime()
	}
	return creds, nil
}
