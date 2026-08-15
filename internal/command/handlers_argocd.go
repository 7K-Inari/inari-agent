package command

import (
	"context"
	"fmt"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/argocd"
)

// ArgoCDGate verifies the tenant-local ArgoCD is available under the
// configured mode and returns its namespace (argocd.Lifecycle).
type ArgoCDGate interface {
	EnsureReady(ctx context.Context) (string, error)
}

// RegisterArgoCDAppHandler registers tenant-local ArgoCD Applications for
// rendered instances (plan §5.3 phase 4). The gate enforces the
// bundle-managed/BYO skew policy before any object is touched.
func RegisterArgoCDAppHandler(gate ArgoCDGate, newClient func(namespace string) *argocd.Client) KindHandler {
	return func(ctx context.Context, ev *agentv1.Event) (agentv1.CommandResult, string, error) {
		var m agentv1.RegisterArgoCDApp
		if err := ev.Payload.UnmarshalTo(&m); err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, "", fmt.Errorf("decode: %w", err)
		}
		ns, err := gate.EnsureReady(ctx)
		if err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, err.Error(), nil
		}
		client := newClient(ns)
		if m.Project != "" && m.Project != "default" {
			if err := client.EnsureAppProject(ctx, m.Project); err != nil {
				return agentv1.CommandResult_COMMAND_RESULT_FAILED, err.Error(), nil
			}
		}
		if err := client.EnsureApplication(ctx, &m); err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, err.Error(), nil
		}
		return agentv1.CommandResult_COMMAND_RESULT_APPLIED,
			fmt.Sprintf("application %s registered in namespace %s", m.Name, ns), nil
	}
}
