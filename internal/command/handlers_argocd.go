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

// RegisterArgoCDAppSetHandler registers fleet addon ApplicationSets from
// server-rendered manifests (Feature C bridge command). The manifest is
// upserted verbatim — the agent never composes ApplicationSets itself.
func RegisterArgoCDAppSetHandler(gate ArgoCDGate, newClient func(namespace string) *argocd.Client) KindHandler {
	return registerManifestHandler(gate, newClient, "applicationset",
		func(ctx context.Context, c *argocd.Client, name, manifest string) error {
			return c.EnsureApplicationSet(ctx, name, manifest)
		})
}

// RegisterArgoCDAppManifestHandler registers single-cluster addon
// Applications from server-rendered manifests (Feature C bridge command).
// Verbatim upsert preserves multi-source helm apps and TakeOwnership
// sync options that RegisterArgoCDApp fields cannot express.
func RegisterArgoCDAppManifestHandler(gate ArgoCDGate, newClient func(namespace string) *argocd.Client) KindHandler {
	return registerManifestHandler(gate, newClient, "application",
		func(ctx context.Context, c *argocd.Client, name, manifest string) error {
			return c.EnsureApplicationManifest(ctx, name, manifest)
		})
}

func registerManifestHandler(gate ArgoCDGate, newClient func(namespace string) *argocd.Client, kind string,
	apply func(context.Context, *argocd.Client, string, string) error) KindHandler {
	return func(ctx context.Context, ev *agentv1.Event) (agentv1.CommandResult, string, error) {
		_, name, _, manifest, err := DecodeBridgeManifest(ev.Payload)
		if err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, err.Error(), nil
		}
		ns, err := gate.EnsureReady(ctx)
		if err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, err.Error(), nil
		}
		if err := apply(ctx, newClient(ns), name, manifest); err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, err.Error(), nil
		}
		return agentv1.CommandResult_COMMAND_RESULT_APPLIED,
			fmt.Sprintf("%s %s registered in namespace %s", kind, name, ns), nil
	}
}
