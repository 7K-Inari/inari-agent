package command

import (
	"context"
	"fmt"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/git"
	"github.com/7K-Inari/inari-agent/internal/render"
)

// providerFromProto maps the command's provider oneof to the render-local
// form. Credential material never appears here — references only (plan §4.1).
func providerFromProto(m *agentv1.SecretStoreApply) (render.SecretStoreProvider, error) {
	switch p := m.Provider.(type) {
	case *agentv1.SecretStoreApply_AwsSm:
		return render.SecretStoreProvider{
			Kind: "awsSM", Region: p.AwsSm.Region,
			AuthName: p.AwsSm.GetAuthSecretRef().GetName(), AuthNamespace: p.AwsSm.GetAuthSecretRef().GetNamespace(),
		}, nil
	case *agentv1.SecretStoreApply_Vault:
		return render.SecretStoreProvider{
			Kind: "vault", Server: p.Vault.Server, Path: p.Vault.Path,
			AuthName: p.Vault.GetAuthSecretRef().GetName(), AuthNamespace: p.Vault.GetAuthSecretRef().GetNamespace(),
		}, nil
	case *agentv1.SecretStoreApply_GcpSm:
		return render.SecretStoreProvider{
			Kind: "gcpsm", ProjectID: p.GcpSm.ProjectId,
			AuthName: p.GcpSm.GetAuthSecretRef().GetName(), AuthNamespace: p.GcpSm.GetAuthSecretRef().GetNamespace(),
		}, nil
	case *agentv1.SecretStoreApply_AzureKv:
		return render.SecretStoreProvider{
			Kind: "azurekv", VaultURL: p.AzureKv.VaultUrl, TenantID: p.AzureKv.TenantId,
			AuthName: p.AzureKv.GetAuthSecretRef().GetName(), AuthNamespace: p.AzureKv.GetAuthSecretRef().GetNamespace(),
		}, nil
	}
	return render.SecretStoreProvider{}, fmt.Errorf("secret store %q: no provider set", m.Name)
}

// SecretStoreApplyHandler renders an ESO SecretStore/ClusterSecretStore
// manifest and commits it to the tenant state repo.
func SecretStoreApplyHandler(deps GitOpsDeps) KindHandler {
	return func(ctx context.Context, ev *agentv1.Event) (agentv1.CommandResult, string, error) {
		var m agentv1.SecretStoreApply
		if err := ev.Payload.UnmarshalTo(&m); err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, "", fmt.Errorf("decode: %w", err)
		}
		target, err := deps.resolveTarget(m.Target)
		if err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, err.Error(), nil
		}
		provider, err := providerFromProto(&m)
		if err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, err.Error(), nil
		}
		files, err := render.RenderSecretStore(m.Name, m.Scope, provider)
		if err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, err.Error(), nil
		}
		if err := validateFiles(files); err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, err.Error(), nil
		}
		outcome, err := deps.commit(ctx, target, files, m.Policy, m.CommandId,
			fmt.Sprintf("inari: apply secret store %s", m.Name))
		if err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_UNSPECIFIED, "", err
		}
		return agentv1.CommandResult_COMMAND_RESULT_APPLIED, outcome, nil
	}
}

// SecretStoreDeleteHandler prunes a previously applied ESO SecretStore
// manifest from the tenant state repo. Deleting an absent file is a no-op,
// keeping redelivery idempotent.
func SecretStoreDeleteHandler(deps GitOpsDeps) KindHandler {
	return func(ctx context.Context, ev *agentv1.Event) (agentv1.CommandResult, string, error) {
		var m agentv1.SecretStoreDelete
		if err := ev.Payload.UnmarshalTo(&m); err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, "", fmt.Errorf("decode: %w", err)
		}
		target, err := deps.resolveTarget(m.Target)
		if err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, err.Error(), nil
		}
		path := m.Name + ".yaml"
		if err := git.ValidateFilePath(path); err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, err.Error(), nil
		}
		if m.Policy == agentv1.CommitPolicy_COMMIT_POLICY_PULL_REQUEST {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED,
				"pull-request policy is not supported for secret store deletes", nil
		}
		sha, changed, err := deps.Git.DeleteFiles(ctx, target, []string{path},
			fmt.Sprintf("inari: delete secret store %s", m.Name))
		if err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_UNSPECIFIED, "", err
		}
		if !changed {
			return agentv1.CommandResult_COMMAND_RESULT_APPLIED, "already absent", nil
		}
		return agentv1.CommandResult_COMMAND_RESULT_APPLIED, "commit " + sha, nil
	}
}
