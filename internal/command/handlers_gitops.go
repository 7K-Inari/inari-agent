package command

import (
	"context"
	"fmt"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/git"
	"github.com/7K-Inari/inari-agent/internal/render"
)

// GitOpsDeps are the shared dependencies of the render/apply handlers.
type GitOpsDeps struct {
	Git git.Provider
	// DefaultStateRepo is the platform-owned `<tenant>-inari-state`
	// repository ("owner/name"). Commands may not target any other repo:
	// application repos stay untouched (plan §5.3 GitOps decisions).
	DefaultStateRepo string
}

// resolveTarget validates the command target against the tenant state
// repo constraint.
func (d GitOpsDeps) resolveTarget(t *agentv1.GitTarget) (git.Target, error) {
	gt := git.Target{}
	if t != nil {
		gt = git.Target{Repo: t.Repo, Path: t.Path, Branch: t.Branch}
	}
	if gt.Repo != "" && gt.Repo != d.DefaultStateRepo {
		return gt, fmt.Errorf("target repo %q is not the platform-owned state repo %q", gt.Repo, d.DefaultStateRepo)
	}
	return gt.Validate(d.DefaultStateRepo)
}

// validateFiles rejects file paths that could escape the target
// directory in the state repo.
func validateFiles(files []git.File) error {
	for _, f := range files {
		if err := git.ValidateFilePath(f.Path); err != nil {
			return err
		}
	}
	return nil
}

// commit writes files per the command's commit policy and reports the
// outcome. UNSPECIFIED defaults to direct commit (tenant policy resolved
// upstream by the control plane).
func (d GitOpsDeps) commit(ctx context.Context, target git.Target, files []git.File, policy agentv1.CommitPolicy, commandID, message string) (string, error) {
	switch policy {
	case agentv1.CommitPolicy_COMMIT_POLICY_PULL_REQUEST:
		branch := "inari/" + commandID
		prURL, err := d.Git.OpenPR(ctx, target, files, branch, message, "Rendered by inari-agent (command "+commandID+").")
		if err != nil {
			return "", err
		}
		return "pull request: " + prURL, nil
	default:
		sha, changed, err := d.Git.CommitFiles(ctx, target, files, message)
		if err != nil {
			return "", err
		}
		if !changed {
			return "commit " + sha + " (no changes)", nil
		}
		return "commit " + sha, nil
	}
}

// RenderRGDInstanceHandler renders a KRO instance and commits it to the
// tenant state repo.
func RenderRGDInstanceHandler(deps GitOpsDeps, getter render.RGDGetter) KindHandler {
	return func(ctx context.Context, ev *agentv1.Event) (agentv1.CommandResult, string, error) {
		var m agentv1.RenderRgdInstance
		if err := ev.Payload.UnmarshalTo(&m); err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, "", fmt.Errorf("decode: %w", err)
		}
		target, err := deps.resolveTarget(m.Target)
		if err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, err.Error(), nil
		}
		params := map[string]interface{}{}
		if m.Parameters != nil {
			params = m.Parameters.AsMap()
		}
		files, err := render.RenderRGDInstance(ctx, getter, m.RgdRef, m.InstanceName, m.TargetNamespace, params)
		if err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, err.Error(), nil
		}
		if err := validateFiles(files); err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, err.Error(), nil
		}
		outcome, err := deps.commit(ctx, target, files, m.Policy, m.CommandId,
			fmt.Sprintf("inari: render %s instance %s", m.RgdRef, m.InstanceName))
		if err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_UNSPECIFIED, "", err
		}
		return agentv1.CommandResult_COMMAND_RESULT_APPLIED, outcome, nil
	}
}

// ApplyBundleHandler fetches a bundle (OCI artifact or git source,
// checksum-verified) and commits it to the tenant state repo.
func ApplyBundleHandler(deps GitOpsDeps, bundles render.BundleSource) KindHandler {
	return func(ctx context.Context, ev *agentv1.Event) (agentv1.CommandResult, string, error) {
		var m agentv1.ApplyBundle
		if err := ev.Payload.UnmarshalTo(&m); err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, "", fmt.Errorf("decode: %w", err)
		}
		target, err := deps.resolveTarget(m.Target)
		if err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, err.Error(), nil
		}
		ref := render.BundleRef{Checksum: m.Checksum}
		switch src := m.Source.(type) {
		case *agentv1.ApplyBundle_OciRef:
			ref.OCIRef = src.OciRef
		case *agentv1.ApplyBundle_GitUrl:
			ref.GitURL = src.GitUrl
		}
		files, err := bundles.Fetch(ctx, ref)
		if err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, err.Error(), nil
		}
		if err := validateFiles(files); err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, err.Error(), nil
		}
		outcome, err := deps.commit(ctx, target, files, m.Policy, m.CommandId, "inari: apply bundle")
		if err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_UNSPECIFIED, "", err
		}
		return agentv1.CommandResult_COMMAND_RESULT_APPLIED, outcome, nil
	}
}
