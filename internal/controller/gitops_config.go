package controller

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/7K-Inari/inari-agent/internal/argocd"
	"github.com/7K-Inari/inari-agent/internal/command"
	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/git"
	"github.com/7K-Inari/inari-agent/internal/render"
	"github.com/7K-Inari/inari-agent/internal/status"
)

// GitOpsConfig carries the M2 GitOps dependencies: rendered manifests go
// to the platform-owned <tenant>-inari-state repo and are applied by the
// tenant-local ArgoCD (plan §5.3, §11/1-2, §12.1/2).
type GitOpsConfig struct {
	Kube kubernetes.Interface
	Dyn  dynamic.Interface

	// Git writes to the tenant state repo (GitHub App auth via ESO).
	Git git.Provider
	// StateRepo overrides the default "<tenant>-inari-state" repo
	// ("owner/name"); the tenant prefix is applied when empty and
	// StateRepoOrg is set.
	StateRepo    string
	StateRepoOrg string

	// Bundles fetches apply-bundle content (OCI/git). Defaults to the
	// real fetcher.
	Bundles render.BundleSource
	// RGDs resolves KRO ResourceGraphDefinitions. Defaults to the
	// cluster's dynamic client.
	RGDs render.RGDGetter

	// ArgoCD lifecycle: bundle-managed default or BYO adoption.
	ArgoCDMode      argocd.Mode
	ArgoCDNamespace string
	ArgoCDManifests []byte
	// ArgoCDAPI is the narrow REST client for invoke-action; nil disables
	// invoke-action with an explicit error.
	ArgoCDAPI *argocd.APIClient

	// InstanceGVRs are KRO instance GVRs the status streamer watches.
	InstanceGVRs []schema.GroupVersionResource

	// JournalNamespace hosts the command journal ConfigMap (agent
	// namespace); empty disables durable idempotency.
	JournalNamespace string
}

// stateRepo resolves the platform-owned repo for the tenant.
func (c *GitOpsConfig) stateRepo(tenantID string) string {
	if c.StateRepo != "" {
		return c.StateRepo
	}
	if c.StateRepoOrg != "" && tenantID != "" {
		return c.StateRepoOrg + "/" + tenantID + "-inari-state"
	}
	return ""
}

// configure registers the real command handlers on the dispatcher and
// attaches the durable journal.
func (c *GitOpsConfig) configure(ctx context.Context, handler command.Handler, tenantID string) error {
	d, ok := handler.(*command.Dispatcher)
	if !ok {
		return fmt.Errorf("gitops requires the command dispatcher, got %T", handler)
	}
	bundles := c.Bundles
	if bundles == nil {
		bundles = render.NewFetcher()
	}
	rgds := c.RGDs
	if rgds == nil {
		rgds = render.DynamicRGDGetter{Dyn: c.Dyn}
	}
	deps := command.GitOpsDeps{Git: c.Git, DefaultStateRepo: c.stateRepo(tenantID)}
	d.Register(agentv1.EventType_EVENT_TYPE_RENDER_RGD_INSTANCE, command.RenderRGDInstanceHandler(deps, rgds))
	d.Register(agentv1.EventType_EVENT_TYPE_APPLY_BUNDLE, command.ApplyBundleHandler(deps, bundles))

	lifecycle := &argocd.Lifecycle{
		Kube:      c.Kube,
		Dyn:       c.Dyn,
		Mode:      c.ArgoCDMode,
		Namespace: c.ArgoCDNamespace,
		Manifests: c.ArgoCDManifests,
	}
	d.Register(agentv1.EventType_EVENT_TYPE_REGISTER_ARGOCD_APP, command.RegisterArgoCDAppHandler(lifecycle, func(ns string) *argocd.Client {
		return argocd.NewClient(c.Dyn, ns)
	}))
	if c.ArgoCDAPI != nil {
		ns := c.ArgoCDNamespace
		if ns == "" {
			ns = "argocd"
		}
		d.Register(agentv1.EventType_EVENT_TYPE_INVOKE_ACTION, command.InvokeActionHandler(command.InvokeActionDeps{
			API:       c.ArgoCDAPI,
			Dyn:       c.Dyn,
			Namespace: ns,
		}))
	}
	if c.JournalNamespace != "" {
		if err := d.AttachJournal(ctx, command.NewConfigMapJournal(c.Kube, c.JournalNamespace, "inari-agent-command-journal")); err != nil {
			return err
		}
	}
	return nil
}

// streamer builds the status streamer for this configuration.
func (c *GitOpsConfig) streamer(log logr.Logger, dyn dynamic.Interface, sender status.Sender) *status.Streamer {
	ns := c.ArgoCDNamespace
	if ns == "" {
		ns = "argocd"
	}
	return status.NewStreamer(log.WithName("status"), dyn, sender, ns, c.InstanceGVRs)
}
