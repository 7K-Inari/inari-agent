package argocd

import (
	"context"
	"fmt"
	"reflect"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

// Client manages argoproj.io/v1alpha1 Applications and AppProjects in the
// tenant-local ArgoCD namespace.
type Client struct {
	Dyn       dynamic.Interface
	Namespace string
}

// NewClient returns a Client bound to the ArgoCD install namespace.
func NewClient(dyn dynamic.Interface, namespace string) *Client {
	return &Client{Dyn: dyn, Namespace: namespace}
}

// EnsureAppProject idempotently creates/updates a per-team AppProject.
// Restrictive BYO RBAC surfaces as an explicit error (spike condition 4).
func (c *Client) EnsureAppProject(ctx context.Context, name string) error {
	project := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "argoproj.io/v1alpha1",
		"kind":       "AppProject",
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": c.Namespace,
			"labels": map[string]interface{}{
				ManagedByLabel: ManagedByValue,
			},
		},
		"spec": map[string]interface{}{
			"sourceRepos": []interface{}{"*"},
			"destinations": []interface{}{
				map[string]interface{}{"server": "*", "namespace": "*"},
			},
			"clusterResourceWhitelist": []interface{}{
				map[string]interface{}{"group": "*", "kind": "*"},
			},
		},
	}}
	return c.upsert(ctx, AppProjectGVR, project)
}

// EnsureApplication idempotently creates/updates the Application for a
// rendered instance, labeled for Inari ownership.
func (c *Client) EnsureApplication(ctx context.Context, m *agentv1.RegisterArgoCDApp) error {
	if m.Name == "" {
		return fmt.Errorf("argocd: application name is required")
	}
	if m.Source == nil || m.Source.RepoUrl == "" {
		return fmt.Errorf("argocd: application %q needs a source repo_url", m.Name)
	}
	project := m.Project
	if project == "" {
		project = "default"
	}
	destServer := m.DestinationServer
	if destServer == "" {
		destServer = "https://kubernetes.default.svc"
	}
	source := map[string]interface{}{
		"repoURL":        m.Source.RepoUrl,
		"targetRevision": orDefault(m.Source.TargetRevision, "main"),
	}
	if m.Source.Chart != "" {
		source["chart"] = m.Source.Chart
	} else {
		source["path"] = orDefault(m.Source.Path, ".")
	}
	spec := map[string]interface{}{
		"project": project,
		"source":  source,
		"destination": map[string]interface{}{
			"server":    destServer,
			"namespace": orDefault(m.DestinationNamespace, "default"),
		},
	}
	if sp := syncPolicy(m.SyncPolicy); sp != nil {
		spec["syncPolicy"] = sp
	}
	app := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "argoproj.io/v1alpha1",
		"kind":       "Application",
		"metadata": map[string]interface{}{
			"name":      m.Name,
			"namespace": c.Namespace,
			"labels": map[string]interface{}{
				ManagedByLabel: ManagedByValue,
				InstanceLabel:  m.Name,
			},
		},
		"spec": spec,
	}}
	return c.upsert(ctx, ApplicationGVR, app)
}

// upsert creates the object, or updates spec when it drifted. Ownership
// is enforced: pre-existing objects without the Inari label are
// observe-only and are never mutated (brownfield default, §12.1/3).
func (c *Client) upsert(ctx context.Context, gvr schema.GroupVersionResource, obj *unstructured.Unstructured) error {
	ri := c.Dyn.Resource(gvr).Namespace(c.Namespace)
	existing, err := ri.Get(ctx, obj.GetName(), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := ri.Create(ctx, obj, metav1.CreateOptions{}); err != nil {
			if apierrors.IsForbidden(err) {
				return fmt.Errorf("argocd: creating %s %s forbidden — the ArgoCD install's RBAC/AppProject configuration rejects Inari-managed objects: %w", obj.GetKind(), obj.GetName(), err)
			}
			return fmt.Errorf("argocd: create %s %s: %w", obj.GetKind(), obj.GetName(), err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("argocd: get %s %s: %w", obj.GetKind(), obj.GetName(), err)
	}
	if existing.GetLabels()[ManagedByLabel] != ManagedByValue {
		return fmt.Errorf("argocd: %s %s exists and is not Inari-managed; refusing to adopt implicitly (observe-only)", obj.GetKind(), obj.GetName())
	}
	newSpec, _, _ := unstructured.NestedMap(obj.Object, "spec")
	oldSpec, _, _ := unstructured.NestedMap(existing.Object, "spec")
	if reflect.DeepEqual(newSpec, oldSpec) {
		return nil
	}
	updated := existing.DeepCopy()
	if err := unstructured.SetNestedMap(updated.Object, newSpec, "spec"); err != nil {
		return err
	}
	if _, err := ri.Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("argocd: update %s %s: %w", obj.GetKind(), obj.GetName(), err)
	}
	return nil
}

func syncPolicy(sp *agentv1.SyncPolicy) map[string]interface{} {
	if sp == nil {
		return nil
	}
	out := map[string]interface{}{}
	if sp.GetAutomated() {
		out["automated"] = map[string]interface{}{
			"prune":    sp.GetPrune(),
			"selfHeal": sp.GetSelfHeal(),
		}
	}
	return out
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
