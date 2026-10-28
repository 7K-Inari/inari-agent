package argocd

import (
	"context"
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/yaml"
)

// Manifest-verbatim registration (Feature C, plan §6): addon Application
// and ApplicationSet registrations arrive as the committed
// application.yaml / applicationset.yaml — the authoritative desired
// state — and are upserted unchanged. This carries what the
// RegisterArgoCDApp contract cannot express: multi-source helm apps
// (chart + git $values) and TakeOwnership sync options for in-place
// adoption (prune stays off, risk item 2).

// EnsureApplicationSet upserts a rendered ApplicationSet manifest.
func (c *Client) EnsureApplicationSet(ctx context.Context, name, manifest string) error {
	return c.ensureManifest(ctx, ApplicationSetGVR, "ApplicationSet", name, manifest)
}

// EnsureApplicationManifest upserts a rendered Application manifest.
func (c *Client) EnsureApplicationManifest(ctx context.Context, name, manifest string) error {
	return c.ensureManifest(ctx, ApplicationGVR, "Application", name, manifest)
}

// ensureManifest decodes the YAML manifest, pins name/namespace, marks
// Inari ownership, and upserts through the brownfield-safe path.
func (c *Client) ensureManifest(ctx context.Context, gvr schema.GroupVersionResource, kind, name, manifest string) error {
	raw, err := yaml.YAMLToJSON([]byte(manifest))
	if err != nil {
		return fmt.Errorf("argocd: decode %s manifest: %w", kind, err)
	}
	obj := &unstructured.Unstructured{}
	if err := json.Unmarshal(raw, &obj.Object); err != nil {
		return fmt.Errorf("argocd: decode %s manifest: %w", kind, err)
	}
	if obj.GetKind() != kind {
		return fmt.Errorf("argocd: manifest kind %q does not match %s", obj.GetKind(), kind)
	}
	if got := obj.GetName(); got != "" && got != name {
		return fmt.Errorf("argocd: manifest name %q does not match %q", got, name)
	}
	obj.SetName(name)
	obj.SetNamespace(c.Namespace)
	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[ManagedByLabel] = ManagedByValue
	obj.SetLabels(labels)
	return c.upsert(ctx, gvr, obj)
}
