package render

import (
	"fmt"

	"sigs.k8s.io/yaml"

	"github.com/7K-Inari/inari-agent/internal/git"
)

// SystemNamespace is the Inari-owned namespace namespaced SecretStores (and
// their auth secrets) live in.
const SystemNamespace = "inari-system"

// Secret store scopes (mirror the control-plane registry values).
const (
	SecretStoreScopePlatform = "platform"
	SecretStoreScopeCluster  = "cluster"
)

// SecretStoreProvider is the render-local form of one ESO backend config.
// Exactly the fields of the selected Kind are set; AuthName/AuthNamespace
// reference the cluster-side credential Secret (never its values).
type SecretStoreProvider struct {
	Kind      string // "awsSM" | "vault" | "gcpsm" | "azurekv"
	Region    string // awsSM
	Server    string // vault
	Path      string // vault, optional
	ProjectID string // gcpsm
	VaultURL  string // azurekv
	TenantID  string // azurekv, optional

	AuthName      string
	AuthNamespace string
}

// secretRef renders one ESO secret reference. Namespaced SecretStores cannot
// reference secrets across namespaces, so the namespace field is emitted only
// for ClusterSecretStores.
func secretRef(includeNS bool, name, namespace, key string) map[string]interface{} {
	ref := map[string]interface{}{"name": name, "key": key}
	if includeNS {
		ref["namespace"] = namespace
	}
	return ref
}

// providerSpec builds the ESO spec.provider block for the backend. The key
// names follow ESO conventions; credential values never appear here.
func providerSpec(p SecretStoreProvider, clusterScoped bool) (map[string]interface{}, error) {
	switch p.Kind {
	case "awsSM":
		return map[string]interface{}{"aws": map[string]interface{}{
			"service": "SecretsManager",
			"region":  p.Region,
			"auth": map[string]interface{}{"secretRef": map[string]interface{}{
				"accessKeyIdSecretRef":     secretRef(clusterScoped, p.AuthName, p.AuthNamespace, "access-key-id"),
				"secretAccessKeySecretRef": secretRef(clusterScoped, p.AuthName, p.AuthNamespace, "secret-access-key"),
			}},
		}}, nil
	case "vault":
		v := map[string]interface{}{
			"server": p.Server,
			"auth": map[string]interface{}{
				"tokenSecretRef": secretRef(clusterScoped, p.AuthName, p.AuthNamespace, "token"),
			},
		}
		if p.Path != "" {
			v["path"] = p.Path
		}
		return map[string]interface{}{"vault": v}, nil
	case "gcpsm":
		return map[string]interface{}{"gcpsm": map[string]interface{}{
			"projectID": p.ProjectID,
			"auth": map[string]interface{}{"secretRef": map[string]interface{}{
				"secretAccessTokenSecretRef": secretRef(clusterScoped, p.AuthName, p.AuthNamespace, "secret-access-token"),
			}},
		}}, nil
	case "azurekv":
		az := map[string]interface{}{
			"vaultUrl": p.VaultURL,
			"authSecretRef": map[string]interface{}{
				"clientIdSecretRef":     secretRef(clusterScoped, p.AuthName, p.AuthNamespace, "client-id"),
				"clientSecretSecretRef": secretRef(clusterScoped, p.AuthName, p.AuthNamespace, "client-secret"),
			},
		}
		if p.TenantID != "" {
			az["tenantId"] = p.TenantID
		}
		return map[string]interface{}{"azurekv": az}, nil
	}
	return nil, fmt.Errorf("render: unknown secret store provider kind %q", p.Kind)
}

// RenderSecretStore renders one ESO SecretStore (scope "cluster", namespaced
// into inari-system) or ClusterSecretStore (scope "platform") manifest as a
// single <name>.yaml file for the tenant state repository.
func RenderSecretStore(name, scope string, p SecretStoreProvider) ([]git.File, error) {
	if name == "" {
		return nil, fmt.Errorf("render: missing secret store name")
	}
	if err := git.ValidateFilePath(name + ".yaml"); err != nil {
		return nil, fmt.Errorf("render: invalid secret store name: %w", err)
	}
	clusterScoped := scope == SecretStoreScopePlatform
	if scope != SecretStoreScopePlatform && scope != SecretStoreScopeCluster {
		return nil, fmt.Errorf("render: unknown secret store scope %q", scope)
	}
	if p.AuthName == "" || p.AuthNamespace == "" {
		return nil, fmt.Errorf("render: provider %s auth secret name and namespace are required", p.Kind)
	}
	if !clusterScoped && p.AuthNamespace != SystemNamespace {
		return nil, fmt.Errorf("render: cluster-scoped store auth secret must live in namespace %q (namespaced secretRefs cannot cross namespaces), got %q", SystemNamespace, p.AuthNamespace)
	}
	provider, err := providerSpec(p, clusterScoped)
	if err != nil {
		return nil, err
	}
	kind := "SecretStore"
	metadata := map[string]interface{}{
		"name":      name,
		"namespace": SystemNamespace,
		"labels":    map[string]interface{}{ManagedByLabel: ManagedByValue},
	}
	if clusterScoped {
		kind = "ClusterSecretStore"
		delete(metadata, "namespace")
	}
	cr := map[string]interface{}{
		"apiVersion": "external-secrets.io/v1beta1",
		"kind":       kind,
		"metadata":   metadata,
		"spec":       map[string]interface{}{"provider": provider},
	}
	out, err := yaml.Marshal(cr)
	if err != nil {
		return nil, fmt.Errorf("render: marshal secret store: %w", err)
	}
	return []git.File{{Path: name + ".yaml", Content: out}}, nil
}
