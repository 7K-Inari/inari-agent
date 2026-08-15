// Package render turns control-plane render/apply commands into manifest
// file sets destined for the tenant state repository (plan §5.3 phase 4).
// Rendering is pure: cluster state is read through small interfaces so the
// package is unit-testable without a cluster.
package render

import (
	"context"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/yaml"

	"github.com/7K-Inari/inari-agent/internal/git"
)

// ManagedByLabel marks every Inari-rendered resource; mutation handlers
// refuse to touch resources without it (brownfield observe-only default).
const ManagedByLabel = "inari.dev/managed-by"

// ManagedByValue is the label value for agent-rendered resources.
const ManagedByValue = "inari-agent"

// InstanceLabel links a rendered resource to its resource instance.
const InstanceLabel = "inari.dev/instance"

// RGDGetter resolves a KRO ResourceGraphDefinition by name.
type RGDGetter interface {
	GetRGD(ctx context.Context, name string) (*unstructured.Unstructured, error)
}

var rgdGVR = schema.GroupVersionResource{
	Group: "kro.run", Version: "v1alpha1", Resource: "resourcegraphdefinitions",
}

// RenderRGDInstance renders a KRO instance CR for the RGD named by rgdRef.
// The RGD's spec.schema supplies apiVersion/kind and the parameter schema;
// parameters must satisfy its required fields. The result is a single
// file, instance.yaml, labeled for Inari ownership.
func RenderRGDInstance(ctx context.Context, getter RGDGetter, rgdRef, instanceName, namespace string, parameters map[string]interface{}) ([]git.File, error) {
	if rgdRef == "" {
		return nil, fmt.Errorf("render: missing rgd_ref")
	}
	if instanceName == "" {
		return nil, fmt.Errorf("render: missing instance_name")
	}
	if err := git.ValidateFilePath(instanceName); err != nil {
		return nil, fmt.Errorf("render: invalid instance_name: %w", err)
	}
	rgd, err := getter.GetRGD(ctx, rgdRef)
	if err != nil {
		return nil, fmt.Errorf("render: resolve RGD %q: %w", rgdRef, err)
	}
	apiVersion, _, _ := unstructured.NestedString(rgd.Object, "spec", "schema", "apiVersion")
	kind, _, _ := unstructured.NestedString(rgd.Object, "spec", "schema", "kind")
	if apiVersion == "" || kind == "" {
		return nil, fmt.Errorf("render: RGD %q has no spec.schema apiVersion/kind", rgdRef)
	}
	if err := validateRequired(rgd, parameters); err != nil {
		return nil, fmt.Errorf("render: RGD %q: %w", rgdRef, err)
	}
	cr := map[string]interface{}{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata": map[string]interface{}{
			"name":      instanceName,
			"namespace": namespace,
			"labels": map[string]interface{}{
				ManagedByLabel: ManagedByValue,
				InstanceLabel:  instanceName,
			},
		},
		"spec": parameters,
	}
	out, err := yaml.Marshal(cr)
	if err != nil {
		return nil, fmt.Errorf("render: marshal instance: %w", err)
	}
	return []git.File{{Path: instanceName + "/instance.yaml", Content: out}}, nil
}

// validateRequired checks the RGD schema's required parameter names.
func validateRequired(rgd *unstructured.Unstructured, parameters map[string]interface{}) error {
	required, _, _ := unstructured.NestedStringSlice(rgd.Object, "spec", "schema", "spec", "required")
	missing := []string{}
	for _, name := range required {
		if _, ok := parameters[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("missing required parameters: %v", missing)
	}
	return nil
}
