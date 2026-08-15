package argocd

import (
	"bytes"
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"io"
)

// Lifecycle gates command handling on the tenant-local ArgoCD being
// available under the configured mode (bundle-managed default, BYO flag).
type Lifecycle struct {
	Kube kubernetes.Interface
	Dyn  dynamic.Interface
	Mode Mode
	// Namespace is the install namespace for bundle mode (default
	// "argocd") or the expected namespace for BYO (empty = detect).
	Namespace string
	// Manifests is the pinned bundle install YAML (multi-doc) applied
	// when bundle mode finds no existing installation. The bundle pins
	// application.resourceTrackingMethod: annotation (spike condition 2).
	Manifests []byte
}

const fieldManager = "inari-agent"

// EnsureReady verifies (and for bundle mode, establishes) the ArgoCD
// installation, returning its namespace. It never upgrades an existing
// installation: out-of-skew finds are refused (spike skew policy).
func (l *Lifecycle) EnsureReady(ctx context.Context) (string, error) {
	mode := l.Mode
	if mode == "" {
		mode = ModeBundle
	}
	inst, err := Detect(ctx, l.Kube)
	if err != nil {
		return "", err
	}

	switch mode {
	case ModeBYO:
		if inst == nil {
			return "", fmt.Errorf("argocd: BYO mode but no argocd-server found in any namespace")
		}
		if inst.Classification != ClassAdoptable {
			return "", fmt.Errorf("argocd: refusing to adopt %s in %s (version %s out of supported skew %s/%s, min %s); the agent runs observe-only — upgrade the tenant ArgoCD or install side-by-side",
				"argocd-server", inst.Namespace, inst.Version, BundleLine, PreviousLine, SupportedMin)
		}
		return inst.Namespace, nil

	default: // bundle
		ns := l.Namespace
		if ns == "" {
			ns = "argocd"
		}
		if inst == nil {
			if len(l.Manifests) == 0 {
				return "", fmt.Errorf("argocd: no installation found and no bundle manifests configured")
			}
			if err := ApplyManifests(ctx, l.Dyn, l.Manifests, ns); err != nil {
				return "", fmt.Errorf("argocd: install bundle: %w", err)
			}
			return ns, nil
		}
		if inst.Namespace == ns && inst.Classification == ClassAdoptable {
			return ns, nil
		}
		return "", fmt.Errorf("argocd: existing installation in %s (version %s) conflicts with bundle-managed mode in %s; set BYO mode to adopt or remove it",
			inst.Namespace, inst.Version, ns)
	}
}

// ApplyManifests server-side-applies a multi-doc YAML bundle. Existing
// tenant-owned customizations in shared ConfigMaps (argocd-cm) are merged
// by SSA field ownership — never wholesale overwritten.
func ApplyManifests(ctx context.Context, dyn dynamic.Interface, manifests []byte, defaultNS string) error {
	dec := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(manifests), 4096)
	for {
		var raw map[string]interface{}
		if err := dec.Decode(&raw); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("argocd: decode bundle manifest: %w", err)
		}
		if len(raw) == 0 {
			continue
		}
		obj := &unstructured.Unstructured{Object: raw}
		gvk := obj.GroupVersionKind()
		mapping, err := resourceFor(dyn, gvk)
		if err != nil {
			return err
		}
		ns := obj.GetNamespace()
		if ns == "" {
			ns = defaultNS
			obj.SetNamespace(ns)
		}
		data, err := obj.MarshalJSON()
		if err != nil {
			return err
		}
		if _, err := dyn.Resource(mapping).Namespace(ns).Patch(ctx, obj.GetName(),
			types.ApplyPatchType, data, metav1.PatchOptions{FieldManager: fieldManager, Force: ptr(true)}); err != nil {
			if apierrors.IsNotFound(err) {
				if _, cerr := dyn.Resource(mapping).Namespace(ns).Create(ctx, obj, metav1.CreateOptions{FieldManager: fieldManager}); cerr != nil {
					return fmt.Errorf("argocd: apply %s %s/%s: %w", obj.GetKind(), ns, obj.GetName(), cerr)
				}
				continue
			}
			return fmt.Errorf("argocd: apply %s %s/%s: %w", obj.GetKind(), ns, obj.GetName(), err)
		}
	}
}

func ptr[T any](v T) *T { return &v }

// resourceFor resolves a GVK to a GVR via the RESTMapper embedded in the
// dynamic client is unavailable; use a naive pluralization adequate for
// the pinned bundle contents, overridable by tests through GVRFor.
var GVRFor = func(gvk schema.GroupVersionKind) (schema.GroupVersionResource, error) {
	kind := gvk.Kind
	var resource string
	switch kind {
	case "":
		return schema.GroupVersionResource{}, fmt.Errorf("argocd: manifest with empty kind")
	default:
		// naive pluralization: lowercase + "s"/"es"/"ies"
		lower := lowerASCII(kind)
		switch {
		case hasSuffix(lower, "s"):
			resource = lower + "es"
		case hasSuffix(lower, "y"):
			resource = lower[:len(lower)-1] + "ies"
		default:
			resource = lower + "s"
		}
	}
	return schema.GroupVersionResource{Group: gvk.Group, Version: gvk.Version, Resource: resource}, nil
}

func resourceFor(_ dynamic.Interface, gvk schema.GroupVersionKind) (schema.GroupVersionResource, error) {
	return GVRFor(gvk)
}

func lowerASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return string(b)
}

func hasSuffix(s, suf string) bool {
	return len(s) >= len(suf) && s[len(s)-len(suf):] == suf
}
