package render

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
)

// DynamicRGDGetter resolves RGDs from the cluster via a dynamic client.
type DynamicRGDGetter struct {
	Dyn dynamic.Interface
}

// GetRGD implements RGDGetter.
func (g DynamicRGDGetter) GetRGD(ctx context.Context, name string) (*unstructured.Unstructured, error) {
	return g.Dyn.Resource(rgdGVR).Get(ctx, name, metav1.GetOptions{})
}

var _ RGDGetter = DynamicRGDGetter{}
