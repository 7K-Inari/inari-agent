package command

// Test doubles for the GitOps handler tests.

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/7K-Inari/inari-agent/internal/git"
	"github.com/7K-Inari/inari-agent/internal/render"
)

type fakeGetter struct {
	obj *unstructured.Unstructured
	err error
}

func (f fakeGetter) GetRGD(context.Context, string) (*unstructured.Unstructured, error) {
	return f.obj, f.err
}

func testRGD() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"spec": map[string]interface{}{
			"schema": map[string]interface{}{
				"apiVersion": "v1alpha1",
				"kind":       "WebService",
				"spec": map[string]interface{}{
					"required": []interface{}{"image"},
				},
			},
		},
	}}
}

type fakeBundles struct {
	files []git.File
	err   error
	seen  render.BundleRef
}

func (f *fakeBundles) Fetch(_ context.Context, ref render.BundleRef) ([]git.File, error) {
	f.seen = ref
	return f.files, f.err
}

var _ render.RGDGetter = fakeGetter{}
var _ render.BundleSource = (*fakeBundles)(nil)
