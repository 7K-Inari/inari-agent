package capability

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

// MetadataWatcher reports cluster metadata (plan §5.3): Kubernetes server
// version, node labels/taints, and installed addon versions. Read-only.
type MetadataWatcher struct {
	Client kubernetes.Interface
	// AddonsNamespace is scanned for addon deployments carrying the
	// app.kubernetes.io/version label (default "kube-system").
	AddonsNamespace string
}

func (w *MetadataWatcher) Source() Source { return SourceClusterMetadata }

func (w *MetadataWatcher) Start(ctx context.Context) (<-chan *agentv1.Capability, error) {
	out := make(chan *agentv1.Capability, 4)

	emit := func() {
		cap, err := w.snapshot(ctx)
		if err != nil || cap == nil {
			return
		}
		select {
		case out <- cap:
		case <-ctx.Done():
		}
	}

	factory := informers.NewSharedInformerFactory(w.Client, 0)
	nodeInformer := factory.Core().V1().Nodes().Informer()
	if _, err := nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(interface{}) { emit() },
		UpdateFunc: func(_, newObj interface{}) { emit() },
		DeleteFunc: func(interface{}) { emit() },
	}); err != nil {
		return nil, fmt.Errorf("capability metadata: register node handler: %w", err)
	}

	go func() {
		defer close(out)
		factory.Start(ctx.Done())
		factory.WaitForCacheSync(ctx.Done())
		emit()
		<-ctx.Done()
	}()
	return out, nil
}

// snapshot builds the single cluster-metadata capability from the current
// cluster state.
func (w *MetadataWatcher) snapshot(ctx context.Context) (*agentv1.Capability, error) {
	versionInfo, err := w.Client.Discovery().ServerVersion()
	if err != nil {
		return nil, fmt.Errorf("capability metadata: server version: %w", err)
	}
	nodes, err := w.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("capability metadata: list nodes: %w", err)
	}
	nodeSummaries := make([]interface{}, 0, len(nodes.Items))
	for _, n := range nodes.Items {
		taints := make([]interface{}, 0, len(n.Spec.Taints))
		for _, t := range n.Spec.Taints {
			taints = append(taints, fmt.Sprintf("%s=%s:%s", t.Key, t.Value, t.Effect))
		}
		nodeSummaries = append(nodeSummaries, map[string]interface{}{
			"name":   n.Name,
			"labels": stringMapToAny(n.Labels),
			"taints": taints,
		})
	}

	addonsNS := w.AddonsNamespace
	if addonsNS == "" {
		addonsNS = "kube-system"
	}
	addons := map[string]interface{}{}
	deploys, err := w.Client.AppsV1().Deployments(addonsNS).List(ctx, metav1.ListOptions{})
	if err == nil {
		for _, d := range deploys.Items {
			if v := addonVersion(&d); v != "" {
				addons[d.Name] = v
			}
		}
	}

	return &agentv1.Capability{
		Kind:    agentv1.CapabilityKind_CAPABILITY_KIND_CLUSTER_METADATA,
		Name:    "cluster",
		Group:   "inari.dev",
		Version: versionInfo.GitVersion,
		Action:  agentv1.CapabilityAction_CAPABILITY_ACTION_UPSERT,
		UiHints: mustStruct(map[string]interface{}{
			"kubernetesVersion": versionInfo.GitVersion,
			"platform":          versionInfo.Platform,
			"nodes":             nodeSummaries,
			"addons":            addons,
		}),
		// Cluster metadata is platform-observed; never adoptable.
		ManagementMode: agentv1.ManagementMode_MANAGEMENT_MODE_OBSERVE_ONLY,
	}, nil
}

func addonVersion(d *appsv1.Deployment) string {
	if v := d.Labels["app.kubernetes.io/version"]; v != "" {
		return v
	}
	for _, c := range d.Spec.Template.Spec.Containers {
		if c.Name == d.Name || len(d.Spec.Template.Spec.Containers) == 1 {
			if _, tag, ok := splitImage(c.Image); ok {
				return tag
			}
		}
	}
	return ""
}

func splitImage(image string) (string, string, bool) {
	for i := len(image) - 1; i >= 0; i-- {
		if image[i] == ':' && i+1 < len(image) {
			return image[:i], image[i+1:], true
		}
		if image[i] == '/' || image[i] == '@' {
			return "", "", false
		}
	}
	return "", "", false
}

func stringMapToAny(in map[string]string) map[string]interface{} {
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
