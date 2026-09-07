package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/7K-Inari/inari-api/gen/go/inari/agent/v1/agentv1connect"

	"github.com/7K-Inari/inari-agent/internal/argocd"
	"github.com/7K-Inari/inari-agent/internal/capability"
	"github.com/7K-Inari/inari-agent/internal/controller"
	"github.com/7K-Inari/inari-agent/internal/git"
	"github.com/7K-Inari/inari-agent/internal/registration"
	"github.com/7K-Inari/inari-agent/internal/stream"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")

	version = "dev" // overridden at release build time via -ldflags
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var probeAddr string
	var enableLeaderElection bool

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metrics endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. Off by default: the agent runs as a single replica per tenant cluster.")

	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	restConfig := ctrl.GetConfigOrDie()
	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "inari-agent.inari.dev",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	reconciler, err := buildLifecycle(restConfig, mgr)
	if err != nil {
		setupLog.Error(err, "unable to build agent lifecycle")
		os.Exit(1)
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to set up agent lifecycle")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting inari-agent manager", "version", version)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

// buildLifecycle wires the M1 agent lifecycle from environment configuration:
//
//	INARI_REGISTRATION_TOKEN  one-time TTL'd bootstrap token (install manifest)
//	INARI_TENANT_ID           owning tenant
//	INARI_CONTROL_PLANE       Agent Gateway base URL
//	INARI_CLUSTER_LABELS      comma-separated k=v pairs (ClusterSet targeting)
//
// Without INARI_REGISTRATION_TOKEN the agent runs standalone (healthy, no
// upstream connection) until a control plane is configured.
func buildLifecycle(restConfig *rest.Config, mgr manager.Manager) (*controller.AgentReconciler, error) {
	r := controller.NewAgentReconciler(mgr)

	token := os.Getenv("INARI_REGISTRATION_TOKEN")
	controlPlane := os.Getenv("INARI_CONTROL_PLANE")
	if token == "" || controlPlane == "" {
		return r, nil // standalone mode
	}

	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}
	dynClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}
	metaClient, err := metadata.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}
	serverVersion, err := kubeClient.Discovery().ServerVersion()
	if err != nil {
		return nil, err
	}

	r.BootstrapToken = token
	r.Kube = kubeClient
	r.Dynamic = dynClient
	r.Meta = metaClient
	r.SecretReader = &registration.KubeSecretReader{Client: kubeClient}
	r.Registrar = &registration.ConnectRegistrar{
		Client:            agentv1connect.NewRegistrationServiceClient(stream.DefaultHTTPClient(controlPlane), controlPlane),
		AgentVersion:      version,
		TenantID:          os.Getenv("INARI_TENANT_ID"),
		ControlPlane:      controlPlane,
		ClusterLabels:     parseLabels(os.Getenv("INARI_CLUSTER_LABELS")),
		KubernetesVersion: serverVersion.GitVersion,
	}
	r.NewWatchers = func(kube kubernetes.Interface, dyn dynamic.Interface, meta metadata.Interface) []capability.Watcher {
		watchers := []capability.Watcher{
			capability.NewCRDWatcher(dyn, meta),
			capability.NewOLMWatcher(dyn, ""),
			capability.NewCrossplaneProviderWatcher(dyn),
			capability.NewKROWatcher(dyn),
			&capability.MetadataWatcher{Client: kube},
		}
		watchers = append(watchers, capability.NewCrossplaneXRDWatcher(dyn)...)
		// Helm release Secrets are watched per-namespace (least privilege:
		// no cluster-wide secrets read, see config/rbac).
		for _, ns := range parseNamespaces(os.Getenv("INARI_HELM_NAMESPACES")) {
			watchers = append(watchers, capability.NewHelmReleaseWatcher(meta, ns))
		}
		// Skip watchers for optional platforms absent from this cluster
		// (OLM, Crossplane, KRO) instead of hot-looping reflector errors.
		return capability.FilterAvailable(kube.Discovery(), ctrl.Log.WithName("capability"), watchers)
	}
	r.NewStreamClient = func(creds *registration.Credentials, clientSecret string, checksum func() string) stream.Client {
		return stream.NewConnectClient(
			creds.ControlPlane,
			&stream.OAuth2TokenSource{
				TokenURL:     creds.TokenURL,
				ClientID:     creds.ClientID,
				ClientSecret: clientSecret,
			},
			version,
			creds.TenantID,
			checksum,
		)
	}
	gitOps, err := buildGitOps(context.Background(), kubeClient, dynClient)
	if err != nil {
		return nil, err
	}
	r.GitOps = gitOps
	return r, nil
}

// buildGitOps wires the M2 command handlers from environment configuration:
//
//	INARI_GIT_CREDS_SECRET   ESO-materialized GitHub App credentials Secret
//	                         (keys app-id, installation-id, private-key)
//	INARI_GIT_CREDS_NAMESPACE Secret namespace (default: agent namespace)
//	INARI_STATE_REPO         override state repo (owner/name); default
//	                         <org>/<tenant>-inari-state
//	INARI_STATE_REPO_ORG     GitHub org for the default state repo
//	INARI_ARGOCD_MODE        bundle (default) | byo
//	INARI_ARGOCD_NAMESPACE   ArgoCD install namespace (default argocd)
//	INARI_ARGOCD_API_URL     ArgoCD API base for invoke-action (default
//	                         in-cluster service URL)
//	INARI_ARGOCD_API_TOKEN   bearer token for invoke-action (optional)
//
// Without INARI_GIT_CREDS_SECRET the agent runs M1-only (commands stay
// no-op), preserving standalone deployability.
func buildGitOps(ctx context.Context, kube kubernetes.Interface, dyn dynamic.Interface) (*controller.GitOpsConfig, error) {
	credsSecret := os.Getenv("INARI_GIT_CREDS_SECRET")
	if credsSecret == "" {
		return nil, nil
	}
	credsNS := os.Getenv("INARI_GIT_CREDS_NAMESPACE")
	if credsNS == "" {
		credsNS = "inari-system"
	}
	secret, err := kube.CoreV1().Secrets(credsNS).Get(ctx, credsSecret, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read git credentials secret %s/%s: %w", credsNS, credsSecret, err)
	}
	appCreds, err := git.ParseAppCredentials(secret.Data)
	if err != nil {
		return nil, err
	}
	provider, err := git.NewGitHubProvider(appCreds, os.Getenv("INARI_GIT_API_BASE"))
	if err != nil {
		return nil, err
	}

	cfg := &controller.GitOpsConfig{
		Kube:             kube,
		Dyn:              dyn,
		Git:              provider,
		StateRepo:        os.Getenv("INARI_STATE_REPO"),
		StateRepoOrg:     os.Getenv("INARI_STATE_REPO_ORG"),
		ArgoCDMode:       argocd.Mode(os.Getenv("INARI_ARGOCD_MODE")),
		ArgoCDNamespace:  os.Getenv("INARI_ARGOCD_NAMESPACE"),
		JournalNamespace: credsNS,
	}
	argocdNS := cfg.ArgoCDNamespace
	if argocdNS == "" {
		argocdNS = "argocd"
	}
	apiURL := os.Getenv("INARI_ARGOCD_API_URL")
	if apiURL == "" {
		apiURL = "https://argocd-server." + argocdNS + ".svc"
	}
	cfg.ArgoCDAPI = &argocd.APIClient{
		BaseURL: apiURL,
		Token:   os.Getenv("INARI_ARGOCD_API_TOKEN"),
		Timeout: 30 * time.Second,
	}
	return cfg, nil
}

func parseLabels(in string) map[string]string {
	out := map[string]string{}
	for _, pair := range strings.Split(in, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if ok && k != "" {
			out[k] = v
		}
	}
	return out
}

// parseNamespaces splits a comma-separated namespace list, defaulting to the
// agent's own namespace.
func parseNamespaces(in string) []string {
	var out []string
	for _, ns := range strings.Split(in, ",") {
		if ns = strings.TrimSpace(ns); ns != "" {
			out = append(out, ns)
		}
	}
	if len(out) == 0 {
		out = []string{"inari-system"}
	}
	return out
}
