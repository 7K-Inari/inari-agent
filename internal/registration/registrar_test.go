package registration

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
	"github.com/7K-Inari/inari-api/gen/go/inari/agent/v1/agentv1connect"
)

type fakeRegistrationHandler struct {
	agentv1connect.UnimplementedRegistrationServiceHandler
	gotToken  string
	gotLabels map[string]string
	resp      *agentv1.RegisterClusterResponse
	err       *connect.Error
}

func (f *fakeRegistrationHandler) RegisterCluster(
	_ context.Context,
	req *connect.Request[agentv1.RegisterClusterRequest],
) (*connect.Response[agentv1.RegisterClusterResponse], error) {
	f.gotToken = req.Msg.GetRegistrationToken()
	f.gotLabels = req.Msg.GetClusterLabels()
	if f.err != nil {
		return nil, f.err
	}
	return connect.NewResponse(f.resp), nil
}

func newTestRegistrar(t *testing.T, h agentv1connect.RegistrationServiceHandler) *ConnectRegistrar {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(agentv1connect.NewRegistrationServiceHandler(h))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &ConnectRegistrar{
		Client:            agentv1connect.NewRegistrationServiceClient(srv.Client(), srv.URL),
		AgentVersion:      "0.1.0-test",
		TenantID:          "tenant-1",
		ControlPlane:      srv.URL,
		ClusterLabels:     map[string]string{"env": "test"},
		KubernetesVersion: "v1.32.0",
	}
}

func TestRegisterExchangesTokenForCredentials(t *testing.T) {
	h := &fakeRegistrationHandler{resp: &agentv1.RegisterClusterResponse{
		ClusterId:     "cluster-42",
		OidcIssuerUrl: "https://keycloak.example/realms/inari",
		ClientId:      "cluster-cluster-42",
		ClientSecretDelivery: &agentv1.SecretDeliveryReference{
			EsoSecretStore:  "inari-cluster-store",
			SecretName:      "cluster-42-oidc",
			SecretNamespace: "inari-system",
			SecretKey:       "client-secret",
		},
	}}
	r := newTestRegistrar(t, h)

	creds, err := r.Register(context.Background(), "bootstrap-token-abc")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if h.gotToken != "bootstrap-token-abc" {
		t.Errorf("server saw token %q, want bootstrap-token-abc", h.gotToken)
	}
	if creds.ClusterID != "cluster-42" {
		t.Errorf("ClusterID = %q", creds.ClusterID)
	}
	if creds.ClientID != "cluster-cluster-42" {
		t.Errorf("ClientID = %q", creds.ClientID)
	}
	wantTokenURL := "https://keycloak.example/realms/inari/protocol/openid-connect/token"
	if creds.TokenURL != wantTokenURL {
		t.Errorf("TokenURL = %q, want %q", creds.TokenURL, wantTokenURL)
	}
	if creds.TenantID != "tenant-1" {
		t.Errorf("TenantID = %q", creds.TenantID)
	}
	if creds.ControlPlane == "" {
		t.Error("ControlPlane must be carried through")
	}
	if creds.SecretRef.Name != "cluster-42-oidc" || creds.SecretRef.Key != "client-secret" {
		t.Errorf("SecretRef = %+v", creds.SecretRef)
	}
	if h.gotLabels["env"] != "test" {
		t.Errorf("cluster labels not propagated: %v", h.gotLabels)
	}
}

func TestRegisterRejectsExpiredToken(t *testing.T) {
	h := &fakeRegistrationHandler{err: connect.NewError(connect.CodeUnauthenticated, errors.New("token expired"))}
	r := newTestRegistrar(t, h)
	if _, err := r.Register(context.Background(), "stale"); err == nil {
		t.Fatal("expected error for expired token")
	}
}

func TestKubeSecretReaderWaitsForESOSecret(t *testing.T) {
	ref := SecretReference{Name: "cluster-42-oidc", Namespace: "inari-system", Key: "client-secret"}
	client := fake.NewSimpleClientset()
	reader := &KubeSecretReader{
		Client:       client,
		PollInterval: 10 * time.Millisecond,
		PollTimeout:  5 * time.Second,
	}

	// ESO materialises the Secret asynchronously.
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = client.CoreV1().Secrets("inari-system").Create(context.Background(), &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace},
			Data:       map[string][]byte{"client-secret": []byte("s3cr3t")},
		}, metav1.CreateOptions{})
	}()

	secret, err := reader.ReadSecret(context.Background(), ref)
	if err != nil {
		t.Fatalf("ReadSecret: %v", err)
	}
	if secret != "s3cr3t" {
		t.Errorf("secret = %q", secret)
	}
}

func TestKubeSecretReaderTimeout(t *testing.T) {
	reader := &KubeSecretReader{
		Client:       fake.NewSimpleClientset(),
		PollInterval: 10 * time.Millisecond,
		PollTimeout:  50 * time.Millisecond,
	}
	_, err := reader.ReadSecret(context.Background(), SecretReference{Name: "nope", Namespace: "inari-system", Key: "k"})
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestKubeSecretReaderMissingKey(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "inari-system"},
		Data:       map[string][]byte{"other": []byte("x")},
	})
	reader := &KubeSecretReader{Client: client, PollInterval: 10 * time.Millisecond, PollTimeout: 50 * time.Millisecond}
	_, err := reader.ReadSecret(context.Background(), SecretReference{Name: "s", Namespace: "inari-system", Key: "client-secret"})
	if err == nil {
		t.Fatal("expected missing-key error")
	}
}
