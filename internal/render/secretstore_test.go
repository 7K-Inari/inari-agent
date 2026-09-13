package render

import (
	"strings"
	"testing"
)

func TestRenderSecretStoreProviders(t *testing.T) {
	cases := []struct {
		name     string
		scope    string
		provider SecretStoreProvider
		want     string
	}{
		{
			name:  "corp-aws",
			scope: "platform",
			provider: SecretStoreProvider{
				Kind: "awsSM", Region: "eu-west-1",
				AuthName: "aws-creds", AuthNamespace: "inari-system",
			},
			want: `apiVersion: external-secrets.io/v1beta1
kind: ClusterSecretStore
metadata:
  labels:
    inari.dev/managed-by: inari-agent
  name: corp-aws
spec:
  provider:
    aws:
      auth:
        secretRef:
          accessKeyIdSecretRef:
            key: access-key-id
            name: aws-creds
            namespace: inari-system
          secretAccessKeySecretRef:
            key: secret-access-key
            name: aws-creds
            namespace: inari-system
      region: eu-west-1
      service: SecretsManager
`,
		},
		{
			name:  "corp-vault",
			scope: "cluster",
			provider: SecretStoreProvider{
				Kind: "vault", Server: "https://vault.corp:8200", Path: "secret",
				AuthName: "vault-token", AuthNamespace: "inari-system",
			},
			want: `apiVersion: external-secrets.io/v1beta1
kind: SecretStore
metadata:
  labels:
    inari.dev/managed-by: inari-agent
  name: corp-vault
  namespace: inari-system
spec:
  provider:
    vault:
      auth:
        tokenSecretRef:
          key: token
          name: vault-token
      path: secret
      server: https://vault.corp:8200
`,
		},
		{
			name:  "corp-gcp",
			scope: "platform",
			provider: SecretStoreProvider{
				Kind: "gcpsm", ProjectID: "corp-prod",
				AuthName: "gcp-creds", AuthNamespace: "inari-system",
			},
			want: `apiVersion: external-secrets.io/v1beta1
kind: ClusterSecretStore
metadata:
  labels:
    inari.dev/managed-by: inari-agent
  name: corp-gcp
spec:
  provider:
    gcpsm:
      auth:
        secretRef:
          secretAccessTokenSecretRef:
            key: secret-access-token
            name: gcp-creds
            namespace: inari-system
      projectID: corp-prod
`,
		},
		{
			name:  "corp-azure",
			scope: "platform",
			provider: SecretStoreProvider{
				Kind: "azurekv", VaultURL: "https://corp.vault.azure.net", TenantID: "t-1",
				AuthName: "azure-creds", AuthNamespace: "inari-system",
			},
			want: `apiVersion: external-secrets.io/v1beta1
kind: ClusterSecretStore
metadata:
  labels:
    inari.dev/managed-by: inari-agent
  name: corp-azure
spec:
  provider:
    azurekv:
      authSecretRef:
        clientIdSecretRef:
          key: client-id
          name: azure-creds
          namespace: inari-system
        clientSecretSecretRef:
          key: client-secret
          name: azure-creds
          namespace: inari-system
      tenantId: t-1
      vaultUrl: https://corp.vault.azure.net
`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			files, err := RenderSecretStore(tc.name, tc.scope, tc.provider)
			if err != nil {
				t.Fatal(err)
			}
			if len(files) != 1 {
				t.Fatalf("files = %d, want 1", len(files))
			}
			if files[0].Path != tc.name+".yaml" {
				t.Fatalf("path = %q, want %q", files[0].Path, tc.name+".yaml")
			}
			if got := string(files[0].Content); got != tc.want {
				t.Errorf("manifest mismatch:\n--- got ---\n%s\n--- want ---\n%s", got, tc.want)
			}
		})
	}
}

func TestRenderSecretStoreValidation(t *testing.T) {
	p := SecretStoreProvider{Kind: "vault", Server: "https://v", AuthName: "tok", AuthNamespace: "inari-system"}
	if _, err := RenderSecretStore("", "cluster", p); err == nil {
		t.Error("empty name must fail")
	}
	if _, err := RenderSecretStore("x", "bogus", p); err == nil {
		t.Error("unknown scope must fail")
	}
	if _, err := RenderSecretStore("x", "cluster", SecretStoreProvider{Kind: "bogus"}); err == nil {
		t.Error("unknown provider kind must fail")
	}
	// Namespaced SecretStore auth secrets must live in the store namespace
	// (ESO namespaced secretRefs carry no namespace field).
	bad := p
	bad.AuthNamespace = "other"
	if _, err := RenderSecretStore("x", "cluster", bad); err == nil ||
		!strings.Contains(err.Error(), "namespace") {
		t.Errorf("cluster-scope auth namespace mismatch must fail, got %v", err)
	}
	// Same name as file path must be a safe repo-relative path.
	if _, err := RenderSecretStore("../escape", "cluster", p); err == nil {
		t.Error("path-escaping name must fail")
	}
}
