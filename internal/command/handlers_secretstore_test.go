package command

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/git"
)

func secretStoreApplyEvent(t *testing.T, m *agentv1.SecretStoreApply) *agentv1.Event {
	t.Helper()
	a, err := anypb.New(m)
	if err != nil {
		t.Fatal(err)
	}
	return &agentv1.Event{Type: agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_SECRET_STORE_APPLY), Payload: a}
}

func TestSecretStoreApplyHandlerDirectCommit(t *testing.T) {
	gp := git.NewMemProvider()
	deps := GitOpsDeps{Git: gp, DefaultStateRepo: "org/acme-inari-state"}
	h := SecretStoreApplyHandler(deps)

	d := NewDispatcher()
	d.Register(agentv1.EventType_EVENT_TYPE_SECRET_STORE_APPLY, h)

	ack, err := d.HandleEvent(context.Background(), secretStoreApplyEvent(t, &agentv1.SecretStoreApply{
		CommandId: "cmd-ss1",
		Name:      "corp-vault",
		Scope:     "cluster",
		Provider: &agentv1.SecretStoreApply_Vault{Vault: &agentv1.VaultProvider{
			Server:        "https://vault.corp:8200",
			Path:          "secret",
			AuthSecretRef: &agentv1.SecretRef{Name: "vault-token", Namespace: "inari-system"},
		}},
		Target: &agentv1.GitTarget{Path: "secretstores"},
		Policy: agentv1.CommitPolicy_COMMIT_POLICY_DIRECT_COMMIT,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if ack.Result != agentv1.CommandResult_COMMAND_RESULT_APPLIED {
		t.Fatalf("ack %+v", ack)
	}
	if len(gp.Commits) != 1 {
		t.Fatalf("commits %+v", gp.Commits)
	}
	c := gp.Commits[0]
	if c.Target.Repo != "org/acme-inari-state" || c.Target.Path != "secretstores" {
		t.Fatalf("target %+v", c.Target)
	}
	if len(c.Files) != 1 || c.Files[0].Path != "corp-vault.yaml" {
		t.Fatalf("files %+v", c.Files)
	}
	got := string(c.Files[0].Content)
	for _, want := range []string{"kind: SecretStore", "namespace: inari-system", "server: https://vault.corp:8200", "tokenSecretRef"} {
		if !strings.Contains(got, want) {
			t.Errorf("manifest missing %q:\n%s", want, got)
		}
	}
	// Cluster-scope namespaced store: auth ref must not cross namespaces.
	if strings.Contains(got, "namespace: inari-system\n      path") {
		t.Errorf("auth secretRef must not carry namespace in namespaced store:\n%s", got)
	}
}

func TestSecretStoreApplyHandlerPlatformScope(t *testing.T) {
	gp := git.NewMemProvider()
	deps := GitOpsDeps{Git: gp, DefaultStateRepo: "org/acme-inari-state"}
	h := SecretStoreApplyHandler(deps)

	result, _, err := h(context.Background(), secretStoreApplyEvent(t, &agentv1.SecretStoreApply{
		CommandId: "cmd-ss2",
		Name:      "corp-aws",
		Scope:     "platform",
		Provider: &agentv1.SecretStoreApply_AwsSm{AwsSm: &agentv1.AwsSMProvider{
			Region:        "eu-west-1",
			AuthSecretRef: &agentv1.SecretRef{Name: "aws-creds", Namespace: "inari-system"},
		}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if result != agentv1.CommandResult_COMMAND_RESULT_APPLIED {
		t.Fatalf("result %v", result)
	}
	got := string(gp.Commits[0].Files[0].Content)
	if !strings.Contains(got, "kind: ClusterSecretStore") || !strings.Contains(got, "region: eu-west-1") {
		t.Fatalf("manifest:\n%s", got)
	}
}

func TestSecretStoreApplyRejectsForeignRepo(t *testing.T) {
	gp := git.NewMemProvider()
	deps := GitOpsDeps{Git: gp, DefaultStateRepo: "org/acme-inari-state"}
	h := SecretStoreApplyHandler(deps)

	result, _, err := h(context.Background(), secretStoreApplyEvent(t, &agentv1.SecretStoreApply{
		CommandId: "cmd-ss3",
		Name:      "x",
		Scope:     "cluster",
		Provider: &agentv1.SecretStoreApply_GcpSm{GcpSm: &agentv1.GcpSMProvider{
			ProjectId:     "p",
			AuthSecretRef: &agentv1.SecretRef{Name: "s", Namespace: "inari-system"},
		}},
		Target: &agentv1.GitTarget{Repo: "org/application-repo"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if result != agentv1.CommandResult_COMMAND_RESULT_FAILED {
		t.Fatalf("foreign repo must fail: %v", result)
	}
	if len(gp.Commits) != 0 {
		t.Fatal("no commit may happen on rejection")
	}
}

func TestSecretStoreApplyTransientGitError(t *testing.T) {
	gp := git.NewMemProvider()
	deps := GitOpsDeps{Git: gp, DefaultStateRepo: "org/acme-inari-state"}
	h := SecretStoreApplyHandler(deps)
	gp.Err = context.DeadlineExceeded

	_, _, err := h(context.Background(), secretStoreApplyEvent(t, &agentv1.SecretStoreApply{
		CommandId: "cmd-ss4",
		Name:      "x",
		Scope:     "cluster",
		Provider: &agentv1.SecretStoreApply_AzureKv{AzureKv: &agentv1.AzureKVProvider{
			VaultUrl:      "https://v.azure.net",
			AuthSecretRef: &agentv1.SecretRef{Name: "s", Namespace: "inari-system"},
		}},
	}))
	if err == nil {
		t.Fatal("transient git error must surface for redelivery")
	}
}

func TestSecretStoreDeleteHandler(t *testing.T) {
	gp := git.NewMemProvider()
	deps := GitOpsDeps{Git: gp, DefaultStateRepo: "org/acme-inari-state"}
	apply := SecretStoreApplyHandler(deps)
	del := SecretStoreDeleteHandler(deps)

	_, _, err := apply(context.Background(), secretStoreApplyEvent(t, &agentv1.SecretStoreApply{
		CommandId: "cmd-ss5",
		Name:      "corp-vault",
		Scope:     "cluster",
		Provider: &agentv1.SecretStoreApply_Vault{Vault: &agentv1.VaultProvider{
			Server:        "https://vault.corp:8200",
			AuthSecretRef: &agentv1.SecretRef{Name: "vault-token", Namespace: "inari-system"},
		}},
		Target: &agentv1.GitTarget{Path: "secretstores"},
	}))
	if err != nil {
		t.Fatal(err)
	}

	d := NewDispatcher()
	d.Register(agentv1.EventType_EVENT_TYPE_SECRET_STORE_DELETE, del)
	a, _ := anypb.New(&agentv1.SecretStoreDelete{
		CommandId: "cmd-ss6",
		Name:      "corp-vault",
		Scope:     "cluster",
		Target:    &agentv1.GitTarget{Path: "secretstores"},
	})
	ack, err := d.HandleEvent(context.Background(), &agentv1.Event{
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_SECRET_STORE_DELETE),
		Payload: a,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ack.Result != agentv1.CommandResult_COMMAND_RESULT_APPLIED {
		t.Fatalf("ack %+v", ack)
	}
	if len(gp.Deletes) != 1 || gp.Deletes[0].Paths[0] != "corp-vault.yaml" ||
		gp.Deletes[0].Target.Path != "secretstores" {
		t.Fatalf("deletes %+v", gp.Deletes)
	}

	// Redelivery of the delete is a no-op but still applied.
	a2, _ := anypb.New(&agentv1.SecretStoreDelete{CommandId: "cmd-ss7", Name: "corp-vault", Scope: "cluster",
		Target: &agentv1.GitTarget{Path: "secretstores"}})
	ack, err = d.HandleEvent(context.Background(), &agentv1.Event{
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_SECRET_STORE_DELETE),
		Payload: a2,
	})
	if err != nil || ack.Result != agentv1.CommandResult_COMMAND_RESULT_APPLIED {
		t.Fatalf("idempotent delete: ack %+v err %v", ack, err)
	}
}
