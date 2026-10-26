package controller

import (
	"context"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/command"
	"github.com/7K-Inari/inari-agent/internal/git"
)

func TestGitOpsConfigureRegistersHandlers(t *testing.T) {
	cfg := &GitOpsConfig{
		Kube:            k8sfake.NewSimpleClientset(),
		Dyn:             dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		Git:             git.NewMemProvider(),
		StateRepo:       "org/acme-inari-state",
		ArgoCDMode:      "bundle",
		ArgoCDManifests: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: argocd-cm\n"),
		// no ArgoCDAPI: invoke-action must stay unregistered (no-op accept)
	}
	d := command.NewDispatcher()
	if err := cfg.configure(context.Background(), d, "acme", nil); err != nil {
		t.Fatal(err)
	}

	// render-rgd-instance is now real: unknown RGD fails via handler, not
	// the M1 no-op accept.
	a, _ := anypb.New(&agentv1.RenderRgdInstance{CommandId: "c1", RgdRef: "ghost", InstanceName: "x"})
	ack, err := d.HandleEvent(context.Background(), &agentv1.Event{
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_RENDER_RGD_INSTANCE),
		Payload: a,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ack.Result != agentv1.CommandResult_COMMAND_RESULT_FAILED {
		t.Fatalf("expected FAILED from real handler, got %+v", ack)
	}

	// invoke-action without API client stays a no-op accept.
	b, _ := anypb.New(&agentv1.InvokeAction{CommandId: "c2", Action: "sync"})
	ack, err = d.HandleEvent(context.Background(), &agentv1.Event{
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_INVOKE_ACTION),
		Payload: b,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ack.Result != agentv1.CommandResult_COMMAND_RESULT_ACCEPTED {
		t.Fatalf("expected ACCEPTED no-op, got %+v", ack)
	}
}

func TestGitOpsStateRepoDefault(t *testing.T) {
	cfg := &GitOpsConfig{StateRepoOrg: "7K-Inari"}
	if got := cfg.stateRepo("acme"); got != "7K-Inari/acme-inari-state" {
		t.Fatalf("got %q", got)
	}
	if got := (&GitOpsConfig{}).stateRepo("acme"); got != "" {
		t.Fatalf("got %q", got)
	}
}

func TestGitOpsStateRepoResolutionOrder(t *testing.T) {
	tests := []struct {
		name     string
		cfg      GitOpsConfig
		tenantID string
		want     string
	}{
		{
			name:     "explicit binding wins over env and convention",
			cfg:      GitOpsConfig{BoundStateRepo: "org/bound-repo", StateRepo: "org/env-repo", StateRepoOrg: "org"},
			tenantID: "acme",
			want:     "org/bound-repo",
		},
		{
			name:     "env override wins over convention when no binding",
			cfg:      GitOpsConfig{StateRepo: "org/env-repo", StateRepoOrg: "org"},
			tenantID: "acme",
			want:     "org/env-repo",
		},
		{
			name:     "convention when neither binding nor env",
			cfg:      GitOpsConfig{StateRepoOrg: "org"},
			tenantID: "acme",
			want:     "org/acme-inari-state",
		},
		{
			name:     "empty when nothing set",
			cfg:      GitOpsConfig{},
			tenantID: "acme",
			want:     "",
		},
		{
			name:     "binding wins even when env is set",
			cfg:      GitOpsConfig{BoundStateRepo: "org/bound-repo", StateRepo: "org/env-repo"},
			tenantID: "acme",
			want:     "org/bound-repo",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.stateRepo(tt.tenantID); got != tt.want {
				t.Errorf("stateRepo(%q) = %q, want %q", tt.tenantID, got, tt.want)
			}
		})
	}
}
