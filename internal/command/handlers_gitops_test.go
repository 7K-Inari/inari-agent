package command

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/git"
)

func TestConfigMapJournalRoundTrip(t *testing.T) {
	kube := fake.NewSimpleClientset()
	j := NewConfigMapJournal(kube, "inari-system", "inari-agent-command-journal")

	if got, err := j.Load(context.Background()); err != nil || len(got) != 0 {
		t.Fatalf("empty load: %v %v", got, err)
	}
	ack := &agentv1.CommandAck{CommandId: "c1", Result: agentv1.CommandResult_COMMAND_RESULT_APPLIED, Message: "ok"}
	if err := j.Record(context.Background(), ack); err != nil {
		t.Fatal(err)
	}
	ack2 := &agentv1.CommandAck{CommandId: "c2", Result: agentv1.CommandResult_COMMAND_RESULT_FAILED, Message: "bad"}
	if err := j.Record(context.Background(), ack2); err != nil {
		t.Fatal(err)
	}
	got, err := j.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["c1"].Result != agentv1.CommandResult_COMMAND_RESULT_APPLIED || got["c2"].Message != "bad" {
		t.Fatalf("loaded %+v", got)
	}
	cm, err := kube.CoreV1().ConfigMaps("inari-system").Get(context.Background(), "inari-agent-command-journal", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(cm.Data) != 2 {
		t.Fatalf("configmap data %+v", cm.Data)
	}
	_ = corev1.ConfigMap{}
}

func TestDispatcherJournalPersistsAcrossRestart(t *testing.T) {
	kube := fake.NewSimpleClientset()
	j := NewConfigMapJournal(kube, "inari-system", "journal")

	d1 := NewDispatcher()
	if err := d1.AttachJournal(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	a, _ := anypb.New(&agentv1.ApplyBundle{CommandId: "cmd-persist"})
	ev := &agentv1.Event{Type: agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_APPLY_BUNDLE), Payload: a}
	if _, err := d1.HandleEvent(context.Background(), ev); err != nil {
		t.Fatal(err)
	}

	// "Restart": fresh dispatcher, same journal.
	d2 := NewDispatcher()
	if err := d2.AttachJournal(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	ack, err := d2.HandleEvent(context.Background(), ev)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Result != agentv1.CommandResult_COMMAND_RESULT_ACCEPTED {
		t.Fatalf("replay after restart must return recorded ack, got %+v", ack)
	}
	if d2.HandledCount() != 0 {
		t.Fatalf("replayed command must not re-execute, handled=%d", d2.HandledCount())
	}
}

func renderEvent(t *testing.T, m *agentv1.RenderRgdInstance) *agentv1.Event {
	t.Helper()
	a, err := anypb.New(m)
	if err != nil {
		t.Fatal(err)
	}
	return &agentv1.Event{Type: agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_RENDER_RGD_INSTANCE), Payload: a}
}

func TestRenderRGDInstanceHandlerDirectCommit(t *testing.T) {
	gp := git.NewMemProvider()
	deps := GitOpsDeps{Git: gp, DefaultStateRepo: "org/acme-inari-state"}
	params, _ := structpb.NewStruct(map[string]interface{}{"image": "nginx:1.27"})
	h := RenderRGDInstanceHandler(deps, fakeGetter{obj: testRGD()})

	d := NewDispatcher()
	d.Register(agentv1.EventType_EVENT_TYPE_RENDER_RGD_INSTANCE, h)

	ack, err := d.HandleEvent(context.Background(), renderEvent(t, &agentv1.RenderRgdInstance{
		CommandId:       "cmd-r1",
		RgdRef:          "web-service",
		InstanceName:    "my-web",
		TargetNamespace: "apps",
		Parameters:      params,
		Policy:          agentv1.CommitPolicy_COMMIT_POLICY_DIRECT_COMMIT,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if ack.Result != agentv1.CommandResult_COMMAND_RESULT_APPLIED || !strings.HasPrefix(ack.Message, "commit ") {
		t.Fatalf("ack %+v", ack)
	}
	if len(gp.Commits) != 1 || gp.Commits[0].Target.Repo != "org/acme-inari-state" {
		t.Fatalf("commits %+v", gp.Commits)
	}
	if got := string(gp.Commits[0].Files[0].Content); !strings.Contains(got, "kind: WebService") {
		t.Fatalf("rendered content:\n%s", got)
	}
}

func TestRenderRGDInstanceHandlerPullRequest(t *testing.T) {
	gp := git.NewMemProvider()
	deps := GitOpsDeps{Git: gp, DefaultStateRepo: "org/acme-inari-state"}
	h := RenderRGDInstanceHandler(deps, fakeGetter{obj: testRGD()})

	result, msg, err := h(context.Background(), renderEvent(t, &agentv1.RenderRgdInstance{
		CommandId:       "cmd-pr1",
		RgdRef:          "web-service",
		InstanceName:    "my-web",
		TargetNamespace: "apps",
		Parameters:      mustStruct(t, map[string]interface{}{"image": "nginx"}),
		Policy:          agentv1.CommitPolicy_COMMIT_POLICY_PULL_REQUEST,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if result != agentv1.CommandResult_COMMAND_RESULT_APPLIED || !strings.HasPrefix(msg, "pull request: ") {
		t.Fatalf("result=%v msg=%q", result, msg)
	}
	if len(gp.PRs) != 1 || gp.PRs[0].Branch != "inari/cmd-pr1" {
		t.Fatalf("prs %+v", gp.PRs)
	}
}

func TestRenderHandlerRejectsForeignRepo(t *testing.T) {
	gp := git.NewMemProvider()
	deps := GitOpsDeps{Git: gp, DefaultStateRepo: "org/acme-inari-state"}
	h := RenderRGDInstanceHandler(deps, fakeGetter{obj: testRGD()})

	result, _, err := h(context.Background(), renderEvent(t, &agentv1.RenderRgdInstance{
		CommandId:    "cmd-bad",
		RgdRef:       "web-service",
		InstanceName: "x",
		Parameters:   mustStruct(t, map[string]interface{}{"image": "i"}),
		Target:       &agentv1.GitTarget{Repo: "org/application-repo"},
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

func TestApplyBundleHandler(t *testing.T) {
	gp := git.NewMemProvider()
	deps := GitOpsDeps{Git: gp, DefaultStateRepo: "org/acme-inari-state"}
	bundles := &fakeBundles{files: []git.File{{Path: "manifests/app.yaml", Content: []byte("apiVersion: v1\n")}}}
	h := ApplyBundleHandler(deps, bundles)

	d := NewDispatcher()
	d.Register(agentv1.EventType_EVENT_TYPE_APPLY_BUNDLE, h)

	a, _ := anypb.New(&agentv1.ApplyBundle{
		CommandId: "cmd-b1",
		Source:    &agentv1.ApplyBundle_OciRef{OciRef: "oci://ghcr.io/inari/bundle:v1"},
		Checksum:  "sha256:whatever",
	})
	ack, err := d.HandleEvent(context.Background(), &agentv1.Event{
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_APPLY_BUNDLE),
		Payload: a,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ack.Result != agentv1.CommandResult_COMMAND_RESULT_APPLIED {
		t.Fatalf("ack %+v", ack)
	}
	if bundles.seen.Checksum != "sha256:whatever" {
		t.Fatalf("checksum not forwarded: %+v", bundles.seen)
	}
}

func TestApplyBundleRejectsEscapingFilePath(t *testing.T) {
	gp := git.NewMemProvider()
	deps := GitOpsDeps{Git: gp, DefaultStateRepo: "org/acme-inari-state"}
	bundles := &fakeBundles{files: []git.File{{Path: "../../other-instance/app.yaml", Content: []byte("x")}}}
	h := ApplyBundleHandler(deps, bundles)

	a, _ := anypb.New(&agentv1.ApplyBundle{
		CommandId: "cmd-evil",
		Source:    &agentv1.ApplyBundle_OciRef{OciRef: "oci://ghcr.io/inari/bundle:v1"},
	})
	result, msg, err := h(context.Background(), &agentv1.Event{
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_APPLY_BUNDLE),
		Payload: a,
	})
	if err != nil || result != agentv1.CommandResult_COMMAND_RESULT_FAILED {
		t.Fatalf("path traversal must fail the command, got result=%v msg=%q err=%v", result, msg, err)
	}
	if len(gp.Commits) != 0 {
		t.Fatal("no commit may happen on rejection")
	}
}

func mustStruct(t *testing.T, m map[string]interface{}) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(m)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
