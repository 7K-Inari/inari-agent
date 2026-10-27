package controller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/metadata"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/capability"
	"github.com/7K-Inari/inari-agent/internal/command"
	"github.com/7K-Inari/inari-agent/internal/git"
	"github.com/7K-Inari/inari-agent/internal/health"
	"github.com/7K-Inari/inari-agent/internal/registration"
	"github.com/7K-Inari/inari-agent/internal/stream"
)

type fakeRegistrar struct {
	creds    *registration.Credentials
	mu       sync.Mutex
	gotToken string
}

func (f *fakeRegistrar) Register(_ context.Context, token string) (*registration.Credentials, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotToken = token
	return f.creds, nil
}

func (f *fakeRegistrar) token() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gotToken
}

type fakeSecretReader struct{ secret string }

func (f *fakeSecretReader) ReadSecret(context.Context, registration.SecretReference) (string, error) {
	return f.secret, nil
}

type fakeStreamClient struct {
	mu           sync.Mutex
	sent         []*agentv1.Event
	events       chan *agentv1.Event
	gotSecret    string
	onConn       func(bool)
	neverConnect bool // simulate a stream that never comes up
}

func (f *fakeStreamClient) SetOnConnectedChange(cb func(bool)) {
	f.mu.Lock()
	f.onConn = cb
	never := f.neverConnect
	f.mu.Unlock()
	// Report "connected" synchronously at wiring time: the controller wires
	// the callback before starting the event loop, so this keeps tests
	// deterministic (no connect/event race).
	if !never {
		cb(true)
	}
}

func (f *fakeStreamClient) setConn(v bool) {
	f.mu.Lock()
	cb := f.onConn
	f.mu.Unlock()
	if cb != nil {
		cb(v)
	}
}

func (f *fakeStreamClient) secret() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gotSecret
}

func (f *fakeStreamClient) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}
func (f *fakeStreamClient) Send(_ context.Context, ev *agentv1.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, ev)
	return nil
}
func (f *fakeStreamClient) Events() <-chan *agentv1.Event { return f.events }
func (f *fakeStreamClient) sentEvents() []*agentv1.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*agentv1.Event(nil), f.sent...)
}

type fakeWatcher struct{ ch chan *agentv1.Capability }

func (f *fakeWatcher) Source() capability.Source { return capability.SourceCRD }
func (f *fakeWatcher) Start(context.Context) (<-chan *agentv1.Capability, error) {
	return f.ch, nil
}

func newTestReconciler(fc *fakeStreamClient, watcher *fakeWatcher) (*AgentReconciler, *fakeRegistrar) {
	reg := &fakeRegistrar{creds: &registration.Credentials{
		TenantID: "tenant-1", ClusterID: "cluster-1", ClientID: "cluster-cluster-1",
		TokenURL: "https://keycloak/token", ControlPlane: "https://gw",
		SecretRef: registration.SecretReference{Name: "s", Namespace: "inari-system", Key: "client-secret"},
	}}
	r := &AgentReconciler{
		Log:            logr.Discard(),
		BootstrapToken: "one-time-token",
		Registrar:      reg,
		SecretReader:   &fakeSecretReader{secret: "s3cr3t"},
		NewStreamClient: func(_ *registration.Credentials, clientSecret string, _ func() string) stream.Client {
			fc.mu.Lock()
			fc.gotSecret = clientSecret
			fc.mu.Unlock()
			return fc
		},
		NewWatchers: func(kubernetes.Interface, dynamic.Interface, metadata.Interface) []capability.Watcher {
			return []capability.Watcher{watcher}
		},
	}
	return r, reg
}

func TestPersistCredentialsIncludesBaselineRepo(t *testing.T) {
	r := &AgentReconciler{
		Log:  logr.Discard(),
		Kube: k8sfake.NewSimpleClientset(),
	}
	creds := &registration.Credentials{
		TenantID:     "tenant-1",
		ClusterID:    "cluster-1",
		ClientID:     "cluster-cluster-1",
		TokenURL:     "https://keycloak/token",
		ControlPlane: "https://gw",
		SecretRef:    registration.SecretReference{Name: "s", Namespace: "inari-system", Key: "client-secret"},
		BaselineRepo: "myorg/platform-baseline",
	}
	if err := r.persistCredentials(context.Background(), creds); err != nil {
		t.Fatalf("persistCredentials: %v", err)
	}
	cm, err := r.Kube.CoreV1().ConfigMaps("inari-system").Get(context.Background(), registrationConfigMap, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get ConfigMap: %v", err)
	}
	if got := cm.Data["baseline-repo"]; got != "myorg/platform-baseline" {
		t.Errorf("baseline-repo = %q, want %q", got, "myorg/platform-baseline")
	}
}

func TestLoadPersistedCredentialsReadsBaselineRepo(t *testing.T) {
	r := &AgentReconciler{
		Log: logr.Discard(),
		Kube: k8sfake.NewSimpleClientset(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: registrationConfigMap, Namespace: "inari-system"},
			Data: map[string]string{
				"tenant-id":        "tenant-1",
				"cluster-id":       "cluster-1",
				"client-id":        "cluster-cluster-1",
				"token-url":        "https://keycloak/token",
				"control-plane":    "https://gw",
				"secret-store":     "store",
				"secret-name":      "s",
				"secret-namespace": "inari-system",
				"secret-key":       "client-secret",
				"baseline-repo":    "myorg/platform-baseline",
			},
		}),
	}
	creds, err := r.loadPersistedCredentials(context.Background())
	if err != nil {
		t.Fatalf("loadPersistedCredentials: %v", err)
	}
	if creds == nil {
		t.Fatal("expected credentials, got nil")
	}
	if creds.BaselineRepo != "myorg/platform-baseline" {
		t.Errorf("BaselineRepo = %q, want %q", creds.BaselineRepo, "myorg/platform-baseline")
	}
}

func TestLoadPersistedCredentialsEmptyBaselineRepo(t *testing.T) {
	r := &AgentReconciler{
		Log: logr.Discard(),
		Kube: k8sfake.NewSimpleClientset(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: registrationConfigMap, Namespace: "inari-system"},
			Data: map[string]string{
				"tenant-id":        "tenant-1",
				"cluster-id":       "cluster-1",
				"client-id":        "cluster-cluster-1",
				"token-url":        "https://keycloak/token",
				"control-plane":    "https://gw",
				"secret-store":     "store",
				"secret-name":      "s",
				"secret-namespace": "inari-system",
				"secret-key":       "client-secret",
			},
		}),
	}
	creds, err := r.loadPersistedCredentials(context.Background())
	if err != nil {
		t.Fatalf("loadPersistedCredentials: %v", err)
	}
	if creds == nil {
		t.Fatal("expected credentials, got nil")
	}
	if creds.BaselineRepo != "" {
		t.Errorf("BaselineRepo = %q, want empty", creds.BaselineRepo)
	}
}

func TestStandaloneModeBlocksUntilCancelled(t *testing.T) {
	r := &AgentReconciler{Log: logr.Discard()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("standalone mode did not return on cancel")
	}
}

func TestLifecycleRegistersForgetsTokenAndStreamsCapabilities(t *testing.T) {
	fc := &fakeStreamClient{events: make(chan *agentv1.Event, 8)}
	watcher := &fakeWatcher{ch: make(chan *agentv1.Capability, 1)}
	r, reg := newTestReconciler(fc, watcher)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Start(ctx) }()

	waitFor(t, "registration", func() bool { return reg.token() == "one-time-token" })
	waitFor(t, "token forgotten", func() bool { return r.TokenForgotten() })
	waitFor(t, "stream client built with resolved secret", func() bool { return fc.secret() == "s3cr3t" })

	watcher.ch <- &agentv1.Capability{
		Kind: agentv1.CapabilityKind_CAPABILITY_KIND_CRD, Name: "widgets.example.com", Version: "v1",
	}
	waitFor(t, "capability update upstream", func() bool {
		for _, ev := range fc.sentEvents() {
			if agentv1.EventTypeFromString(ev.Type) == agentv1.EventType_EVENT_TYPE_CAPABILITY_UPDATE {
				return true
			}
		}
		return false
	})
}

func TestLifecycleAcksCommandsIdempotently(t *testing.T) {
	fc := &fakeStreamClient{events: make(chan *agentv1.Event, 8)}
	watcher := &fakeWatcher{ch: make(chan *agentv1.Capability)}
	r, _ := newTestReconciler(fc, watcher)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Start(ctx) }()

	payload, _ := anypb.New(&agentv1.ApplyBundle{CommandId: "cmd-1"})
	cmd := &agentv1.Event{
		EventId: "evt-cmd-1",
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_APPLY_BUNDLE),
		Payload: payload,
	}
	fc.events <- cmd
	fc.events <- cmd // duplicate delivery

	waitFor(t, "two acks (one per delivery)", func() bool {
		n := 0
		for _, ev := range fc.sentEvents() {
			if agentv1.EventTypeFromString(ev.Type) == agentv1.EventType_EVENT_TYPE_COMMAND_ACK {
				n++
			}
		}
		return n == 2
	})
}

func TestLifecycleReplaysFullStateOnResync(t *testing.T) {
	fc := &fakeStreamClient{events: make(chan *agentv1.Event, 8)}
	watcher := &fakeWatcher{ch: make(chan *agentv1.Capability, 1)}
	r, _ := newTestReconciler(fc, watcher)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Start(ctx) }()

	watcher.ch <- &agentv1.Capability{Kind: agentv1.CapabilityKind_CAPABILITY_KIND_CRD, Name: "a", Version: "v1"}
	waitFor(t, "initial capability", func() bool { return len(fc.sentEvents()) >= 1 })

	fc.events <- &agentv1.Event{
		EventId: "resync-1",
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_RESYNC_REQUEST),
	}
	waitFor(t, "full-sync replay", func() bool {
		for _, ev := range fc.sentEvents() {
			if agentv1.EventTypeFromString(ev.Type) != agentv1.EventType_EVENT_TYPE_CAPABILITY_UPDATE {
				continue
			}
			var upd agentv1.CapabilityUpdate
			if err := ev.Payload.UnmarshalTo(&upd); err == nil && upd.FullSync {
				return true
			}
		}
		return false
	})
}

type transientFailHandler struct{ err error }

func (h transientFailHandler) HandleEvent(context.Context, *agentv1.Event) (*agentv1.CommandAck, error) {
	return nil, h.err
}

// Issue #29/#30: a transient command failure must NACK with the command_id
// from the payload, not the event id, so the server surfaces a typed error
// instead of an invisible timeout.
func TestLifecycleNackCarriesCommandID(t *testing.T) {
	fc := &fakeStreamClient{events: make(chan *agentv1.Event, 8)}
	watcher := &fakeWatcher{ch: make(chan *agentv1.Capability)}
	r, _ := newTestReconciler(fc, watcher)
	r.Handler = transientFailHandler{
		err: &command.Error{CommandID: "cmd-42", Err: errors.New("argocd: connection reset")},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Start(ctx) }()

	payload, _ := anypb.New(&agentv1.RegisterArgoCDApp{CommandId: "cmd-42", Name: "app"})
	fc.events <- &agentv1.Event{
		EventId: "evt-xyz",
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_REGISTER_ARGOCD_APP),
		Payload: payload,
	}

	waitFor(t, "NACK correlated by command id", func() bool {
		for _, ev := range fc.sentEvents() {
			if agentv1.EventTypeFromString(ev.Type) != agentv1.EventType_EVENT_TYPE_COMMAND_NACK {
				continue
			}
			var ack agentv1.CommandAck
			if err := ev.Payload.UnmarshalTo(&ack); err != nil {
				continue
			}
			if ack.CommandId == "cmd-42" &&
				ack.Result == agentv1.CommandResult_COMMAND_RESULT_FAILED &&
				ack.Message != "" &&
				ev.EventId == "ack-cmd-42" {
				return true
			}
		}
		return false
	})
}

// The readiness tracker arms disconnected when the stream client is built,
// follows connection transitions, and tolerates partitions within the grace
// period — so /readyz reflects control-plane connectivity without flapping
// on brief reconnects (plan §5.3 HA posture).
func TestLifecycleReadinessTracksStreamConnectivity(t *testing.T) {
	fc := &fakeStreamClient{events: make(chan *agentv1.Event, 8), neverConnect: true}
	watcher := &fakeWatcher{ch: make(chan *agentv1.Capability)}
	r, _ := newTestReconciler(fc, watcher)
	tr := health.NewStreamTracker(100 * time.Millisecond)
	r.Readiness = tr

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Start(ctx) }()

	// Armed disconnected at client build: unready once the initial-connect
	// grace expires.
	waitFor(t, "unready while never connected past grace", func() bool { return tr.Check(nil) != nil })

	// A connect restores readiness...
	fc.setConn(true)
	if err := tr.Check(nil); err != nil {
		t.Fatalf("connected stream must be ready, got %v", err)
	}
	// ...a partition within grace does not flap...
	fc.setConn(false)
	if err := tr.Check(nil); err != nil {
		t.Fatalf("partition within grace must stay ready, got %v", err)
	}
	// ...but a sustained partition goes unready.
	waitFor(t, "unready after sustained partition", func() bool { return tr.Check(nil) != nil })
}

// Standalone mode (no control plane configured) never builds a stream
// client, so the tracker stays unarmed and /readyz stays green.
func TestStandaloneModeLeavesReadinessUnarmed(t *testing.T) {
	tr := health.NewStreamTracker(time.Nanosecond)
	r := &AgentReconciler{Log: logr.Discard(), Readiness: tr}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := tr.Check(nil); err != nil {
		t.Fatalf("standalone mode must stay ready, got %v", err)
	}
}

// Bridge commands (Feature C addon registrations) arrive as structpb
// payloads under event types the pinned proto enum does not know; the
// reconciler must route them to the command handler, not drop-and-log.
func TestDispatchRoutesBridgeCommands(t *testing.T) {
	fc := &fakeStreamClient{events: make(chan *agentv1.Event, 8)}
	r := &AgentReconciler{Log: logr.Discard()}
	handler := command.NewDispatcher()
	handler.RegisterBridge(command.RegisterArgoCDAppSetEventType,
		func(ctx context.Context, ev *agentv1.Event) (agentv1.CommandResult, string, error) {
			return agentv1.CommandResult_COMMAND_RESULT_APPLIED, "ok", nil
		})

	payload, err := structpb.NewStruct(map[string]interface{}{
		"commandId": "inari-addonset-01J",
		"name":      "inari-addonset-01J",
		"namespace": "argocd",
		"manifest":  "apiVersion: argoproj.io/v1alpha1\nkind: ApplicationSet\n",
	})
	if err != nil {
		t.Fatal(err)
	}
	any, err := anypb.New(payload)
	if err != nil {
		t.Fatal(err)
	}
	ev := &agentv1.Event{EventId: "evt-appset", Type: command.RegisterArgoCDAppSetEventType, Payload: any}
	r.dispatch(context.Background(), fc, nil, handler, ev)

	waitFor(t, "bridge command acked", func() bool {
		for _, ev := range fc.sentEvents() {
			if agentv1.EventTypeFromString(ev.Type) == agentv1.EventType_EVENT_TYPE_COMMAND_ACK {
				var ack agentv1.CommandAck
				if err := ev.Payload.UnmarshalTo(&ack); err == nil &&
					ack.CommandId == "inari-addonset-01J" && ack.Result == agentv1.CommandResult_COMMAND_RESULT_APPLIED {
					return true
				}
			}
		}
		return false
	})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestDispatchRoutesGitopsConfigUpdate(t *testing.T) {
	fc := &fakeStreamClient{events: make(chan *agentv1.Event, 8)}
	r := &AgentReconciler{Log: logr.Discard()}
	handler := command.NewDispatcher()
	deps := &command.GitOpsDeps{Git: git.NewMemProvider(), DefaultStateRepo: "org/old-baseline"}
	kube := k8sfake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "inari-agent-registration", Namespace: "inari-system"},
		Data:       map[string]string{"baseline-repo": "org/old-baseline"},
	})
	handler.Register(agentv1.EventType_EVENT_TYPE_GITOPS_CONFIG_UPDATE,
		command.GitopsConfigUpdateHandler(deps, kube, "inari-system"))

	payload, _ := anypb.New(&agentv1.GitopsConfigUpdate{CommandId: "cmd-rebind", BaselineRepo: "org/new-baseline"})
	ev := &agentv1.Event{
		EventId: "evt-rebind",
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_GITOPS_CONFIG_UPDATE),
		Payload: payload,
	}
	r.dispatch(context.Background(), fc, nil, handler, ev)

	waitFor(t, "gitops-config-update acked", func() bool {
		for _, ev := range fc.sentEvents() {
			if agentv1.EventTypeFromString(ev.Type) == agentv1.EventType_EVENT_TYPE_COMMAND_ACK {
				var ack agentv1.CommandAck
				if err := ev.Payload.UnmarshalTo(&ack); err == nil && ack.CommandId == "cmd-rebind" {
					return true
				}
			}
		}
		return false
	})
	if deps.DefaultStateRepo != "org/new-baseline" {
		t.Errorf("DefaultStateRepo = %q, want %q", deps.DefaultStateRepo, "org/new-baseline")
	}
}

// TestEndToEndBaselineBinding exercises the full flow: registration returns a
// baseline repo, the agent persists it, GitOps handlers are configured with
// it, a gitops-config-update rebinds, and subsequent render/apply commands
// target the new repo.
func TestEndToEndBaselineBinding(t *testing.T) {
	ctx := context.Background()
	kube := k8sfake.NewSimpleClientset()
	gp := git.NewMemProvider()

	// 1. Registration returns a baseline repo.
	creds := &registration.Credentials{
		TenantID: "tenant-1", ClusterID: "cluster-1", ClientID: "cluster-cluster-1",
		TokenURL: "https://keycloak/token", ControlPlane: "https://gw",
		SecretRef:    registration.SecretReference{Name: "s", Namespace: "inari-system", Key: "client-secret"},
		BaselineRepo: "myorg/platform-baseline",
	}

	// 2. Agent persists it (simulating what Start does after register).
	r := &AgentReconciler{Log: logr.Discard(), Kube: kube}
	if err := r.persistCredentials(ctx, creds); err != nil {
		t.Fatal(err)
	}
	cm, err := kube.CoreV1().ConfigMaps("inari-system").Get(ctx, registrationConfigMap, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := cm.Data["baseline-repo"]; got != "myorg/platform-baseline" {
		t.Fatalf("persisted baseline-repo = %q, want %q", got, "myorg/platform-baseline")
	}

	// 3. GitOps configure wires the bound baseline into the handlers.
	cfg := &GitOpsConfig{
		Kube:             kube,
		Dyn:              dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		Git:              gp,
		BoundStateRepo:   creds.BaselineRepo,
		JournalNamespace: "inari-system",
	}
	d := command.NewDispatcher()
	if err := cfg.configure(ctx, d, creds.TenantID, nil); err != nil {
		t.Fatal(err)
	}

	// 4. Verify the initial state: render targets the bound baseline.
	rgd := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "kro.run/v1alpha1",
		"kind":       "ResourceGraphDefinition",
		"metadata":   map[string]interface{}{"name": "web-service"},
		"spec": map[string]interface{}{
			"schema": map[string]interface{}{
				"apiVersion": "v1alpha1",
				"kind":       "WebService",
				"spec":       map[string]interface{}{"required": []interface{}{"image"}},
			},
		},
	}}
	listKinds := map[schema.GroupVersionResource]string{
		{Group: "kro.run", Version: "v1alpha1", Resource: "resourcegraphdefinitions"}: "ResourceGraphDefinitionList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, rgd)
	cfg.Dyn = dyn
	d2 := command.NewDispatcher()
	if err := cfg.configure(ctx, d2, creds.TenantID, nil); err != nil {
		t.Fatal(err)
	}

	params, _ := structpb.NewStruct(map[string]interface{}{"image": "nginx:1.27"})
	renderPayload, _ := anypb.New(&agentv1.RenderRgdInstance{
		CommandId: "cmd-render-1", RgdRef: "web-service", InstanceName: "my-web",
		TargetNamespace: "apps", Parameters: params,
		Policy: agentv1.CommitPolicy_COMMIT_POLICY_DIRECT_COMMIT,
	})
	ack, err := d2.HandleEvent(ctx, &agentv1.Event{
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_RENDER_RGD_INSTANCE),
		Payload: renderPayload,
	})
	if err != nil || ack.Result != agentv1.CommandResult_COMMAND_RESULT_APPLIED {
		t.Fatalf("render: %+v %v", ack, err)
	}
	if len(gp.Commits) != 1 || gp.Commits[0].Target.Repo != "myorg/platform-baseline" {
		t.Fatalf("render committed to %q, want %q", gp.Commits[0].Target.Repo, "myorg/platform-baseline")
	}

	// 5. Server pushes a rebind.
	rebindPayload, _ := anypb.New(&agentv1.GitopsConfigUpdate{CommandId: "cmd-rebind-e2e", BaselineRepo: "myorg/team-a-baseline"})
	ack, err = d2.HandleEvent(ctx, &agentv1.Event{
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_GITOPS_CONFIG_UPDATE),
		Payload: rebindPayload,
	})
	if err != nil || ack.Result != agentv1.CommandResult_COMMAND_RESULT_APPLIED {
		t.Fatalf("rebind: %+v %v", ack, err)
	}

	// 6. ConfigMap updated.
	cm, err = kube.CoreV1().ConfigMaps("inari-system").Get(ctx, registrationConfigMap, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := cm.Data["baseline-repo"]; got != "myorg/team-a-baseline" {
		t.Fatalf("post-rebind baseline-repo = %q, want %q", got, "myorg/team-a-baseline")
	}

	// 7. Subsequent render targets the NEW baseline.
	renderPayload2, _ := anypb.New(&agentv1.RenderRgdInstance{
		CommandId: "cmd-render-2", RgdRef: "web-service", InstanceName: "my-web-2",
		TargetNamespace: "apps", Parameters: params,
		Policy: agentv1.CommitPolicy_COMMIT_POLICY_DIRECT_COMMIT,
	})
	ack, err = d2.HandleEvent(ctx, &agentv1.Event{
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_RENDER_RGD_INSTANCE),
		Payload: renderPayload2,
	})
	if err != nil || ack.Result != agentv1.CommandResult_COMMAND_RESULT_APPLIED {
		t.Fatalf("render after rebind: %+v %v", ack, err)
	}
	if len(gp.Commits) != 2 || gp.Commits[1].Target.Repo != "myorg/team-a-baseline" {
		t.Fatalf("post-rebind render committed to %q, want %q", gp.Commits[1].Target.Repo, "myorg/team-a-baseline")
	}
}

func TestLifecycleFailsCommandsClosedUntilStreamConnects(t *testing.T) {
	fc := &fakeStreamClient{events: make(chan *agentv1.Event, 8), neverConnect: true}
	watcher := &fakeWatcher{ch: make(chan *agentv1.Capability)}
	r, _ := newTestReconciler(fc, watcher)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Start(ctx) }()

	payload, _ := anypb.New(&agentv1.ApplyBundle{CommandId: "cmd-down"})
	fc.events <- &agentv1.Event{
		EventId: "evt-cmd-down",
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_APPLY_BUNDLE),
		Payload: payload,
	}

	waitFor(t, "command NACKed while stream down", func() bool {
		for _, ev := range fc.sentEvents() {
			if agentv1.EventTypeFromString(ev.Type) == agentv1.EventType_EVENT_TYPE_COMMAND_NACK {
				return true
			}
		}
		return false
	})
}
