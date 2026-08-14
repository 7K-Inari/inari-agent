package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/protobuf/types/known/anypb"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/capability"
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
	mu        sync.Mutex
	sent      []*agentv1.Event
	events    chan *agentv1.Event
	gotSecret string
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
		NewWatchers: func(kubernetes.Interface, dynamic.Interface) []capability.Watcher {
			return []capability.Watcher{watcher}
		},
	}
	return r, reg
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
	if fc.secret() != "s3cr3t" {
		t.Errorf("stream client built with secret %q", fc.secret())
	}

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
