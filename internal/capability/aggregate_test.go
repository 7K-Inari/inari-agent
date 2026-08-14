package capability

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

type fakeWatcher struct {
	source Source
	ch     chan *agentv1.Capability
}

func (f *fakeWatcher) Source() Source { return f.source }
func (f *fakeWatcher) Start(context.Context) (<-chan *agentv1.Capability, error) {
	return f.ch, nil
}

type fakeStreamClient struct {
	mu     sync.Mutex
	sent   []*agentv1.Event
	events chan *agentv1.Event
}

func (f *fakeStreamClient) Run(context.Context) error { return nil }
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

func capabilityUpdates(t *testing.T, events []*agentv1.Event) []*agentv1.CapabilityUpdate {
	t.Helper()
	var out []*agentv1.CapabilityUpdate
	for _, ev := range events {
		var upd agentv1.CapabilityUpdate
		if ev.Payload == nil || !ev.Payload.MessageIs(&upd) {
			continue
		}
		if err := ev.Payload.UnmarshalTo(&upd); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		out = append(out, &upd)
	}
	return out
}

func TestAggregatorForwardsIncrementalUpdates(t *testing.T) {
	w := &fakeWatcher{source: SourceCRD, ch: make(chan *agentv1.Capability, 1)}
	client := &fakeStreamClient{events: make(chan *agentv1.Event)}
	agg := NewAggregator("tenant-1", "cluster-1", client, []Watcher{w})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = agg.Run(ctx) }()

	w.ch <- &agentv1.Capability{
		Kind: agentv1.CapabilityKind_CAPABILITY_KIND_CRD, Name: "a", Version: "v1",
		Action: agentv1.CapabilityAction_CAPABILITY_ACTION_UPSERT,
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(client.sentEvents()) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	updates := capabilityUpdates(t, client.sentEvents())
	if len(updates) != 1 {
		t.Fatalf("sent %d updates, want 1", len(updates))
	}
	if updates[0].FullSync {
		t.Error("incremental update must not be a full sync")
	}
	if updates[0].StateChecksum == "" {
		t.Error("state checksum must be set")
	}
	if updates[0].StateChecksum != agg.Checksum() {
		t.Error("update checksum must match aggregator snapshot")
	}
}

func TestAggregatorReplaysFullSnapshotOnResyncWithoutDuplicates(t *testing.T) {
	w := &fakeWatcher{source: SourceCRD, ch: make(chan *agentv1.Capability, 2)}
	client := &fakeStreamClient{events: make(chan *agentv1.Event, 1)}
	agg := NewAggregator("tenant-1", "cluster-1", client, []Watcher{w})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = agg.Run(ctx) }()

	w.ch <- &agentv1.Capability{Kind: agentv1.CapabilityKind_CAPABILITY_KIND_CRD, Name: "a", Version: "v1"}
	w.ch <- &agentv1.Capability{Kind: agentv1.CapabilityKind_CAPABILITY_KIND_CRD, Name: "b", Version: "v1"}
	waitForSent(t, client, 2)

	resyncPayload, _ := anypb.New(&agentv1.Event{})
	_ = resyncPayload
	if err := agg.ReplayFullState(ctx); err != nil {
		t.Fatalf("ReplayFullState: %v", err)
	}
	waitForSent(t, client, 3)

	updates := capabilityUpdates(t, client.sentEvents())
	full := updates[len(updates)-1]
	if !full.FullSync {
		t.Fatal("resync replay must be a full sync")
	}
	names := map[string]int{}
	for _, c := range full.Capabilities {
		names[c.Name]++
	}
	if len(full.Capabilities) != 2 || names["a"] != 1 || names["b"] != 1 {
		t.Errorf("full sync must contain each capability exactly once, got %v", names)
	}
}

func TestAggregatorDeleteRemovesFromSnapshot(t *testing.T) {
	w := &fakeWatcher{source: SourceCRD, ch: make(chan *agentv1.Capability, 2)}
	client := &fakeStreamClient{events: make(chan *agentv1.Event)}
	agg := NewAggregator("tenant-1", "cluster-1", client, []Watcher{w})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = agg.Run(ctx) }()

	w.ch <- &agentv1.Capability{Kind: agentv1.CapabilityKind_CAPABILITY_KIND_CRD, Name: "a", Version: "v1"}
	waitForSent(t, client, 1)
	before := agg.Checksum()

	w.ch <- &agentv1.Capability{
		Kind: agentv1.CapabilityKind_CAPABILITY_KIND_CRD, Name: "a", Version: "v1",
		Action: agentv1.CapabilityAction_CAPABILITY_ACTION_DELETE,
	}
	waitForSent(t, client, 2)
	if agg.Checksum() == before {
		t.Error("checksum must change after delete")
	}
	empty := StateChecksum(nil)
	if agg.Checksum() != empty {
		t.Error("snapshot must be empty after delete")
	}
}

func waitForSent(t *testing.T, client *fakeStreamClient, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(client.sentEvents()) >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d sent events", n)
}
