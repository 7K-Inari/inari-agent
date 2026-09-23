package capability

import (
	"context"
	"fmt"
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
	waitForCapabilities(t, client, 2)

	resyncPayload, _ := anypb.New(&agentv1.Event{})
	_ = resyncPayload
	if err := agg.ReplayFullState(ctx); err != nil {
		t.Fatalf("ReplayFullState: %v", err)
	}
	waitForCapabilities(t, client, 4)

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

func TestAggregatorChecksumMatchesFullRecompute(t *testing.T) {
	w := &fakeWatcher{source: SourceCRD, ch: make(chan *agentv1.Capability, 4)}
	client := &fakeStreamClient{events: make(chan *agentv1.Event, 1)}
	agg := NewAggregator("tenant-1", "cluster-1", client, []Watcher{w})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = agg.Run(ctx) }()

	caps := []*agentv1.Capability{
		{Kind: agentv1.CapabilityKind_CAPABILITY_KIND_CRD, Group: "a.io", Name: "a", Version: "v1"},
		{Kind: agentv1.CapabilityKind_CAPABILITY_KIND_CRD, Group: "b.io", Name: "b", Version: "v1"},
		{Kind: agentv1.CapabilityKind_CAPABILITY_KIND_HELM_RELEASE, Name: "rel", Version: "1.2.3"},
	}
	for _, c := range caps {
		w.ch <- c
	}
	waitForCapabilities(t, client, 3)
	if got, want := agg.Checksum(), StateChecksum(caps); got != want {
		t.Errorf("incremental checksum %q != full recompute %q", got, want)
	}

	// Update one capability: checksum must still match a full recompute.
	// (The snapshot key includes Version, so a version bump is an upsert of
	// a new entry alongside the old one — same semantics as before.)
	updated := &agentv1.Capability{Kind: agentv1.CapabilityKind_CAPABILITY_KIND_CRD, Group: "a.io", Name: "a", Version: "v2"}
	w.ch <- updated
	waitForCapabilities(t, client, 4)
	if got, want := agg.Checksum(), StateChecksum(append(caps, updated)); got != want {
		t.Errorf("after update: incremental checksum %q != full recompute %q", got, want)
	}
}

// waitForCapabilities waits until at least n capabilities have been sent
// across all updates (updates may be batched).
func waitForCapabilities(t *testing.T, client *fakeStreamClient, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		total := 0
		for _, u := range capabilityUpdates(t, client.sentEvents()) {
			total += len(u.Capabilities)
		}
		if total >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d sent capabilities", n)
}

func TestAggregatorCoalescesBursts(t *testing.T) {
	w := &fakeWatcher{source: SourceCRD, ch: make(chan *agentv1.Capability, 10)}
	client := &fakeStreamClient{events: make(chan *agentv1.Event, 1)}
	agg := NewAggregator("tenant-1", "cluster-1", client, []Watcher{w})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = agg.Run(ctx) }()

	for i := 0; i < 10; i++ {
		w.ch <- &agentv1.Capability{
			Kind: agentv1.CapabilityKind_CAPABILITY_KIND_CRD,
			Name: fmt.Sprintf("crd-%d", i), Version: "v1",
		}
	}
	waitForCapabilities(t, client, 10)

	// A back-to-back burst must be coalesced into fewer update events than
	// capabilities (one proto + one checksum per batch, not per event).
	if n := len(client.sentEvents()); n >= 10 {
		t.Errorf("burst of 10 capabilities produced %d update events; want coalescing", n)
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

func TestAggregatorMetadataOnlyDeleteKeepsChecksumConsistent(t *testing.T) {
	w := &fakeWatcher{source: SourceCRD, ch: make(chan *agentv1.Capability, 4)}
	client := &fakeStreamClient{events: make(chan *agentv1.Event, 1)}
	agg := NewAggregator("tenant-1", "cluster-1", client, []Watcher{w})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = agg.Run(ctx) }()

	keep := &agentv1.Capability{Kind: agentv1.CapabilityKind_CAPABILITY_KIND_CRD, Group: "b.io", Name: "keep", Version: "v1"}
	gone := &agentv1.Capability{Kind: agentv1.CapabilityKind_CAPABILITY_KIND_CRD, Group: "a.io", Name: "gone", Version: "v1"}
	w.ch <- keep
	w.ch <- gone
	waitForCapabilities(t, client, 2)

	// Metadata-only delete (empty Group): matches by kind+name across groups.
	w.ch <- &agentv1.Capability{
		Kind: agentv1.CapabilityKind_CAPABILITY_KIND_CRD, Name: "gone",
		Action: agentv1.CapabilityAction_CAPABILITY_ACTION_DELETE,
	}
	waitForCapabilities(t, client, 3)
	if got, want := agg.Checksum(), StateChecksum([]*agentv1.Capability{keep}); got != want {
		t.Errorf("after metadata-only delete: incremental checksum %q != full recompute %q", got, want)
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
