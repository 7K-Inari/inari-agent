package capability

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

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
	var fullSyncs []*agentv1.CapabilityUpdate
	for _, u := range updates {
		if u.FullSync {
			fullSyncs = append(fullSyncs, u)
		}
	}
	if len(fullSyncs) == 0 {
		t.Fatal("resync replay must send full-sync events")
	}
	names := map[string]int{}
	for _, u := range fullSyncs {
		for _, c := range u.Capabilities {
			names[c.Name]++
		}
	}
	if names["a"] != 1 || names["b"] != 1 {
		t.Errorf("full sync must replay each capability exactly once across chunks, got %v", names)
	}
	// The replay ends with an empty terminal chunk (the gateway's finalize
	// signal).
	if last := fullSyncs[len(fullSyncs)-1]; len(last.Capabilities) != 0 {
		t.Errorf("last full-sync event must be the empty terminal chunk, got %d capabilities", len(last.Capabilities))
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

func paddedCapability(t *testing.T, name string, pad int) *agentv1.Capability {
	t.Helper()
	s, err := structpb.NewStruct(map[string]interface{}{"pad": strings.Repeat("x", pad)})
	if err != nil {
		t.Fatal(err)
	}
	return &agentv1.Capability{
		Kind: agentv1.CapabilityKind_CAPABILITY_KIND_CRD,
		Name: name, Group: "example.com", Version: "v1", Schema: s,
	}
}

func chunkBytes(chunk []*agentv1.Capability) int {
	total := 0
	for _, c := range chunk {
		total += proto.Size(c)
	}
	return total
}

func TestSplitByByteBudget(t *testing.T) {
	caps := []*agentv1.Capability{
		paddedCapability(t, "a", 900), paddedCapability(t, "b", 900), paddedCapability(t, "c", 900),
	}
	chunks := splitByByteBudget(caps, 2000)
	if len(chunks) != 2 {
		t.Fatalf("chunks = %d, want 2 (a+b | c)", len(chunks))
	}
	if len(chunks[0]) != 2 || len(chunks[1]) != 1 {
		t.Fatalf("chunk sizes = %d/%d, want 2/1", len(chunks[0]), len(chunks[1]))
	}
	for i, ch := range chunks {
		if b := chunkBytes(ch); b > 2000 {
			t.Errorf("chunk %d = %d bytes, over the 2000 budget", i, b)
		}
	}

	// A capability larger than the budget cannot be split: it gets its own
	// chunk (the gateway limit leaves headroom for one oversize member).
	small := paddedCapability(t, "small", 10)
	huge := paddedCapability(t, "huge", 5000)
	chunks = splitByByteBudget([]*agentv1.Capability{small, huge, small}, 2000)
	if len(chunks) != 3 || len(chunks[1]) != 1 || chunks[1][0].Name != "huge" {
		t.Fatalf("oversize member must get its own chunk, got %d chunks", len(chunks))
	}

	if got := splitByByteBudget(nil, 2000); len(got) != 0 {
		t.Fatalf("nil input must produce no chunks, got %d", len(got))
	}
}

func TestAggregatorBatchesRespectByteBudget(t *testing.T) {
	w := &fakeWatcher{source: SourceCRD, ch: make(chan *agentv1.Capability, 6)}
	client := &fakeStreamClient{events: make(chan *agentv1.Event, 1)}
	agg := NewAggregator("tenant-1", "cluster-1", client, []Watcher{w})
	agg.MaxBatchBytes = 2500

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = agg.Run(ctx) }()

	for i := 0; i < 6; i++ {
		w.ch <- paddedCapability(t, fmt.Sprintf("crd-%d", i), 900)
	}
	waitForCapabilities(t, client, 6)

	updates := capabilityUpdates(t, client.sentEvents())
	if len(updates) < 2 {
		t.Fatalf("6x~1KiB capabilities under a 2500-byte budget must split, got %d update(s)", len(updates))
	}
	for i, u := range updates {
		if b := chunkBytes(u.Capabilities); b > 2500 {
			t.Errorf("update %d = %d bytes, over the 2500 budget", i, b)
		}
		if u.StateChecksum != agg.Checksum() {
			t.Errorf("update %d checksum must match the post-batch snapshot", i)
		}
	}
}

func TestAggregatorReplayFullStateChunksAndTerminates(t *testing.T) {
	w := &fakeWatcher{source: SourceCRD, ch: make(chan *agentv1.Capability, 6)}
	client := &fakeStreamClient{events: make(chan *agentv1.Event, 1)}
	agg := NewAggregator("tenant-1", "cluster-1", client, []Watcher{w})
	agg.MaxBatchBytes = 2500

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = agg.Run(ctx) }()

	names := map[string]bool{}
	for i := 0; i < 6; i++ {
		name := fmt.Sprintf("crd-%d", i)
		names[name] = true
		w.ch <- paddedCapability(t, name, 900)
	}
	waitForCapabilities(t, client, 6)

	if err := agg.ReplayFullState(ctx); err != nil {
		t.Fatalf("ReplayFullState: %v", err)
	}

	var full []*agentv1.CapabilityUpdate
	for _, u := range capabilityUpdates(t, client.sentEvents()) {
		if u.FullSync {
			full = append(full, u)
		}
	}
	if len(full) < 3 {
		t.Fatalf("6x~1KiB snapshot under a 2500-byte budget wants >= 3 data chunks + terminal, got %d full-sync events", len(full))
	}
	seen := map[string]int{}
	for i, u := range full {
		if u.StateChecksum != agg.Checksum() {
			t.Errorf("full-sync event %d checksum must match the snapshot", i)
		}
		if b := chunkBytes(u.Capabilities); b > 2500 {
			t.Errorf("full-sync chunk %d = %d bytes, over the 2500 budget", i, b)
		}
		for _, c := range u.Capabilities {
			seen[c.Name]++
		}
	}
	// Terminal chunk: empty, full-sync — the gateway's finalize signal.
	terminal := full[len(full)-1]
	if len(terminal.Capabilities) != 0 {
		t.Fatalf("last full-sync event must be the empty terminal chunk, got %d capabilities", len(terminal.Capabilities))
	}
	for _, u := range full[:len(full)-1] {
		if len(u.Capabilities) == 0 {
			t.Error("only the terminal chunk may be empty")
		}
	}
	for name := range names {
		if seen[name] != 1 {
			t.Errorf("capability %s replayed %d times, want exactly 1", name, seen[name])
		}
	}
}

func TestAggregatorReplayFullStateEmptySnapshotSendsTerminalChunk(t *testing.T) {
	client := &fakeStreamClient{events: make(chan *agentv1.Event)}
	agg := NewAggregator("tenant-1", "cluster-1", client, nil)

	if err := agg.ReplayFullState(context.Background()); err != nil {
		t.Fatalf("ReplayFullState: %v", err)
	}
	events := client.sentEvents()
	if len(events) != 1 {
		t.Fatalf("empty snapshot replay must send exactly the terminal chunk, got %d events", len(events))
	}
	updates := capabilityUpdates(t, events)
	if len(updates) != 1 || !updates[0].FullSync || len(updates[0].Capabilities) != 0 {
		t.Fatalf("want one empty full-sync terminal chunk, got %+v", updates)
	}
	if updates[0].StateChecksum != StateChecksum(nil) {
		t.Errorf("terminal checksum must be the empty-state checksum")
	}
}
