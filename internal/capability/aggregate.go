package capability

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/stream"
)

// Aggregator fans watcher events in, maintains the full capability snapshot
// (the source of truth for the state checksum and resync replays), and
// streams capability-update events upstream (plan §5.3 step "Discover",
// §5.2 checksum resync).
type Aggregator struct {
	TenantID  string
	ClusterID string
	Client    stream.Client
	Watchers  []Watcher

	// BatchWindow coalesces watcher bursts into one CapabilityUpdate
	// (default 100ms); MaxBatch caps batch size (default 64). On CRD-heavy
	// clusters per-event sends keep the send path saturated (issue #28).
	BatchWindow time.Duration
	MaxBatch    int

	mu       sync.Mutex
	snapshot map[string]*agentv1.Capability
	digests  map[string][]byte // per-capability digest cache (incremental checksum)
}

// NewAggregator builds an Aggregator over the given watchers.
func NewAggregator(tenantID, clusterID string, client stream.Client, watchers []Watcher) *Aggregator {
	return &Aggregator{
		TenantID:    tenantID,
		ClusterID:   clusterID,
		Client:      client,
		Watchers:    watchers,
		BatchWindow: 100 * time.Millisecond,
		MaxBatch:    64,
		snapshot:    map[string]*agentv1.Capability{},
		digests:     map[string][]byte{},
	}
}

// Checksum returns the current full-state checksum (handshake input). It is
// incremental: per-capability digests are cached in apply, so this is O(N)
// hashing without re-marshalling the snapshot.
func (a *Aggregator) Checksum() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return ChecksumDigests(a.digests)
}

// Run starts all watchers and pumps capability updates to the stream until
// ctx is cancelled. Resync handling is driven externally via
// ReplayFullState (the lifecycle owns the stream event channel).
func (a *Aggregator) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	merged := make(chan *agentv1.Capability, 128)
	var wg sync.WaitGroup
	for _, w := range a.Watchers {
		ch, err := w.Start(ctx)
		if err != nil {
			return fmt.Errorf("aggregator: start %s watcher: %w", w.Source(), err)
		}
		wg.Add(1)
		go func(ch <-chan *agentv1.Capability) {
			defer wg.Done()
			for cap := range ch {
				select {
				case merged <- cap:
				case <-ctx.Done():
					return
				}
			}
		}(ch)
	}
	go func() {
		wg.Wait()
		close(merged)
	}()

	batchWindow := a.BatchWindow
	if batchWindow <= 0 {
		batchWindow = 100 * time.Millisecond
	}
	maxBatch := a.MaxBatch
	if maxBatch <= 0 {
		maxBatch = 64
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case cap, ok := <-merged:
			if !ok {
				return nil
			}
			// Coalesce the burst: keep draining for a short window so one
			// CapabilityUpdate (one proto marshal + one checksum) covers many
			// watcher events instead of one per event (issue #28).
			batch := []*agentv1.Capability{cap}
			a.apply(cap)
			timer := time.NewTimer(batchWindow)
		drain:
			for len(batch) < maxBatch {
				select {
				case next, ok := <-merged:
					if !ok {
						break drain
					}
					a.apply(next)
					batch = append(batch, next)
				case <-timer.C:
					break drain
				case <-ctx.Done():
					timer.Stop()
					return nil
				}
			}
			timer.Stop()
			if err := a.send(ctx, batch, false); err != nil {
				return err
			}
		}
	}
}

// apply updates the snapshot (upsert or delete). A DELETE with an empty
// Group (metadata-only delete event, where the spec is already gone)
// matches by kind+name.
func (a *Aggregator) apply(cap *agentv1.Capability) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := capabilityKey(cap)
	if cap.Action == agentv1.CapabilityAction_CAPABILITY_ACTION_DELETE {
		delete(a.snapshot, key)
		delete(a.digests, key)
		if cap.Group == "" {
			prefix := cap.Kind.String() + "/"
			suffix := "/" + cap.Name + "/"
			for k := range a.snapshot {
				if strings.HasPrefix(k, prefix) && strings.Contains(k, suffix) {
					delete(a.snapshot, k)
					delete(a.digests, k)
				}
			}
		}
		return
	}
	a.snapshot[key] = cap
	if sum, ok := DigestCapability(cap); ok {
		a.digests[key] = sum
	}
}

// ReplayFullState re-sends the entire snapshot as a full sync. Called by
// the lifecycle when the gateway requests resync (checksum mismatch or
// sequence gap, plan §5.2); the gateway dedupes by checksum, so replays
// produce no duplicates in the catalog projection.
func (a *Aggregator) ReplayFullState(ctx context.Context) error {
	a.mu.Lock()
	caps := make([]*agentv1.Capability, 0, len(a.snapshot))
	for _, c := range a.snapshot {
		caps = append(caps, c)
	}
	a.mu.Unlock()
	return a.send(ctx, caps, true)
}

func (a *Aggregator) send(ctx context.Context, caps []*agentv1.Capability, fullSync bool) error {
	if a.Client == nil || len(caps) == 0 && !fullSync {
		return nil
	}
	payload, err := anypb.New(&agentv1.CapabilityUpdate{
		FullSync:      fullSync,
		Capabilities:  caps,
		StateChecksum: a.Checksum(),
	})
	if err != nil {
		return fmt.Errorf("aggregator: marshal capability update: %w", err)
	}
	syncKind := "incremental"
	if fullSync {
		syncKind = "full"
	}
	capabilityUpdatesTotal.WithLabelValues(syncKind).Inc()
	capabilityUpdateBatchSize.Observe(float64(len(caps)))
	return a.Client.Send(ctx, &agentv1.Event{
		EventId:    fmt.Sprintf("cap-%s-%d", a.ClusterID, timestamppb.Now().AsTime().UnixNano()),
		ResourceId: a.ClusterID,
		Type:       agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_CAPABILITY_UPDATE),
		Payload:    payload,
	})
}
