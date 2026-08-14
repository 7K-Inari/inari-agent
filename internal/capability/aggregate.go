package capability

import (
	"context"
	"fmt"
	"sync"

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

	mu       sync.Mutex
	snapshot map[string]*agentv1.Capability
}

// NewAggregator builds an Aggregator over the given watchers.
func NewAggregator(tenantID, clusterID string, client stream.Client, watchers []Watcher) *Aggregator {
	return &Aggregator{
		TenantID:  tenantID,
		ClusterID: clusterID,
		Client:    client,
		Watchers:  watchers,
		snapshot:  map[string]*agentv1.Capability{},
	}
}

// Checksum returns the current full-state checksum (handshake input).
func (a *Aggregator) Checksum() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	caps := make([]*agentv1.Capability, 0, len(a.snapshot))
	for _, c := range a.snapshot {
		caps = append(caps, c)
	}
	return StateChecksum(caps)
}

// Run starts all watchers and pumps capability updates to the stream until
// ctx is cancelled. On a resync request from the gateway it replays the
// full snapshot with full_sync=true; the gateway dedupes by checksum
// (at-least-once delivery, no duplicates in the catalog projection).
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

	var streamEvents <-chan *agentv1.Event
	if a.Client != nil {
		streamEvents = a.Client.Events()
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case cap, ok := <-merged:
			if !ok {
				return nil
			}
			a.apply(cap)
			if err := a.send(ctx, []*agentv1.Capability{cap}, false); err != nil {
				return err
			}
		case ev := <-streamEvents:
			if ev == nil {
				continue
			}
			if agentv1.EventTypeFromString(ev.Type) == agentv1.EventType_EVENT_TYPE_RESYNC_REQUEST {
				if err := a.replayFullState(ctx); err != nil {
					return err
				}
			}
		}
	}
}

// apply updates the snapshot (upsert or delete).
func (a *Aggregator) apply(cap *agentv1.Capability) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := capabilityKey(cap)
	if cap.Action == agentv1.CapabilityAction_CAPABILITY_ACTION_DELETE {
		delete(a.snapshot, key)
		return
	}
	a.snapshot[key] = cap
}

// replayFullState re-sends the entire snapshot as a full sync.
func (a *Aggregator) replayFullState(ctx context.Context) error {
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
	return a.Client.Send(ctx, &agentv1.Event{
		EventId:    fmt.Sprintf("cap-%s-%d", a.ClusterID, timestamppb.Now().AsTime().UnixNano()),
		ResourceId: a.ClusterID,
		Type:       agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_CAPABILITY_UPDATE),
		Payload:    payload,
	})
}
