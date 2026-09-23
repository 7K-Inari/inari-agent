package capability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Capability stream metrics (issue #28): on CRD-heavy clusters the send path
// must be observable — how many updates go out, how large batches are.
var (
	capabilityUpdatesTotal = promauto.With(metrics.Registry).NewCounterVec(prometheus.CounterOpts{
		Namespace: "inari_agent",
		Subsystem: "capability",
		Name:      "updates_sent_total",
		Help:      "CapabilityUpdate events sent upstream, by sync kind (incremental, full).",
	}, []string{"sync"})

	capabilityUpdateBatchSize = promauto.With(metrics.Registry).NewHistogram(prometheus.HistogramOpts{
		Namespace: "inari_agent",
		Subsystem: "capability",
		Name:      "update_batch_size",
		Help:      "Number of capabilities per CapabilityUpdate event (burst coalescing).",
		Buckets:   []float64{1, 2, 5, 10, 25, 64, 128, 256},
	})
)
