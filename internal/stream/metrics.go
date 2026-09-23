package stream

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Stream lifecycle metrics (issue #28): the agent must be able to tell from
// metrics alone why sessions end (dead-man switch, recv error, token
// rotation, ...) and how saturated the queues are on CRD-heavy clusters.
var (
	streamSessionsTotal = promauto.With(metrics.Registry).NewCounterVec(prometheus.CounterOpts{
		Namespace: "inari_agent",
		Subsystem: "stream",
		Name:      "sessions_total",
		Help:      "Stream sessions ended, by termination cause (deadman, recv_error, send_error, token_rotation, handshake_error, shutdown).",
	}, []string{"cause"})

	streamSessionDuration = promauto.With(metrics.Registry).NewHistogram(prometheus.HistogramOpts{
		Namespace: "inari_agent",
		Subsystem: "stream",
		Name:      "session_duration_seconds",
		Help:      "Lifetime of one stream session (connect → termination).",
		Buckets:   []float64{1, 5, 30, 60, 120, 300, 600, 1800, 3600},
	})

	streamSendQueueDepth = promauto.With(metrics.Registry).NewGauge(prometheus.GaugeOpts{
		Namespace: "inari_agent",
		Subsystem: "stream",
		Name:      "send_queue_depth",
		Help:      "Current depth of the outbound event queue (backlog toward the gateway).",
	})

	streamEventsQueueDepth = promauto.With(metrics.Registry).NewGauge(prometheus.GaugeOpts{
		Namespace: "inari_agent",
		Subsystem: "stream",
		Name:      "events_queue_depth",
		Help:      "Current depth of the inbound event queue (backlog toward the controller).",
	})
)
