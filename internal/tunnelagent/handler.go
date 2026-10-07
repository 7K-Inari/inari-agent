package tunnelagent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	tunnelv2 "github.com/7K-Inari/inari-api/gen/go/inari/tunnel/v2"
)

const (
	// DefaultFrameSize is the max payload per TunnelFrame (plan §8).
	DefaultFrameSize = 32 * 1024
	// DefaultMaxConnBytes caps total bytes relayed per connection
	// (both directions) — no windowing in v1, so an unbounded stream is
	// terminated instead of throttled.
	DefaultMaxConnBytes = 1 << 30 // 1 GiB
	// DefaultMaxConnLifetime matches the hub-side connection reaper.
	DefaultMaxConnLifetime = 60 * time.Minute
)

// Doer executes a proxied request; *Relay implements it (tests stub it).
type Doer interface {
	Do(ctx context.Context, open *tunnelv2.TunnelOpen, body io.Reader) (*Result, error)
}

// Handler owns the per-connection open/frame/close lifecycle on the agent
// side of the tunnel (plan §8). It is fed TunnelMessages by the stream
// client and emits responses through Send.
type Handler struct {
	Relay Doer
	// Send enqueues a message toward kubeproxy. The stream client installs
	// it per session; between sessions Send drops messages (connections are
	// reset on disconnect anyway).
	Send   func(msg *tunnelv2.TunnelMessage)
	Logger *slog.Logger

	FrameSize       int
	MaxConnBytes    int64
	MaxConnLifetime time.Duration

	mu    sync.Mutex
	conns map[string]*connState
}

type connState struct {
	id     string
	cancel context.CancelFunc
	in     chan *tunnelv2.TunnelFrame
	done   chan struct{}
	bytes  atomic.Int64
}

// NewHandler builds a Handler around the given relay.
func NewHandler(relay Doer) *Handler {
	return &Handler{Relay: relay, conns: map[string]*connState{}}
}

// Handle dispatches one inbound TunnelMessage from kubeproxy.
func (h *Handler) Handle(msg *tunnelv2.TunnelMessage) {
	switch p := msg.GetPayload().(type) {
	case *tunnelv2.TunnelMessage_Open:
		h.openConn(msg.GetConnectionId(), p.Open)
	case *tunnelv2.TunnelMessage_Frame:
		h.deliverFrame(msg.GetConnectionId(), p.Frame)
	case *tunnelv2.TunnelMessage_Close:
		h.closeFromHub(msg.GetConnectionId(), p.Close.GetReason())
	default:
		// OpenResult/Ping are agent→hub only; ignore strays.
	}
}

// ResetAll terminates every tracked connection without notifying the hub
// (the stream that would carry the Close is gone — used on reconnect and
// shutdown). Per-connection cleanup is idempotent.
func (h *Handler) ResetAll(reason string) {
	h.mu.Lock()
	conns := make([]*connState, 0, len(h.conns))
	for _, cs := range h.conns {
		conns = append(conns, cs)
	}
	h.mu.Unlock()
	for _, cs := range conns {
		cs.cancel()
		<-cs.done
	}
	if len(conns) > 0 {
		h.logger().Info("tunnel connections reset", "count", len(conns), "reason", reason)
	}
}

// ActiveConns reports the number of live connections (tests, metrics).
func (h *Handler) ActiveConns() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.conns)
}

func (h *Handler) openConn(id string, open *tunnelv2.TunnelOpen) {
	if id == "" || open == nil {
		return
	}
	lifetime := h.MaxConnLifetime
	if lifetime <= 0 {
		lifetime = DefaultMaxConnLifetime
	}
	ctx, cancel := context.WithTimeout(context.Background(), lifetime)
	cs := &connState{
		id:     id,
		cancel: cancel,
		in:     make(chan *tunnelv2.TunnelFrame, 64),
		done:   make(chan struct{}),
	}
	h.mu.Lock()
	if _, exists := h.conns[id]; exists {
		h.mu.Unlock()
		cancel()
		h.logger().Warn("duplicate tunnel open for connection id, ignoring", "connID", id)
		return
	}
	h.conns[id] = cs
	h.mu.Unlock()
	go h.runConn(ctx, cs, open)
}

func (h *Handler) deliverFrame(id string, f *tunnelv2.TunnelFrame) {
	h.mu.Lock()
	cs := h.conns[id]
	h.mu.Unlock()
	if cs == nil || f == nil {
		return
	}
	select {
	case cs.in <- f:
	case <-cs.done:
	default:
		// Slow consumer (apiserver not reading the request body): blocking
		// here would stall the stream's single receive loop for every
		// other connection — terminate this connection instead.
		h.logger().Warn("inbound frame queue full, closing connection", "connID", id)
		h.send(cs, &tunnelv2.TunnelMessage{ConnectionId: id, Payload: &tunnelv2.TunnelMessage_Close{
			Close: &tunnelv2.TunnelClose{Reason: "inbound-overflow"},
		}})
		cs.cancel()
	}
}

func (h *Handler) closeFromHub(id, reason string) {
	h.mu.Lock()
	cs := h.conns[id]
	h.mu.Unlock()
	if cs == nil {
		return
	}
	h.logger().Debug("tunnel connection closed by hub", "connID", id, "reason", reason)
	cs.cancel()
	// runConn unregisters; no Close echoed back to the hub.
}

// unregister removes cs from the registry and closes done.
func (h *Handler) unregister(cs *connState) {
	h.mu.Lock()
	delete(h.conns, cs.id)
	h.mu.Unlock()
	close(cs.done)
}

// runConn owns one proxied connection: request body pump → relay →
// OpenResult → body→frame pump → terminal Close.
func (h *Handler) runConn(ctx context.Context, cs *connState, open *tunnelv2.TunnelOpen) {
	defer h.unregister(cs)
	defer cs.cancel()

	pr, pw := io.Pipe()
	defer func() { _ = pr.Close() }()

	// Inbound pump: hub frames → request body (pre-upgrade) or the raw
	// upgraded connection (post-upgrade). writer swaps targets under mu.
	//
	// Bodiless methods (GET et al.) discard inbound frames instead of
	// feeding a body pipe: an http.Transport RoundTrip with a streaming
	// body that never reaches EOF can block past the response head, and
	// kubectl never bodies a GET.
	w := &connWriter{w: pw, pipe: pw}
	var reqBody io.Reader = pr
	if !methodAllowsBody(open.GetMethod()) {
		reqBody = nil
		_ = pr.Close()
		w = &connWriter{w: io.Discard}
	}
	go h.inboundPump(ctx, cs, w)

	res, err := h.Relay.Do(ctx, open, reqBody)
	if err != nil {
		h.logger().Warn("tunnel relay open failed", "connID", cs.id, "error", err)
		h.send(cs, &tunnelv2.TunnelMessage{ConnectionId: cs.id, Payload: &tunnelv2.TunnelMessage_OpenResult{
			OpenResult: &tunnelv2.TunnelOpenResult{Error: err.Error()},
		}})
		h.send(cs, &tunnelv2.TunnelMessage{ConnectionId: cs.id, Payload: &tunnelv2.TunnelMessage_Close{
			Close: &tunnelv2.TunnelClose{Reason: "open-failed"},
		}})
		return
	}
	defer func() { _ = res.Body.Close() }()
	// Unblock a socket read on the upgraded/raw body when the connection
	// is torn down (hub close, byte cap, lifetime, stream reset): ctx
	// cancel alone does not interrupt a raw net.Conn read.
	go func() {
		<-ctx.Done()
		_ = res.Body.Close()
	}()

	h.send(cs, &tunnelv2.TunnelMessage{ConnectionId: cs.id, Payload: &tunnelv2.TunnelMessage_OpenResult{
		OpenResult: &tunnelv2.TunnelOpenResult{
			Status:  int32(res.Status),
			Headers: headerMap(res.Header),
		},
	}})

	if res.Upgraded != nil {
		w.upgradeTo(res.Upgraded)
	}

	// Outbound pump: response body / upgraded socket → frames.
	if reason := h.outboundPump(ctx, cs, res.Body); reason != "" {
		h.send(cs, &tunnelv2.TunnelMessage{ConnectionId: cs.id, Payload: &tunnelv2.TunnelMessage_Close{
			Close: &tunnelv2.TunnelClose{Reason: reason},
		}})
	}
}

// inboundPump writes inbound frame data into the current target (request
// body pipe, then the upgraded connection). It ends on hub half-close,
// connection teardown, or byte-cap termination.
func (h *Handler) inboundPump(ctx context.Context, cs *connState, w *connWriter) {
	for {
		select {
		case <-ctx.Done():
			w.closePipe()
			return
		case <-cs.done:
			w.closePipe()
			return
		case f := <-cs.in:
			if len(f.GetData()) > 0 {
				if !h.chargeBytes(cs, int64(len(f.GetData()))) {
					w.closePipe()
					return
				}
				if _, err := w.Write(f.GetData()); err != nil {
					h.logger().Debug("inbound pump write failed", "connID", cs.id, "error", err)
					return
				}
			}
			if f.GetHalfClose() {
				w.halfClose()
				return
			}
		}
	}
}

// outboundPump streams the apiserver response into frames. It returns the
// terminal Close reason, or "" if the hub already closed the connection.
func (h *Handler) outboundPump(ctx context.Context, cs *connState, body io.Reader) string {
	frameSize := h.FrameSize
	if frameSize <= 0 {
		frameSize = DefaultFrameSize
	}
	buf := make([]byte, frameSize)
	for {
		n, err := body.Read(buf)
		if n > 0 {
			if !h.chargeBytes(cs, int64(n)) {
				return "" // cap path already sent Close{byte-cap-exceeded}
			}
			h.send(cs, &tunnelv2.TunnelMessage{ConnectionId: cs.id, Payload: &tunnelv2.TunnelMessage_Frame{
				Frame: &tunnelv2.TunnelFrame{Data: buf[:n]},
			}})
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return "complete"
			}
			if ctx.Err() != nil {
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					return "max-lifetime-exceeded"
				}
				return "" // hub closed / shutdown / reconnect
			}
			h.logger().Debug("outbound pump read failed", "connID", cs.id, "error", err)
			return "read-error"
		}
	}
}

// chargeBytes accounts relayed bytes against the per-connection cap. On
// exceed it terminates the connection and reports false.
func (h *Handler) chargeBytes(cs *connState, n int64) bool {
	maxBytes := h.MaxConnBytes
	if maxBytes <= 0 {
		maxBytes = DefaultMaxConnBytes
	}
	if cs.bytes.Add(n) <= maxBytes {
		return true
	}
	h.logger().Warn("tunnel connection byte cap exceeded, closing", "connID", cs.id, "cap", maxBytes)
	h.send(cs, &tunnelv2.TunnelMessage{ConnectionId: cs.id, Payload: &tunnelv2.TunnelMessage_Close{
		Close: &tunnelv2.TunnelClose{Reason: "byte-cap-exceeded"},
	}})
	cs.cancel()
	return false
}

func (h *Handler) send(cs *connState, msg *tunnelv2.TunnelMessage) {
	h.mu.Lock()
	send := h.Send
	h.mu.Unlock()
	if send != nil {
		send(msg)
	}
}

// SetSend installs the per-session sender (nil between sessions).
func (h *Handler) SetSend(send func(msg *tunnelv2.TunnelMessage)) {
	h.mu.Lock()
	h.Send = send
	h.mu.Unlock()
}

func (h *Handler) logger() *slog.Logger {
	if h.Logger != nil {
		return h.Logger
	}
	return slog.Default()
}

// connWriter serializes inbound frame writes across the pre-upgrade
// (request body pipe) and post-upgrade (raw connection) targets.
type connWriter struct {
	mu         sync.Mutex
	w          io.Writer
	pipe       *io.PipeWriter
	pipeClosed bool
}

func (cw *connWriter) Write(p []byte) (int, error) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.w.Write(p)
}

// upgradeTo switches the write target to the upgraded connection and closes
// the request body pipe (upgrade requests carry no further body).
func (cw *connWriter) upgradeTo(w io.Writer) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.closePipeLocked()
	cw.w = w
}

// halfClose propagates a hub half-close: EOF on the request body pipe, or a
// write-side shutdown on the upgraded connection when supported.
func (cw *connWriter) halfClose() {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.closePipeLocked()
	type closeWriter interface{ CloseWrite() error }
	if w, ok := cw.w.(closeWriter); ok {
		_ = w.CloseWrite()
	}
}

func (cw *connWriter) closePipe() {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.closePipeLocked()
}

func (cw *connWriter) closePipeLocked() {
	if !cw.pipeClosed && cw.pipe != nil {
		cw.pipeClosed = true
		_ = cw.pipe.Close()
	}
}

// methodAllowsBody reports whether inbound frames are streamed as a request
// body for this method. GET/HEAD/OPTIONS/TRACE never carry one here.
func methodAllowsBody(method string) bool {
	switch method {
	case "POST", "PUT", "PATCH", "DELETE":
		return true
	default:
		return false
	}
}

// headerMap carries response headers for TunnelOpenResult with all
// values preserved (multi-valued contract: Set-Cookie must not join).
func headerMap(h http.Header) map[string]*tunnelv2.StringList {
	out := make(map[string]*tunnelv2.StringList, len(h))
	for k, vs := range h {
		out[k] = &tunnelv2.StringList{Values: vs}
	}
	return out
}
