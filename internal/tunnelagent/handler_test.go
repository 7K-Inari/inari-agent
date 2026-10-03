package tunnelagent

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	tunnelv1 "github.com/7K-Inari/inari-api/gen/go/inari/tunnel/v1"
)

// stubDoer returns canned relay results and records the request body it
// received (read fully before responding, to exercise inbound frames).
type stubDoer struct {
	t *testing.T
	// respond, when nil, defaults to a 200 with body "response-bytes".
	respond func(open *tunnelv1.TunnelOpen, body io.Reader) (*Result, error)
}

func (s *stubDoer) Do(ctx context.Context, open *tunnelv1.TunnelOpen, body io.Reader) (*Result, error) {
	if s.respond != nil {
		return s.respond(open, body)
	}
	return &Result{
		Status: 200,
		Header: nil,
		Body:   io.NopCloser(strings.NewReader("response-bytes")),
	}, nil
}

type capture struct {
	mu   sync.Mutex
	msgs []*tunnelv1.TunnelMessage
}

func (c *capture) send(m *tunnelv1.TunnelMessage) {
	c.mu.Lock()
	c.msgs = append(c.msgs, m)
	c.mu.Unlock()
}

func (c *capture) waitFor(t *testing.T, pred func(*tunnelv1.TunnelMessage) bool, what string) *tunnelv1.TunnelMessage {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		for _, m := range c.msgs {
			if pred(m) {
				c.mu.Unlock()
				return m
			}
		}
		c.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
	return nil
}

func openMsg(id string) *tunnelv1.TunnelMessage {
	return &tunnelv1.TunnelMessage{ConnectionId: id, Payload: &tunnelv1.TunnelMessage_Open{
		Open: &tunnelv1.TunnelOpen{Method: "GET", Path: "/api/v1/pods"},
	}}
}

func TestHandlerOpenFrameCloseLifecycle(t *testing.T) {
	cap := &capture{}
	h := NewHandler(&stubDoer{t: t})
	h.SetSend(cap.send)

	h.Handle(openMsg("c1"))

	or := cap.waitFor(t, func(m *tunnelv1.TunnelMessage) bool {
		return m.GetConnectionId() == "c1" && m.GetOpenResult() != nil
	}, "OpenResult")
	if or.GetOpenResult().GetStatus() != 200 {
		t.Errorf("OpenResult status = %d", or.GetOpenResult().GetStatus())
	}

	frame := cap.waitFor(t, func(m *tunnelv1.TunnelMessage) bool {
		return m.GetConnectionId() == "c1" && m.GetFrame() != nil
	}, "response frame")
	if string(frame.GetFrame().GetData()) != "response-bytes" {
		t.Errorf("frame data = %q", frame.GetFrame().GetData())
	}

	cls := cap.waitFor(t, func(m *tunnelv1.TunnelMessage) bool {
		return m.GetConnectionId() == "c1" && m.GetClose() != nil
	}, "Close")
	if cls.GetClose().GetReason() != "complete" {
		t.Errorf("close reason = %q", cls.GetClose().GetReason())
	}

	deadline := time.Now().Add(2 * time.Second)
	for h.ActiveConns() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if h.ActiveConns() != 0 {
		t.Errorf("connection not unregistered, active=%d", h.ActiveConns())
	}
}

func TestHandlerInboundFramesAndHalfClose(t *testing.T) {
	cap := &capture{}
	bodySeen := make(chan string, 1)
	h := NewHandler(&stubDoer{t: t, respond: func(open *tunnelv1.TunnelOpen, body io.Reader) (*Result, error) {
		b, _ := io.ReadAll(body) // blocks until half-close EOF
		bodySeen <- string(b)
		return &Result{Status: 200, Body: io.NopCloser(strings.NewReader("ok"))}, nil
	}})
	h.SetSend(cap.send)

	h.Handle(&tunnelv1.TunnelMessage{ConnectionId: "c2", Payload: &tunnelv1.TunnelMessage_Open{
		Open: &tunnelv1.TunnelOpen{Method: "POST", Path: "/api/v1/namespaces"},
	}})
	h.Handle(&tunnelv1.TunnelMessage{ConnectionId: "c2", Payload: &tunnelv1.TunnelMessage_Frame{
		Frame: &tunnelv1.TunnelFrame{Data: []byte("part1-")},
	}})
	h.Handle(&tunnelv1.TunnelMessage{ConnectionId: "c2", Payload: &tunnelv1.TunnelMessage_Frame{
		Frame: &tunnelv1.TunnelFrame{Data: []byte("part2"), HalfClose: true},
	}})

	select {
	case got := <-bodySeen:
		if got != "part1-part2" {
			t.Errorf("request body = %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for relay to receive half-closed body")
	}

	cap.waitFor(t, func(m *tunnelv1.TunnelMessage) bool {
		return m.GetConnectionId() == "c2" && m.GetClose() != nil
	}, "Close after half-close")
}

// A stalled consumer (apiserver never reads the request body) must not
// block the receive loop: once the per-connection inbound queue fills, the
// handler terminates just that connection.
func TestHandlerInboundQueueOverflow(t *testing.T) {
	cap := &capture{}
	h := NewHandler(&stubDoer{t: t, respond: func(open *tunnelv1.TunnelOpen, body io.Reader) (*Result, error) {
		pr, _ := io.Pipe() // response body never produces bytes either
		return &Result{Status: 200, Body: pr}, nil
	}})
	h.SetSend(cap.send)

	h.Handle(&tunnelv1.TunnelMessage{ConnectionId: "ov", Payload: &tunnelv1.TunnelMessage_Open{
		Open: &tunnelv1.TunnelOpen{Method: "POST", Path: "/api/v1/namespaces"},
	}})
	cap.waitFor(t, func(m *tunnelv1.TunnelMessage) bool {
		return m.GetConnectionId() == "ov" && m.GetOpenResult() != nil
	}, "OpenResult")

	// The relay never reads the body: the pump blocks on the first frame
	// and the queue (cap 64) fills; one more frame overflows it.
	for i := 0; i < 128; i++ {
		h.Handle(&tunnelv1.TunnelMessage{ConnectionId: "ov", Payload: &tunnelv1.TunnelMessage_Frame{
			Frame: &tunnelv1.TunnelFrame{Data: []byte("x")},
		}})
	}

	cls := cap.waitFor(t, func(m *tunnelv1.TunnelMessage) bool {
		return m.GetConnectionId() == "ov" && m.GetClose() != nil
	}, "inbound-overflow Close")
	if cls.GetClose().GetReason() != "inbound-overflow" {
		t.Errorf("close reason = %q", cls.GetClose().GetReason())
	}
	// Handle must keep accepting messages for other connections (receive
	// loop was never blocked).
	h.Handle(openMsg("other"))
	cap.waitFor(t, func(m *tunnelv1.TunnelMessage) bool {
		return m.GetConnectionId() == "other" && m.GetOpenResult() != nil
	}, "OpenResult for other conn")
}

func TestHandlerByteCap(t *testing.T) {
	cap := &capture{}
	big := strings.Repeat("x", 4096)
	h := NewHandler(&stubDoer{t: t, respond: func(open *tunnelv1.TunnelOpen, body io.Reader) (*Result, error) {
		return &Result{Status: 200, Body: io.NopCloser(strings.NewReader(big))}, nil
	}})
	h.MaxConnBytes = 1024
	h.SetSend(cap.send)

	h.Handle(openMsg("c3"))

	cls := cap.waitFor(t, func(m *tunnelv1.TunnelMessage) bool {
		return m.GetConnectionId() == "c3" && m.GetClose() != nil
	}, "byte-cap Close")
	if cls.GetClose().GetReason() != "byte-cap-exceeded" {
		t.Errorf("close reason = %q", cls.GetClose().GetReason())
	}
}

func TestHandlerOpenFailure(t *testing.T) {
	cap := &capture{}
	h := NewHandler(&stubDoer{t: t, respond: func(open *tunnelv1.TunnelOpen, body io.Reader) (*Result, error) {
		return nil, io.ErrUnexpectedEOF
	}})
	h.SetSend(cap.send)

	h.Handle(openMsg("c4"))

	or := cap.waitFor(t, func(m *tunnelv1.TunnelMessage) bool {
		return m.GetConnectionId() == "c4" && m.GetOpenResult() != nil
	}, "OpenResult error")
	if or.GetOpenResult().GetError() == "" {
		t.Errorf("expected OpenResult.Error to be set")
	}
	cap.waitFor(t, func(m *tunnelv1.TunnelMessage) bool {
		return m.GetConnectionId() == "c4" && m.GetClose() != nil
	}, "Close after open failure")
}

func TestHandlerHubCloseTerminatesConn(t *testing.T) {
	cap := &capture{}
	blocked := make(chan struct{})
	h := NewHandler(&stubDoer{t: t, respond: func(open *tunnelv1.TunnelOpen, body io.Reader) (*Result, error) {
		// Body that never produces bytes until closed.
		pr, pw := io.Pipe()
		go func() { <-blocked; _ = pw.Close() }()
		return &Result{Status: 200, Body: pr}, nil
	}})
	h.SetSend(cap.send)

	h.Handle(openMsg("c5"))
	cap.waitFor(t, func(m *tunnelv1.TunnelMessage) bool {
		return m.GetConnectionId() == "c5" && m.GetOpenResult() != nil
	}, "OpenResult")

	h.Handle(&tunnelv1.TunnelMessage{ConnectionId: "c5", Payload: &tunnelv1.TunnelMessage_Close{
		Close: &tunnelv1.TunnelClose{Reason: "client-gone"},
	}})

	deadline := time.Now().Add(2 * time.Second)
	for h.ActiveConns() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if h.ActiveConns() != 0 {
		t.Fatalf("hub close did not terminate connection, active=%d", h.ActiveConns())
	}
	close(blocked)

	// Hub-initiated close must not echo a Close back.
	cap.mu.Lock()
	defer cap.mu.Unlock()
	for _, m := range cap.msgs {
		if m.GetConnectionId() == "c5" && m.GetClose() != nil {
			t.Errorf("agent echoed Close for hub-initiated close")
		}
	}
}

func TestHandlerConcurrentConnsIsolated(t *testing.T) {
	cap := &capture{}
	h := NewHandler(&stubDoer{t: t, respond: func(open *tunnelv1.TunnelOpen, body io.Reader) (*Result, error) {
		return &Result{Status: 200, Body: io.NopCloser(strings.NewReader(open.Path))}, nil
	}})
	h.SetSend(cap.send)

	h.Handle(&tunnelv1.TunnelMessage{ConnectionId: "a", Payload: &tunnelv1.TunnelMessage_Open{
		Open: &tunnelv1.TunnelOpen{Method: "GET", Path: "/conn-a"},
	}})
	h.Handle(&tunnelv1.TunnelMessage{ConnectionId: "b", Payload: &tunnelv1.TunnelMessage_Open{
		Open: &tunnelv1.TunnelOpen{Method: "GET", Path: "/conn-b"},
	}})

	fa := cap.waitFor(t, func(m *tunnelv1.TunnelMessage) bool {
		return m.GetConnectionId() == "a" && m.GetFrame() != nil
	}, "frame for conn a")
	fb := cap.waitFor(t, func(m *tunnelv1.TunnelMessage) bool {
		return m.GetConnectionId() == "b" && m.GetFrame() != nil
	}, "frame for conn b")
	if string(fa.GetFrame().GetData()) != "/conn-a" || string(fb.GetFrame().GetData()) != "/conn-b" {
		t.Errorf("frames crossed: a=%q b=%q", fa.GetFrame().GetData(), fb.GetFrame().GetData())
	}
}

func TestHandlerResetAll(t *testing.T) {
	cap := &capture{}
	h := NewHandler(&stubDoer{t: t, respond: func(open *tunnelv1.TunnelOpen, body io.Reader) (*Result, error) {
		pr, _ := io.Pipe() // never written: outbound pump blocks until reset
		return &Result{Status: 200, Body: pr}, nil
	}})
	h.SetSend(cap.send)

	h.Handle(openMsg("r1"))
	cap.waitFor(t, func(m *tunnelv1.TunnelMessage) bool {
		return m.GetConnectionId() == "r1" && m.GetOpenResult() != nil
	}, "OpenResult")

	done := make(chan struct{})
	go func() { h.ResetAll("stream-ended"); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ResetAll blocked on a stuck connection")
	}
	if h.ActiveConns() != 0 {
		t.Errorf("active conns after reset = %d", h.ActiveConns())
	}
}

func TestHandlerMaxLifetime(t *testing.T) {
	cap := &capture{}
	h := NewHandler(&stubDoer{t: t, respond: func(open *tunnelv1.TunnelOpen, body io.Reader) (*Result, error) {
		pr, _ := io.Pipe()
		return &Result{Status: 200, Body: pr}, nil
	}})
	h.MaxConnLifetime = 50 * time.Millisecond
	h.SetSend(cap.send)

	h.Handle(openMsg("l1"))

	cls := cap.waitFor(t, func(m *tunnelv1.TunnelMessage) bool {
		return m.GetConnectionId() == "l1" && m.GetClose() != nil
	}, "max-lifetime Close")
	if cls.GetClose().GetReason() != "max-lifetime-exceeded" {
		t.Errorf("close reason = %q", cls.GetClose().GetReason())
	}
}
