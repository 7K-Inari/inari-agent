package tunnelagent

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	tunnelv1 "github.com/7K-Inari/inari-api/gen/go/inari/tunnel/v1"
	"github.com/7K-Inari/inari-api/gen/go/inari/tunnel/v1/tunnelv1connect"

	"github.com/7K-Inari/inari-agent/internal/stream"
)

// fakeKubeproxy is an in-process TunnelService harness: it records the
// agent's bearer token and pings, and lets the test drive Open/Frame/Close
// toward the agent and read OpenResult/Frame/Close back.
type fakeKubeproxy struct {
	tunnelv1connect.UnimplementedTunnelServiceHandler

	sessions  atomic.Int32
	pings     atomic.Int32
	authToken chan string

	mu      sync.Mutex
	send    func(*tunnelv1.TunnelMessage) error
	inbound chan *tunnelv1.TunnelMessage // OpenResult/Frame/Close from the agent
	// onSession, when set, runs inside the Connect handler before the
	// receive loop; a non-nil return ends the session (reconnect test).
	onSession func() error
}

func (f *fakeKubeproxy) Connect(ctx context.Context, s *connect.BidiStream[tunnelv1.TunnelMessage, tunnelv1.TunnelMessage]) error {
	n := f.sessions.Add(1)
	if n == 1 {
		f.authToken <- s.RequestHeader().Get("Authorization")
	}
	f.mu.Lock()
	f.send = s.Send
	f.mu.Unlock()
	if f.onSession != nil {
		if err := f.onSession(); err != nil {
			return err
		}
	}
	for {
		msg, err := s.Receive()
		if err != nil {
			return err
		}
		if msg.GetPing() != nil {
			f.pings.Add(1)
			continue
		}
		select {
		case f.inbound <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (f *fakeKubeproxy) sendToAgent(t *testing.T, m *tunnelv1.TunnelMessage) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		send := f.send
		f.mu.Unlock()
		if send != nil {
			if err := send(m); err != nil {
				t.Fatalf("send to agent: %v", err)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no active session to send on")
}

func (f *fakeKubeproxy) receive(t *testing.T, what string) *tunnelv1.TunnelMessage {
	t.Helper()
	select {
	case m := <-f.inbound:
		return m
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

func newFakeKubeproxyServer(t *testing.T, f *fakeKubeproxy) (addr string, cleanup func()) {
	t.Helper()
	mux := http.NewServeMux()
	path, handler := tunnelv1connect.NewTunnelServiceHandler(f)
	mux.Handle(path, handler)
	// Unencrypted HTTP/2 (h2c) via Protocols — h2c.NewHandler is deprecated.
	protocols := &http.Protocols{}
	protocols.SetUnencryptedHTTP2(true)
	srv := &http.Server{Handler: mux, Protocols: protocols}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	return "http://" + ln.Addr().String(), func() { _ = srv.Close() }
}

type staticToken struct{}

func (staticToken) Token(context.Context) (string, error) { return "test-jwt", nil }

func newFakeKubeproxy() *fakeKubeproxy {
	return &fakeKubeproxy{
		authToken: make(chan string, 1),
		inbound:   make(chan *tunnelv1.TunnelMessage, 64),
	}
}

func newTestClient(addr string, relay *Relay, kp *fakeKubeproxy) *Client {
	handler := NewHandler(relay)
	c := NewClient(addr, staticToken{}, handler)
	c.PingInterval = 20 * time.Millisecond
	c.Backoff = stream.Backoff{InitialInterval: 10 * time.Millisecond, MaxInterval: 50 * time.Millisecond, Multiplier: 2}
	return c
}

func TestTunnelEndToEnd(t *testing.T) {
	// Fake apiserver asserts the relay attaches the SA bearer token and
	// passes the hub-minted impersonation headers through verbatim.
	var gotAuth, gotUser string
	apiserver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotUser = r.Header.Get("Impersonate-User")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"PodList"}`))
	}))
	defer apiserver.Close()

	kp := newFakeKubeproxy()
	addr, cleanup := newFakeKubeproxyServer(t, kp)
	defer cleanup()

	client := newTestClient(addr, &Relay{BaseURL: apiserver.URL, BearerToken: "sa-token-it"}, kp)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = client.Run(ctx) }()

	// The stream is authenticated with the client-credentials JWT.
	select {
	case auth := <-kp.authToken:
		if auth != "Bearer test-jwt" {
			t.Errorf("stream Authorization = %q", auth)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no session established")
	}

	kp.sendToAgent(t, &tunnelv1.TunnelMessage{ConnectionId: "it-1", Payload: &tunnelv1.TunnelMessage_Open{
		Open: &tunnelv1.TunnelOpen{
			Method: "GET",
			Path:   "/api/v1/pods",
			Headers: map[string]string{
				"Impersonate-User":  "alice@example.com",
				"Impersonate-Group": "devs",
			},
		},
	}})

	or := kp.receive(t, "OpenResult")
	if or.GetConnectionId() != "it-1" || or.GetOpenResult().GetStatus() != 200 {
		t.Fatalf("OpenResult = %v", or.GetOpenResult())
	}
	if or.GetOpenResult().GetHeaders()["Content-Type"] != "application/json" {
		t.Errorf("OpenResult headers = %v", or.GetOpenResult().GetHeaders())
	}
	frame := kp.receive(t, "response frame")
	if string(frame.GetFrame().GetData()) != `{"kind":"PodList"}` {
		t.Errorf("frame = %q", frame.GetFrame().GetData())
	}
	cls := kp.receive(t, "Close")
	if cls.GetClose().GetReason() != "complete" {
		t.Errorf("close reason = %q", cls.GetClose().GetReason())
	}
	if gotAuth != "Bearer sa-token-it" || gotUser != "alice@example.com" {
		t.Errorf("apiserver saw auth=%q user=%q", gotAuth, gotUser)
	}

	// Session keepalive pings flow without a connection id.
	deadline := time.Now().Add(2 * time.Second)
	for kp.pings.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if kp.pings.Load() == 0 {
		t.Error("no TunnelPing received")
	}
}

func TestTunnelEndToEndUpgrade(t *testing.T) {
	// Fake apiserver: raw listener completing a 101 handshake then echoing.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		br := bufio.NewReader(conn)
		req, err := http.ReadRequest(br)
		if err != nil {
			return
		}
		if req.Header.Get("Impersonate-User") != "alice@example.com" {
			t.Errorf("upgrade Impersonate-User = %q", req.Header.Get("Impersonate-User"))
		}
		_, _ = conn.Write([]byte("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n"))
		_, _ = io.Copy(conn, br)
	}()

	kp := newFakeKubeproxy()
	addr, cleanup := newFakeKubeproxyServer(t, kp)
	defer cleanup()

	client := newTestClient(addr, &Relay{BaseURL: "http://" + ln.Addr().String(), BearerToken: "sa"}, kp)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = client.Run(ctx) }()

	select {
	case <-kp.authToken:
	case <-time.After(5 * time.Second):
		t.Fatal("no session established")
	}

	kp.sendToAgent(t, &tunnelv1.TunnelMessage{ConnectionId: "up-1", Payload: &tunnelv1.TunnelMessage_Open{
		Open: &tunnelv1.TunnelOpen{
			Method: "GET",
			Path:   "/api/v1/namespaces/default/pods/p/exec?command=sh&stdin=true",
			Headers: map[string]string{
				"Connection":       "Upgrade",
				"Upgrade":          "websocket",
				"Impersonate-User": "alice@example.com",
			},
			UpgradeExpected: true,
		},
	}})

	or := kp.receive(t, "upgrade OpenResult")
	if or.GetOpenResult().GetStatus() != 101 {
		t.Fatalf("upgrade OpenResult status = %d", or.GetOpenResult().GetStatus())
	}

	// Bytes framed to the agent must be echoed back over the raw splice.
	kp.sendToAgent(t, &tunnelv1.TunnelMessage{ConnectionId: "up-1", Payload: &tunnelv1.TunnelMessage_Frame{
		Frame: &tunnelv1.TunnelFrame{Data: []byte("stream-stdin-bytes")},
	}})
	frame := kp.receive(t, "upgraded echo frame")
	if string(frame.GetFrame().GetData()) != "stream-stdin-bytes" {
		t.Errorf("echo = %q", frame.GetFrame().GetData())
	}

	// Hub closes; the agent tears the connection down without echoing Close.
	kp.sendToAgent(t, &tunnelv1.TunnelMessage{ConnectionId: "up-1", Payload: &tunnelv1.TunnelMessage_Close{
		Close: &tunnelv1.TunnelClose{Reason: "kubectl-exit"},
	}})
	deadline := time.Now().Add(2 * time.Second)
	for client.Handler.ActiveConns() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := client.Handler.ActiveConns(); n != 0 {
		t.Errorf("active conns after hub close = %d", n)
	}
}

func TestTunnelReconnectAfterStreamDrop(t *testing.T) {
	kp := newFakeKubeproxy()
	// Drop every session almost immediately; the client must keep
	// reconnecting with backoff.
	kp.onSession = func() error {
		time.Sleep(20 * time.Millisecond)
		return connect.NewError(connect.CodeUnavailable, io.ErrUnexpectedEOF)
	}
	addr, cleanup := newFakeKubeproxyServer(t, kp)
	defer cleanup()

	client := newTestClient(addr, &Relay{BaseURL: "http://127.0.0.1:1", BearerToken: "sa"}, kp)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = client.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for kp.sessions.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := kp.sessions.Load(); n < 2 {
		t.Fatalf("expected reconnect after stream drop, sessions = %d", n)
	}
}
