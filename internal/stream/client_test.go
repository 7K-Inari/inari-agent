package stream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/protobuf/types/known/anypb"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
	"github.com/7K-Inari/inari-api/gen/go/inari/agent/v1/agentv1connect"
)

type staticToken string

func (s staticToken) Token(context.Context) (string, error) { return string(s), nil }

// fakeGateway is an in-process Agent Gateway used to exercise the stream
// client over real HTTP.
type fakeGateway struct {
	agentv1connect.UnimplementedEventStreamServiceHandler

	mu           sync.Mutex
	connects     int
	gotAuth      []string
	gotEvents    []*agentv1.Event
	resyncNeeded bool
	// closeFirstAfter closes the first stream after this long (partition
	// simulation); later connections stay open.
	closeFirstAfter time.Duration

	pingOnce sync.Once
}

func (f *fakeGateway) recordConnect(auth string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connects++
	f.gotAuth = append(f.gotAuth, auth)
	return f.connects
}

func (f *fakeGateway) Connect(
	ctx context.Context,
	stream *connect.BidiStream[agentv1.ConnectRequest, agentv1.ConnectResponse],
) error {
	conn := f.recordConnect(stream.RequestHeader().Get("Authorization"))

	type recvResult struct {
		req *agentv1.ConnectRequest
		err error
	}
	recvCh := make(chan recvResult, 1)
	go func() {
		for {
			req, err := stream.Receive()
			recvCh <- recvResult{req, err}
			if err != nil {
				return
			}
		}
	}()

	var closeTimer <-chan time.Time
	if conn == 1 && f.closeFirstAfter > 0 {
		closeTimer = time.After(f.closeFirstAfter)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-closeTimer:
			return nil // simulated partition: server drops the stream
		case rr := <-recvCh:
			if rr.err != nil {
				return nil
			}
			if done := f.handle(stream, rr.req.Event); done {
				return nil
			}
		}
	}
}

// handle processes one inbound event. It returns true if the handler should
// exit.
func (f *fakeGateway) handle(stream *connect.BidiStream[agentv1.ConnectRequest, agentv1.ConnectResponse], ev *agentv1.Event) bool {
	if ev == nil {
		return false
	}
	if agentv1.EventTypeFromString(ev.Type) == agentv1.EventType_EVENT_TYPE_PONG {
		f.mu.Lock()
		f.gotEvents = append(f.gotEvents, ev)
		f.mu.Unlock()
		return false
	}
	var hs agentv1.HandshakeRequest
	if ev.Payload != nil && ev.Payload.MessageIs(&hs) {
		// Reply with the handshake response.
		resp, _ := anypb.New(&agentv1.HandshakeResponse{
			SessionId:              "session-1",
			ServerContractVersions: "inari.agent.v1",
			ResyncRequired:         f.resyncNeeded,
		})
		_ = stream.Send(&agentv1.ConnectResponse{Event: &agentv1.Event{
			EventId: "srv-handshake",
			Type:    "inari.agent.handshake.v1",
			Payload: resp,
		}})
		// Opportunistically send a ping right after the handshake.
		f.pingOnce.Do(func() {
			ping, _ := anypb.New(&agentv1.Ping{})
			_ = stream.Send(&agentv1.ConnectResponse{Event: &agentv1.Event{
				EventId: "srv-ping-1",
				Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_PING),
				Payload: ping,
			}})
		})
		return false
	}
	f.mu.Lock()
	f.gotEvents = append(f.gotEvents, ev)
	f.mu.Unlock()
	return false
}

func (f *fakeGateway) connectCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connects
}

func (f *fakeGateway) received() []*agentv1.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*agentv1.Event(nil), f.gotEvents...)
}

func newTestClient(t *testing.T, gw *fakeGateway, opts ...func(*ConnectClient)) *ConnectClient {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(agentv1connect.NewEventStreamServiceHandler(gw))
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = false // h2c: prior-knowledge HTTP/2 without TLS
	srv.Config.Handler = h2c.NewHandler(mux, &http2.Server{})
	srv.Start()
	t.Cleanup(srv.Close)

	c := &ConnectClient{
		Address:        srv.URL,
		HTTPClient:     DefaultHTTPClient(srv.URL),
		Token:          staticToken("test-jwt"),
		AgentVersion:   "0.1.0-test",
		TenantID:       "tenant-1",
		Checksum:       func() string { return "checksum-abc" },
		Backoff:        Backoff{InitialInterval: 10 * time.Millisecond, MaxInterval: 50 * time.Millisecond, Multiplier: 2},
		ReceiveTimeout: 2 * time.Second,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestHandshakeCarriesAuthTenantAndChecksum(t *testing.T) {
	gw := &fakeGateway{}
	c := newTestClient(t, gw)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	waitFor(t, "handshake", func() bool { return gw.connectCount() >= 1 })
	if got := gw.gotAuth[0]; got != "Bearer test-jwt" {
		t.Errorf("Authorization = %q", got)
	}
}

func TestSendDeliversEventWithSequence(t *testing.T) {
	gw := &fakeGateway{}
	c := newTestClient(t, gw)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()
	waitFor(t, "connect", func() bool { return gw.connectCount() >= 1 })

	payload, _ := anypb.New(&agentv1.CapabilityUpdate{StateChecksum: "checksum-abc"})
	if err := c.Send(ctx, &agentv1.Event{
		EventId: "evt-1",
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_CAPABILITY_UPDATE),
		Payload: payload,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	waitFor(t, "capability event", func() bool {
		for _, ev := range gw.received() {
			if ev.EventId == "evt-1" {
				return true
			}
		}
		return false
	})
	for _, ev := range gw.received() {
		if ev.EventId == "evt-1" && ev.Sequence <= 0 {
			t.Errorf("sequence not stamped: %d", ev.Sequence)
		}
	}
}

func TestPingIsAnsweredWithPong(t *testing.T) {
	gw := &fakeGateway{}
	c := newTestClient(t, gw)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	waitFor(t, "pong reply", func() bool {
		for _, ev := range gw.received() {
			if agentv1.EventTypeFromString(ev.Type) == agentv1.EventType_EVENT_TYPE_PONG {
				return true
			}
		}
		return false
	})
}

func TestReconnectAfterPartitionResendsHandshake(t *testing.T) {
	gw := &fakeGateway{closeFirstAfter: 100 * time.Millisecond}
	c := newTestClient(t, gw)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	waitFor(t, "reconnect after partition", func() bool { return gw.connectCount() >= 2 })
}

func TestResyncRequiredSurfacesResyncRequestEvent(t *testing.T) {
	gw := &fakeGateway{resyncNeeded: true}
	c := newTestClient(t, gw)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	select {
	case ev := <-c.Events():
		if agentv1.EventTypeFromString(ev.Type) != agentv1.EventType_EVENT_TYPE_RESYNC_REQUEST {
			t.Errorf("first event = %q, want resync-request", ev.Type)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no resync event surfaced")
	}
}

func TestOnConnectedChangeFiresOnConnectAndPartition(t *testing.T) {
	gw := &fakeGateway{closeFirstAfter: 100 * time.Millisecond}
	c := newTestClient(t, gw)

	var mu sync.Mutex
	var transitions []bool
	c.SetOnConnectedChange(func(v bool) {
		mu.Lock()
		transitions = append(transitions, v)
		mu.Unlock()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	waitFor(t, "reconnect after partition", func() bool { return gw.connectCount() >= 2 })
	waitFor(t, "connect/disconnect/connect transitions", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(transitions) >= 3
	})
	mu.Lock()
	defer mu.Unlock()
	if !transitions[0] || transitions[1] || !transitions[2] {
		t.Errorf("transitions = %v, want [true false true]", transitions)
	}
}

func TestEmitBackpressuresInsteadOfDropping(t *testing.T) {
	c := NewConnectClient("http://unused", nil, "test", "tenant", nil)
	for i := 0; i < 64; i++ {
		c.events <- &agentv1.Event{EventId: "noise"}
	}
	cmd := &agentv1.Event{EventId: "cmd-1", Type: "inari.agent.apply_bundle.v1"}
	done := make(chan struct{})
	go func() {
		c.emit(context.Background(), cmd)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("emit must backpressure when the buffer is full, not drop")
	case <-time.After(100 * time.Millisecond):
	}
	for i := 0; i < 63; i++ {
		<-c.events
	}
	<-c.events // drain one more, unblocking emit
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("emit did not unblock after consumer drained")
	}
	if ev := <-c.events; ev.EventId != "cmd-1" {
		t.Fatalf("command event lost or reordered: got %q", ev.EventId)
	}
}

func TestBackoffWaitsAreJittered(t *testing.T) {
	seen := map[time.Duration]bool{}
	for i := 0; i < 50; i++ {
		w := jitterDelay(2 * time.Second)
		if w < time.Second || w > 2*time.Second {
			t.Fatalf("jittered wait %v outside [1s,2s]", w)
		}
		seen[w] = true
	}
	if len(seen) < 10 {
		t.Errorf("jitter produced only %d distinct waits; reconnects would synchronise", len(seen))
	}
}

func TestOAuth2TokenSourceCachesTokens(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"tok","token_type":"Bearer","expires_in":3600}`))
	}))
	t.Cleanup(srv.Close)

	ts := &OAuth2TokenSource{TokenURL: srv.URL, ClientID: "id", ClientSecret: "secret"}
	for i := 0; i < 3; i++ {
		if _, err := ts.Token(context.Background()); err != nil {
			t.Fatalf("Token: %v", err)
		}
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("token endpoint hit %d times for 3 Token() calls; want 1 (cached)", got)
	}
}
