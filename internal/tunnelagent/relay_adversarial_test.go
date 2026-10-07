package tunnelagent

// Adversarial tests promoted from QA: resource hygiene, TLS upgrades,
// watch semantics, boundary opens, and header injection.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	tunnelv2 "github.com/7K-Inari/inari-api/gen/go/inari/tunnel/v2"
)

// The non-upgrade client must be shared: a fresh Transport per request
// never reuses connections and never reaps idle ones (FD leak).
func TestRelayReusesConnections(t *testing.T) {
	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	srv.Config.ConnState = func(c net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()

	r := &Relay{BaseURL: srv.URL, BearerToken: "tok"}
	for i := 0; i < 20; i++ {
		res, err := r.Do(context.Background(), &tunnelv2.TunnelOpen{Method: "GET", Path: "/api"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()
	}
	if n := conns.Load(); n > 2 {
		t.Errorf("%d server-side conns for 20 sequential requests, want reuse (<=2)", n)
	}
}

// Upgrade passthrough must also work against the https:// in-cluster
// apiserver (only the plain-http path is covered by the other tests).
func TestRelayUpgradeOverTLS(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Impersonate-User") != "alice@example.com" {
			t.Errorf("impersonate user = %q", r.Header.Get("Impersonate-User"))
		}
		conn, br, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = conn.Write([]byte("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n"))
		_, _ = io.Copy(conn, br)
	}))
	defer srv.Close()

	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	r := &Relay{BaseURL: srv.URL, BearerToken: "tok", TLSConfig: &tls.Config{RootCAs: pool}}
	res, err := r.Do(context.Background(), &tunnelv2.TunnelOpen{
		Method: "GET",
		Path:   "/api/v1/namespaces/default/pods/p/exec?command=sh",
		Headers: map[string]*tunnelv2.StringList{
			"Connection":       {Values: []string{"Upgrade"}},
			"Upgrade":          {Values: []string{"websocket"}},
			"Impersonate-User": {Values: []string{"alice@example.com"}},
		},
		UpgradeExpected: true,
	}, nil)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.Status != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", res.Status)
	}
	if _, err := res.Upgraded.Write([]byte("tls-bytes")); err != nil {
		t.Fatalf("write upgraded: %v", err)
	}
	buf := make([]byte, len("tls-bytes"))
	if _, err := io.ReadFull(res.Body, buf); err != nil {
		t.Fatalf("read upgraded: %v", err)
	}
	if string(buf) != "tls-bytes" {
		t.Errorf("echo = %q", buf)
	}
}

// Watch-style GET: the streaming response flows while stray inbound frames
// are discarded, and a hub close mid-watch terminates promptly.
func TestWatchStreamHubCloseTerminates(t *testing.T) {
	cap := &capture{}
	release := make(chan struct{})
	h := NewHandler(&stubDoer{t: t, respond: func(open *tunnelv2.TunnelOpen, body io.Reader) (*Result, error) {
		pr, pw := io.Pipe()
		go func() {
			for i := 0; ; i++ {
				if _, err := pw.Write([]byte(`{"type":"ADDED"}` + "\n")); err != nil {
					return
				}
				select {
				case <-release:
					_ = pw.Close()
					return
				case <-time.After(time.Millisecond):
				}
				_ = i
			}
		}()
		return &Result{Status: 200, Body: pr}, nil
	}})
	h.SetSend(cap.send)

	h.Handle(&tunnelv2.TunnelMessage{ConnectionId: "w1", Payload: &tunnelv2.TunnelMessage_Open{
		Open: &tunnelv2.TunnelOpen{Method: "GET", Path: "/api/v1/pods?watch=true"},
	}})
	cap.waitFor(t, func(m *tunnelv2.TunnelMessage) bool {
		return m.GetConnectionId() == "w1" && m.GetFrame() != nil
	}, "watch frame")

	h.Handle(&tunnelv2.TunnelMessage{ConnectionId: "w1", Payload: &tunnelv2.TunnelMessage_Frame{
		Frame: &tunnelv2.TunnelFrame{Data: []byte("stray")},
	}})
	h.Handle(&tunnelv2.TunnelMessage{ConnectionId: "w1", Payload: &tunnelv2.TunnelMessage_Close{
		Close: &tunnelv2.TunnelClose{Reason: "client-gone"},
	}})
	deadline := time.Now().Add(2 * time.Second)
	for h.ActiveConns() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	close(release)
	if n := h.ActiveConns(); n != 0 {
		t.Errorf("watch conn not terminated on hub close, active=%d", n)
	}
}

// Boundary opens: empty id and nil open are ignored; a duplicate id does
// not disturb the live connection.
func TestBoundaryOpens(t *testing.T) {
	cap := &capture{}
	h := NewHandler(&stubDoer{t: t})
	h.SetSend(cap.send)

	h.Handle(&tunnelv2.TunnelMessage{ConnectionId: "", Payload: &tunnelv2.TunnelMessage_Open{
		Open: &tunnelv2.TunnelOpen{Method: "GET", Path: "/x"},
	}})
	h.Handle(&tunnelv2.TunnelMessage{ConnectionId: "n1", Payload: &tunnelv2.TunnelMessage_Open{Open: nil}})
	if n := h.ActiveConns(); n != 0 {
		t.Errorf("empty id / nil open created conns, active=%d", n)
	}

	release := make(chan struct{})
	h2 := NewHandler(&stubDoer{t: t, respond: func(open *tunnelv2.TunnelOpen, body io.Reader) (*Result, error) {
		pr, pw := io.Pipe()
		go func() { <-release; _ = pw.Close() }()
		return &Result{Status: 200, Body: pr}, nil
	}})
	h2.SetSend(cap.send)
	h2.Handle(openMsg("dup"))
	cap.waitFor(t, func(m *tunnelv2.TunnelMessage) bool {
		return m.GetConnectionId() == "dup" && m.GetOpenResult() != nil
	}, "first OpenResult")
	h2.Handle(&tunnelv2.TunnelMessage{ConnectionId: "dup", Payload: &tunnelv2.TunnelMessage_Open{
		Open: &tunnelv2.TunnelOpen{Method: "GET", Path: "/impostor"},
	}})
	time.Sleep(50 * time.Millisecond)
	if n := h2.ActiveConns(); n != 1 {
		t.Errorf("active = %d, want 1 (duplicate open ignored)", n)
	}
	close(release)
}

// Query strings and encoded characters must pass through untouched.
func TestRelayPathPassthrough(t *testing.T) {
	got := make(chan string, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.URL.String()
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	r := &Relay{BaseURL: srv.URL, BearerToken: "t"}
	for _, p := range []string{
		"/api/v1/pods?watch=true&resourceVersion=0&labelSelector=a%3Db",
		"/api/v1/namespaces/ns/pods/pod+log",
		"/",
	} {
		res, err := r.Do(context.Background(), &tunnelv2.TunnelOpen{Method: "GET", Path: p}, nil)
		if err != nil {
			t.Fatalf("path %q: %v", p, err)
		}
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()
		if g := <-got; g != p {
			t.Errorf("path %q arrived as %q", p, g)
		}
	}
}

// CRLF in hub-supplied header values must be rejected, never smuggled.
func TestRelayRejectsHeaderInjection(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	r := &Relay{BaseURL: srv.URL, BearerToken: "t"}
	if _, err := r.Do(context.Background(), &tunnelv2.TunnelOpen{
		Method:  "GET",
		Path:    "/api",
		Headers: map[string]*tunnelv2.StringList{"X-Evil": {Values: []string{"a\r\nInjected: yes"}}},
	}, nil); err == nil {
		t.Error("CRLF header value accepted — request smuggling risk")
	}
}
