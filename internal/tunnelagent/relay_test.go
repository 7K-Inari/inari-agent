package tunnelagent

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	tunnelv1 "github.com/7K-Inari/inari-api/gen/go/inari/tunnel/v1"
)

func TestRelayPassesImpersonationAndSAToken(t *testing.T) {
	var gotAuth, gotUser, gotGroup, gotUID, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotUser = r.Header.Get("Impersonate-User")
		gotGroup = r.Header.Get("Impersonate-Group")
		gotUID = r.Header.Get("Impersonate-Uid")
		gotPath = r.URL.String()
		w.Header().Set("X-Test", "yes")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("cluster-info"))
	}))
	defer srv.Close()

	r := &Relay{BaseURL: srv.URL, BearerToken: "sa-token-123"}
	res, err := r.Do(context.Background(), &tunnelv1.TunnelOpen{
		Method: "GET",
		Path:   "/api/v1/namespaces/default/pods?limit=5",
		Headers: map[string]string{
			"Impersonate-User":  "alice@example.com",
			"Impersonate-Group": "system:authenticated",
			"Impersonate-Uid":   "uid-42",
			// An inbound Authorization header must never be trusted: the
			// agent always forces its own SA token.
			"Authorization": "Bearer attacker-token",
		},
	}, strings.NewReader(""))
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer res.Body.Close()

	if gotAuth != "Bearer sa-token-123" {
		t.Errorf("Authorization = %q, want SA token", gotAuth)
	}
	if gotUser != "alice@example.com" || gotGroup != "system:authenticated" || gotUID != "uid-42" {
		t.Errorf("impersonation headers = %q/%q/%q", gotUser, gotGroup, gotUID)
	}
	if gotPath != "/api/v1/namespaces/default/pods?limit=5" {
		t.Errorf("path = %q", gotPath)
	}
	if res.Status != 200 {
		t.Errorf("status = %d", res.Status)
	}
	if res.Header.Get("X-Test") != "yes" {
		t.Errorf("response header missing")
	}
	body, _ := io.ReadAll(res.Body)
	if string(body) != "cluster-info" {
		t.Errorf("body = %q", body)
	}
	if res.Upgraded != nil {
		t.Errorf("unexpected upgraded writer on normal response")
	}
}

func TestRelayRequestBodyRoundTrip(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	r := &Relay{BaseURL: srv.URL, BearerToken: "tok"}
	res, err := r.Do(context.Background(), &tunnelv1.TunnelOpen{
		Method:  "POST",
		Path:    "/api/v1/namespaces",
		Headers: map[string]string{"Content-Type": "application/json"},
	}, strings.NewReader(`{"metadata":{"name":"x"}}`))
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	res.Body.Close()
	if res.Status != 201 {
		t.Errorf("status = %d", res.Status)
	}
	if gotBody != `{"metadata":{"name":"x"}}` {
		t.Errorf("apiserver body = %q", gotBody)
	}
}

// TestRelayUpgradePassthrough stands up a raw TCP server that answers the
// upgrade handshake with 101 and then echoes bytes; the relay must expose a
// raw bidirectional connection over the Response body/writer pair.
func TestRelayUpgradePassthrough(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		br := bufio.NewReader(conn)
		req, err := http.ReadRequest(br)
		if err != nil {
			return
		}
		if req.Header.Get("Authorization") != "Bearer sa-token-123" {
			t.Errorf("upgrade request Authorization = %q", req.Header.Get("Authorization"))
		}
		if req.Header.Get("Impersonate-User") != "alice@example.com" {
			t.Errorf("upgrade request Impersonate-User = %q", req.Header.Get("Impersonate-User"))
		}
		if req.Header.Get("Connection") != "Upgrade" || req.Header.Get("Upgrade") != "SPDY/3.1" {
			t.Errorf("upgrade headers missing: %v %v", req.Header.Get("Connection"), req.Header.Get("Upgrade"))
		}
		_, _ = conn.Write([]byte("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: SPDY/3.1\r\n\r\n"))
		// Echo loop on the raw upgraded socket.
		_, _ = io.Copy(conn, br)
	}()

	r := &Relay{BaseURL: "http://" + ln.Addr().String(), BearerToken: "sa-token-123"}
	res, err := r.Do(context.Background(), &tunnelv1.TunnelOpen{
		Method: "GET",
		Path:   "/api/v1/namespaces/default/pods/mypod/exec?command=sh",
		Headers: map[string]string{
			"Connection":       "Upgrade",
			"Upgrade":          "SPDY/3.1",
			"Impersonate-User": "alice@example.com",
		},
		UpgradeExpected: true,
	}, strings.NewReader(""))
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer res.Body.Close()

	if res.Status != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", res.Status)
	}
	if res.Upgraded == nil {
		t.Fatal("Upgraded writer is nil after 101")
	}

	// Raw bytes written to the upgraded connection must come back verbatim.
	if _, err := res.Upgraded.Write([]byte("ping-frames")); err != nil {
		t.Fatalf("write upgraded: %v", err)
	}
	buf := make([]byte, len("ping-frames"))
	if _, err := io.ReadFull(res.Body, buf); err != nil {
		t.Fatalf("read upgraded: %v", err)
	}
	if string(buf) != "ping-frames" {
		t.Errorf("echo = %q", buf)
	}
}

func TestRelayUpgradeRejected(t *testing.T) {
	// Apiserver declines the upgrade with a normal 4xx response; the relay
	// must surface it as an ordinary result without an upgraded writer.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("upgrade not supported"))
	}))
	defer srv.Close()

	r := &Relay{BaseURL: srv.URL, BearerToken: "tok"}
	res, err := r.Do(context.Background(), &tunnelv1.TunnelOpen{
		Method:          "GET",
		Path:            "/api/v1/namespaces/default/pods/p/exec",
		UpgradeExpected: true,
	}, strings.NewReader(""))
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer res.Body.Close()
	if res.Status != http.StatusBadRequest {
		t.Errorf("status = %d", res.Status)
	}
	if res.Upgraded != nil {
		t.Errorf("Upgraded must be nil on non-101")
	}
	body, _ := io.ReadAll(res.Body)
	if !strings.Contains(string(body), "upgrade not supported") {
		t.Errorf("body = %q", body)
	}
}

func TestRelayDialFailure(t *testing.T) {
	r := &Relay{BaseURL: "http://127.0.0.1:1", BearerToken: "tok"}
	_, err := r.Do(context.Background(), &tunnelv1.TunnelOpen{
		Method:          "GET",
		Path:            "/api",
		UpgradeExpected: true,
	}, strings.NewReader(""))
	if err == nil {
		t.Fatal("expected dial error")
	}
}

func TestHeaderMap(t *testing.T) {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("X-Multi", "a")
	h.Add("X-Multi", "b")
	m := headerMap(h)
	if m["Content-Type"] != "application/json" || m["X-Multi"] != "a" {
		t.Errorf("headerMap = %v", m)
	}
}
