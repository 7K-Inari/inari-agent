// Package tunnelagent implements the in-cluster kubectl tunnel relay
// (plan §8): it dials out to inari-kubeproxy over a single bidirectional
// Connect stream and relays proxied HTTP requests to the tenant apiserver
// using the tunnel-agent ServiceAccount token plus the hub-minted
// Impersonate-* headers. Pull, never push: the control plane never
// connects in.
package tunnelagent

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	tunnelv1 "github.com/7K-Inari/inari-api/gen/go/inari/tunnel/v1"
)

// Relay executes proxied HTTP requests against the tenant apiserver.
//
// Non-upgrade requests go through an ordinary http.Client. Requests with
// UpgradeExpected (exec/logs -f/port-forward/watch over SPDY or websocket)
// are dialed manually so the raw net.Conn can be spliced into the frame
// stream after the 101 handshake — both upgrade protocols pass through
// unchanged (parent plan decision: no SPDY/websocket parsing in the agent).
type Relay struct {
	// BaseURL is the apiserver base, e.g. https://kubernetes.default.svc.
	BaseURL string
	// BearerToken authenticates to the apiserver (tunnel-agent SA token).
	// The hub's headers never carry Authorization; the agent always sets
	// its own so impersonation is the only privilege exercised.
	BearerToken string
	// TokenFunc, when set, overrides BearerToken and is consulted per
	// request — projected SA tokens rotate (~1h), so production wiring
	// re-reads the token file instead of caching it for the process
	// lifetime.
	TokenFunc func() string
	// TLSConfig configures the apiserver TLS connection (in-cluster CA).
	// Nil uses the system roots for https:// BaseURLs.
	TLSConfig *tls.Config
	// Client executes non-upgrade requests. Nil builds one from TLSConfig
	// with no overall timeout (watch requests stream indefinitely).
	Client *http.Client
	// DialContext dials the apiserver for upgrade requests. Nil uses a
	// plain net.Dialer (TLS is layered on top for https:// BaseURLs).
	DialContext func(ctx context.Context, network, addr string) (net.Conn, error)

	Logger *slog.Logger

	// The non-upgrade client is built once and shared: a fresh Transport
	// per request would never reuse connections and its idle conns would
	// never be reaped (FD leak under kubectl list bursts).
	clientOnce   sync.Once
	sharedClient *http.Client
}

// Result is the apiserver response head plus its body stream.
type Result struct {
	Status int
	Header http.Header
	// Body streams response bytes (normal) or upgraded socket bytes (101).
	Body io.ReadCloser
	// Upgraded is the write side of the raw upgraded connection; non-nil
	// exactly when Status is 101. Inbound frames are written here after the
	// upgrade instead of the request body.
	Upgraded io.Writer
}

// Do issues the request described by open against the apiserver. reqBody
// streams the request body (inbound frames); for upgrade requests it is
// typically empty. Do returns once the response head is available; the
// caller streams the body afterwards.
func (r *Relay) Do(ctx context.Context, open *tunnelv1.TunnelOpen, reqBody io.Reader) (*Result, error) {
	target := r.BaseURL + open.Path
	if _, err := url.Parse(target); err != nil {
		return nil, fmt.Errorf("tunnelagent: parse request URL: %w", err)
	}
	if open.UpgradeExpected {
		return r.doUpgrade(ctx, target, open)
	}

	var body io.ReadCloser
	if reqBody != nil {
		if rc, ok := reqBody.(io.ReadCloser); ok {
			body = rc
		} else {
			body = io.NopCloser(reqBody)
		}
	}
	req, err := http.NewRequestWithContext(ctx, open.Method, target, body)
	if err != nil {
		return nil, fmt.Errorf("tunnelagent: build request: %w", err)
	}
	r.applyHeaders(req, open)

	resp, err := r.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("tunnelagent: apiserver request: %w", err)
	}
	return &Result{Status: resp.StatusCode, Header: resp.Header, Body: resp.Body}, nil
}

// doUpgrade handles UpgradeExpected requests with a manually dialed
// connection so the raw socket can be handed to the frame splice after the
// 101 response. On a non-101 response (apiserver rejected the upgrade) the
// response is returned like a normal one, with the connection closed when
// the body is closed.
func (r *Relay) doUpgrade(ctx context.Context, target string, open *tunnelv1.TunnelOpen) (*Result, error) {
	u, err := url.Parse(target)
	if err != nil {
		return nil, fmt.Errorf("tunnelagent: parse request URL: %w", err)
	}
	addr := u.Host
	if u.Port() == "" {
		if u.Scheme == "https" {
			addr += ":443"
		} else {
			addr += ":80"
		}
	}

	dial := r.DialContext
	if dial == nil {
		d := &net.Dialer{Timeout: 10 * time.Second}
		dial = d.DialContext
	}
	conn, err := dial(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("tunnelagent: dial apiserver: %w", err)
	}
	if u.Scheme == "https" {
		tlsCfg := &tls.Config{ServerName: u.Hostname(), MinVersion: tls.VersionTLS12}
		if r.TLSConfig != nil {
			tlsCfg = r.TLSConfig.Clone()
			if tlsCfg.ServerName == "" {
				tlsCfg.ServerName = u.Hostname()
			}
		}
		tlsConn := tls.Client(conn, tlsCfg)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, fmt.Errorf("tunnelagent: apiserver TLS handshake: %w", err)
		}
		conn = tlsConn
	}

	req, err := http.NewRequestWithContext(ctx, open.Method, target, nil)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("tunnelagent: build upgrade request: %w", err)
	}
	r.applyHeaders(req, open)
	// Force HTTP/1.1 wire format: upgrades are an HTTP/1.1 mechanism.
	req.Proto, req.ProtoMajor, req.ProtoMinor = "HTTP/1.1", 1, 1
	if err := req.Write(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("tunnelagent: write upgrade request: %w", err)
	}

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("tunnelagent: read upgrade response: %w", err)
	}
	if resp.StatusCode == http.StatusSwitchingProtocols {
		return &Result{
			Status:   resp.StatusCode,
			Header:   resp.Header,
			Body:     &rawConnReader{r: br, c: conn},
			Upgraded: conn,
		}, nil
	}
	// Upgrade rejected: stream the normal response body, closing the raw
	// connection when the body closes (resp.Body reads from br/conn).
	return &Result{
		Status: resp.StatusCode,
		Header: resp.Header,
		Body:   &connClosingReader{ReadCloser: resp.Body, c: conn},
	}, nil
}

// applyHeaders copies the hub-sanitized headers (already minted with
// Impersonate-* by kubeproxy) onto the request and forces the SA bearer
// token — inbound Authorization is never trusted.
func (r *Relay) applyHeaders(req *http.Request, open *tunnelv1.TunnelOpen) {
	for k, v := range open.Headers {
		req.Header.Set(k, v)
	}
	token := r.BearerToken
	if r.TokenFunc != nil {
		token = r.TokenFunc()
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if req.Host == "" {
		req.Host = req.URL.Host
	}
}

func (r *Relay) httpClient() *http.Client {
	if r.Client != nil {
		return r.Client
	}
	r.clientOnce.Do(func() {
		r.sharedClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig:     r.TLSConfig,
				DisableCompression:  true,
				ForceAttemptHTTP2:   false, // watch/upgrade semantics stay HTTP/1.1
				MaxIdleConnsPerHost: 8,
				IdleConnTimeout:     90 * time.Second,
			},
			// The apiserver does not redirect; pass any redirect response
			// through to the hub verbatim instead of following it.
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	})
	return r.sharedClient
}

// rawConnReader reads upgraded socket bytes; the bufio.Reader must be used
// (not the bare conn) because http.ReadResponse may have buffered bytes.
type rawConnReader struct {
	r *bufio.Reader
	c net.Conn
}

func (b *rawConnReader) Read(p []byte) (int, error) { return b.r.Read(p) }
func (b *rawConnReader) Close() error               { return b.c.Close() }

type connClosingReader struct {
	io.ReadCloser
	c net.Conn
}

func (r *connClosingReader) Close() error {
	err := r.ReadCloser.Close()
	r.c.Close()
	return err
}
