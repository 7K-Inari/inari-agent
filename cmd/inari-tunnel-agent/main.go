// inari-tunnel-agent is the dedicated in-cluster kubectl tunnel relay
// (plan §8). It dials out to inari-kubeproxy over a bidirectional Connect
// stream and relays proxied requests to the tenant apiserver using its own
// ServiceAccount token plus the hub-minted Impersonate-* headers. It is a
// separate binary from the inari-agent control-plane manager: its SA holds
// only the impersonate verb and is never merged into the agent SA.
//
// Configuration is strictly environment-based:
//
//	INARI_KUBEPROXY_URL           kubeproxy base URL (http:// h2c or https://) [required]
//	INARI_CLUSTER_ID              cluster identifier (logging) [required]
//	INARI_OIDC_ISSUER             Keycloak issuer; token URL derived as
//	                              {issuer}/protocol/openid-connect/token
//	INARI_OIDC_TOKEN_URL          explicit token URL (takes precedence)
//	INARI_CLIENT_ID               tunnel OIDC client (default tunnel-<cluster-id>)
//	INARI_CLIENT_SECRET           client secret, or INARI_CLIENT_SECRET_FILE
//	INARI_APISERVER_URL           default https://kubernetes.default.svc
//	INARI_SA_TOKEN_FILE           SA token path (default projected SA path)
//	INARI_SA_CA_FILE              apiserver CA path (default projected SA path)
//	INARI_TUNNEL_PING_INTERVAL    session keepalive (default 30s)
//	INARI_TUNNEL_MAX_CONN_BYTES   per-connection byte cap (default 1GiB)
//	INARI_TUNNEL_MAX_CONN_LIFETIME per-connection cap (default 60m, matches hub reaper)
//	INARI_TUNNEL_HEALTH_ADDR      probe listener (default :8082)
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/7K-Inari/inari-agent/internal/stream"
	"github.com/7K-Inari/inari-agent/internal/tunnelagent"
)

var version = "dev" // overridden at release build time via -ldflags

const (
	defaultSAPathPrefix = "/var/run/secrets/kubernetes.io/serviceaccount"
	defaultAPIServerURL = "https://kubernetes.default.svc"
)

func main() {
	if err := run(); err != nil {
		slog.Error("inari-tunnel-agent failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := configFromEnv()
	if err != nil {
		return err
	}

	saToken, err := newFileTokenSource(cfg.saTokenFile)
	if err != nil {
		return fmt.Errorf("load service account token: %w", err)
	}
	tlsConfig, err := apiserverTLSConfig(cfg.saCAFile)
	if err != nil {
		return fmt.Errorf("load apiserver CA: %w", err)
	}

	tokenURL := cfg.oidcTokenURL
	if tokenURL == "" {
		tokenURL = strings.TrimSuffix(cfg.oidcIssuer, "/") + "/protocol/openid-connect/token"
	}

	relay := &tunnelagent.Relay{
		BaseURL:   cfg.apiserverURL,
		TokenFunc: saToken.Get,
		TLSConfig: tlsConfig,
	}
	handler := tunnelagent.NewHandler(relay)
	handler.MaxConnBytes = cfg.maxConnBytes
	handler.MaxConnLifetime = cfg.maxConnLifetime

	client := tunnelagent.NewClient(cfg.kubeproxyURL, &stream.OAuth2TokenSource{
		TokenURL:     tokenURL,
		ClientID:     cfg.clientID,
		ClientSecret: cfg.clientSecret,
	}, handler)
	client.PingInterval = cfg.pingInterval

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	healthErr := make(chan error, 1)
	go func() { healthErr <- serveHealth(ctx, cfg.healthAddr, client) }()

	slog.Info("starting inari-tunnel-agent",
		"version", version,
		"clusterID", cfg.clusterID,
		"kubeproxy", cfg.kubeproxyURL,
		"apiserver", cfg.apiserverURL,
		"clientID", cfg.clientID)
	// Supervise both loops: a dead health listener (e.g. port conflict)
	// must crash the process, not leave it running probeless.
	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(ctx) }()
	select {
	case err := <-healthErr:
		return err
	case runErr := <-runDone:
		if err := <-healthErr; err != nil {
			return err
		}
		return runErr
	}
}

type config struct {
	kubeproxyURL    string
	clusterID       string
	oidcIssuer      string
	oidcTokenURL    string
	clientID        string
	clientSecret    string
	apiserverURL    string
	saTokenFile     string
	saCAFile        string
	pingInterval    time.Duration
	maxConnBytes    int64
	maxConnLifetime time.Duration
	healthAddr      string
}

func configFromEnv() (*config, error) {
	cfg := &config{
		kubeproxyURL: os.Getenv("INARI_KUBEPROXY_URL"),
		clusterID:    os.Getenv("INARI_CLUSTER_ID"),
		oidcIssuer:   os.Getenv("INARI_OIDC_ISSUER"),
		oidcTokenURL: os.Getenv("INARI_OIDC_TOKEN_URL"),
		clientID:     os.Getenv("INARI_CLIENT_ID"),
		apiserverURL: envOr("INARI_APISERVER_URL", defaultAPIServerURL),
		saTokenFile:  envOr("INARI_SA_TOKEN_FILE", defaultSAPathPrefix+"/token"),
		saCAFile:     envOr("INARI_SA_CA_FILE", defaultSAPathPrefix+"/ca.crt"),
		healthAddr:   envOr("INARI_TUNNEL_HEALTH_ADDR", ":8082"),
	}
	if cfg.kubeproxyURL == "" {
		return nil, errors.New("INARI_KUBEPROXY_URL is required")
	}
	if cfg.clusterID == "" {
		return nil, errors.New("INARI_CLUSTER_ID is required")
	}
	if cfg.oidcIssuer == "" && cfg.oidcTokenURL == "" {
		return nil, errors.New("one of INARI_OIDC_ISSUER or INARI_OIDC_TOKEN_URL is required")
	}
	if cfg.clientID == "" {
		cfg.clientID = "tunnel-" + cfg.clusterID
	}
	cfg.clientSecret = os.Getenv("INARI_CLIENT_SECRET")
	if cfg.clientSecret == "" {
		if f := os.Getenv("INARI_CLIENT_SECRET_FILE"); f != "" {
			b, err := os.ReadFile(f)
			if err != nil {
				return nil, fmt.Errorf("read INARI_CLIENT_SECRET_FILE: %w", err)
			}
			cfg.clientSecret = strings.TrimSpace(string(b))
		}
	}
	if cfg.clientSecret == "" {
		return nil, errors.New("INARI_CLIENT_SECRET or INARI_CLIENT_SECRET_FILE is required")
	}

	var err error
	if cfg.pingInterval, err = durationEnv("INARI_TUNNEL_PING_INTERVAL", 30*time.Second); err != nil {
		return nil, err
	}
	if cfg.maxConnLifetime, err = durationEnv("INARI_TUNNEL_MAX_CONN_LIFETIME", tunnelagent.DefaultMaxConnLifetime); err != nil {
		return nil, err
	}
	if v := os.Getenv("INARI_TUNNEL_MAX_CONN_BYTES"); v != "" {
		cfg.maxConnBytes, err = strconv.ParseInt(v, 10, 64)
		if err != nil || cfg.maxConnBytes <= 0 {
			return nil, fmt.Errorf("invalid INARI_TUNNEL_MAX_CONN_BYTES %q", v)
		}
	} else {
		cfg.maxConnBytes = tunnelagent.DefaultMaxConnBytes
	}
	return cfg, nil
}

// serveHealth exposes /healthz (process alive) and /readyz (tunnel stream
// connected) for kubelet probes.
func serveHealth(ctx context.Context, addr string, client *tunnelagent.Client) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !client.Connected() {
			http.Error(w, "tunnel disconnected", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("health listener: %w", err)
	}
	return nil
}

// fileTokenSource re-reads the projected SA token file at most once per
// minute: BoundServiceAccountTokenVolume tokens rotate (~1h TTL), so a
// static read at startup would expire mid-process.
type fileTokenSource struct {
	path string

	mu     sync.Mutex
	cached string
	at     time.Time
}

func newFileTokenSource(path string) (*fileTokenSource, error) {
	ts := &fileTokenSource{path: path}
	if _, err := ts.get(); err != nil {
		return nil, err
	}
	return ts, nil
}

func (s *fileTokenSource) Get() string {
	tok, err := s.get()
	if err != nil {
		slog.Warn("re-reading SA token failed, using cached value", "error", err)
		return s.cached
	}
	return tok
}

func (s *fileTokenSource) get() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cached != "" && time.Since(s.at) < time.Minute {
		return s.cached, nil
	}
	b, err := os.ReadFile(s.path)
	if err != nil {
		return "", err
	}
	tok := strings.TrimSpace(string(b))
	if tok == "" {
		return "", fmt.Errorf("empty token file %s", s.path)
	}
	s.cached, s.at = tok, time.Now()
	return tok, nil
}

func apiserverTLSConfig(caFile string) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Warn("apiserver CA file not found, falling back to system roots", "path", caFile)
			return cfg, nil
		}
		return nil, err
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates found in %s", caFile)
	}
	cfg.RootCAs = pool
	return cfg, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func durationEnv(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid %s %q", key, v)
	}
	return d, nil
}
