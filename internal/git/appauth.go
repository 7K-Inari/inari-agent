package git

import (
	"context"
	"crypto/rsa"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// AppCredentials are the GitHub App credentials delivered to the cluster
// via ESO (never PATs, never in git — plan §12.1/2).
type AppCredentials struct {
	AppID          int64
	InstallationID int64
	PrivateKey     *rsa.PrivateKey
}

// ParseAppCredentials builds AppCredentials from the ESO-materialized
// Secret keys: app-id, installation-id, private-key (PEM).
func ParseAppCredentials(data map[string][]byte) (AppCredentials, error) {
	var c AppCredentials
	appID, err := strconv.ParseInt(string(data["app-id"]), 10, 64)
	if err != nil {
		return c, fmt.Errorf("git: github app credentials: bad app-id: %w", err)
	}
	instID, err := strconv.ParseInt(string(data["installation-id"]), 10, 64)
	if err != nil {
		return c, fmt.Errorf("git: github app credentials: bad installation-id: %w", err)
	}
	key, err := jwt.ParseRSAPrivateKeyFromPEM(data["private-key"])
	if err != nil {
		return c, fmt.Errorf("git: github app credentials: bad private-key: %w", err)
	}
	return AppCredentials{AppID: appID, InstallationID: instID, PrivateKey: key}, nil
}

// appTokenSource vends installation access tokens, minting short-lived
// app JWTs and caching the installation token until near expiry.
type appTokenSource struct {
	creds AppCredentials
	// newToken exchanges an app JWT for an installation token; injectable
	// for tests. Returns the token and its expiry.
	newToken func(ctx context.Context, appJWT string) (string, time.Time, error)

	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

func (s *appTokenSource) appJWT(now time.Time) (string, error) {
	claims := jwt.RegisteredClaims{
		Issuer:    strconv.FormatInt(s.creds.AppID, 10),
		IssuedAt:  jwt.NewNumericDate(now.Add(-30 * time.Second)),
		ExpiresAt: jwt.NewNumericDate(now.Add(9 * time.Minute)),
	}
	return jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(s.creds.PrivateKey)
}

// Token returns a valid installation access token.
func (s *appTokenSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token != "" && time.Until(s.expiresAt) > time.Minute {
		return s.token, nil
	}
	appJWT, err := s.appJWT(time.Now())
	if err != nil {
		return "", fmt.Errorf("git: sign app JWT: %w", err)
	}
	tok, exp, err := s.newToken(ctx, appJWT)
	if err != nil {
		return "", err
	}
	s.token, s.expiresAt = tok, exp
	return tok, nil
}

// RoundTripper injects the installation token into outbound requests.
func (s *appTokenSource) RoundTripper(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &tokenTransport{base: base, src: s}
}

type tokenTransport struct {
	base http.RoundTripper
	src  *appTokenSource
}

func (t *tokenTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	tok, err := t.src.Token(req.Context())
	if err != nil {
		return nil, err
	}
	clone := req.Clone(req.Context())
	clone.Header.Set("Authorization", "Bearer "+tok)
	clone.Header.Set("Accept", "application/vnd.github+json")
	return t.base.RoundTrip(clone)
}
