package stream

import (
	"context"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// OAuth2TokenSource fetches short-lived JWTs via the OAuth2
// client-credentials grant against the per-cluster Keycloak client (the JWT
// carries the hardcoded cluster_id claim, plan §5.3). Tokens are cached and
// auto-refreshed as they approach expiry (oauth2.ReuseTokenSource).
type OAuth2TokenSource struct {
	TokenURL     string
	ClientID     string
	ClientSecret string
	Scopes       []string

	mu sync.Mutex
	ts oauth2.TokenSource
}

// Token implements TokenSource.
func (s *OAuth2TokenSource) Token(ctx context.Context) (string, error) {
	tok, _, err := s.TokenWithExpiry(ctx)
	return tok, err
}

// TokenWithExpiry implements ExpiringTokenSource so the stream can rotate
// the session before the JWT expires (issue #28: Keycloak's default 300s
// token lifespan terminates the stream on a fixed cadence when the gateway
// enforces exp on open streams).
func (s *OAuth2TokenSource) TokenWithExpiry(ctx context.Context) (string, time.Time, error) {
	tok, err := s.token(ctx)
	if err != nil {
		return "", time.Time{}, err
	}
	return tok.AccessToken, tok.Expiry, nil
}

func (s *OAuth2TokenSource) token(_ context.Context) (*oauth2.Token, error) {
	s.mu.Lock()
	if s.ts == nil {
		cfg := clientcredentials.Config{
			ClientID:     s.ClientID,
			ClientSecret: s.ClientSecret,
			TokenURL:     s.TokenURL,
			Scopes:       s.Scopes,
		}
		// The underlying source outlives any single stream session, so it
		// must not capture a per-session ctx (refreshes would fail after
		// the session ends).
		s.ts = oauth2.ReuseTokenSource(nil, cfg.TokenSource(context.Background()))
	}
	ts := s.ts
	s.mu.Unlock()

	return ts.Token()
}

var _ TokenSource = (*OAuth2TokenSource)(nil)
var _ ExpiringTokenSource = (*OAuth2TokenSource)(nil)
