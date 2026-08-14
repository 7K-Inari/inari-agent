package stream

import (
	"context"
	"sync"

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

	tok, err := ts.Token()
	if err != nil {
		return "", err
	}
	return tok.AccessToken, nil
}

var _ TokenSource = (*OAuth2TokenSource)(nil)
