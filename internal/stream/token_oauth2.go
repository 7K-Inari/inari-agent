package stream

import (
	"context"

	"golang.org/x/oauth2/clientcredentials"
)

// OAuth2TokenSource fetches short-lived JWTs via the OAuth2
// client-credentials grant against the per-cluster Keycloak client (the JWT
// carries the hardcoded cluster_id claim, plan §5.3). Tokens are cached and
// auto-refreshed by the oauth2 package as they approach expiry.
type OAuth2TokenSource struct {
	TokenURL     string
	ClientID     string
	ClientSecret string
	Scopes       []string
}

// Token implements TokenSource.
func (s *OAuth2TokenSource) Token(ctx context.Context) (string, error) {
	cfg := clientcredentials.Config{
		ClientID:     s.ClientID,
		ClientSecret: s.ClientSecret,
		TokenURL:     s.TokenURL,
		Scopes:       s.Scopes,
	}
	tok, err := cfg.TokenSource(ctx).Token()
	if err != nil {
		return "", err
	}
	return tok.AccessToken, nil
}

var _ TokenSource = (*OAuth2TokenSource)(nil)
