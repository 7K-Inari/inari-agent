package argocd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// bearerKey is the package-private context key carrying a per-command user
// ArgoCD bearer (Extension OIDC pass-through). Tokens travel in memory only
// and are never logged or persisted.
type bearerKey struct{}

// WithBearer returns a context carrying the per-command user ArgoCD bearer
// token redeemed from the control plane. The token applies only to API calls
// made with this context and is never written to logs, errors, or files.
func WithBearer(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, bearerKey{}, token)
}

// APIClient is a narrow client for the tenant-local ArgoCD API, used only
// for imperative out-of-band ops tunneled from the control plane (plan
// §5.3 phase 4). It is intentionally limited to application-level
// sync/refresh/rollback; custom resource actions are out of M2 scope.
type APIClient struct {
	BaseURL string
	// Token is the static INARI_ARGOCD_API_TOKEN, used only as the explicit
	// legacy/break-glass fallback when the command carries no
	// user_credential_ref (no context bearer).
	Token   string
	HTTP    *http.Client
	// Timeout bounds each call when the command carries none.
	Timeout time.Duration
}

func (c *APIClient) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: c.Timeout}
}

func (c *APIClient) do(ctx context.Context, method, path string, body any) error {
	var r io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimSuffix(c.BaseURL, "/")+path, r)
	if err != nil {
		return err
	}
	// A per-command user bearer (context) wins; the static token is the
	// legacy/break-glass fallback only.
	token := c.Token
	if userTok, _ := ctx.Value(bearerKey{}).(string); userTok != "" {
		token = userTok
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("argocd api %s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(raw)))
	}
	return nil
}

// Sync triggers an application sync.
func (c *APIClient) Sync(ctx context.Context, name string, params map[string]any) error {
	payload := map[string]any{"name": name}
	for k, v := range params {
		payload[k] = v
	}
	return c.do(ctx, http.MethodPost, "/api/v1/applications/"+name+"/sync", payload)
}

// Refresh requests a normal (or hard) application refresh.
func (c *APIClient) Refresh(ctx context.Context, name string, hard bool) error {
	refresh := "normal"
	if hard {
		refresh = "hard"
	}
	return c.do(ctx, http.MethodGet, "/api/v1/applications/"+name+"?refresh="+refresh, nil)
}

// Rollback rolls the application back to a previous deployment ID.
func (c *APIClient) Rollback(ctx context.Context, name string, id int64, dryRun bool) error {
	return c.do(ctx, http.MethodPost, "/api/v1/applications/"+name+"/rollback", map[string]any{
		"name": name, "id": id, "dryRun": dryRun,
	})
}
