package git

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestGitHubProviderCredentialRedirects(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name                  string
		redirectTokenExchange bool
	}{
		{name: "app JWT", redirectTokenExchange: true},
		{name: "installation token", redirectTokenExchange: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			auth := crossOriginRedirectAuthorization(t, key, tt.redirectTokenExchange)
			if auth != "" {
				t.Fatal("credential was forwarded to a different origin")
			}
		})
	}
}

func TestGitHubProviderCredentialSameOriginRedirect(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	auth := make(chan string, 1)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/app/installations/2/access_tokens":
			writeInstallationToken(w)
		case "/repos/o/r/git/ref/heads/main":
			http.Redirect(w, r, "/repos/o/r/git/ref/heads/resolved", http.StatusFound)
		case "/repos/o/r/git/ref/heads/resolved":
			auth <- r.Header.Get("Authorization")
			_ = json.NewEncoder(w).Encode(map[string]any{"ref": "refs/heads/main", "object": map[string]any{"sha": "commit0", "type": "commit"}})
		default:
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"message": "Not Found"})
		}
	}))
	t.Cleanup(api.Close)

	provider, err := NewGitHubProvider(AppCredentials{AppID: 1, InstallationID: 2, PrivateKey: key}, api.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _, _ = provider.CommitFiles(ctx, Target{Repo: "o/r", Branch: "main"}, []File{{Path: "app.yaml", Content: []byte("x")}}, "qa")

	select {
	case got := <-auth:
		if got != "Bearer inst-tok" {
			t.Fatalf("same-origin redirect authorization = %q, want installation token", got)
		}
	default:
		t.Fatal("same-origin redirect was not exercised")
	}
}

func crossOriginRedirectAuthorization(t *testing.T, key *rsa.PrivateKey, redirectTokenExchange bool) string {
	t.Helper()
	auth := make(chan string, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth <- r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		if redirectTokenExchange {
			writeInstallationToken(w)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"message": "Not Found"})
	}))
	t.Cleanup(target.Close)

	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/app/installations/2/access_tokens":
			if redirectTokenExchange {
				http.Redirect(w, r, target.URL+"/token", http.StatusFound)
				return
			}
			writeInstallationToken(w)
		case "/repos/o/r/git/ref/heads/main":
			if redirectTokenExchange {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]string{"message": "Not Found"})
				return
			}
			http.Redirect(w, r, target.URL+"/repository", http.StatusFound)
		default:
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"message": "Not Found"})
		}
	}))
	t.Cleanup(api.Close)

	provider, err := NewGitHubProvider(AppCredentials{AppID: 1, InstallationID: 2, PrivateKey: key}, api.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _, _ = provider.CommitFiles(ctx, Target{Repo: "o/r", Branch: "main"}, []File{{Path: "app.yaml", Content: []byte("x")}}, "qa")

	select {
	case got := <-auth:
		return got
	default:
		return ""
	}
}

func writeInstallationToken(w http.ResponseWriter) {
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"token": "inst-tok", "expires_at": time.Now().Add(time.Hour).UTC()})
}

func TestSameOriginForCredentials(t *testing.T) {
	tests := []struct {
		name      string
		origin    string
		candidate string
		want      bool
	}{
		{name: "same origin and different path", origin: "https://ghe.example.com/api/v3/", candidate: "https://ghe.example.com/repos", want: true},
		{name: "HTTPS default port", origin: "https://ghe.example.com", candidate: "https://GHE.EXAMPLE.COM:443/repos", want: true},
		{name: "HTTP default port", origin: "http://ghe.example.com", candidate: "http://ghe.example.com:80/repos", want: true},
		{name: "different host", origin: "https://ghe.example.com", candidate: "https://evil.example.com", want: false},
		{name: "host suffix is not enough", origin: "https://ghe.example.com", candidate: "https://api.ghe.example.com", want: false},
		{name: "different scheme", origin: "https://ghe.example.com", candidate: "http://ghe.example.com", want: false},
		{name: "different port", origin: "https://ghe.example.com", candidate: "https://ghe.example.com:8443", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			origin, err := url.Parse(tt.origin)
			if err != nil {
				t.Fatal(err)
			}
			candidate, err := url.Parse(tt.candidate)
			if err != nil {
				t.Fatal(err)
			}
			if got := sameOrigin(candidate, origin); got != tt.want {
				t.Fatalf("sameOrigin(%q, %q) = %t, want %t", tt.candidate, tt.origin, got, tt.want)
			}
		})
	}
	origin, err := url.Parse("https://ghe.example.com")
	if err != nil {
		t.Fatal(err)
	}
	if sameOrigin(nil, origin) {
		t.Fatal("nil request URL must not match")
	}
}
