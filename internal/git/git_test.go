package git

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTargetValidate(t *testing.T) {
	t.Run("default repo and branch", func(t *testing.T) {
		got, err := Target{}.Validate("org/acme-inari-state")
		if err != nil {
			t.Fatal(err)
		}
		if got.Repo != "org/acme-inari-state" || got.Branch != "main" {
			t.Fatalf("got %+v", got)
		}
	})
	t.Run("rejects bad repo", func(t *testing.T) {
		if _, err := (Target{Repo: "noslash"}).Validate(""); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("rejects path escape", func(t *testing.T) {
		if _, err := (Target{Repo: "o/r", Path: "../x"}).Validate(""); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("rejects missing default", func(t *testing.T) {
		if _, err := (Target{}).Validate(""); err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestParseAppCredentials(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	creds, err := ParseAppCredentials(map[string][]byte{
		"app-id":          []byte("123"),
		"installation-id": []byte("456"),
		"private-key":     pemBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	if creds.AppID != 123 || creds.InstallationID != 456 {
		t.Fatalf("got %+v", creds)
	}
	if _, err := ParseAppCredentials(map[string][]byte{"app-id": []byte("x")}); err == nil {
		t.Fatal("expected error for bad app-id")
	}
}

func TestAppTokenSourceCachesAndRefreshes(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	src := &appTokenSource{
		creds: AppCredentials{AppID: 1, InstallationID: 2, PrivateKey: key},
	}
	calls := 0
	src.newToken = func(_ context.Context, appJWT string) (string, time.Time, error) {
		calls++
		if appJWT == "" {
			t.Error("empty app JWT")
		}
		return "tok", time.Now().Add(time.Hour), nil
	}
	for range 3 {
		tok, err := src.Token(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if tok != "tok" {
			t.Fatalf("got %q", tok)
		}
	}
	if calls != 1 {
		t.Fatalf("expected 1 token exchange, got %d", calls)
	}
	src.expiresAt = time.Now() // force refresh
	if _, err := src.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("expected refresh, got %d calls", calls)
	}
}

func TestMemProviderIdempotentCommit(t *testing.T) {
	m := NewMemProvider()
	tgt := Target{Repo: "o/r", Branch: "main"}
	files := []File{{Path: "a.yaml", Content: []byte("x")}}
	sha1, changed, err := m.CommitFiles(context.Background(), tgt, files, "msg")
	if err != nil || !changed {
		t.Fatalf("first commit: sha=%s changed=%v err=%v", sha1, changed, err)
	}
	sha2, changed, err := m.CommitFiles(context.Background(), tgt, files, "msg")
	if err != nil {
		t.Fatal(err)
	}
	if changed || sha2 != sha1 {
		t.Fatalf("second commit should be no-op: sha=%s changed=%v", sha2, changed)
	}
	files[0].Content = []byte("y")
	if _, changed, err := m.CommitFiles(context.Background(), tgt, files, "msg"); err != nil || !changed {
		t.Fatalf("changed content must commit: changed=%v err=%v", changed, err)
	}
}

func TestMemProviderPRIdempotent(t *testing.T) {
	m := NewMemProvider()
	tgt := Target{Repo: "o/r", Branch: "main"}
	u1, err := m.OpenPR(context.Background(), tgt, nil, "inari/x", "t", "b")
	if err != nil {
		t.Fatal(err)
	}
	u2, err := m.OpenPR(context.Background(), tgt, nil, "inari/x", "t", "b")
	if err != nil {
		t.Fatal(err)
	}
	if u1 != u2 || len(m.PRs) != 1 {
		t.Fatalf("expected one PR, got %q %q (%d)", u1, u2, len(m.PRs))
	}
}

// fakeGitHub implements the minimal Git Data + token endpoints the
// provider exercises.
type fakeGitHub struct {
	mu        sync.Mutex
	trees     map[string][]string // tree sha -> paths
	commits   []string
	refs      map[string]string
	prs       []map[string]any
	blobCount int
	nextSHA   int
}

func newFakeGitHub() *fakeGitHub {
	return &fakeGitHub{
		trees: map[string][]string{"tree0": {}},
		refs:  map[string]string{"heads/main": "commit0"},
	}
}

func (f *fakeGitHub) sha() string {
	f.nextSHA++
	return strings.Repeat("0", 39-len(string(rune('a'+f.nextSHA%26)))) + strings.TrimSpace(strings.Repeat("x", 0)) + "sha" + string(rune('a'+f.nextSHA%26)) + string(rune('0'+f.nextSHA%10))
}

func (f *fakeGitHub) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /app/installations/2/access_tokens", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"token": "inst-tok", "expires_at": time.Now().Add(time.Hour).UTC()})
	})
	mux.HandleFunc("GET /repos/o/r/git/ref/heads/main", func(w http.ResponseWriter, _ *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"ref": "refs/heads/main", "object": map[string]any{"sha": f.refs["heads/main"], "type": "commit"}})
	})
	mux.HandleFunc("GET /repos/o/r/git/commits/{sha}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		tree := "tree0"
		if r.PathValue("sha") != "commit0" && len(f.trees) > 1 {
			for k := range f.trees {
				tree = k
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"sha": r.PathValue("sha"), "tree": map[string]any{"sha": tree}})
	})
	mux.HandleFunc("POST /repos/o/r/git/blobs", func(w http.ResponseWriter, _ *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.blobCount++
		_ = json.NewEncoder(w).Encode(map[string]any{"sha": f.sha()})
	})
	mux.HandleFunc("POST /repos/o/r/git/trees", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		var body struct {
			BaseTree string `json:"base_tree"`
			Tree     []struct {
				Path string `json:"path"`
				SHA  string `json:"sha"`
			} `json:"tree"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		// No files => identical to base tree (no-op commit path).
		if len(body.Tree) == 0 {
			_ = json.NewEncoder(w).Encode(map[string]any{"sha": body.BaseTree})
			return
		}
		sha := f.sha()
		paths := []string{}
		for _, e := range body.Tree {
			paths = append(paths, e.Path)
		}
		f.trees[sha] = paths
		_ = json.NewEncoder(w).Encode(map[string]any{"sha": sha})
	})
	mux.HandleFunc("POST /repos/o/r/git/commits", func(w http.ResponseWriter, _ *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		sha := f.sha()
		f.commits = append(f.commits, sha)
		_ = json.NewEncoder(w).Encode(map[string]any{"sha": sha})
	})
	mux.HandleFunc("PATCH /repos/o/r/git/refs/heads/main", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		var body struct {
			SHA string `json:"sha"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.refs["heads/main"] = body.SHA
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"ref": "refs/heads/main", "object": map[string]any{"sha": body.SHA, "type": "commit"}})
	})
	return mux
}

func TestGitHubProviderCommitFiles(t *testing.T) {
	fake := newFakeGitHub()
	srv := httptest.NewServer(fake.handler())
	t.Cleanup(srv.Close)

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewGitHubProvider(AppCredentials{AppID: 1, InstallationID: 2, PrivateKey: key}, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	tgt := Target{Repo: "o/r", Branch: "main"}
	files := []File{{Path: "apps/web/instance.yaml", Content: []byte("apiVersion: v1\n")}}

	_, changed, err := p.CommitFiles(context.Background(), tgt, files, "inari: render web")
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("first commit must report changed")
	}
	fake.mu.Lock()
	if fake.refs["heads/main"] == "commit0" {
		t.Error("ref not updated")
	}
	fake.mu.Unlock()

	// Empty change set => no-op (fake returns base tree for empty entries).
	_, changed, err = p.CommitFiles(context.Background(), tgt, nil, "inari: noop")
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatal("empty file set must be a no-op")
	}
}
