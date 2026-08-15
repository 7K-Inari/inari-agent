package git

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
)

// MemProvider is an in-memory Provider fake for tests.
type MemProvider struct {
	mu      sync.Mutex
	Commits []CommitRecord
	PRs     []PRRecord
	// Err forces the next call to fail (then clears).
	Err error
}

// CommitRecord captures a CommitFiles invocation.
type CommitRecord struct {
	Target  Target
	Files   []File
	Message string
	SHA     string
	Changed bool
}

// PRRecord captures an OpenPR invocation.
type PRRecord struct {
	Target Target
	Files  []File
	Branch string
	Title  string
	Body   string
	URL    string
}

// NewMemProvider returns an empty in-memory fake.
func NewMemProvider() *MemProvider { return &MemProvider{} }

// CommitFiles implements Provider.
func (m *MemProvider) CommitFiles(_ context.Context, target Target, files []File, message string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		err := m.Err
		m.Err = nil
		return "", false, err
	}
	sum := hashFiles(files)
	changed := true
	if n := len(m.Commits); n > 0 && m.Commits[n-1].Target == target && m.Commits[n-1].SHA == sum {
		changed = false
	}
	m.Commits = append(m.Commits, CommitRecord{Target: target, Files: files, Message: message, SHA: sum, Changed: changed})
	return sum, changed, nil
}

// OpenPR implements Provider.
func (m *MemProvider) OpenPR(_ context.Context, target Target, files []File, branch, title, body string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		err := m.Err
		m.Err = nil
		return "", err
	}
	for _, pr := range m.PRs {
		if pr.Target == target && pr.Branch == branch {
			return pr.URL, nil
		}
	}
	url := fmt.Sprintf("https://example.invalid/%s/pull/%d", target.Repo, len(m.PRs)+1)
	m.PRs = append(m.PRs, PRRecord{Target: target, Files: files, Branch: branch, Title: title, Body: body, URL: url})
	return url, nil
}

func hashFiles(files []File) string {
	paths := make([]string, 0, len(files))
	byPath := map[string][]byte{}
	for _, f := range files {
		paths = append(paths, f.Path)
		byPath[f.Path] = f.Content
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, p := range paths {
		h.Write([]byte(p))
		h.Write([]byte{0})
		h.Write(byPath[p])
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

var _ Provider = (*MemProvider)(nil)
