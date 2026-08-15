// Package git writes rendered manifests to the platform-owned
// `<tenant>-inari-state` repository (plan §5.3 GitOps decisions).
// Application repos are never touched; authentication is via GitHub App
// credentials delivered through ESO — never PATs, never in git (§12.1/2).
package git

import (
	"context"
	"fmt"
	"strings"
)

// File is a single manifest to write into the target repository.
type File struct {
	// Path is repo-relative (joined under Target.Path when set).
	Path    string
	Content []byte
}

// Target identifies the state repository location for a write.
type Target struct {
	// Repo is "owner/name". Empty means the caller applies the tenant
	// default (`<tenant>-inari-state`).
	Repo string
	// Path is an optional base directory inside the repo.
	Path string
	// Branch is the direct-commit branch or PR base branch.
	Branch string
}

// Validate enforces the platform-owned-repo constraint.
func (t Target) Validate(defaultRepo string) (Target, error) {
	if t.Repo == "" {
		t.Repo = defaultRepo
	}
	if t.Repo == "" {
		return t, fmt.Errorf("git: no target repo and no tenant default configured")
	}
	parts := strings.Split(t.Repo, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return t, fmt.Errorf("git: repo %q must be owner/name", t.Repo)
	}
	if strings.Contains(t.Path, "..") {
		return t, fmt.Errorf("git: path %q must not escape the repository", t.Path)
	}
	if t.Branch == "" {
		t.Branch = "main"
	}
	return t, nil
}

// OwnerName splits Repo into owner and name; caller must Validate first.
func (t Target) OwnerName() (string, string) {
	parts := strings.SplitN(t.Repo, "/", 2)
	return parts[0], parts[1]
}

// FullPath joins the target base path with a file path.
func (t Target) FullPath(p string) string {
	if t.Path == "" {
		return p
	}
	return strings.TrimSuffix(t.Path, "/") + "/" + p
}

// Provider writes rendered manifests to a git host. Implementations must
// be safe for concurrent use and idempotent: committing an unchanged file
// set reports changed=false.
type Provider interface {
	// CommitFiles writes files to Target.Branch and returns the new head
	// SHA. changed is false when the tree already matched.
	CommitFiles(ctx context.Context, target Target, files []File, message string) (sha string, changed bool, err error)
	// OpenPR writes files to a topic branch off Target.Branch and opens a
	// pull request, returning its URL. Re-invoking with the same branch
	// returns the existing open PR.
	OpenPR(ctx context.Context, target Target, files []File, branch, title, body string) (prURL string, err error)
}
