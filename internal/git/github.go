package git

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/go-github/v68/github"
)

// GitHubClient is the subset of the go-github API the provider uses
// (kept narrow for fakes/tests).
type GitHubClient struct {
	c *github.Client
}

// NewGitHubProvider returns a Provider authenticating as a GitHub App
// installation. apiBase may be empty for github.com (tests pass an
// httptest URL).
func NewGitHubProvider(creds AppCredentials, apiBase string) (Provider, error) {
	src := &appTokenSource{creds: creds}
	gh := github.NewClient(&http.Client{Transport: src.RoundTripper(nil)})
	if err := setAPIBase(gh, apiBase); err != nil {
		return nil, err
	}
	src.newToken = func(ctx context.Context, appJWT string) (string, time.Time, error) {
		appClient := github.NewClient(&http.Client{Transport: &jwtTransport{jwt: appJWT}})
		if err := setAPIBase(appClient, apiBase); err != nil {
			return "", time.Time{}, err
		}
		tok, _, err := appClient.Apps.CreateInstallationToken(ctx, creds.InstallationID, nil)
		if err != nil {
			return "", time.Time{}, fmt.Errorf("git: create installation token: %w", err)
		}
		return tok.GetToken(), tok.GetExpiresAt().Time, nil
	}
	return &gitHubProvider{gh: gh}, nil
}

// setAPIBase points a client at a non-default API base (tests). apiBase
// may be empty for github.com.
func setAPIBase(c *github.Client, apiBase string) error {
	if apiBase == "" {
		return nil
	}
	u, err := url.Parse(apiBase)
	if err != nil {
		return fmt.Errorf("git: bad API base %q: %w", apiBase, err)
	}
	if !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}
	c.BaseURL = u
	c.UploadURL = u
	return nil
}

type jwtTransport struct{ jwt string }

func (t *jwtTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Header.Set("Authorization", "Bearer "+t.jwt)
	clone.Header.Set("Accept", "application/vnd.github+json")
	return http.DefaultTransport.RoundTrip(clone)
}

type gitHubProvider struct {
	gh *github.Client
}

// CommitFiles implements Provider.
func (p *gitHubProvider) CommitFiles(ctx context.Context, target Target, files []File, message string) (string, bool, error) {
	owner, repo := target.OwnerName()
	ref, _, err := p.gh.Git.GetRef(ctx, owner, repo, "heads/"+target.Branch)
	if err != nil {
		return "", false, fmt.Errorf("git: get ref heads/%s in %s: %w", target.Branch, target.Repo, err)
	}
	headSHA := ref.GetObject().GetSHA()
	sha, changed, err := p.commitOnto(ctx, owner, repo, headSHA, target, files, message)
	if err != nil {
		return "", false, err
	}
	if !changed {
		return headSHA, false, nil
	}
	_, _, err = p.gh.Git.UpdateRef(ctx, owner, repo, &github.Reference{
		Ref:    github.Ptr("refs/heads/" + target.Branch),
		Object: &github.GitObject{SHA: github.Ptr(sha)},
	}, false)
	if err != nil {
		return "", false, fmt.Errorf("git: update ref heads/%s in %s: %w", target.Branch, target.Repo, err)
	}
	return sha, true, nil
}

// commitOnto builds a tree over baseSHA and creates a commit; changed is
// false when the resulting tree equals the base tree.
func (p *gitHubProvider) commitOnto(ctx context.Context, owner, repo, baseSHA string, target Target, files []File, message string) (string, bool, error) {
	baseCommit, _, err := p.gh.Git.GetCommit(ctx, owner, repo, baseSHA)
	if err != nil {
		return "", false, fmt.Errorf("git: get commit %s: %w", baseSHA, err)
	}
	entries := make([]*github.TreeEntry, 0, len(files))
	for _, f := range files {
		blob, _, err := p.gh.Git.CreateBlob(ctx, owner, repo, &github.Blob{
			Content:  github.Ptr(string(f.Content)),
			Encoding: github.Ptr("utf-8"),
		})
		if err != nil {
			return "", false, fmt.Errorf("git: create blob for %s: %w", f.Path, err)
		}
		entries = append(entries, &github.TreeEntry{
			Path: github.Ptr(target.FullPath(f.Path)),
			Mode: github.Ptr("100644"),
			Type: github.Ptr("blob"),
			SHA:  blob.SHA,
		})
	}
	tree, _, err := p.gh.Git.CreateTree(ctx, owner, repo, baseCommit.GetTree().GetSHA(), entries)
	if err != nil {
		return "", false, fmt.Errorf("git: create tree: %w", err)
	}
	if tree.GetSHA() == baseCommit.GetTree().GetSHA() {
		return baseSHA, false, nil
	}
	commit, _, err := p.gh.Git.CreateCommit(ctx, owner, repo, &github.Commit{
		Message: github.Ptr(message),
		Tree:    tree,
		Parents: []*github.Commit{{SHA: github.Ptr(baseSHA)}},
	}, nil)
	if err != nil {
		return "", false, fmt.Errorf("git: create commit: %w", err)
	}
	return commit.GetSHA(), true, nil
}

// OpenPR implements Provider.
func (p *gitHubProvider) OpenPR(ctx context.Context, target Target, files []File, branch, title, body string) (string, error) {
	owner, repo := target.OwnerName()
	baseRef, _, err := p.gh.Git.GetRef(ctx, owner, repo, "heads/"+target.Branch)
	if err != nil {
		return "", fmt.Errorf("git: get base ref heads/%s in %s: %w", target.Branch, target.Repo, err)
	}
	topic, _, err := p.gh.Git.GetRef(ctx, owner, repo, "heads/"+branch)
	if err != nil {
		topic, _, err = p.gh.Git.CreateRef(ctx, owner, repo, &github.Reference{
			Ref:    github.Ptr("refs/heads/" + branch),
			Object: &github.GitObject{SHA: baseRef.GetObject().SHA},
		})
		if err != nil {
			return "", fmt.Errorf("git: create topic branch %s: %w", branch, err)
		}
	}
	sha, changed, err := p.commitOnto(ctx, owner, repo, topic.GetObject().GetSHA(), target, files, title)
	if err != nil {
		return "", err
	}
	if changed {
		if _, _, err := p.gh.Git.UpdateRef(ctx, owner, repo, &github.Reference{
			Ref:    github.Ptr("refs/heads/" + branch),
			Object: &github.GitObject{SHA: github.Ptr(sha)},
		}, false); err != nil {
			return "", fmt.Errorf("git: update topic branch %s: %w", branch, err)
		}
	}
	head := owner + ":" + branch
	prs, _, err := p.gh.PullRequests.List(ctx, owner, repo, &github.PullRequestListOptions{
		State: "open",
		Head:  head,
		Base:  target.Branch,
	})
	if err != nil {
		return "", fmt.Errorf("git: list PRs for %s: %w", head, err)
	}
	if len(prs) > 0 {
		return prs[0].GetHTMLURL(), nil
	}
	pr, _, err := p.gh.PullRequests.Create(ctx, owner, repo, &github.NewPullRequest{
		Title: github.Ptr(title),
		Head:  github.Ptr(branch),
		Base:  github.Ptr(target.Branch),
		Body:  github.Ptr(body),
	})
	if err != nil {
		var ghErr *github.ErrorResponse
		if errors.As(err, &ghErr) && ghErr.Response.StatusCode == http.StatusUnprocessableEntity {
			return "", fmt.Errorf("git: open PR %s -> %s: %w (a PR may already exist)", branch, target.Branch, err)
		}
		return "", fmt.Errorf("git: open PR %s -> %s: %w", branch, target.Branch, err)
	}
	return pr.GetHTMLURL(), nil
}

var _ Provider = (*gitHubProvider)(nil)
