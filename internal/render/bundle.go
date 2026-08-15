package render

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"sort"
	"strings"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	gogit "github.com/go-git/go-git/v5"
	gitmem "github.com/go-git/go-git/v5/storage/memory"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/registry/remote"

	"github.com/7K-Inari/inari-agent/internal/git"
)

// BundleSource fetches bundle content referenced by apply-bundle commands
// (plan §5.3): either an OCI artifact (checksum-verified) or a git URL.
// Returns the bundle's file set, minus VCS metadata.
type BundleSource interface {
	Fetch(ctx context.Context, ref BundleRef) ([]git.File, error)
}

// BundleRef is one bundle source variant.
type BundleRef struct {
	OCIRef   string // oci://registry/bundle:v1.2.3
	GitURL   string // git source URL
	Checksum string // expected digest of the fetched file set (optional)
}

// Validate enforces exactly one source.
func (r BundleRef) Validate() error {
	if (r.OCIRef == "") == (r.GitURL == "") {
		return fmt.Errorf("render: bundle needs exactly one of oci_ref or git_url")
	}
	return nil
}

// Fetcher implements BundleSource against real registries/remotes.
type Fetcher struct{}

// NewFetcher returns the default BundleSource.
func NewFetcher() *Fetcher { return &Fetcher{} }

// Fetch implements BundleSource.
func (f *Fetcher) Fetch(ctx context.Context, ref BundleRef) ([]git.File, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	var files []git.File
	var err error
	switch {
	case ref.OCIRef != "":
		files, err = fetchOCI(ctx, ref.OCIRef)
	default:
		files, err = fetchGit(ctx, ref.GitURL)
	}
	if err != nil {
		return nil, err
	}
	if ref.Checksum != "" {
		sum := ChecksumFiles(files)
		if sum != ref.Checksum {
			return nil, fmt.Errorf("render: bundle checksum mismatch: got %s, want %s", sum, ref.Checksum)
		}
	}
	return files, nil
}

// ChecksumFiles is the content digest of a file set (paths + content).
func ChecksumFiles(files []git.File) string {
	sorted := make([]git.File, len(files))
	copy(sorted, files)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Path < sorted[j].Path })
	h := sha256.New()
	for _, f := range sorted {
		h.Write([]byte(f.Path))
		h.Write([]byte{0})
		h.Write(f.Content)
		h.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// fetchOCI pulls an OCI artifact and materializes each layer as a file,
// keyed by its org.opencontainers.image.title annotation.
func fetchOCI(ctx context.Context, ref string) ([]git.File, error) {
	ref = strings.TrimPrefix(ref, "oci://")
	repo, err := remote.NewRepository(ref)
	if err != nil {
		return nil, fmt.Errorf("render: parse OCI ref %q: %w", ref, err)
	}
	desc, err := repo.Resolve(ctx, repo.Reference.Reference)
	if err != nil {
		return nil, fmt.Errorf("render: resolve OCI bundle %q: %w", ref, err)
	}
	manifestBytes, err := content.FetchAll(ctx, repo, desc)
	if err != nil {
		return nil, fmt.Errorf("render: fetch OCI manifest %q: %w", ref, err)
	}
	var manifest ocispec.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return nil, fmt.Errorf("render: decode OCI manifest %q: %w", ref, err)
	}
	files := make([]git.File, 0, len(manifest.Layers))
	for i, layer := range manifest.Layers {
		path := layer.Annotations[ocispec.AnnotationTitle]
		if path == "" {
			path = fmt.Sprintf("layer-%d", i)
		}
		blob, err := content.FetchAll(ctx, repo, layer)
		if err != nil {
			return nil, fmt.Errorf("render: fetch OCI layer %s: %w", layer.Digest, err)
		}
		files = append(files, git.File{Path: path, Content: blob})
	}
	return files, nil
}

// fetchGit shallow-clones a bundle repo into memory and reads its files.
func fetchGit(ctx context.Context, url string) ([]git.File, error) {
	fsys := memfs.New()
	if _, err := gogit.CloneContext(ctx, gitmem.NewStorage(), fsys, &gogit.CloneOptions{
		URL:   url,
		Depth: 1,
	}); err != nil {
		return nil, fmt.Errorf("render: clone bundle %q: %w", url, err)
	}
	var files []git.File
	err := walkBilly(fsys, "/", func(path string, r io.Reader) error {
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		files = append(files, git.File{Path: strings.TrimPrefix(path, "/"), Content: data})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("render: read bundle %q: %w", url, err)
	}
	return files, nil
}

// walkBilly visits every regular file, skipping VCS metadata.
func walkBilly(fsys billy.Filesystem, dir string, visit func(path string, r io.Reader) error) error {
	infos, err := fsys.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, info := range infos {
		path := fsys.Join(dir, info.Name())
		if info.IsDir() {
			if info.Name() == ".git" {
				continue
			}
			if err := walkBilly(fsys, path, visit); err != nil {
				return err
			}
			continue
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			continue
		}
		f, err := fsys.Open(path)
		if err != nil {
			return err
		}
		err = visit(path, f)
		_ = f.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
