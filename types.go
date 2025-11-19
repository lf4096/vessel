// Package vessel provides a containerized workspace for AI agents, built on containerd to
// run commands, manage snapshots, and plug in custom storage.
package vessel

import (
	"context"
	"io"
	"os"
	"time"
)

// Manager coordinates lifecycle of containerized workspaces.
type Manager interface {
	CreateVessel(ctx context.Context, imageRef string, workDir string) (Vessel, error)
	OpenVessel(ctx context.Context, vesselID string) (Vessel, error)
	DeleteVessel(ctx context.Context, vesselID string) error
	Close() error
}

// ManagerConfig configures how a Manager connects to containerd and storage.
type ManagerConfig struct {
	// ContainerdAddress is the containerd socket path, defaults to `/run/containerd/containerd.sock`
	// or `XDG_RUNTIME_DIR/containerd/containerd.sock` for rootless usage.
	ContainerdAddress string
	// Namespace scopes images and snapshots inside containerd, defaults to "vessel".
	Namespace string
	// Rootless enables fuse-overlayfs and adjusts runtime paths for non-root usage.
	Rootless bool
	// Storage provides snapshot and manifest persistence, defaults to `LocalStorage` with `CompressedStorage` decorator.
	Storage Storage
	// MountRoot is the directory where vessel mounts are created, defaults to `~/.local/share/vessel/mounts`.
	MountRoot string
	// IDGenerator generates unique identifiers for vessels and snapshots, defaults to `UniqueIDGenerator`.
	IDGenerator IDGenerator
}

// Vessel represents a mutable container workspace backed by snapshots.
type Vessel interface {
	ID() string
	GetManifest() *VesselManifest

	OpenFile(path string, flag int, perm os.FileMode) (File, error)
	ReadFile(path string) ([]byte, error)
	WriteFile(path string, data []byte, perm os.FileMode) error
	DeleteFile(path string) error
	RenameFile(oldPath, newPath string) error
	Stat(path string) (os.FileInfo, error)

	MakeDir(path string, perm os.FileMode) error
	MakeDirAll(path string, perm os.FileMode) error
	ReadDir(path string) ([]os.DirEntry, error)

	Exec(ctx context.Context, opts *ExecOptions) (*ExecResult, error)

	CreateSnapshot(ctx context.Context, message string) (*SnapshotManifest, error)
	RestoreSnapshot(ctx context.Context, snapshotID string) error
	DeleteSnapshot(ctx context.Context, snapshotID string) error
	ListSnapshots(ctx context.Context) ([]*SnapshotManifest, error)

	Close() error
}

// VesselManifest tracks the vessel state persisted in storage.
type VesselManifest struct {
	ID                string              `json:"id"`
	BaseImageRef      string              `json:"baseImageRef"`
	CurrentSnapshotID string              `json:"currentSnapshotId"`
	WorkDir           string              `json:"workDir"`
	CreateTime        time.Time           `json:"createTime"`
	UpdateTime        time.Time           `json:"updateTime"`
	Labels            map[string]string   `json:"labels"`
	Snapshots         []*SnapshotManifest `json:"snapshots"`
}

// SnapshotManifest describes an immutable snapshot diff for a vessel.
type SnapshotManifest struct {
	ID         string            `json:"id"`
	VesselID   string            `json:"vesselId"`
	ParentID   string            `json:"parentId"`
	Message    string            `json:"message"`
	Checksum   string            `json:"checksum"`
	Size       int64             `json:"size"`
	CreateTime time.Time         `json:"createTime"`
	Labels     map[string]string `json:"labels"`
}

// File reuses os.File-like semantics for interacting with workspace files.
type File interface {
	io.Closer
	io.Reader
	io.ReaderAt
	io.Writer
	io.WriterAt
	io.Seeker
	Name() string
	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(size int64) error
}

// ExecOptions controls how commands run inside a vessel container.
type ExecOptions struct {
	// Command names the command and arguments to run inside the vessel.
	Command []string
	// WorkDir overrides the working directory inside the vessel.
	WorkDir string
	// Env carries optional environment variables in KEY=VALUE form.
	Env []string
	// Stdin supplies an optional stdin reader.
	Stdin io.Reader
	// Stdout captures stdout; defaults to discard.
	Stdout io.Writer
	// Stderr captures stderr; defaults to discard.
	Stderr io.Writer
	// PersistResult keeps filesystem changes after the command finishes.
	PersistResult bool
}

// ExecResult contains the exit status returned by Exec.
type ExecResult struct {
	// ExitCode is the exit status reported by containerd.
	ExitCode int
}

// Storage abstracts persistence for manifests, layers, and container images.
type Storage interface {
	SaveVesselManifest(ctx context.Context, vesselID string, manifest []byte) error
	LoadVesselManifest(ctx context.Context, vesselID string) ([]byte, error)

	LoadImage(ctx context.Context, imageRef string) (io.ReadCloser, error)

	SaveSnapshot(ctx context.Context, vesselID, snapshotID string, reader io.Reader) error
	LoadSnapshot(ctx context.Context, vesselID, snapshotID string) (io.ReadCloser, error)
	DeleteSnapshot(ctx context.Context, vesselID, snapshotID string) error

	DeleteVessel(ctx context.Context, vesselID string) error
}

// IDGenerator produces unique identifiers for vessels and snapshots.
type IDGenerator interface {
	NewID() string
}
