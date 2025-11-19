package vessel

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/diff"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type vessel struct {
	id                 string
	manifest           *VesselManifest
	manager            *manager
	lease              leases.Lease
	baseSnapshotKey    string
	currentSnapshotKey string
	currentMounts      []mount.Mount
	mountPoint         string
}

func newVessel(ctx context.Context, vesselID, imageRef, workDir string, m *manager) (Vessel, error) {
	if workDir == "" {
		workDir = "/"
	} else {
		workDir = filepath.Clean(workDir)
		if !strings.HasPrefix(workDir, "/") {
			workDir = "/"
		}
	}

	var err error
	v := &vessel{
		id:      vesselID,
		manager: m,
	}

	leaseID := fmt.Sprintf("lease-%s-%s", vesselID, m.idGenerator.NewID())
	v.lease, err = m.client.LeasesService().Create(ctx, leases.WithID(leaseID))
	if err != nil {
		return nil, fmt.Errorf("failed to create lease: %w", err)
	}
	ctx = v.withLease(ctx)
	defer func() {
		if err != nil {
			v.Close()
		}
	}()

	v.baseSnapshotKey, err = m.getImage(ctx, imageRef)
	if err != nil {
		return nil, fmt.Errorf("failed to get image snapshot: %w", err)
	}

	v.manifest = &VesselManifest{
		ID:                vesselID,
		BaseImageRef:      imageRef,
		CurrentSnapshotID: "",
		WorkDir:           workDir,
		CreateTime:        time.Now(),
		UpdateTime:        time.Now(),
		Labels:            make(map[string]string),
		Snapshots:         []*SnapshotManifest{},
	}
	err = v.saveManifest(ctx)
	if err != nil {
		return nil, err
	}

	if err = v.createNewLayer(ctx, v.baseSnapshotKey); err != nil {
		return nil, err
	}
	return v, nil
}

func openVessel(ctx context.Context, vesselID string, m *manager) (Vessel, error) {
	var err error
	v := &vessel{
		id:       vesselID,
		manifest: &VesselManifest{},
		manager:  m,
	}

	manifestBytes, err := m.storage.LoadVesselManifest(ctx, vesselID)
	if err != nil {
		return nil, fmt.Errorf("failed to load manifest: %w", err)
	}
	if err = json.Unmarshal(manifestBytes, v.manifest); err != nil {
		return nil, fmt.Errorf("failed to unmarshal manifest: %w", err)
	}

	leaseID := fmt.Sprintf("lease-%s-%s", vesselID, m.idGenerator.NewID())
	v.lease, err = m.client.LeasesService().Create(ctx, leases.WithID(leaseID))
	if err != nil {
		return nil, fmt.Errorf("failed to create lease: %w", err)
	}
	ctx = v.withLease(ctx)
	defer func() {
		if err != nil {
			v.Close()
		}
	}()

	v.baseSnapshotKey, err = m.getImage(ctx, v.manifest.BaseImageRef)
	if err != nil {
		return nil, fmt.Errorf("base image not found: %s", v.manifest.BaseImageRef)
	}

	var parentSnapshotKey string
	if v.manifest.CurrentSnapshotID == "" {
		parentSnapshotKey = v.baseSnapshotKey
	} else {
		if err := v.ensureSnapshotChainExists(ctx, v.manifest.CurrentSnapshotID); err != nil {
			return nil, fmt.Errorf("failed to ensure snapshot chain exists: %w", err)
		}
		parentSnapshotKey = v.manifest.CurrentSnapshotID
	}

	if err := v.createNewLayer(ctx, parentSnapshotKey); err != nil {
		return nil, err
	}
	return v, nil
}

// ID returns the vessel identifier.
func (v *vessel) ID() string {
	return v.id
}

// GetManifest returns a defensive copy of the vessel manifest and snapshots.
func (v *vessel) GetManifest() *VesselManifest {
	labels := make(map[string]string, len(v.manifest.Labels))
	maps.Copy(labels, v.manifest.Labels)
	snapshots, _ := v.ListSnapshots(context.Background())
	manifest := &VesselManifest{
		ID:                v.id,
		BaseImageRef:      v.manifest.BaseImageRef,
		CurrentSnapshotID: v.manifest.CurrentSnapshotID,
		WorkDir:           v.manifest.WorkDir,
		CreateTime:        v.manifest.CreateTime,
		UpdateTime:        v.manifest.UpdateTime,
		Labels:            labels,
		Snapshots:         snapshots,
	}
	return manifest
}

// OpenFile opens a file inside the vessel workspace.
func (v *vessel) OpenFile(path string, flag int, perm os.FileMode) (File, error) {
	fullPath := v.resolvePath(path)
	file, err := os.OpenFile(fullPath, flag, perm)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	return file, nil
}

// ReadFile reads a file inside the vessel workspace.
func (v *vessel) ReadFile(path string) ([]byte, error) {
	fullPath := v.resolvePath(path)
	data, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	return data, nil
}

// WriteFile writes bytes to a file inside the vessel workspace.
func (v *vessel) WriteFile(path string, data []byte, perm os.FileMode) error {
	fullPath := v.resolvePath(path)
	if err := os.WriteFile(fullPath, data, perm); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}
	return nil
}

// DeleteFile removes a file inside the vessel workspace.
func (v *vessel) DeleteFile(path string) error {
	fullPath := v.resolvePath(path)
	if err := os.Remove(fullPath); err != nil {
		return fmt.Errorf("failed to delete file: %w", err)
	}
	return nil
}

// RenameFile renames or moves a file inside the vessel workspace.
func (v *vessel) RenameFile(oldPath, newPath string) error {
	fullOldPath := v.resolvePath(oldPath)
	fullNewPath := v.resolvePath(newPath)
	if err := os.Rename(fullOldPath, fullNewPath); err != nil {
		return fmt.Errorf("failed to rename file: %w", err)
	}
	return nil
}

// Stat retrieves metadata for a path inside the vessel workspace.
func (v *vessel) Stat(path string) (os.FileInfo, error) {
	fullPath := v.resolvePath(path)
	info, err := os.Stat(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}
	return info, nil
}

// MakeDir creates a directory inside the vessel workspace.
func (v *vessel) MakeDir(path string, perm os.FileMode) error {
	fullPath := v.resolvePath(path)
	if err := os.Mkdir(fullPath, perm); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	return nil
}

// MakeDirAll creates nested directories inside the vessel workspace.
func (v *vessel) MakeDirAll(path string, perm os.FileMode) error {
	fullPath := v.resolvePath(path)
	if err := os.MkdirAll(fullPath, perm); err != nil {
		return fmt.Errorf("failed to create directories: %w", err)
	}
	return nil
}

// ReadDir lists entries inside the vessel workspace.
func (v *vessel) ReadDir(path string) ([]os.DirEntry, error) {
	fullPath := v.resolvePath(path)
	entries, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}
	return entries, nil
}

// Exec spawns a one-off container process rooted at the vessel workspace.
func (v *vessel) Exec(ctx context.Context, opts *ExecOptions) (*ExecResult, error) {
	ctx = v.withLease(ctx)
	if opts == nil || len(opts.Command) == 0 {
		return nil, fmt.Errorf("command is required")
	}

	execID := v.manager.idGenerator.NewID()
	if !opts.PersistResult {
		execBaseSnapshotKey := v.manager.idGenerator.NewID()
		if err := v.manager.snapshotter.Commit(ctx, execBaseSnapshotKey, v.currentSnapshotKey); err != nil {
			return nil, fmt.Errorf("failed to commit temporary snapshot: %w", err)
		}
		v.currentSnapshotKey = ""
		v.createNewLayer(ctx, execBaseSnapshotKey)
		defer v.createNewLayer(ctx, execBaseSnapshotKey)
	}

	workDir := opts.WorkDir
	if workDir == "" {
		workDir = v.manifest.WorkDir
	}
	if err := v.ensureDirExists(workDir); err != nil {
		return nil, fmt.Errorf("failed to ensure workspace directory: %w", err)
	}

	containerID := fmt.Sprintf("vessel-%s-exec-%s", v.id, execID)
	specOpts := []oci.SpecOpts{
		oci.WithDefaultSpec(),
		oci.WithDefaultPathEnv,
		oci.WithEnv(opts.Env),
		oci.WithoutRunMount,
		oci.WithRootFSPath(v.mountPoint),
		oci.WithCgroup(""),
		oci.WithProcessCwd(workDir),
		oci.WithProcessArgs(opts.Command...),
	}
	container, err := v.manager.client.NewContainer(
		ctx,
		containerID,
		client.WithNewSpec(specOpts...),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create container: %w", err)
	}
	defer container.Delete(ctx)

	stdin := opts.Stdin
	if stdin == nil {
		stdin = io.NopCloser(strings.NewReader(""))
	}
	stdout := opts.Stdout
	if stdout == nil {
		stdout = io.Discard
	}
	stderr := opts.Stderr
	if stderr == nil {
		stderr = io.Discard
	}

	fifoDir := filepath.Join(v.manager.runRoot, "fifo")
	ioCreator := cio.NewCreator(
		cio.WithStreams(stdin, stdout, stderr),
		cio.WithFIFODir(fifoDir),
	)
	task, err := container.NewTask(ctx, ioCreator)
	if err != nil {
		return nil, fmt.Errorf("failed to create task: %w", err)
	}
	defer task.Delete(ctx)
	exitStatusC, err := task.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to wait for task: %w", err)
	}
	if err := task.Start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start task: %w", err)
	}

	select {
	case <-ctx.Done():
		task.Kill(ctx, syscall.SIGKILL)
		<-exitStatusC
		return &ExecResult{ExitCode: 137}, ctx.Err()
	case status := <-exitStatusC:
		code, _, err := status.Result()
		if err != nil {
			return &ExecResult{ExitCode: int(code)}, err
		}
		return &ExecResult{ExitCode: int(code)}, nil
	}
}

// CreateSnapshot commits the current workspace changes and persists the diff.
func (v *vessel) CreateSnapshot(ctx context.Context, message string) (*SnapshotManifest, error) {
	ctx = v.withLease(ctx)
	snapshotID := v.manager.idGenerator.NewID()
	parentID := v.manifest.CurrentSnapshotID
	if parentID == "" {
		parentID = v.baseSnapshotKey
	}

	parentViewKey := parentID + "-view"
	parentMounts, err := v.manager.snapshotter.View(ctx, parentViewKey, parentID)
	if err != nil {
		return nil, fmt.Errorf("failed to create parent view: %w", err)
	}
	defer v.manager.snapshotter.Remove(ctx, parentViewKey)

	differ := v.manager.client.DiffService()
	diffDesc, err := differ.Compare(
		ctx,
		parentMounts,
		v.currentMounts,
		diff.WithMediaType(ocispec.MediaTypeImageLayer),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to generate diff: %w", err)
	}
	diffReader, err := v.manager.client.ContentStore().ReaderAt(ctx, diffDesc)
	if err != nil {
		return nil, fmt.Errorf("failed to read diff: %w", err)
	}
	defer diffReader.Close()
	reader := io.NewSectionReader(diffReader, 0, diffDesc.Size)
	if err := v.manager.storage.SaveSnapshot(ctx, v.id, snapshotID, reader); err != nil {
		return nil, fmt.Errorf("failed to save snapshot: %w", err)
	}

	if err := v.manager.snapshotter.Commit(ctx, snapshotID, v.currentSnapshotKey); err != nil {
		return nil, fmt.Errorf("failed to commit working snapshot: %w", err)
	}
	v.currentSnapshotKey = ""

	snapshot := &SnapshotManifest{
		ID:         snapshotID,
		VesselID:   v.id,
		ParentID:   v.manifest.CurrentSnapshotID,
		Message:    message,
		Checksum:   diffDesc.Digest.String(),
		Size:       diffDesc.Size,
		CreateTime: time.Now(),
		Labels:     make(map[string]string),
	}
	v.manifest.Snapshots = append(v.manifest.Snapshots, snapshot)
	v.manifest.CurrentSnapshotID = snapshotID
	if err := v.saveManifest(ctx); err != nil {
		return nil, fmt.Errorf("failed to save manifest: %w", err)
	}

	if err := v.createNewLayer(ctx, snapshotID); err != nil {
		return nil, fmt.Errorf("failed to rebuild working layer: %w", err)
	}

	return snapshot, nil
}

// RestoreSnapshot rebuilds the workspace from a given snapshot.
func (v *vessel) RestoreSnapshot(ctx context.Context, snapshotID string) error {
	ctx = v.withLease(ctx)

	if v.findSnapshot(snapshotID) == nil {
		return fmt.Errorf("snapshot not found: %s", snapshotID)
	}

	if err := v.ensureSnapshotChainExists(ctx, snapshotID); err != nil {
		return fmt.Errorf("failed to ensure snapshot chain exists: %w", err)
	}

	v.manifest.CurrentSnapshotID = snapshotID
	if err := v.saveManifest(ctx); err != nil {
		return fmt.Errorf("failed to save manifest: %w", err)
	}

	if err := v.createNewLayer(ctx, snapshotID); err != nil {
		return fmt.Errorf("failed to rebuild working layer: %w", err)
	}

	return nil
}

// DeleteSnapshot removes a stored snapshot.
func (v *vessel) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	ctx = v.withLease(ctx)

	snapshot := v.findSnapshot(snapshotID)
	if snapshot == nil {
		return fmt.Errorf("snapshot not found: %s", snapshotID)
	}
	for _, snap := range v.manifest.Snapshots {
		if snap.ParentID == snapshotID {
			return fmt.Errorf("cannot delete snapshot with children: %s", snapshotID)
		}
	}

	if v.manifest.CurrentSnapshotID == snapshotID {
		if snapshot.ParentID == "" {
			return fmt.Errorf("cannot delete the only snapshot")
		} else {
			if err := v.RestoreSnapshot(ctx, snapshot.ParentID); err != nil {
				return fmt.Errorf("failed to restore parent snapshot: %w", err)
			}
		}
	}

	if _, err := v.manager.snapshotter.Stat(ctx, snapshotID); err == nil {
		if err := v.manager.snapshotter.Remove(ctx, snapshotID); err != nil {
			return fmt.Errorf("failed to delete snapshot from containerd: %w", err)
		}
	}

	if err := v.manager.storage.DeleteSnapshot(ctx, v.id, snapshotID); err != nil {
		return fmt.Errorf("failed to delete snapshot from storage: %w", err)
	}

	for i, snap := range v.manifest.Snapshots {
		if snap.ID == snapshotID {
			v.manifest.Snapshots = append(v.manifest.Snapshots[:i], v.manifest.Snapshots[i+1:]...)
			break
		}
	}

	if err := v.saveManifest(ctx); err != nil {
		return fmt.Errorf("failed to save manifest: %w", err)
	}

	return nil
}

// ListSnapshots returns all snapshot manifests owned by the vessel.
func (v *vessel) ListSnapshots(ctx context.Context) ([]*SnapshotManifest, error) {
	result := make([]*SnapshotManifest, len(v.manifest.Snapshots))
	for i, snap := range v.manifest.Snapshots {
		labels := make(map[string]string, len(snap.Labels))
		maps.Copy(labels, snap.Labels)
		result[i] = &SnapshotManifest{
			ID:         snap.ID,
			VesselID:   snap.VesselID,
			ParentID:   snap.ParentID,
			Message:    snap.Message,
			Checksum:   snap.Checksum,
			Size:       snap.Size,
			CreateTime: snap.CreateTime,
			Labels:     labels,
		}
	}
	return result, nil
}

// Close tears down mounts and releases the containerd lease.
func (v *vessel) Close() error {
	ctx := v.withLease(context.Background())
	v.unmountAndCleanup(ctx)
	if err := v.manager.client.LeasesService().Delete(ctx, v.lease); err != nil {
		return fmt.Errorf("failed to delete lease: %w", err)
	}
	return nil
}

func (v *vessel) saveManifest(ctx context.Context) error {
	v.manifest.UpdateTime = time.Now()
	manifestBytes, err := json.Marshal(v.manifest)
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}
	if err := v.manager.storage.SaveVesselManifest(ctx, v.id, manifestBytes); err != nil {
		return fmt.Errorf("failed to save manifest: %w", err)
	}
	return nil
}

func (v *vessel) withLease(ctx context.Context) context.Context {
	return leases.WithLease(ctx, v.lease.ID)
}

func (v *vessel) createNewLayer(ctx context.Context, parentSnapshotKey string) error {
	if v.currentSnapshotKey != "" {
		v.unmountAndCleanup(ctx)
	}

	newSnapshotKey := v.manager.idGenerator.NewID()
	mounts, err := v.manager.snapshotter.Prepare(ctx, newSnapshotKey, parentSnapshotKey)
	if err != nil {
		return fmt.Errorf("failed to prepare snapshot: %w", err)
	}

	mountPoint := filepath.Join(v.manager.mountRoot, newSnapshotKey)
	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		v.manager.snapshotter.Remove(ctx, newSnapshotKey)
		return fmt.Errorf("failed to create mount point: %w", err)
	}

	if err := mount.All(mounts, mountPoint); err != nil {
		os.RemoveAll(mountPoint)
		v.manager.snapshotter.Remove(ctx, newSnapshotKey)
		return fmt.Errorf("failed to mount snapshot: %w", err)
	}

	v.currentSnapshotKey = newSnapshotKey
	v.currentMounts = mounts
	v.mountPoint = mountPoint

	if parentSnapshotKey == v.baseSnapshotKey {
		if err := v.ensureDirExists(v.manifest.WorkDir); err != nil {
			return fmt.Errorf("failed to ensure workdir: %w", err)
		}
	}
	return nil
}

func (v *vessel) ensureSnapshotChainExists(ctx context.Context, targetSnapshotID string) error {
	chain, err := v.findSnapshotChain(targetSnapshotID)
	if err != nil {
		return err
	}

	differ := v.manager.client.DiffService()
	parentSnapshotKey := v.baseSnapshotKey

	for _, snap := range chain {
		if _, err := v.manager.snapshotter.Stat(ctx, snap.ID); err == nil {
			parentSnapshotKey = snap.ID
			continue
		}

		diffReader, err := v.manager.storage.LoadSnapshot(ctx, v.id, snap.ID)
		if err != nil {
			return fmt.Errorf("failed to load snapshot %s: %w", snap.ID, err)
		}

		expectedDigest, err := digest.Parse(snap.Checksum)
		if err != nil {
			diffReader.Close()
			return fmt.Errorf("failed to parse checksum for snapshot %s: %w", snap.ID, err)
		}
		writer, err := v.manager.client.ContentStore().Writer(ctx, content.WithRef(snap.ID), content.WithDescriptor(ocispec.Descriptor{Digest: expectedDigest}))
		if err != nil {
			diffReader.Close()
			return fmt.Errorf("failed to create content writer for snapshot %s: %w", snap.ID, err)
		}
		if _, err := io.Copy(writer, diffReader); err != nil {
			diffReader.Close()
			writer.Close()
			return fmt.Errorf("failed to write diff content for snapshot %s: %w", snap.ID, err)
		}
		diffReader.Close()
		if err := writer.Commit(ctx, snap.Size, expectedDigest); err != nil {
			writer.Close()
			return fmt.Errorf("failed to commit diff content for snapshot %s (verification failed): %w", snap.ID, err)
		}
		writer.Close()

		diffDesc := ocispec.Descriptor{
			MediaType: "application/vnd.oci.image.layer.v1.tar",
			Digest:    expectedDigest,
			Size:      snap.Size,
		}
		tmpKey := snap.ID + "-temp"
		mounts, err := v.manager.snapshotter.Prepare(ctx, tmpKey, parentSnapshotKey)
		if err != nil {
			return fmt.Errorf("failed to prepare snapshot: %w", err)
		}
		if _, err := differ.Apply(ctx, diffDesc, mounts); err != nil {
			v.manager.snapshotter.Remove(ctx, tmpKey)
			return fmt.Errorf("failed to apply diff for snapshot %s: %w", snap.ID, err)
		}
		if err := v.manager.snapshotter.Commit(ctx, snap.ID, tmpKey); err != nil {
			v.manager.snapshotter.Remove(ctx, tmpKey)
			return fmt.Errorf("failed to commit snapshot %s: %w", snap.ID, err)
		}

		parentSnapshotKey = snap.ID
	}

	return nil
}

func (v *vessel) findSnapshotChain(targetSnapshotID string) ([]*SnapshotManifest, error) {
	snapshotMap := make(map[string]*SnapshotManifest, len(v.manifest.Snapshots))
	for _, snap := range v.manifest.Snapshots {
		snapshotMap[snap.ID] = snap
	}

	var chain []*SnapshotManifest
	currentID := targetSnapshotID
	for currentID != "" {
		snap, ok := snapshotMap[currentID]
		if !ok {
			return nil, fmt.Errorf("snapshot not found in chain: %s", currentID)
		}
		chain = append(chain, snap)
		currentID = snap.ParentID
	}
	slices.Reverse(chain)
	return chain, nil
}

func (v *vessel) findSnapshot(snapshotID string) *SnapshotManifest {
	for _, snap := range v.manifest.Snapshots {
		if snap.ID == snapshotID {
			return snap
		}
	}
	return nil
}

func (v *vessel) unmountAndCleanup(ctx context.Context) {
	if v.mountPoint != "" {
		mount.UnmountAll(v.mountPoint, 0)
		os.Remove(v.mountPoint)
		v.currentMounts = nil
		v.mountPoint = ""
	}
	if v.currentSnapshotKey != "" {
		v.manager.snapshotter.Remove(ctx, v.currentSnapshotKey)
		v.currentSnapshotKey = ""
	}
}

// resolvePath normalizes a user supplied path to its location inside the vessel workspace.
func (v *vessel) resolvePath(path string) string {
	if !strings.HasPrefix(path, "/") {
		path = filepath.Join(v.manifest.WorkDir, path)
	}
	cleanPath := filepath.Clean(path)
	cleanPath = strings.TrimPrefix(cleanPath, "/")
	return filepath.Join(v.mountPoint, cleanPath)
}

// ensureDirExists creates the directory inside the vessel workspace if it is missing.
func (v *vessel) ensureDirExists(path string) error {
	if path == "" {
		return fmt.Errorf("path is empty")
	}
	stat, err := v.Stat(path)
	if err == nil {
		if !stat.IsDir() {
			return fmt.Errorf("path is not a directory: %s", path)
		}
		return nil
	}
	if err := v.MakeDirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	return nil
}
