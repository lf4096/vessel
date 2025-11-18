package vessel

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// LocalStorage stores vessel metadata and layers on the filesystem.
type LocalStorage struct {
	baseDir   string
	extension string
}

// NewLocalStorage creates a filesystem-backed Storage optionally wrapping with compression.
func NewLocalStorage(baseDir string, compressed bool) (Storage, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}
	extension := ".tar"
	if compressed {
		extension += ".gz"
	}
	var storage Storage = &LocalStorage{baseDir: baseDir, extension: extension}
	if compressed {
		storage = NewCompressedStorage(storage)
	}
	return storage, nil
}

// SaveVesselManifest writes the current vessel manifest to disk.
func (s *LocalStorage) SaveVesselManifest(ctx context.Context, vesselID string, manifest []byte) error {
	vesselDir := filepath.Join(s.baseDir, "vessels", vesselID)
	if err := os.MkdirAll(vesselDir, 0755); err != nil {
		return fmt.Errorf("failed to create vessel directory: %w", err)
	}
	manifestPath := filepath.Join(vesselDir, "vessel.dat")
	if err := os.WriteFile(manifestPath, manifest, 0644); err != nil {
		return fmt.Errorf("failed to write manifest: %w", err)
	}
	return nil
}

// LoadImage loads a persisted container image for import into containerd.
func (s *LocalStorage) LoadImage(ctx context.Context, imageRef string) (io.ReadCloser, error) {
	imagePath := filepath.Join(s.baseDir, "images", imageRef+s.extension)
	file, err := os.Open(imagePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load image %s: %w", imageRef, err)
	}
	return file, nil
}

// LoadVesselManifest returns the serialized manifest bytes for a vessel.
func (s *LocalStorage) LoadVesselManifest(ctx context.Context, vesselID string) ([]byte, error) {
	manifestPath := filepath.Join(s.baseDir, "vessels", vesselID, "vessel.dat")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load manifest: %w", err)
	}
	return data, nil
}

// SaveSnapshot stores a new snapshot diff stream for a vessel.
func (s *LocalStorage) SaveSnapshot(ctx context.Context, vesselID, snapshotID string, reader io.Reader) error {
	snapshotDir := filepath.Join(s.baseDir, "vessels", vesselID, "snapshots")
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return fmt.Errorf("failed to create snapshot directory: %w", err)
	}
	snapshotPath := filepath.Join(snapshotDir, snapshotID+s.extension)
	file, err := os.Create(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to create snapshot file: %w", err)
	}
	defer file.Close()
	if _, err := io.Copy(file, reader); err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}
	return nil
}

// LoadSnapshot opens a stored snapshot diff for replay.
func (s *LocalStorage) LoadSnapshot(ctx context.Context, vesselID, snapshotID string) (io.ReadCloser, error) {
	snapshotPath := filepath.Join(s.baseDir, "vessels", vesselID, "snapshots", snapshotID+s.extension)
	file, err := os.Open(snapshotPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load snapshot %s: %w", snapshotID, err)
	}
	return file, nil
}

// DeleteSnapshot removes a snapshot diff from disk.
func (s *LocalStorage) DeleteSnapshot(ctx context.Context, vesselID, snapshotID string) error {
	snapshotPath := filepath.Join(s.baseDir, "vessels", vesselID, "snapshots", snapshotID+s.extension)
	if err := os.Remove(snapshotPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete snapshot: %w", err)
	}
	return nil
}

// DeleteVessel removes all local data associated with a vessel.
func (s *LocalStorage) DeleteVessel(ctx context.Context, vesselID string) error {
	vesselDir := filepath.Join(s.baseDir, "vessels", vesselID)
	if err := os.RemoveAll(vesselDir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete vessel data: %w", err)
	}
	return nil
}
