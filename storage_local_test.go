package vessel

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/gzip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLocalStorage(t *testing.T) {
	tests := []struct {
		name       string
		compressed bool
	}{
		{"Uncompressed", false},
		{"Compressed", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir := t.TempDir()

			storage, err := NewLocalStorage(baseDir, tt.compressed)
			require.NoError(t, err, "Storage creation should succeed")
			require.NotNil(t, storage, "Storage should not be nil")

			assert.DirExists(t, baseDir, "Base directory should exist")
		})
	}
}

func TestLocalStorage_VesselManifest(t *testing.T) {
	tests := []struct {
		name       string
		compressed bool
	}{
		{"Uncompressed", false},
		{"Compressed", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			baseDir := t.TempDir()

			storage, err := NewLocalStorage(baseDir, tt.compressed)
			require.NoError(t, err)

			vesselID := "test-vessel-001"
			manifestData := []byte(`{"id":"test-vessel-001","baseImageRef":"ubuntu:20.04"}`)

			t.Run("SaveAndLoad", func(t *testing.T) {
				err := storage.SaveVesselManifest(ctx, vesselID, manifestData)
				require.NoError(t, err, "Save manifest should succeed")

				manifestPath := filepath.Join(baseDir, "vessels", vesselID, "vessel.dat")
				assert.FileExists(t, manifestPath, "Manifest file should exist")

				loadedData, err := storage.LoadVesselManifest(ctx, vesselID)
				require.NoError(t, err, "Load manifest should succeed")
				assert.Equal(t, manifestData, loadedData, "Loaded manifest should match saved data")
			})

			t.Run("LoadAfterDelete", func(t *testing.T) {
				err := storage.SaveVesselManifest(ctx, vesselID, manifestData)
				require.NoError(t, err)

				err = storage.DeleteVessel(ctx, vesselID)
				require.NoError(t, err, "Delete vessel should succeed")

				_, err = storage.LoadVesselManifest(ctx, vesselID)
				assert.Error(t, err, "Load manifest after delete should fail")
			})
		})
	}
}

func TestLocalStorage_LoadImage(t *testing.T) {
	tests := []struct {
		name       string
		compressed bool
	}{
		{"Uncompressed", false},
		{"Compressed", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			baseDir := t.TempDir()

			storage, err := NewLocalStorage(baseDir, tt.compressed)
			require.NoError(t, err)

			imageRef := "ubuntu:20.04"
			testData := []byte("This is test image data representing a container image tar file.")

			extension := ".tar"
			if tt.compressed {
				extension = ".tar.gz"
			}

			t.Run("LoadExistingImage", func(t *testing.T) {
				imageDir := filepath.Join(baseDir, "images")
				err := os.MkdirAll(imageDir, 0755)
				require.NoError(t, err)

				imagePath := filepath.Join(imageDir, imageRef+extension)

				if tt.compressed {
					file, err := os.Create(imagePath)
					require.NoError(t, err)

					gzWriter := gzip.NewWriter(file)
					_, err = gzWriter.Write(testData)
					require.NoError(t, err)
					err = gzWriter.Close()
					require.NoError(t, err)
					err = file.Close()
					require.NoError(t, err)
				} else {
					err = os.WriteFile(imagePath, testData, 0644)
					require.NoError(t, err)
				}

				reader, err := storage.LoadImage(ctx, imageRef)
				require.NoError(t, err, "Load image should succeed")
				defer reader.Close()

				loadedData, err := io.ReadAll(reader)
				require.NoError(t, err)
				assert.Equal(t, testData, loadedData, "Loaded image data should match")
			})

			t.Run("LoadNonExistentImage", func(t *testing.T) {
				_, err := storage.LoadImage(ctx, "non-existent-image:latest")
				assert.Error(t, err, "Loading non-existent image should fail")
			})
		})
	}
}

func TestLocalStorage_Snapshot(t *testing.T) {
	tests := []struct {
		name       string
		compressed bool
	}{
		{"Uncompressed", false},
		{"Compressed", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			baseDir := t.TempDir()

			storage, err := NewLocalStorage(baseDir, tt.compressed)
			require.NoError(t, err)

			vesselID := "test-vessel-002"
			snapshotID := "snapshot-001"
			testData := []byte("This is test snapshot data with some content to make it compressible. " +
				"Repeating data helps compression: test test test test test.")

			t.Run("SaveAndLoad", func(t *testing.T) {
				reader := bytes.NewReader(testData)
				err := storage.SaveSnapshot(ctx, vesselID, snapshotID, reader)
				require.NoError(t, err, "Save snapshot should succeed")

				extension := ".tar"
				if tt.compressed {
					extension = ".tar.gz"
				}
				snapshotPath := filepath.Join(baseDir, "vessels", vesselID, "snapshots", snapshotID+extension)
				assert.FileExists(t, snapshotPath, "Snapshot file should exist")

				loadedReader, err := storage.LoadSnapshot(ctx, vesselID, snapshotID)
				require.NoError(t, err, "Load snapshot should succeed")
				defer loadedReader.Close()

				loadedData, err := io.ReadAll(loadedReader)
				require.NoError(t, err, "Reading snapshot should succeed")
				assert.Equal(t, testData, loadedData, "Loaded snapshot should match saved data")
			})

			t.Run("LoadAfterDelete", func(t *testing.T) {
				reader := bytes.NewReader(testData)
				err := storage.SaveSnapshot(ctx, vesselID, snapshotID, reader)
				require.NoError(t, err)

				err = storage.DeleteSnapshot(ctx, vesselID, snapshotID)
				require.NoError(t, err, "Delete snapshot should succeed")

				_, err = storage.LoadSnapshot(ctx, vesselID, snapshotID)
				assert.Error(t, err, "Load snapshot after delete should fail")
			})

			t.Run("MultipleSnapshots", func(t *testing.T) {
				snapshots := map[string][]byte{
					"snap-1": []byte("Snapshot 1 data"),
					"snap-2": []byte("Snapshot 2 data"),
					"snap-3": []byte("Snapshot 3 data"),
				}

				for snapID, data := range snapshots {
					reader := bytes.NewReader(data)
					err := storage.SaveSnapshot(ctx, vesselID, snapID, reader)
					require.NoError(t, err, "Save snapshot %s should succeed", snapID)
				}

				for snapID, expectedData := range snapshots {
					loadedReader, err := storage.LoadSnapshot(ctx, vesselID, snapID)
					require.NoError(t, err, "Load snapshot %s should succeed", snapID)

					loadedData, err := io.ReadAll(loadedReader)
					loadedReader.Close()
					require.NoError(t, err)
					assert.Equal(t, expectedData, loadedData, "Snapshot %s data should match", snapID)
				}
			})

			if tt.compressed {
				t.Run("CompressionVerification", func(t *testing.T) {
					compressedBaseDir := t.TempDir()
					uncompressedBaseDir := t.TempDir()

					compressedStorage, err := NewLocalStorage(compressedBaseDir, true)
					require.NoError(t, err)

					uncompressedStorage, err := NewLocalStorage(uncompressedBaseDir, false)
					require.NoError(t, err)

					largeData := bytes.Repeat([]byte("test data for compression "), 1000)

					err = compressedStorage.SaveSnapshot(ctx, vesselID, snapshotID, bytes.NewReader(largeData))
					require.NoError(t, err)

					err = uncompressedStorage.SaveSnapshot(ctx, vesselID, snapshotID, bytes.NewReader(largeData))
					require.NoError(t, err)

					compressedPath := filepath.Join(compressedBaseDir, "vessels", vesselID, "snapshots", snapshotID+".tar.gz")
					uncompressedPath := filepath.Join(uncompressedBaseDir, "vessels", vesselID, "snapshots", snapshotID+".tar")

					compressedInfo, err := os.Stat(compressedPath)
					require.NoError(t, err)

					uncompressedInfo, err := os.Stat(uncompressedPath)
					require.NoError(t, err)

					assert.Less(t, compressedInfo.Size(), uncompressedInfo.Size(),
						"Compressed file should be smaller than uncompressed")
				})
			}
		})
	}
}

func TestLocalStorage_DeleteVessel(t *testing.T) {
	tests := []struct {
		name       string
		compressed bool
	}{
		{"Uncompressed", false},
		{"Compressed", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			baseDir := t.TempDir()

			storage, err := NewLocalStorage(baseDir, tt.compressed)
			require.NoError(t, err)

			vesselID := "test-vessel-003"
			manifestData := []byte(`{"id":"test-vessel-003"}`)

			err = storage.SaveVesselManifest(ctx, vesselID, manifestData)
			require.NoError(t, err)

			snapshots := []string{"snap-1", "snap-2", "snap-3"}
			for _, snapID := range snapshots {
				data := []byte("snapshot data for " + snapID)
				err = storage.SaveSnapshot(ctx, vesselID, snapID, bytes.NewReader(data))
				require.NoError(t, err)
			}

			vesselDir := filepath.Join(baseDir, "vessels", vesselID)
			assert.DirExists(t, vesselDir, "Vessel directory should exist before deletion")

			err = storage.DeleteVessel(ctx, vesselID)
			require.NoError(t, err, "Delete vessel should succeed")

			assert.NoDirExists(t, vesselDir, "Vessel directory should not exist after deletion")

			_, err = storage.LoadVesselManifest(ctx, vesselID)
			assert.Error(t, err, "Loading manifest after vessel deletion should fail")

			for _, snapID := range snapshots {
				_, err = storage.LoadSnapshot(ctx, vesselID, snapID)
				assert.Error(t, err, "Loading snapshot %s after vessel deletion should fail", snapID)
			}
		})
	}
}

func TestLocalStorage_DeleteIdempotent(t *testing.T) {
	tests := []struct {
		name       string
		compressed bool
	}{
		{"Uncompressed", false},
		{"Compressed", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			baseDir := t.TempDir()

			storage, err := NewLocalStorage(baseDir, tt.compressed)
			require.NoError(t, err)

			t.Run("DeleteNonExistentSnapshot", func(t *testing.T) {
				err := storage.DeleteSnapshot(ctx, "non-existent-vessel", "non-existent-snapshot")
				assert.NoError(t, err, "Deleting non-existent snapshot should not error")
			})

			t.Run("DeleteNonExistentVessel", func(t *testing.T) {
				err := storage.DeleteVessel(ctx, "non-existent-vessel")
				assert.NoError(t, err, "Deleting non-existent vessel should not error")
			})

			t.Run("DoubleDeleteSnapshot", func(t *testing.T) {
				vesselID := "test-vessel-004"
				snapshotID := "snapshot-001"
				data := []byte("test data")

				err := storage.SaveSnapshot(ctx, vesselID, snapshotID, bytes.NewReader(data))
				require.NoError(t, err)

				err = storage.DeleteSnapshot(ctx, vesselID, snapshotID)
				require.NoError(t, err, "First delete should succeed")

				err = storage.DeleteSnapshot(ctx, vesselID, snapshotID)
				assert.NoError(t, err, "Second delete should not error")
			})

			t.Run("DoubleDeleteVessel", func(t *testing.T) {
				vesselID := "test-vessel-005"
				manifestData := []byte(`{"id":"test-vessel-005"}`)

				err := storage.SaveVesselManifest(ctx, vesselID, manifestData)
				require.NoError(t, err)

				err = storage.DeleteVessel(ctx, vesselID)
				require.NoError(t, err, "First delete should succeed")

				err = storage.DeleteVessel(ctx, vesselID)
				assert.NoError(t, err, "Second delete should not error")
			})
		})
	}
}
