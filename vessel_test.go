package vessel

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/images/archive"
	"github.com/containerd/platforms"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testImageFullRef = "docker.io/library/alpine:latest"
	testImageRef     = "alpine:latest"
	testNamespace    = "vessel-test"
)

var (
	testStorageDir string
	testMountDir   string
	testStorage    Storage
	testManager    Manager
)

func prepareTestImage(ctx context.Context, cli *client.Client, snapshotterName string, storage Storage, storageDir string) error {
	fmt.Printf("Preparing test image %s\n", testImageRef)

	platform := platforms.Only(platforms.DefaultSpec())
	image, err := cli.Pull(ctx, testImageFullRef,
		client.WithPlatformMatcher(platform),
		client.WithPullSnapshotter(snapshotterName),
		client.WithPullUnpack,
	)
	if err != nil {
		return fmt.Errorf("failed to pull %s image: %w", testImageFullRef, err)
	}

	imgService := cli.ImageService()
	contentStore := cli.ContentStore()
	imageDir := filepath.Join(storageDir, "images")
	if err := os.MkdirAll(imageDir, 0755); err != nil {
		return fmt.Errorf("failed to create image directory: %w", err)
	}
	destImageFile := filepath.Join(imageDir, testImageRef+".tar.gz")
	tmpDir := filepath.Dir(storageDir)
	tmpImageFile := filepath.Join(tmpDir, "temp-image.tar")
	tmpFile, err := os.Create(tmpImageFile)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	// Export current platform only by projecting to a single-manifest image
	target := image.Target()
	mDesc := target
	if images.IsIndexType(target.MediaType) {
		mfsts, mfErr := images.LimitManifests(images.ChildrenHandler(contentStore), platform, 1)(ctx, target)
		if mfErr != nil {
			tmpFile.Close()
			return fmt.Errorf("resolve platform manifest: %w", mfErr)
		}
		if len(mfsts) == 0 {
			tmpFile.Close()
			return fmt.Errorf("resolve platform manifest: no match")
		}
		mDesc = mfsts[0]
	} else if !images.IsManifestType(target.MediaType) {
		tmpFile.Close()
		return fmt.Errorf("unsupported target media type: %s", target.MediaType)
	}
	tmpName := image.Name() + "-sp-" + fmt.Sprintf("%d", time.Now().UnixNano())
	spImg := images.Image{
		Name:   tmpName,
		Target: mDesc,
	}
	if _, err := imgService.Create(ctx, spImg); err != nil {
		if _, uerr := imgService.Update(ctx, spImg); uerr != nil {
			tmpFile.Close()
			return fmt.Errorf("ensure single-platform image: %v / %v", err, uerr)
		}
	}
	defer func() { _ = imgService.Delete(ctx, tmpName) }()
	err = archive.Export(
		ctx, contentStore, tmpFile,
		archive.WithImage(imgService, tmpName),
		archive.WithSkipMissing(contentStore),
	)
	tmpFile.Close()
	if err != nil {
		return fmt.Errorf("failed to export %s image: %w", testImageFullRef, err)
	}

	tarFile, err := os.Open(tmpImageFile)
	if err != nil {
		return fmt.Errorf("failed to open tar file: %w", err)
	}
	gzFile, err := os.Create(destImageFile)
	if err != nil {
		tarFile.Close()
		return fmt.Errorf("failed to create gzip file: %w", err)
	}
	gzWriter := gzip.NewWriter(gzFile)
	_, err = io.Copy(gzWriter, tarFile)
	tarFile.Close()
	gzWriter.Close()
	gzFile.Close()
	if err != nil {
		return fmt.Errorf("failed to compress image: %w", err)
	}

	os.Remove(tmpImageFile)
	err = imgService.Delete(ctx, testImageFullRef)
	if err != nil {
		return fmt.Errorf("failed to delete full image from containerd: %w", err)
	}

	fmt.Printf("Successfully prepared test image at %s\n", destImageFile)
	return nil
}

func TestMain(m *testing.M) {
	var exitCode int
	defer func() {
		if testManager != nil {
			testManager.Close()
		}
		os.Exit(exitCode)
	}()

	fmt.Printf("Vessel testing environment initializing\n")

	tmpDir, err := os.MkdirTemp("", "vessel-test-*")
	if err != nil {
		fmt.Printf("Failed to create temp directory: %v\n", err)
		exitCode = 1
		return
	}
	defer os.RemoveAll(tmpDir)
	testStorageDir = filepath.Join(tmpDir, "storage")
	testMountDir = filepath.Join(tmpDir, "mounts")

	containerdSock := "/run/containerd/containerd.sock"
	currentUser := os.Getuid()
	isRootless := currentUser != 0
	snapshotterName := "overlayfs"
	if isRootless {
		snapshotterName = "fuse-overlayfs"
	}

	if isRootless {
		runtimeDir := os.Getenv("XDG_RUNTIME_DIR")
		if runtimeDir == "" {
			runtimeDir = fmt.Sprintf("/run/user/%d", currentUser)
		}
		containerdSock = filepath.Join(runtimeDir, "containerd/containerd.sock")
	}

	if _, err := os.Stat(containerdSock); os.IsNotExist(err) {
		fmt.Printf("WARNING: containerd socket not found at %s, skipping integration tests\n", containerdSock)
		return
	}

	cli, err := client.New(containerdSock, client.WithDefaultNamespace(testNamespace))
	if err != nil {
		fmt.Printf("Failed to connect to containerd: %v\n", err)
		exitCode = 1
		return
	}
	defer cli.Close()

	testStorage, err = NewLocalStorage(testStorageDir, true)
	if err != nil {
		fmt.Printf("Failed to create local storage: %v\n", err)
		exitCode = 1
		return
	}

	if err := prepareTestImage(context.Background(), cli, snapshotterName, testStorage, testStorageDir); err != nil {
		fmt.Printf("Failed to prepare test image: %v\n", err)
		exitCode = 1
		return
	}

	config := &ManagerConfig{
		Storage:   testStorage,
		Namespace: testNamespace,
		Rootless:  isRootless,
		MountRoot: testMountDir,
	}

	testManager, err = NewManager(config)
	if err != nil {
		fmt.Printf("Failed to create test manager: %v\n", err)
		exitCode = 1
		return
	}

	fmt.Printf("Vessel testing environment initialized\n")

	exitCode = m.Run()
}

func TestVerifyImage(t *testing.T) {
	require.NotNil(t, testStorage, "Global LocalStorage should be initialized")
	require.NotNil(t, testManager, "Global Manager should be initialized")

	ctx := context.Background()

	t.Run("LoadImageFromGlobalStorage", func(t *testing.T) {
		reader, err := testStorage.LoadImage(ctx, testImageRef)
		require.NoError(t, err, "Should be able to load image from global LocalStorage")
		defer reader.Close()

		buf := make([]byte, 512)
		n, err := reader.Read(buf)
		require.NoError(t, err, "Should be able to read from image")
		assert.Greater(t, n, 0, "Should read some bytes from image")
	})

	t.Run("CreateVesselWithImage", func(t *testing.T) {
		vessel, err := testManager.CreateVessel(ctx, testImageRef, "/")
		require.NoError(t, err, "Should be able to create vessel with image")
		require.NotNil(t, vessel, "Vessel should not be nil")
		defer vessel.Close()

		vesselID := vessel.ID()
		assert.NotEmpty(t, vesselID, "Vessel ID should not be empty")

		manifest := vessel.GetManifest()
		assert.Equal(t, testImageRef, manifest.BaseImageRef, "Manifest should have correct base image")
		assert.Equal(t, "/", manifest.WorkDir, "Manifest should have correct work directory")
	})
}

func TestVesselFileOperations(t *testing.T) {
	require.NotNil(t, testManager, "Global Manager should be initialized")

	ctx := context.Background()

	vessel, err := testManager.CreateVessel(ctx, testImageRef, "/tmp")
	require.NoError(t, err, "Should be able to create vessel")
	require.NotNil(t, vessel)
	defer vessel.Close()

	t.Run("WriteFile", func(t *testing.T) {
		testData := []byte("hello world")
		err := vessel.WriteFile("test.txt", testData, 0644)
		require.NoError(t, err, "Should be able to write file")
	})

	t.Run("ReadFile", func(t *testing.T) {
		data, err := vessel.ReadFile("test.txt")
		require.NoError(t, err, "Should be able to read file")
		assert.Equal(t, []byte("hello world"), data, "File content should match")
	})

	t.Run("Stat", func(t *testing.T) {
		info, err := vessel.Stat("test.txt")
		require.NoError(t, err, "Should be able to stat file")
		assert.Equal(t, "test.txt", info.Name(), "File name should match")
		assert.Equal(t, int64(11), info.Size(), "File size should match")
		assert.False(t, info.IsDir(), "Should not be a directory")
	})

	t.Run("OpenFile", func(t *testing.T) {
		file, err := vessel.OpenFile("test.txt", os.O_RDONLY, 0)
		require.NoError(t, err, "Should be able to open file")
		defer file.Close()

		data := make([]byte, 11)
		n, err := file.Read(data)
		require.NoError(t, err, "Should be able to read from opened file")
		assert.Equal(t, 11, n, "Should read correct number of bytes")
		assert.Equal(t, []byte("hello world"), data, "File content should match")
	})

	t.Run("RenameFile", func(t *testing.T) {
		err := vessel.RenameFile("test.txt", "renamed.txt")
		require.NoError(t, err, "Should be able to rename file")

		_, err = vessel.Stat("test.txt")
		assert.Error(t, err, "Old file should not exist")

		info, err := vessel.Stat("renamed.txt")
		require.NoError(t, err, "New file should exist")
		assert.Equal(t, "renamed.txt", info.Name())
	})

	t.Run("DeleteFile", func(t *testing.T) {
		err := vessel.DeleteFile("renamed.txt")
		require.NoError(t, err, "Should be able to delete file")

		_, err = vessel.Stat("renamed.txt")
		assert.Error(t, err, "Deleted file should not exist")
	})

	t.Run("WriteFileWithAbsolutePath", func(t *testing.T) {
		testData := []byte("absolute path test")
		err := vessel.WriteFile("/tmp/absolute.txt", testData, 0644)
		require.NoError(t, err, "Should be able to write file with absolute path")

		data, err := vessel.ReadFile("/tmp/absolute.txt")
		require.NoError(t, err, "Should be able to read file with absolute path")
		assert.Equal(t, testData, data, "File content should match")
	})
}

func TestVesselDirectoryOperations(t *testing.T) {
	require.NotNil(t, testManager, "Global Manager should be initialized")

	ctx := context.Background()

	vessel, err := testManager.CreateVessel(ctx, testImageRef, "/tmp")
	require.NoError(t, err, "Should be able to create vessel")
	require.NotNil(t, vessel)
	defer vessel.Close()

	t.Run("MakeDir", func(t *testing.T) {
		err := vessel.MakeDir("testdir", 0755)
		require.NoError(t, err, "Should be able to create directory")

		info, err := vessel.Stat("testdir")
		require.NoError(t, err, "Should be able to stat directory")
		assert.True(t, info.IsDir(), "Should be a directory")
	})

	t.Run("MakeDirAll", func(t *testing.T) {
		err := vessel.MakeDirAll("nested/deep/dir", 0755)
		require.NoError(t, err, "Should be able to create nested directories")

		info, err := vessel.Stat("nested/deep/dir")
		require.NoError(t, err, "Should be able to stat nested directory")
		assert.True(t, info.IsDir(), "Should be a directory")
	})

	t.Run("ReadDir", func(t *testing.T) {
		err := vessel.WriteFile("nested/file1.txt", []byte("content1"), 0644)
		require.NoError(t, err)
		err = vessel.WriteFile("nested/file2.txt", []byte("content2"), 0644)
		require.NoError(t, err)

		entries, err := vessel.ReadDir("nested")
		require.NoError(t, err, "Should be able to read directory")
		assert.GreaterOrEqual(t, len(entries), 3, "Should have at least 3 entries (deep dir + 2 files)")

		fileNames := make(map[string]bool)
		for _, entry := range entries {
			fileNames[entry.Name()] = true
		}
		assert.True(t, fileNames["file1.txt"], "Should contain file1.txt")
		assert.True(t, fileNames["file2.txt"], "Should contain file2.txt")
		assert.True(t, fileNames["deep"], "Should contain deep directory")
	})
}

func TestVesselManifest(t *testing.T) {
	require.NotNil(t, testManager, "Global Manager should be initialized")

	ctx := context.Background()

	t.Run("GetManifest", func(t *testing.T) {
		vessel, err := testManager.CreateVessel(ctx, testImageRef, "/workspace")
		require.NoError(t, err, "Should be able to create vessel")
		require.NotNil(t, vessel)
		defer vessel.Close()

		manifest := vessel.GetManifest()
		assert.NotEmpty(t, manifest.ID, "Manifest should have ID")
		assert.Equal(t, testImageRef, manifest.BaseImageRef, "Manifest should have correct base image")
		assert.Equal(t, "/workspace", manifest.WorkDir, "Manifest should have correct work directory")
		assert.NotNil(t, manifest.Labels, "Manifest should have labels map")
		assert.NotNil(t, manifest.Snapshots, "Manifest should have snapshots list")
		assert.False(t, manifest.CreateTime.IsZero(), "Create time should be set")
		assert.False(t, manifest.UpdateTime.IsZero(), "Update time should be set")
	})

	t.Run("WorkDirHandling", func(t *testing.T) {
		testCases := []struct {
			name           string
			inputWorkDir   string
			expectedResult string
		}{
			{"EmptyWorkDir", "", "/"},
			{"RootWorkDir", "/", "/"},
			{"ValidWorkDir", "/home/user", "/home/user"},
			{"RelativeWorkDir", "relative", "/"},
			{"WorkDirWithoutPrefix", "home/user", "/"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				vessel, err := testManager.CreateVessel(ctx, testImageRef, tc.inputWorkDir)
				require.NoError(t, err, "Should be able to create vessel")
				defer vessel.Close()

				manifest := vessel.GetManifest()
				assert.Equal(t, tc.expectedResult, manifest.WorkDir, "WorkDir should be handled correctly")
			})
		}
	})
}

func TestVesselOpenAndReopen(t *testing.T) {
	require.NotNil(t, testManager, "Global Manager should be initialized")

	ctx := context.Background()

	vessel, err := testManager.CreateVessel(ctx, testImageRef, "/tmp")
	require.NoError(t, err, "Should be able to create vessel")
	require.NotNil(t, vessel)

	vesselID := vessel.ID()

	err = vessel.Close()
	require.NoError(t, err, "Should be able to close vessel")

	reopenedVessel, err := testManager.OpenVessel(ctx, vesselID)
	require.NoError(t, err, "Should be able to reopen vessel")
	require.NotNil(t, reopenedVessel)
	defer reopenedVessel.Close()

	assert.Equal(t, vesselID, reopenedVessel.ID(), "Vessel ID should match")

	manifest := reopenedVessel.GetManifest()
	assert.Equal(t, testImageRef, manifest.BaseImageRef, "Base image should persist")
	assert.Equal(t, "/tmp", manifest.WorkDir, "Work directory should persist")
}

func TestResolvePathSecurity(t *testing.T) {
	require.NotNil(t, testManager, "Global Manager should be initialized")

	ctx := context.Background()

	ves, err := testManager.CreateVessel(ctx, testImageRef, "/workspace")
	require.NoError(t, err, "Should be able to create vessel")
	require.NotNil(t, ves)
	defer ves.Close()

	v := ves.(*vessel)
	mountPoint := v.mountPoint

	testCases := []struct {
		name      string
		inputPath string
		expected  string
	}{
		{
			name:      "SimpleRelativePath",
			inputPath: "file.txt",
			expected:  "workspace/file.txt",
		},
		{
			name:      "SimpleAbsolutePath",
			inputPath: "/tmp/file.txt",
			expected:  "tmp/file.txt",
		},
		{
			name:      "PathTraversalWithDotDot",
			inputPath: "../../../etc/passwd",
			expected:  "etc/passwd",
		},
		{
			name:      "PathTraversalMixedWithDotDot",
			inputPath: "foo/../../../etc/passwd",
			expected:  "etc/passwd",
		},
		{
			name:      "AbsolutePathWithDotDot",
			inputPath: "/tmp/../../../etc/passwd",
			expected:  "etc/passwd",
		},
		{
			name:      "CurrentDirSymbol",
			inputPath: "./././file.txt",
			expected:  "workspace/file.txt",
		},
		{
			name:      "DoubleSlashes",
			inputPath: "//tmp//file.txt",
			expected:  "tmp/file.txt",
		},
		{
			name:      "TrailingSlash",
			inputPath: "dir/",
			expected:  "workspace/dir",
		},
		{
			name:      "ComplexPathTraversal",
			inputPath: "../../../../../../../etc/shadow",
			expected:  "etc/shadow",
		},
		{
			name:      "PathTraversalFromRoot",
			inputPath: "/../../../etc/passwd",
			expected:  "etc/passwd",
		},
		{
			name:      "EmptyPath",
			inputPath: "",
			expected:  "workspace",
		},
		{
			name:      "DotOnly",
			inputPath: ".",
			expected:  "workspace",
		},
		{
			name:      "DotDotFromWorkDir",
			inputPath: "..",
			expected:  "",
		},
		{
			name:      "PathWithSpaces",
			inputPath: "my file.txt",
			expected:  "workspace/my file.txt",
		},
		{
			name:      "PathWithSpecialChars",
			inputPath: "file@#$%.txt",
			expected:  "workspace/file@#$%.txt",
		},
		{
			name:      "PathWithUnicode",
			inputPath: "文件.txt",
			expected:  "workspace/文件.txt",
		},
		{
			name:      "PathWithNewlineInName",
			inputPath: "file\nname.txt",
			expected:  "workspace/file\nname.txt",
		},
		{
			name:      "PathWithTabInName",
			inputPath: "file\tname.txt",
			expected:  "workspace/file\tname.txt",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resolved := v.resolvePath(tc.inputPath)
			expected := filepath.Join(mountPoint, tc.expected)
			assert.Equal(t, expected, resolved, "Resolved path should exactly match expected path")
			assert.True(t, filepath.IsAbs(resolved), "Resolved path should be absolute")
			cleaned := filepath.Clean(resolved)
			assert.Equal(t, cleaned, resolved,
				"Resolved path should already be cleaned (no redundant ./ or ../ segments)")
		})
	}
}

func TestResolvePathFileOperationsSecurity(t *testing.T) {
	require.NotNil(t, testManager, "Global Manager should be initialized")

	ctx := context.Background()

	ves, err := testManager.CreateVessel(ctx, testImageRef, "/workspace")
	require.NoError(t, err, "Should be able to create vessel")
	require.NotNil(t, ves)
	defer ves.Close()

	testCases := []struct {
		name        string
		writePath   string
		readPath    string
		expectError bool
	}{
		{
			name:      "NormalFile",
			writePath: "normal.txt",
			readPath:  "/workspace/normal.txt",
		},
		{
			name:      "PathTraversalAttack",
			writePath: "../../../etc/evil.txt",
			readPath:  "/etc/evil.txt",
		},
		{
			name:      "CleanedRelativePath",
			writePath: "subdir/../file.txt",
			readPath:  "/workspace/file.txt",
		},
		{
			name:      "AbsolutePathInWorkspace",
			writePath: "/workspace/abs.txt",
			readPath:  "/workspace/abs.txt",
		},
		{
			name:      "AbsolutePathOutside",
			writePath: "/tmp/outside.txt",
			readPath:  "/tmp/outside.txt",
		},
		{
			name:        "FileWithSpaces",
			writePath:   "my file.txt",
			readPath:    "/workspace/my file.txt",
			expectError: false,
		},
		{
			name:      "FileWithSpecialChars",
			writePath: "file@#$%.txt",
			readPath:  "/workspace/file@#$%.txt",
		},
		{
			name:      "FileWithUnicode",
			writePath: "文件.txt",
			readPath:  "/workspace/文件.txt",
		},
		{
			name:        "FileWithNewline",
			writePath:   "file\x00name.txt",
			expectError: true,
		},
	}

	testContent := []byte("test content")

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.expectError {
				dir := filepath.Dir(tc.readPath)
				err := ves.MakeDirAll(dir, 0755)
				require.NoError(t, err, "Should be able to create directory")
			}

			err := ves.WriteFile(tc.writePath, testContent, 0644)
			if tc.expectError {
				require.Error(t, err, "Write should fail")
				return
			} else {
				require.NoError(t, err, "Write should succeed")
			}

			data, err := ves.ReadFile(tc.readPath)
			require.NoError(t, err, "Read should succeed")
			assert.Equal(t, testContent, data, "Content should match")

			err = ves.DeleteFile(tc.readPath)
			require.NoError(t, err, "Cleanup should succeed")
		})
	}
}
func TestVesselSnapshotOperations(t *testing.T) {
	require.NotNil(t, testManager, "Global Manager should be initialized")

	ctx := context.Background()

	vessel, err := testManager.CreateVessel(ctx, testImageRef, "/workspace")
	require.NoError(t, err, "Should be able to create vessel")
	require.NotNil(t, vessel)
	defer vessel.Close()

	vesselID := vessel.ID()

	t.Run("CreateSnapshot", func(t *testing.T) {
		err := vessel.WriteFile("file1.txt", []byte("content1"), 0644)
		require.NoError(t, err)

		snapshot1, err := vessel.CreateSnapshot(ctx, "First snapshot")
		require.NoError(t, err, "Should be able to create first snapshot")
		require.NotNil(t, snapshot1)

		assert.NotEmpty(t, snapshot1.ID, "Snapshot ID should not be empty")
		assert.Equal(t, vesselID, snapshot1.VesselID, "Snapshot VesselID should match")
		assert.Equal(t, "", snapshot1.ParentID, "First snapshot should have empty ParentID")
		assert.Equal(t, "First snapshot", snapshot1.Message, "Snapshot message should match")
		assert.NotEmpty(t, snapshot1.Checksum, "Snapshot checksum should not be empty")
		assert.Greater(t, snapshot1.Size, int64(0), "Snapshot size should be positive")
		assert.False(t, snapshot1.CreateTime.IsZero(), "Snapshot create time should be set")
		assert.NotNil(t, snapshot1.Labels, "Snapshot labels should not be nil")

		err = vessel.WriteFile("file2.txt", []byte("content2"), 0644)
		require.NoError(t, err)

		snapshot2, err := vessel.CreateSnapshot(ctx, "Second snapshot")
		require.NoError(t, err, "Should be able to create second snapshot")
		require.NotNil(t, snapshot2)

		assert.Equal(t, snapshot1.ID, snapshot2.ParentID, "Second snapshot parent should be first snapshot")
		assert.Equal(t, "Second snapshot", snapshot2.Message)

		manifest := vessel.GetManifest()
		assert.Equal(t, snapshot2.ID, manifest.CurrentSnapshotID, "Current snapshot should be the latest")
	})

	t.Run("ListSnapshots", func(t *testing.T) {
		snapshots, err := vessel.ListSnapshots(ctx)
		require.NoError(t, err, "Should be able to list snapshots")
		require.Len(t, snapshots, 2, "Should have 2 snapshots")

		assert.Equal(t, "First snapshot", snapshots[0].Message)
		assert.Equal(t, "Second snapshot", snapshots[1].Message)

		assert.NotNil(t, snapshots[0].Labels, "Snapshot labels should be copied")
		assert.NotNil(t, snapshots[1].Labels, "Snapshot labels should be copied")
	})

	t.Run("RestoreSnapshot", func(t *testing.T) {
		snapshots, err := vessel.ListSnapshots(ctx)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(snapshots), 1, "Should have at least 1 snapshot")
		snapshot1ID := snapshots[0].ID

		data, err := vessel.ReadFile("file2.txt")
		require.NoError(t, err, "file2.txt should exist before restore")
		assert.Equal(t, []byte("content2"), data)

		err = vessel.RestoreSnapshot(ctx, snapshot1ID)
		require.NoError(t, err, "Should be able to restore to first snapshot")

		manifest := vessel.GetManifest()
		assert.Equal(t, snapshot1ID, manifest.CurrentSnapshotID, "Current snapshot should be updated")

		data, err = vessel.ReadFile("file1.txt")
		require.NoError(t, err, "file1.txt should exist after restore")
		assert.Equal(t, []byte("content1"), data)

		_, err = vessel.ReadFile("file2.txt")
		assert.Error(t, err, "file2.txt should not exist after restore to snapshot1")

		err = vessel.WriteFile("file3.txt", []byte("content3"), 0644)
		require.NoError(t, err)

		snapshot3, err := vessel.CreateSnapshot(ctx, "Third snapshot from restored state")
		require.NoError(t, err)
		assert.Equal(t, snapshot1ID, snapshot3.ParentID, "New snapshot parent should be snapshot1")
	})

	t.Run("DeleteSnapshot", func(t *testing.T) {
		snapshots, err := vessel.ListSnapshots(ctx)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(snapshots), 3, "Should have at least 3 snapshots")

		snapshot1ID := snapshots[0].ID
		snapshot2ID := snapshots[1].ID
		snapshot3ID := snapshots[2].ID

		err = vessel.DeleteSnapshot(ctx, snapshot1ID)
		require.Error(t, err, "Should not be able to delete snapshot with children")
		assert.Contains(t, err.Error(), "cannot delete snapshot with children")

		err = vessel.DeleteSnapshot(ctx, snapshot2ID)
		require.NoError(t, err, "Should be able to delete snapshot2 without children")

		remainingSnapshots, err := vessel.ListSnapshots(ctx)
		require.NoError(t, err)
		for _, snap := range remainingSnapshots {
			assert.NotEqual(t, snapshot2ID, snap.ID, "Deleted snapshot2 should not be in list")
		}

		data, err := vessel.ReadFile("file3.txt")
		require.NoError(t, err, "file3.txt should exist")
		assert.Equal(t, []byte("content3"), data)

		err = vessel.DeleteSnapshot(ctx, snapshot3ID)
		require.NoError(t, err, "Should be able to delete current snapshot without children")

		manifest := vessel.GetManifest()
		assert.NotEqual(t, snapshot3ID, manifest.CurrentSnapshotID, "Current snapshot should change after deletion")
		assert.Equal(t, snapshot1ID, manifest.CurrentSnapshotID, "Should automatically restore to parent snapshot1 after deleting current snapshot")

		data, err = vessel.ReadFile("file1.txt")
		require.NoError(t, err, "file1.txt should exist after restoring to parent snapshot1")
		assert.Equal(t, []byte("content1"), data)

		_, err = vessel.ReadFile("file3.txt")
		assert.Error(t, err, "file3.txt should not exist after restoring to parent snapshot1")

		err = vessel.RestoreSnapshot(ctx, snapshot3ID)
		assert.Error(t, err, "Should not be able to restore to deleted snapshot3")

		remainingSnapshots, err = vessel.ListSnapshots(ctx)
		require.NoError(t, err)
		for _, snap := range remainingSnapshots {
			assert.NotEqual(t, snapshot3ID, snap.ID, "Deleted snapshot3 should not be in list")
		}

		if len(remainingSnapshots) == 1 {
			err = vessel.DeleteSnapshot(ctx, remainingSnapshots[0].ID)
			assert.Error(t, err, "Should not be able to delete the only snapshot")
			assert.Contains(t, err.Error(), "cannot delete the only snapshot")
		}
	})
}

func TestVesselSnapshotChain(t *testing.T) {
	require.NotNil(t, testManager, "Global Manager should be initialized")

	ctx := context.Background()

	vessel, err := testManager.CreateVessel(ctx, testImageRef, "/workspace")
	require.NoError(t, err, "Should be able to create vessel")
	require.NotNil(t, vessel)

	vesselID := vessel.ID()

	createSnapshot := func(t *testing.T, name string) *SnapshotManifest {
		err := vessel.WriteFile(fmt.Sprintf("state%s.txt", name), []byte(fmt.Sprintf("state %s", name)), 0644)
		require.NoError(t, err)
		snapshot, err := vessel.CreateSnapshot(ctx, fmt.Sprintf("Snapshot %s", name))
		require.NoError(t, err)
		assert.NotNil(t, snapshot)
		return snapshot
	}

	verifySnapshot := func(t *testing.T, v Vessel, name string, expectedError bool) {
		data, err := v.ReadFile(fmt.Sprintf("state%s.txt", name))
		if expectedError {
			assert.Error(t, err, "File state%s.txt should not exist", name)
			return
		}
		require.NoError(t, err, "File state%s.txt should exist", name)
		assert.Equal(t, []byte(fmt.Sprintf("state %s", name)), data)
	}

	snapshotA := createSnapshot(t, "A")
	snapshotB := createSnapshot(t, "B")
	snapshotC := createSnapshot(t, "C")

	err = vessel.DeleteFile("stateA.txt")
	require.NoError(t, err)
	snapshotD, err := vessel.CreateSnapshot(ctx, "Snapshot D")
	require.NoError(t, err)
	assert.NotNil(t, snapshotD)
	t.Run("RestoreToMiddleSnapshot", func(t *testing.T) {
		err = vessel.RestoreSnapshot(ctx, snapshotB.ID)
		require.NoError(t, err, "Should be able to restore to snapshot B")

		verifySnapshot(t, vessel, "A", false)
		verifySnapshot(t, vessel, "B", false)
		verifySnapshot(t, vessel, "C", true)

		manifest := vessel.GetManifest()
		assert.Equal(t, snapshotB.ID, manifest.CurrentSnapshotID)
	})

	err = vessel.Close()
	require.NoError(t, err, "Should be able to close vessel")

	t.Run("ReopenAndVerifySnapshots", func(t *testing.T) {
		reopenedVessel, err := testManager.OpenVessel(ctx, vesselID)
		require.NoError(t, err, "Should be able to reopen vessel")
		require.NotNil(t, reopenedVessel)
		defer reopenedVessel.Close()

		snapshots, err := reopenedVessel.ListSnapshots(ctx)
		require.NoError(t, err, "Should be able to list snapshots after reopen")
		assert.Len(t, snapshots, 4, "Should have 4 snapshots")

		assert.Equal(t, "Snapshot A", snapshots[0].Message)
		assert.Equal(t, "Snapshot B", snapshots[1].Message)
		assert.Equal(t, "Snapshot C", snapshots[2].Message)

		manifest := reopenedVessel.GetManifest()
		assert.Equal(t, snapshotB.ID, manifest.CurrentSnapshotID, "Current snapshot should be B after reopen")

		verifySnapshot(t, reopenedVessel, "A", false)
		verifySnapshot(t, reopenedVessel, "B", false)
		verifySnapshot(t, reopenedVessel, "C", true)

		err = reopenedVessel.RestoreSnapshot(ctx, snapshotA.ID)
		require.NoError(t, err, "Should be able to restore to snapshot A after reopen")

		verifySnapshot(t, reopenedVessel, "A", false)
		verifySnapshot(t, reopenedVessel, "B", true)
		verifySnapshot(t, reopenedVessel, "C", true)

		err = reopenedVessel.RestoreSnapshot(ctx, snapshotC.ID)
		require.NoError(t, err, "Should be able to restore to snapshot D")

		verifySnapshot(t, reopenedVessel, "A", false)
		verifySnapshot(t, reopenedVessel, "B", false)
		verifySnapshot(t, reopenedVessel, "C", false)

		err = reopenedVessel.RestoreSnapshot(ctx, snapshotD.ID)
		require.NoError(t, err, "Should be able to restore to snapshot D")

		verifySnapshot(t, reopenedVessel, "A", true)
		verifySnapshot(t, reopenedVessel, "B", false)
		verifySnapshot(t, reopenedVessel, "C", false)
	})
}

func TestVesselExec(t *testing.T) {
	require.NotNil(t, testManager, "Global Manager should be initialized")

	ctx := context.Background()

	vessel, err := testManager.CreateVessel(ctx, testImageRef, "/workspace")
	require.NoError(t, err, "Should be able to create vessel")
	require.NotNil(t, vessel)
	defer vessel.Close()

	t.Run("ComprehensiveIOAndEnvironment", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		stdin := strings.NewReader("input from stdin\n")

		opts := &ExecOptions{
			Command: []string{"/bin/sh", "-c", `
				set -e
				echo "stdout:$TEST_VAR"
				echo "stderr:$TEST_VAR2" >&2
				echo "pwd:$(pwd)"
				read line
				echo "$line"
			`},
			WorkDir: "/tmp",
			Env: []string{
				"TEST_VAR=value1",
				"TEST_VAR2=value2",
			},
			Stdin:         stdin,
			Stdout:        &stdout,
			Stderr:        &stderr,
			PersistResult: false,
		}

		result, err := vessel.Exec(ctx, opts)
		require.NoError(t, err, "Exec should not return error")
		require.NotNil(t, result, "Result should not be nil")
		assert.Equal(t, 0, result.ExitCode, "Exit code should be 0")

		stdoutStr := stdout.String()
		assert.Contains(t, stdoutStr, "stdout:value1", "Stdout should contain env var TEST_VAR")
		assert.Contains(t, stdoutStr, "pwd:/tmp", "Stdout should show custom WorkDir")
		assert.Contains(t, stdoutStr, "input from stdin", "Stdout should contain stdin input")

		stderrStr := stderr.String()
		assert.Contains(t, stderrStr, "stderr:value2", "Stderr should contain env var TEST_VAR2")
	})

	t.Run("PersistenceBehavior", func(t *testing.T) {
		t.Run("PersistResultFalse", func(t *testing.T) {
			opts := &ExecOptions{
				Command:       []string{"/bin/sh", "-c", "echo 'temporary content' > /workspace/temp.txt"},
				PersistResult: false,
			}

			result, err := vessel.Exec(ctx, opts)
			require.NoError(t, err, "Exec should not return error")
			require.NotNil(t, result)
			assert.Equal(t, 0, result.ExitCode, "Exit code should be 0")

			_, err = vessel.ReadFile("/workspace/temp.txt")
			assert.Error(t, err, "Temporary file should not exist after non-persistent exec")
		})

		t.Run("PersistResultTrue", func(t *testing.T) {
			opts := &ExecOptions{
				Command:       []string{"/bin/sh", "-c", "echo 'persistent content' > /workspace/persist.txt"},
				PersistResult: true,
			}

			result, err := vessel.Exec(ctx, opts)
			require.NoError(t, err, "Exec should not return error")
			require.NotNil(t, result)
			assert.Equal(t, 0, result.ExitCode, "Exit code should be 0")

			data, err := vessel.ReadFile("/workspace/persist.txt")
			require.NoError(t, err, "Persistent file should exist after persistent exec")
			assert.Contains(t, string(data), "persistent content", "File should contain expected content")
		})
	})
	t.Run("WriteFileThenExecCanReadContent", func(t *testing.T) {
		const filePath = "/workspace/exec_read.txt"
		const fileContent = "content-from-writefile"

		err := vessel.WriteFile(filePath, []byte(fileContent), 0644)
		require.NoError(t, err, "Should write file before exec")

		var stdout bytes.Buffer
		opts := &ExecOptions{
			Command:       []string{"/bin/sh", "-c", "cat " + filePath},
			Stdout:        &stdout,
			PersistResult: false,
		}

		result, err := vessel.Exec(ctx, opts)
		require.NoError(t, err, "Exec should not return error")
		require.NotNil(t, result)
		assert.Equal(t, 0, result.ExitCode, "Exit code should be 0")
		assert.Equal(t, fileContent, strings.TrimSpace(stdout.String()), "Exec should read content written by WriteFile")
	})
	t.Run("SnapshotAndExecCanReadPreviousLayerFile", func(t *testing.T) {
		const filePath = "/workspace/prev.txt"
		const fileContent = "prev-layer-content"

		err := vessel.WriteFile(filePath, []byte(fileContent), 0644)
		require.NoError(t, err, "Should write file before snapshot")

		_, err = vessel.CreateSnapshot(ctx, "snapshot for prev layer test")
		require.NoError(t, err, "Should create snapshot successfully")

		var stdout bytes.Buffer
		opts := &ExecOptions{
			Command:       []string{"/bin/sh", "-c", "cat " + filePath},
			Stdout:        &stdout,
			PersistResult: false,
		}

		result, err := vessel.Exec(ctx, opts)
		require.NoError(t, err, "Exec should not return error")
		require.NotNil(t, result)
		assert.Equal(t, 0, result.ExitCode, "Exit code should be 0")
		assert.Equal(t, fileContent, strings.TrimSpace(stdout.String()), "Exec should read content from previous layer file")
	})

	t.Run("ErrorHandlingAndEdgeCases", func(t *testing.T) {
		t.Run("InvalidCommand", func(t *testing.T) {
			opts := &ExecOptions{
				Command: []string{},
			}

			_, err := vessel.Exec(ctx, opts)
			assert.Error(t, err, "Should return error for empty command")
			assert.Contains(t, err.Error(), "command is required", "Error should mention required command")

			opts = &ExecOptions{
				Command: nil,
			}

			_, err = vessel.Exec(ctx, opts)
			assert.Error(t, err, "Should return error for nil command")
			assert.Contains(t, err.Error(), "command is required", "Error should mention required command")

			opts = nil
			_, err = vessel.Exec(ctx, opts)
			assert.Error(t, err, "Should return error for nil options")
		})

		t.Run("NonZeroExitCode", func(t *testing.T) {
			opts := &ExecOptions{
				Command:       []string{"/bin/sh", "-c", "exit 42"},
				PersistResult: false,
			}

			result, err := vessel.Exec(ctx, opts)
			require.NoError(t, err, "Exec should not return error even with non-zero exit")
			require.NotNil(t, result)
			assert.Equal(t, 42, result.ExitCode, "Exit code should be 42")
		})

		t.Run("ContextCancellation", func(t *testing.T) {
			timeoutCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()

			opts := &ExecOptions{
				Command:       []string{"/bin/sh", "-c", "sleep 30"},
				PersistResult: false,
			}

			result, err := vessel.Exec(timeoutCtx, opts)
			require.Error(t, err, "Exec should return context error")
			require.NotNil(t, result)
			assert.Equal(t, 137, result.ExitCode, "Exit code should be 137 (SIGKILL)")
			assert.ErrorIs(t, err, context.DeadlineExceeded, "Error should be context deadline exceeded")
		})
	})
}
