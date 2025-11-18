package vessel

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/opencontainers/image-spec/identity"
)

type manager struct {
	client          *client.Client
	namespace       string
	snapshotter     snapshots.Snapshotter
	snapshotterName string
	storage         Storage
	mountRoot       string
	runRoot         string
	managerLease    leases.Lease
	imageCache      map[string]string
	imageMu         sync.RWMutex
}

// NewManager creates a Manager backed by containerd and the provided storage.
func NewManager(config *ManagerConfig) (Manager, error) {
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}

	address := config.ContainerdAddress
	if address == "" {
		if config.Rootless {
			runtimeDir := os.Getenv("XDG_RUNTIME_DIR")
			if runtimeDir == "" {
				runtimeDir = fmt.Sprintf("/run/user/%d", os.Getuid())
			}
			address = filepath.Join(runtimeDir, "containerd/containerd.sock")
		} else {
			address = "/run/containerd/containerd.sock"
		}
	}

	namespace := config.Namespace
	if namespace == "" {
		namespace = "vessel"
	}

	cli, err := client.New(address, client.WithDefaultNamespace(namespace))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to containerd at %s: %w", address, err)
	}

	snapshotterName := "overlayfs"
	if config.Rootless {
		snapshotterName = "fuse-overlayfs"
	}
	snapshotter := cli.SnapshotService(snapshotterName)

	runRoot := "/run/containerd"
	if config.Rootless {
		runtimeDir := os.Getenv("XDG_RUNTIME_DIR")
		if runtimeDir == "" {
			runtimeDir = fmt.Sprintf("/run/user/%d", os.Getuid())
		}
		runRoot = filepath.Join(runtimeDir, "containerd")
	}

	storage := config.Storage
	if storage == nil {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			cli.Close()
			return nil, fmt.Errorf("failed to get home directory: %w", err)
		}
		storageDir := filepath.Join(homeDir, ".local/share/vessel/data")
		storage, err = NewLocalStorage(storageDir, true)
		if err != nil {
			cli.Close()
			return nil, fmt.Errorf("failed to create local storage: %w", err)
		}
	}

	mountRoot := config.MountRoot
	if mountRoot == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			cli.Close()
			return nil, fmt.Errorf("failed to get home directory: %w", err)
		}
		mountRoot = filepath.Join(homeDir, ".local/share/vessel/mounts")
	}
	if err := os.MkdirAll(mountRoot, 0755); err != nil {
		cli.Close()
		return nil, fmt.Errorf("failed to create mount root directory: %w", err)
	}

	leaseID := fmt.Sprintf("lease-manager-%s", GenerateUniqueID())
	managerLease, err := cli.LeasesService().Create(context.Background(), leases.WithID(leaseID))
	if err != nil {
		cli.Close()
		return nil, fmt.Errorf("failed to create manager lease: %w", err)
	}

	return &manager{
		client:          cli,
		namespace:       namespace,
		snapshotter:     snapshotter,
		snapshotterName: snapshotterName,
		storage:         storage,
		mountRoot:       mountRoot,
		runRoot:         runRoot,
		managerLease:    managerLease,
		imageCache:      make(map[string]string),
	}, nil
}

// CreateVessel creates a new vessel based on image.
func (m *manager) CreateVessel(ctx context.Context, imageRef string, workDir string) (Vessel, error) {
	vesselID := GenerateUniqueID()
	return newVessel(ctx, vesselID, imageRef, workDir, m)
}

// OpenVessel loads an existing vessel.
func (m *manager) OpenVessel(ctx context.Context, vesselID string) (Vessel, error) {
	return openVessel(ctx, vesselID, m)
}

// DeleteVessel removes a vessel's stored manifests and snapshots.
func (m *manager) DeleteVessel(ctx context.Context, vesselID string) error {
	if err := m.storage.DeleteVessel(ctx, vesselID); err != nil {
		return fmt.Errorf("failed to delete vessel data: %w", err)
	}
	return nil
}

// Close releases the manager lease and underlying containerd client.
func (m *manager) Close() error {
	if m.managerLease.ID != "" {
		ctx := leases.WithLease(context.Background(), m.managerLease.ID)
		m.client.LeasesService().Delete(ctx, m.managerLease)
	}
	if m.client != nil {
		return m.client.Close()
	}
	return nil
}

func (m *manager) getImage(ctx context.Context, imageRef string) (string, error) {
	m.imageMu.RLock()
	if snapshotKey, ok := m.imageCache[imageRef]; ok {
		m.imageMu.RUnlock()
		return snapshotKey, nil
	}
	m.imageMu.RUnlock()

	managerCtx := leases.WithLease(context.Background(), m.managerLease.ID)

	img, err := m.client.GetImage(managerCtx, imageRef)
	if err != nil {
		imageReader, err := m.storage.LoadImage(ctx, imageRef)
		if err != nil {
			return "", fmt.Errorf("failed to load image from storage: %w", err)
		}
		defer imageReader.Close()
		imgs, err := m.client.Import(managerCtx, imageReader)
		if err != nil {
			return "", fmt.Errorf("failed to import image: %w", err)
		}
		if len(imgs) == 0 {
			return "", fmt.Errorf("no images imported")
		}
		img, err = m.client.GetImage(managerCtx, imgs[0].Name)
		if err != nil {
			return "", fmt.Errorf("failed to get imported image: %w", err)
		}
	}

	if err := img.Unpack(managerCtx, m.snapshotterName); err != nil {
		return "", fmt.Errorf("failed to unpack image: %w", err)
	}
	diffIDs, err := img.RootFS(managerCtx)
	if err != nil {
		return "", fmt.Errorf("failed to get rootfs: %w", err)
	}
	snapshotKey := identity.ChainID(diffIDs).String()

	m.imageMu.Lock()
	m.imageCache[imageRef] = snapshotKey
	m.imageMu.Unlock()

	return snapshotKey, nil
}
