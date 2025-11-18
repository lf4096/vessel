# Vessel

Vessel is a lightweight Go library that gives AI agents or automation services a container-style workspace. It builds on `containerd` snapshots and images so you can safely run commands, change files, and capture snapshots for later replay.

## Features

- **Workspace-first abstraction** – every Vessel mounts a writable layer on top of a base image and exposes file APIs together with process execution.
- **Snapshot pipeline** – create, list, delete, and restore incremental diffs that can be shipped across machines through the pluggable `Storage` interface.
- **Storage flexibility** – ships with local filesystem storage but can be extended with your own storage implementation.
- **Agent-friendly API** – `Manager` and `Vessel` interfaces are small, synchronous, and easy to embed inside orchestration services.

## Prerequisites

- containerd 1.7+ running on the host, with permissions to create snapshots.
- For rootless environments: `fuse-overlayfs` installed and `XDG_RUNTIME_DIR` exported.
- Optional offline usage: container images saved under `~/.local/share/vessel/data/images/<image-ref>.tar(.gz)`.

## Installation

```bash
go get github.com/lf4096/vessel
```

## Image Setup

By default, `LocalStorage` looks for a file named `<image-ref>.tar.gz` (or `.tar`) under `~/.local/share/vessel/data/images/`, which should be an OCI/Docker image tarball. You can prepare these files in either of the following ways:

1. Docker

```bash
# example: ubuntu:22.04
docker pull docker.io/library/alpine:latest
docker save docker.io/library/alpine:latest | gzip > ~/.local/share/vessel/data/images/docker.io/library/alpine:latest.tar.gz
```

2. containerd / ctr

```bash
ctr -n vessel images pull docker.io/library/alpine:latest
ctr -n vessel images export ~/.local/share/vessel/data/images/docker.io/library/alpine:latest.tar docker.io/library/alpine:latest
gzip ~/.local/share/vessel/data/images/docker.io/library/alpine:latest.tar
```

## Usage

### Quick Start

```go
package main

import (
	"context"
	"log"

	"github.com/lf4096/vessel"
)

func main() {
	ctx := context.Background()
	mgr, err := vessel.NewManager(&vessel.ManagerConfig{
		ContainerdAddress: "/run/containerd/containerd.sock",
		Namespace:         "vessel-demo",
		Storage:           nil, // default: ~/.local/share/vessel/data
	})
	if err != nil {
		log.Fatal(err)
	}
	defer mgr.Close()

	v, err := mgr.CreateVessel(ctx, "docker.io/library/alpine:latest", "/workspace")
	if err != nil {
		log.Fatal(err)
	}
	defer v.Close()

	if _, err := v.Exec(ctx, &vessel.ExecOptions{
		Command:       []string{"bash", "-lc", "echo hello > output.txt"},
		PersistResult: true,
	}); err != nil {
		log.Fatal(err)
	}

	data, err := v.ReadFile("output.txt")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("output: %s", data)

	if _, err := v.CreateSnapshot(ctx, "init workspace"); err != nil {
		log.Fatal(err)
	}
}
```

### File Operations

Requires `import "os"`.

```go
if err := v.WriteFile("notes/hello.txt", []byte("Born ready"), 0644); err != nil {
	log.Fatal(err)
}

f, err := v.OpenFile("notes/hello.txt", os.O_APPEND|os.O_WRONLY, 0644)
if err != nil {
	log.Fatal(err)
}
defer f.Close()
if _, err := f.WriteString("\nsecond line"); err != nil {
	log.Fatal(err)
}

data, err := v.ReadFile("notes/hello.txt")
if err != nil {
	log.Fatal(err)
}
log.Printf("current content: %s", data)
```

### Snapshot Operations

```go
if err := v.WriteFile("notes/hello.txt", []byte("Born ready"), 0644); err != nil {
	log.Fatal(err)
}

snap, err := v.CreateSnapshot(ctx, "baseline after bootstrap")
if err != nil {
	log.Fatal(err)
}

_ = v.DeleteFile("notes/hello.txt")

if err := v.RestoreSnapshot(ctx, snap.ID); err != nil {
	log.Fatal(err)
}

data, err := v.ReadFile("notes/hello.txt")
if err != nil {
	log.Fatal(err)
}
log.Printf("restored content: %s", data)
```

### Command Execution

```go
_, err := v.Exec(ctx, &vessel.ExecOptions{
	Command:       []string{"bash", "-lc", "go test ./..."},
	WorkDir:       "/workspace/project",
	PersistResult: false, // discard any writes after the command finishes
})
if err != nil {
	log.Fatal(err)
}
```

## API Reference

- `Manager`: `CreateVessel`, `OpenVessel`, `DeleteVessel`, `Close`.
- `Vessel`: workspace file methods, `Exec`, snapshot methods, `GetManifest`, `Close`. 
- `Storage`: handles manifests, snapshots, and image tarballs—swap in your own backend if needed.

See `vessel/types.go` for the full interface definitions and comments.

## Architecture

1. **Manager Layer**
   - Maintains the containerd client and manages a global lease.
   - Handles image fetching and import with cache.
   - Selects the appropriate snapshotter (overlayfs or fuse-overlayfs) as needed.
   - Manages the mount root directory to ensure proper workspace isolation.

2. **Vessel Layer**  
   - Each Vessel instance has its own lease, mount point, and manifest.
   - Manages the writable mount point and snapshot chain for the workspace.
   - `Exec` executes commands via a containerd task, manages IO using FIFO, and supports both persistent and ephemeral modes.

3. **Storage Layer**  
   - `LocalStorage` organizes manifests and snapshots under `~/.local/share/vessel`.  
   - The `CompressedStorage` decorator adds gzip compression for snapshots and images to reduce transfer and disk usage.  
   - The `Storage` interface allows plugging in custom backends, such as S3 or databases, enabling snapshot distribution across nodes.
