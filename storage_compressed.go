package vessel

import (
	"context"
	"fmt"
	"io"

	"github.com/klauspost/compress/gzip"
)

// CompressedStorage wraps another Storage and transparently compresses artifacts.
type CompressedStorage struct {
	underlying Storage
}

// NewCompressedStorage decorates a Storage with gzip compression.
func NewCompressedStorage(underlying Storage) Storage {
	return &CompressedStorage{
		underlying: underlying,
	}
}

// gzipReadCloser ensures both gzip and underlying resources close together.
type gzipReadCloser struct {
	gzipReader *gzip.Reader
	underlying io.ReadCloser
}

func newGzipReadCloser(underlying io.ReadCloser) (*gzipReadCloser, error) {
	gzipReader, err := gzip.NewReader(underlying)
	if err != nil {
		underlying.Close()
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	return &gzipReadCloser{
		gzipReader: gzipReader,
		underlying: underlying,
	}, nil
}

func (g *gzipReadCloser) Read(p []byte) (int, error) {
	return g.gzipReader.Read(p)
}

func (g *gzipReadCloser) Close() error {
	gzipErr := g.gzipReader.Close()
	underlyingErr := g.underlying.Close()
	if gzipErr != nil {
		return gzipErr
	}
	return underlyingErr
}

// SaveVesselManifest delegates manifest persistence without compression.
func (s *CompressedStorage) SaveVesselManifest(ctx context.Context, vesselID string, manifest []byte) error {
	return s.underlying.SaveVesselManifest(ctx, vesselID, manifest)
}

// LoadVesselManifest delegates manifest retrieval without compression.
func (s *CompressedStorage) LoadVesselManifest(ctx context.Context, vesselID string) ([]byte, error) {
	return s.underlying.LoadVesselManifest(ctx, vesselID)
}

// LoadImage wraps the underlying reader with gzip decompression.
func (s *CompressedStorage) LoadImage(ctx context.Context, imageRef string) (io.ReadCloser, error) {
	reader, err := s.underlying.LoadImage(ctx, imageRef)
	if err != nil {
		return nil, err
	}
	return newGzipReadCloser(reader)
}

// SaveSnapshot compresses snapshot diffs before writing to storage.
func (s *CompressedStorage) SaveSnapshot(ctx context.Context, vesselID, snapshotID string, reader io.Reader) error {
	pr, pw := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		gzipWriter := gzip.NewWriter(pw)
		_, err := io.Copy(gzipWriter, reader)
		if err != nil {
			gzipWriter.Close()
			pw.CloseWithError(err)
			errCh <- err
			return
		}
		if err := gzipWriter.Close(); err != nil {
			pw.CloseWithError(err)
			errCh <- err
			return
		}
		pw.Close()
		errCh <- nil
	}()

	saveErr := s.underlying.SaveSnapshot(ctx, vesselID, snapshotID, pr)
	compressErr := <-errCh
	if compressErr != nil {
		return fmt.Errorf("failed to compress snapshot: %w", compressErr)
	}
	if saveErr != nil {
		return saveErr
	}
	return nil
}

// LoadSnapshot returns a decompressed snapshot Reader.
func (s *CompressedStorage) LoadSnapshot(ctx context.Context, vesselID, snapshotID string) (io.ReadCloser, error) {
	reader, err := s.underlying.LoadSnapshot(ctx, vesselID, snapshotID)
	if err != nil {
		return nil, err
	}
	return newGzipReadCloser(reader)
}

// DeleteSnapshot forwards deletion to the underlying store.
func (s *CompressedStorage) DeleteSnapshot(ctx context.Context, vesselID, snapshotID string) error {
	return s.underlying.DeleteSnapshot(ctx, vesselID, snapshotID)
}

// DeleteVessel removes vessel data in the underlying store.
func (s *CompressedStorage) DeleteVessel(ctx context.Context, vesselID string) error {
	return s.underlying.DeleteVessel(ctx, vesselID)
}
