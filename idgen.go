package vessel

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	epoch         = 1577836800000 // 2020-01-01T00:00:00Z
	timestampBits = 42
)

// UniqueIDGenerator is the default implementation of IDGenerator that produces
// monotonic-ish hex identifiers mixing timestamp and randomness.
type UniqueIDGenerator struct{}

// NewUniqueIDGenerator creates a new UniqueIDGenerator instance.
func NewUniqueIDGenerator() *UniqueIDGenerator {
	return &UniqueIDGenerator{}
}

// NewID returns a unique 16-character hex identifier.
func (g *UniqueIDGenerator) NewID() string {
	now := uint64(time.Now().UnixMilli())
	timestamp := now - epoch
	randomNumber := rand.Uint64()
	id := (randomNumber << timestampBits) | timestamp
	return fmt.Sprintf("%016x", id)
}
