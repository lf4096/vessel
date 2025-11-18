package vessel

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	epoch         = 1577836800000 // 2020-01-01T00:00:00Z
	randomBits    = 22
	timestampBits = 42
)

// GenerateUniqueID returns a monotonic-ish hex identifier that mixes time and randomness.
func GenerateUniqueID() string {
	now := uint64(time.Now().UnixMilli())
	timestamp := now - epoch
	randomNumber := rand.Uint64()
	id := (randomNumber << timestampBits) | timestamp
	return fmt.Sprintf("%016x", id)
}
