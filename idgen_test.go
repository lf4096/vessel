package vessel

import (
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUniqueIDGenerator(t *testing.T) {
	generator := NewUniqueIDGenerator()

	beforeTime := uint64(time.Now().UnixMilli()) - epoch
	id1 := generator.NewID()
	afterTime := uint64(time.Now().UnixMilli()) - epoch

	assert.Len(t, id1, 16, "ID should be 16 characters long")

	hexPattern := regexp.MustCompile("^[0-9a-f]{16}$")
	assert.Regexp(t, hexPattern, id1, "ID should match hex format")

	var parsedID uint64
	_, err := fmt.Sscanf(id1, "%x", &parsedID)
	require.NoError(t, err, "ID should be parseable as hex")

	timestampMask := uint64((1 << timestampBits) - 1)
	timestamp := parsedID & timestampMask

	assert.GreaterOrEqual(t, timestamp, beforeTime, "Timestamp should be after start time")
	assert.LessOrEqual(t, timestamp, afterTime, "Timestamp should be before end time")

	id2 := generator.NewID()

	assert.NotEqual(t, id1, id2, "Two generated IDs should be different")
}

func TestIDGeneratorInterface(t *testing.T) {
	var _ IDGenerator = (*UniqueIDGenerator)(nil)
}
