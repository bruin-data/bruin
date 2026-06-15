//nolint:paralleltest
package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewRunIDIsUniqueAcrossSameSecond(t *testing.T) {
	// Generating many ids in a tight loop (all within the same second) must not
	// collide, otherwise fast back-to-back runs would overwrite each other's
	// run-log file (logs/runs/<pipeline>/<run-id>.json).
	const n = 10000
	seen := make(map[string]struct{}, n)
	for range n {
		id := NewRunID()
		_, dup := seen[id]
		require.Falsef(t, dup, "duplicate run id generated: %s", id)
		seen[id] = struct{}{}
	}
}

func TestNewRunIDHonorsEnvOverride(t *testing.T) {
	t.Setenv("BRUIN_RUN_ID", "fixed-id")
	require.Equal(t, "fixed-id", NewRunID())
}
