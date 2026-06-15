//nolint:paralleltest
package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewRunIDIsUniqueAcrossSameSecond(t *testing.T) {
	// run_id doubles as the run-log filename and is written into the log body,
	// so fast back-to-back runs (e.g. backfill chunks) must each get a distinct
	// id instead of colliding on a second-granularity timestamp.
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
