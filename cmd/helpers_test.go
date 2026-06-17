//nolint:paralleltest
package cmd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewRunIDHonorsEnvOverride(t *testing.T) {
	t.Setenv("BRUIN_RUN_ID", "fixed-id")
	require.Equal(t, "fixed-id", NewRunID())
}

func TestBackfillRunID(t *testing.T) {
	// The start date is formatted with the same layout as a normal run id, so
	// the result is filesystem-safe without any extra sanitization.
	day1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	require.Equal(t, "bf_x__2024_01_01_00_00_00", BackfillRunID("bf_x", day1))

	hour := time.Date(2024, 1, 1, 13, 30, 0, 0, time.UTC)
	require.Equal(t, "bf_x__2024_01_01_13_30_00", BackfillRunID("bf_x", hour))

	// Chunks of one backfill have distinct start dates, so their run ids differ.
	day2 := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	require.NotEqual(t, BackfillRunID("bf_x", day1), BackfillRunID("bf_x", day2))
}

func TestValidateBackfillID(t *testing.T) {
	for _, id := range []string{"", "bf_x", "bf_2024_q1", "abc-123"} {
		require.NoError(t, validateBackfillID(id), "id %q should be accepted", id)
	}
	// Path separators would let the run-log filename escape the logs directory.
	for _, id := range []string{"../../etc/passwd", "a/b", `a\b`, "/abs"} {
		require.Error(t, validateBackfillID(id), "id %q should be rejected", id)
	}
}
