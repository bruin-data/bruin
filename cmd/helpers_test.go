//nolint:paralleltest
package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewRunIDHonorsEnvOverride(t *testing.T) {
	t.Setenv("BRUIN_RUN_ID", "fixed-id")
	require.Equal(t, "fixed-id", NewRunID())
}

func TestBackfillRunID(t *testing.T) {
	cases := []struct {
		backfillID string
		startDate  string
		want       string
	}{
		{"bf_x", "2024-01-01", "bf_x__2024_01_01"},
		{"bf_x", "2024-01-01 23:59:59.999999", "bf_x__2024_01_01_23_59_59_999999"},
		{"bf_x", "2024-01-02T00:00:00.000000000Z", "bf_x__2024_01_02T00_00_00_000000000Z"},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, BackfillRunID(tc.backfillID, tc.startDate))
	}

	// Chunks of one backfill have distinct start dates, so their run ids differ.
	require.NotEqual(t,
		BackfillRunID("bf_x", "2024-01-01"),
		BackfillRunID("bf_x", "2024-01-02"),
	)
}
