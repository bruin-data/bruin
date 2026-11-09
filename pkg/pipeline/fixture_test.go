package pipeline_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestFixtureTimestampRoundTrip(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		literal string
		zoned   bool
	}{
		{"2026-10-09T11:00:00Z", true},
		{"2026-10-09T00:00:00Z", true},
		{"2026-10-09T11:00:00+00:00", true},
		{"2026-10-09T11:00:00+01:00", true},
		{"2026-10-09T11:00:00.123456-04:00", true},
		{"2026-1-9t1:2:3Z", true},
		{"2026-10-09 11:00:00", false},
		{"2026-10-09", false},
	} {
		t.Run(tc.literal, func(t *testing.T) {
			t.Parallel()
			input := "asset: events\nrows:\n  - at: " + tc.literal + "\n"
			var fixture pipeline.UnitTestInput
			require.NoError(t, yaml.Unmarshal([]byte(input), &fixture))
			stamp, ok := fixture.Rows[0]["at"].(pipeline.FixtureTimestamp)
			require.True(t, ok)
			require.Equal(t, tc.zoned, stamp.HasTimezone())

			encoded, err := yaml.Marshal(fixture)
			require.NoError(t, err)
			require.Contains(t, string(encoded), "at: "+tc.literal)
			var decoded pipeline.UnitTestInput
			require.NoError(t, yaml.Unmarshal(encoded, &decoded))
			require.Equal(t, fixture, decoded)

			encodedJSON, err := json.Marshal(stamp)
			require.NoError(t, err)
			require.JSONEq(t, `"`+stamp.Format(time.RFC3339Nano)+`"`, string(encodedJSON))
		})
	}
}

func TestFixtureRowsAliasesAndOtherValues(t *testing.T) {
	t.Parallel()
	var fixture pipeline.Fixture
	require.NoError(t, yaml.Unmarshal([]byte(`
name: events
asset: events
rows:
  - &base
    at: &stamp 2026-10-09T11:00:00Z
    quoted: "2026-10-09T11:00:00Z"
    id: 1
    enabled: true
    empty: null
    nested: {key: value}
  - <<: *base
    at: *stamp
  - null
`), &fixture))
	require.Equal(t, fixture.Rows[0], fixture.Rows[1])
	require.True(t, fixture.Rows[1]["at"].(pipeline.FixtureTimestamp).HasTimezone())
	require.Equal(t, "2026-10-09T11:00:00Z", fixture.Rows[0]["quoted"])
	require.Equal(t, 1, fixture.Rows[0]["id"])
	require.Equal(t, true, fixture.Rows[0]["enabled"])
	require.Nil(t, fixture.Rows[0]["empty"])
	require.Equal(t, map[string]interface{}{"key": "value"}, fixture.Rows[0]["nested"])
	require.Nil(t, fixture.Rows[2])
}
