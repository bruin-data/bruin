package backfill

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testPlan(t *testing.T, start, end, partition, zone string) Plan {
	t.Helper()
	a, b, err := ParseRange(start, end, zone)
	require.NoError(t, err)
	p := Plan{Target: "/repo/pipeline", Environment: "dev", RunFlags: map[string][]string{"var": {"x=1"}}, Start: a, End: b, Timezone: zone, Partition: partition}
	require.NoError(t, p.Validate())
	return p
}

func TestIntervals(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, start, end, partition, zone string
		count                             int
		hours                             []int
	}{
		{"inclusive day", "2024-02-29", "2024-02-29", "daily", "UTC", 1, []int{24}},
		{"leap years", "2019-01-01", "2024-12-31", "daily", "UTC", 2192, nil},
		{"multi year months", "2019-01-01", "2025-12-31", "monthly", "UTC", 84, nil},
		{"clipped months", "2024-01-31T12:00:00Z", "2024-03-02T12:00:00Z", "monthly", "UTC", 3, []int{12, 696, 36}},
		{"years", "2023-07-01", "2025-01-01", "yearly", "UTC", 3, nil},
		{"weeks Monday", "2024-03-06", "2024-03-12", "weekly", "UTC", 2, []int{120, 48}},
		{"spring daily", "2024-03-09", "2024-03-11", "daily", "America/New_York", 3, []int{24, 23, 24}},
		{"fall daily", "2024-11-02", "2024-11-04", "daily", "America/New_York", 3, []int{24, 25, 24}},
		{"spring hourly", "2024-03-10", "2024-03-10", "hourly", "America/New_York", 23, nil},
		{"fall hourly", "2024-11-03", "2024-11-03", "hourly", "America/New_York", 25, nil},
		{"midnight DST", "2018-11-03", "2018-11-05", "daily", "America/Sao_Paulo", 3, []int{24, 23, 24}},
		{"skipped end date", "2011-12-29", "2011-12-30", "daily", "Pacific/Apia", 1, []int{24}},
		{"skipped date", "2011-12-29", "2011-12-31", "daily", "Pacific/Apia", 2, []int{24, 24}},
		{"half hour DST", "2024-10-06", "2024-10-06", "hourly", "Australia/Lord_Howe", 24, nil},
		{"duration", "2024-03-10", "2024-03-10", "6h", "America/New_York", 4, []int{6, 6, 6, 5}},
		{"offset converted", "2024-03-10T05:00:00Z", "2024-03-11T04:00:00Z", "daily", "America/New_York", 1, []int{23}},
		{"microseconds", "2024-01-01T00:00:00.000001Z", "2024-01-01T00:00:00.000008Z", "3us", "UTC", 3, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := testPlan(t, tc.start, tc.end, tc.partition, tc.zone)
			got := slices.Collect(p.Intervals(false))
			require.Len(t, got, tc.count)
			count, err := p.Count(t.Context())
			require.NoError(t, err)
			require.Equal(t, tc.count, count)
			require.True(t, got[0].Start.Equal(p.Start))
			require.True(t, got[len(got)-1].End.Equal(p.End))
			ids := make(map[string]bool)
			for n, i := range got {
				require.True(t, i.Start.Before(i.End))
				require.False(t, ids[i.ID])
				ids[i.ID] = true
				if n > 0 {
					require.True(t, got[n-1].End.Equal(i.Start))
				}
				if tc.hours != nil {
					require.Equal(t, time.Duration(tc.hours[n])*time.Hour, i.End.Sub(i.Start))
				}
			}
			reverse := slices.Collect(p.Intervals(true))
			slices.Reverse(reverse)
			require.Equal(t, got, reverse)
			// Persisted time.Time values retain an offset, not the IANA location. The
			// regenerated calendar must still honor later daylight-saving transitions.
			p.Start = p.Start.In(time.FixedZone("", int(p.Start.Sub(p.Start.UTC()).Seconds())))
			require.Equal(t, got, slices.Collect(p.Intervals(false)))
		})
	}
}

func TestRangeValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ start, end, zone string }{
		{"bad", "2024-01-02", "UTC"},
		{"2024-01-01", "bad", "UTC"},
		{"2024-01-02", "2024-01-01", "UTC"},
		{"2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z", "UTC"},
		{"2024-01-01", "2024-01-02", "Unknown"},
		{"2024-01-01", "2024-01-02", "Local"},
		{"2024-03-10 02:30:00", "2024-03-11", "America/New_York"},
	} {
		_, _, err := ParseRange(tc.start, tc.end, tc.zone)
		require.Error(t, err)
	}
	p := testPlan(t, "2024-01-01", "2024-01-02", "daily", "UTC")
	for _, v := range []string{"0s", "-1h", "1ns", "1.1us", "nonsense", ""} {
		p.Partition = v
		require.Error(t, p.Validate())
	}
}

func TestPartitionIdentity(t *testing.T) {
	t.Parallel()
	p := testPlan(t, "2024-01-01", "2024-01-02", "daily", "UTC")
	id := slices.Collect(p.Intervals(false))[0].ID
	for _, change := range []func(*Plan){
		func(p *Plan) { p.Target = "/other/pipeline" }, func(p *Plan) { p.Environment = "prod" },
		func(p *Plan) { p.RunFlags = map[string][]string{"var": {"x=2"}} }, func(p *Plan) { p.RunFlags = map[string][]string{"selector": {"tag:finance"}} },
		func(p *Plan) { p.Start = p.Start.Add(time.Hour) },
	} {
		q := p
		change(&q)
		require.NotEqual(t, id, slices.Collect(q.Intervals(false))[0].ID)
	}
	p.End = p.End.AddDate(1, 0, 0)
	require.Equal(t, id, slices.Collect(p.Intervals(false))[0].ID, "extending a backfill does not rename existing intervals")
}

func TestVeryLargePlanIsLazy(t *testing.T) {
	t.Parallel()
	p := testPlan(t, "0001-01-01", "9998-12-31", "1us", "UTC")
	n := 0
	for i := range p.Intervals(false) {
		require.Equal(t, time.Microsecond, i.End.Sub(i.Start))
		n++
		if n == 3 {
			break
		}
	}
	require.Equal(t, 3, n)
	n = 0
	for i := range p.Intervals(true) {
		require.Equal(t, time.Microsecond, i.End.Sub(i.Start))
		n++
		if n == 3 {
			break
		}
	}
	require.Equal(t, 3, n)
}
