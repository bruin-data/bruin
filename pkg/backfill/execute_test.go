package backfill

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testStore(t *testing.T) (Manifest, *Store) {
	t.Helper()
	m := Manifest{Version: Version, ID: "test-backfill", CreatedAt: time.Now().UTC(), Plan: testPlan(t, "2024-01-01", "2024-01-04", "daily", "UTC")}
	s, err := Open(t.TempDir(), m.ID)
	require.NoError(t, err)
	lock, err := s.Lock()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, lock.Close()) })
	require.NoError(t, s.Create(m))
	return m, s
}

func testOptions() Options { return Options{MaxParallel: 1, Workers: 2, OnFailure: "continue"} }

func TestResumeAndRerun(t *testing.T) {
	t.Parallel()
	m, s := testStore(t)
	ids := slices.Collect(m.Plan.Intervals(false))
	var calls []string
	o := testOptions()
	summary, err := Execute(t.Context(), m, s, o, func(_ context.Context, i Interval, runID string) error {
		calls = append(calls, runID)
		if i.ID == ids[1].ID {
			return errors.New("test failure")
		}
		return nil
	})
	require.Error(t, err)
	require.Equal(t, 3, summary.Succeeded)
	require.Equal(t, 1, summary.Failed)
	saved, err := s.Manifest()
	require.NoError(t, err)
	summary, err = Execute(t.Context(), saved, s, o, func(_ context.Context, i Interval, runID string) error {
		require.Equal(t, ids[1].ID, i.ID)
		calls = append(calls, runID)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 4, summary.Succeeded)
	require.Equal(t, 3, summary.Skipped)
	require.Len(t, calls, 5)
	require.Len(t, slices.Compact(slices.Sorted(slices.Values(calls))), 5)
	for n, i := range ids {
		record, err := s.Read(i)
		require.NoError(t, err)
		want := 1
		if n == 1 {
			want = 2
		}
		require.Len(t, record.Attempts, want)
		for _, a := range record.Attempts {
			require.NotNil(t, a.FinishedAt)
			require.False(t, a.FinishedAt.Before(a.StartedAt))
		}
	}
	summary, err = Execute(t.Context(), m, s, o, func(context.Context, Interval, string) error { t.Error("success ran twice"); return nil })
	require.NoError(t, err)
	require.Equal(t, 4, summary.Skipped)
	o.Rerun = "all"
	var count atomic.Int32
	summary, err = Execute(t.Context(), m, s, o, func(context.Context, Interval, string) error { count.Add(1); return nil })
	require.NoError(t, err)
	require.EqualValues(t, 4, count.Load())
	require.Zero(t, summary.Skipped)
}

func TestRetryAndPersistBeforeLaunch(t *testing.T) {
	t.Parallel()
	m, s := testStore(t)
	o := testOptions()
	o.Retries = 1
	summary, err := Execute(t.Context(), m, s, o, func(_ context.Context, i Interval, runID string) error {
		r, err := s.Read(i)
		require.NoError(t, err)
		require.Equal(t, Running, r.Status)
		require.Equal(t, runID, r.Attempts[len(r.Attempts)-1].RunID)
		if len(r.Attempts) == 1 {
			return errors.New("transient")
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 4, summary.Succeeded)
	for i := range m.Plan.Intervals(false) {
		r, err := s.Read(i)
		require.NoError(t, err)
		require.Len(t, r.Attempts, 2)
		require.Equal(t, Failed, r.Attempts[0].Status)
	}
}

func TestCancellationAndPartialCompletion(t *testing.T) {
	t.Parallel()
	m, s := testStore(t)
	o := testOptions()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var count atomic.Int32
	summary, err := Execute(ctx, m, s, o, func(ctx context.Context, _ Interval, _ string) error {
		if count.Add(1) == 2 {
			cancel()
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, summary.Succeeded)
	require.Equal(t, 1, summary.Cancelled)
	require.Equal(t, 2, summary.Queued)
	o.Rerun = "failed"
	summary, err = Execute(t.Context(), m, s, o, func(context.Context, Interval, string) error { t.Error("no failed partitions"); return nil })
	require.NoError(t, err)
	require.Equal(t, 4, summary.Skipped)
	o.Rerun = "missing"
	summary, err = Execute(t.Context(), m, s, o, func(context.Context, Interval, string) error { count.Add(1); return nil })
	require.NoError(t, err)
	require.Equal(t, 4, summary.Succeeded)
	require.EqualValues(t, 5, count.Load())
}

func TestBoundedParallelismAndFailurePolicies(t *testing.T) {
	t.Parallel()
	for _, policy := range []string{"continue", "fail-fast"} {
		t.Run(policy, func(t *testing.T) {
			t.Parallel()
			m, s := testStore(t)
			o := testOptions()
			o.MaxParallel = 2
			o.OnFailure = policy
			var started, active, peak atomic.Int32
			bothStarted := make(chan struct{})
			var once sync.Once
			summary, err := Execute(t.Context(), m, s, o, func(ctx context.Context, _ Interval, _ string) error {
				n := started.Add(1)
				now := active.Add(1)
				defer active.Add(-1)
				for old := peak.Load(); now > old; old = peak.Load() {
					if peak.CompareAndSwap(old, now) {
						break
					}
				}
				if n == 2 {
					once.Do(func() { close(bothStarted) })
				}
				<-bothStarted
				if n == 1 {
					return errors.New("permanent failure")
				}
				if policy == "fail-fast" {
					<-ctx.Done()
					return ctx.Err()
				}
				time.Sleep(20 * time.Millisecond)
				return nil
			})
			require.Error(t, err)
			require.EqualValues(t, 2, peak.Load())
			require.Zero(t, active.Load())
			switch policy {
			case "continue":
				require.Equal(t, 3, summary.Succeeded)
				require.EqualValues(t, 4, started.Load())
			case "fail-fast":
				require.Equal(t, 1, summary.Cancelled)
				require.Equal(t, 2, summary.Queued)
			}
		})
	}
}

func TestInterruptedRecordAndCorruption(t *testing.T) {
	t.Parallel()
	m, s := testStore(t)
	i := slices.Collect(m.Plan.Intervals(false))[0]
	r := Record{Interval: i, Status: Running, Attempts: []Attempt{{RunID: "abandoned", Status: Running, StartedAt: time.Now()}}}
	require.NoError(t, s.Save(r))
	summary, err := Execute(t.Context(), m, s, testOptions(), func(context.Context, Interval, string) error { return nil })
	require.NoError(t, err)
	require.Equal(t, 4, summary.Succeeded)
	r, err = s.Read(i)
	require.NoError(t, err)
	require.Len(t, r.Attempts, 2)
	require.Equal(t, Cancelled, r.Attempts[0].Status)
	require.NotNil(t, r.Attempts[0].FinishedAt)
	require.NoError(t, os.WriteFile(filepath.Join(s.Dir, "partitions", i.ID+".json"), []byte("corrupted"), 0o600))
	_, err = Execute(t.Context(), m, s, testOptions(), func(context.Context, Interval, string) error {
		t.Error("must not execute with corrupt state")
		return nil
	})
	require.Error(t, err)
}

func TestStoreLockAndValidation(t *testing.T) {
	t.Parallel()
	m, s := testStore(t)
	_, err := s.Lock()
	require.ErrorContains(t, err, "already running")
	require.Error(t, s.Create(m))
	for _, id := range []string{"../escape", ".", "a/b", "a\\b", ""} {
		_, err := Open(t.TempDir(), id)
		require.Error(t, err)
	}
	for _, o := range []Options{{MaxParallel: 0, Workers: 1}, {MaxParallel: 1, Workers: 0}, {MaxParallel: 1, Workers: 1, Retries: -1}, {MaxParallel: 1, Workers: 1, OnFailure: "bad"}, {MaxParallel: 1, Workers: 1, OnFailure: "continue", Rerun: "bad"}} {
		require.Error(t, o.Validate())
	}
}

func TestHugeBackfillStartsWithoutMaterializingPlan(t *testing.T) {
	t.Parallel()
	m, s := testStore(t)
	m.Plan = testPlan(t, "2000-01-01", "2025-12-31", "1us", "UTC")
	o := testOptions()
	o.OnFailure = "stop"
	var calls int
	summary, err := Execute(t.Context(), m, s, o, func(context.Context, Interval, string) error { calls++; return errors.New("stop here") })
	require.Error(t, err)
	require.Equal(t, 1, calls)
	require.Equal(t, 1, summary.Failed)
	require.Greater(t, summary.Queued, 1_000_000_000)
	records, err := os.ReadDir(filepath.Join(s.Dir, "partitions"))
	require.NoError(t, err)
	require.Len(t, records, 1)
	o.Rerun = "failed"
	summary, err = Execute(t.Context(), m, s, o, func(context.Context, Interval, string) error { calls++; return nil })
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Equal(t, 1, summary.Succeeded)
	require.Equal(t, summary.Total-1, summary.Skipped)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = Summarize(ctx, m.Plan, s, "")
	require.ErrorIs(t, err, context.Canceled)
}

func TestStopAfterFailure(t *testing.T) {
	t.Parallel()
	m, s := testStore(t)
	o := testOptions()
	o.OnFailure = "stop"
	calls := 0
	summary, err := Execute(t.Context(), m, s, o, func(context.Context, Interval, string) error {
		calls++
		if calls == 2 {
			return errors.New("permanent failure")
		}
		return nil
	})
	require.Error(t, err)
	require.Equal(t, 2, calls)
	require.Equal(t, 1, summary.Succeeded)
	require.Equal(t, 1, summary.Failed)
	require.Equal(t, 2, summary.Queued)
}
