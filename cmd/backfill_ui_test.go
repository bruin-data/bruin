package cmd

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/backfill"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/stretchr/testify/require"
)

func TestBackfillDisplayTransitions(t *testing.T) {
	var output bytes.Buffer
	d := &backfillDisplay{summary: backfill.Summary{Total: 1, Queued: 1}, active: make(map[string]backfill.Record), writer: &output}
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	r := backfill.Record{Interval: backfill.Interval{ID: "one", Start: start, End: start.Add(24 * time.Hour)}, Status: backfill.Running, Attempts: []backfill.Attempt{{StartedAt: start, Status: backfill.Running}}}
	d.observe(backfill.Queued, r)
	require.Equal(t, 1, d.summary.Running)
	r.Attempts[0].Status = backfill.Failed
	require.Equal(t, backfill.Running, d.active["one"].Attempts[0].Status)
	r.Status = backfill.Failed
	d.observe(backfill.Running, r)
	r.Status = backfill.Running
	d.observe(backfill.Failed, r)
	r.Status = backfill.Succeeded
	d.observe(backfill.Running, r)
	require.Equal(t, 1, d.summary.Succeeded)
	require.Zero(t, d.summary.Running)
	require.Zero(t, d.summary.Failed)
	require.Empty(t, d.active)
	require.NotContains(t, output.String(), "\x1b")
	require.Contains(t, output.String(), "attempt 1")
	require.False(t, backfillTerminal(&output))
}

func TestBackfillDashboardResizeAndCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &backfillDisplay{summary: backfill.Summary{Total: 100, Queued: 100}, active: make(map[string]backfill.Record), started: time.Now()}
	m := backfillModel{display: d, cancel: cancel, width: 80, height: 24}
	for _, size := range []tea.WindowSizeMsg{{Width: 80, Height: 24}, {Width: 24, Height: 8}} {
		updated, _ := m.Update(size)
		m = updated.(backfillModel)
		view := m.View()
		require.LessOrEqual(t, len(strings.Split(view, "\n")), size.Height)
		require.NotContains(t, view, "\x1b")
	}
	updated, cmd := m.Update(tea.KeyMsg{Type: tea.KeyCtrlC})
	require.True(t, updated.(backfillModel).stopping)
	require.Nil(t, cmd, "must wait for child completion before quitting")
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	_, cmd = m.Update(backfillFinished{})
	require.NotNil(t, cmd)
}

func TestBackfillDashboardWaitsForCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	released := make(chan struct{})
	var output bytes.Buffer
	d := &backfillDisplay{summary: backfill.Summary{Total: 1, Queued: 1}, active: make(map[string]backfill.Record), started: time.Now()}
	model := backfillModel{display: d, cancel: cancel, width: 80, height: 24, run: func() backfillFinished {
		close(started)
		<-ctx.Done()
		<-released
		return backfillFinished{err: ctx.Err()}
	}}
	program := tea.NewProgram(model, tea.WithInput(nil), tea.WithOutput(&output), tea.WithoutSignalHandler())
	done := make(chan tea.Model, 1)
	go func() { final, _ := program.Run(); done <- final }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("dashboard did not start")
	}
	program.Send(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}})
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("q did not cancel execution")
	}
	select {
	case <-done:
		t.Fatal("dashboard quit before execution finished")
	default:
	}
	close(released)
	select {
	case final := <-done:
		require.ErrorIs(t, final.(backfillModel).result.err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("dashboard did not exit")
	}
}

func TestBackfillSummaryOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result backfillFinished
		title  string
		resume bool
	}{
		{"success", backfillFinished{summary: backfill.Summary{Total: 3, Succeeded: 3}}, "Backfill complete", false},
		{"failure", backfillFinished{summary: backfill.Summary{Total: 3, Succeeded: 1, Failed: 1, Queued: 1}}, "Backfill failed", true},
		{"cancelled", backfillFinished{summary: backfill.Summary{Total: 3, Cancelled: 1, Queued: 2}, err: context.Canceled}, "Backfill stopped", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			m := backfill.Manifest{ID: "demo", Plan: backfill.Plan{Target: "pipeline", Environment: "local", Partition: "daily"}}
			require.NoError(t, writeBackfillSummary(&output, m, &backfill.Store{Dir: "logs/backfills/demo"}, tc.result, 37*time.Second, false))
			require.Contains(t, output.String(), tc.title)
			require.Contains(t, output.String(), "37s")
			require.Equal(t, tc.resume, strings.Contains(output.String(), "Resume:"))
			require.NotContains(t, output.String(), "0 failed")
			require.NotContains(t, output.String(), "\x1b")
		})
	}
}

func TestBackfillPartitionViewport(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
			plan := backfill.Plan{Target: "pipeline", Partition: "daily", Timezone: "UTC", Start: start, End: start.AddDate(0, 0, 10)}
			var all []backfill.Interval
			for i := range plan.Intervals(reverse) {
				all = append(all, i)
			}
			d := &backfillDisplay{summary: backfill.Summary{Total: 10, Queued: 10}, active: make(map[string]backfill.Record), started: time.Now()}
			m := backfillModel{display: d, manifest: backfill.Manifest{Plan: plan}, options: backfill.Options{Reverse: reverse}, width: 110, height: 18}
			require.Equal(t, all[:3], m.window())
			require.Equal(t, 3, strings.Count(m.View(), "queued")-1) // Three queued rows plus the summary.
			r := backfill.Record{Interval: all[1], Status: backfill.Running, Attempts: []backfill.Attempt{{StartedAt: time.Now()}}}
			d.observe(backfill.Queued, r)
			m.follow()
			require.Equal(t, all[:3], m.window(), "visible work must update without reordering rows")
			require.Contains(t, m.View(), "running")
			r.Status = backfill.Succeeded
			d.observe(backfill.Running, r)
			require.Contains(t, m.View(), backfillTableRange(all[1]))
			require.Contains(t, m.View(), "✓ succeeded")
			d.focus = &all[4]
			m.follow()
			require.Equal(t, all[4:7], m.window())
			m.scroll(-1)
			require.Equal(t, all[3:6], m.window())
			d.focus = &all[8]
			m.follow()
			require.Equal(t, all[3:6], m.window(), "manual browsing pauses follow")
			updated, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'f'}})
			m = updated.(backfillModel)
			require.Equal(t, all[7:], m.window())
			updated, _ = m.Update(tea.KeyMsg{Type: tea.KeyHome})
			m = updated.(backfillModel)
			require.Equal(t, all[:3], m.window())
		})
	}
}

func TestBackfillScrollingBounds(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		for _, total := range []int{3, 10} {
			t.Run(fmt.Sprintf("reverse=%v/total=%d", reverse, total), func(t *testing.T) {
				start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
				plan := backfill.Plan{Target: "pipeline", Partition: "daily", Timezone: "UTC", Start: start, End: start.AddDate(0, 0, total)}
				var all []backfill.Interval
				for i := range plan.Intervals(reverse) {
					all = append(all, i)
				}
				d := &backfillDisplay{active: make(map[string]backfill.Record)}
				m := backfillModel{display: d, manifest: backfill.Manifest{Plan: plan}, options: backfill.Options{Reverse: reverse}, height: 18}
				for n := 0; n < 5; n++ {
					updated, _ := m.Update(tea.KeyMsg{Type: tea.KeyPgDown})
					m = updated.(backfillModel)
				}
				require.Equal(t, all[max(0, total-3):], m.window())
				if total == 3 {
					require.False(t, m.manual, "a no-op must not disable automatic following")
				}
				m.height = 30
				require.Equal(t, all, m.window(), "resize should fill the available screen")
				for n := 0; n < 5; n++ {
					m.scroll(-3)
				}
				require.Equal(t, all, m.window())
			})
		}
	}
}
