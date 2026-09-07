package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/backfill"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	terminalansi "github.com/charmbracelet/x/ansi"
	"github.com/urfave/cli/v3"
	"golang.org/x/term"
)

type backfillDisplay struct {
	mu      sync.Mutex
	summary backfill.Summary
	active  map[string]backfill.Record
	focus   *backfill.Interval
	recent  []backfill.Record
	writer  io.Writer
	color   bool
	err     error
	started time.Time
}

func backfillTerminal(w io.Writer) bool {
	f, ok := w.(*os.File)
	return ok && term.IsTerminal(int(f.Fd())) //nolint:gosec // File descriptors fit in int.
}

func backfillPaint(s, color string, enabled bool) string {
	if !enabled {
		return s
	}
	return lipgloss.NewStyle().Foreground(lipgloss.Color(color)).Render(s)
}

func backfillStatus(s backfill.Status, color bool) string {
	colors := map[backfill.Status]string{backfill.Running: "#60A5FA", backfill.Succeeded: "#34D399", backfill.Failed: "#F87171", backfill.Cancelled: "#FBBF24", backfill.Queued: "#9CA3AF"}
	return backfillPaint(fmt.Sprintf("%-9s", s), colors[s], color)
}

func backfillRange(i backfill.Interval) string {
	layout := "2006-01-02 15:04 MST"
	if i.Start.Second() != 0 || i.End.Second() != 0 || i.Start.Nanosecond() != 0 || i.End.Nanosecond() != 0 {
		layout = "2006-01-02 15:04:05.999999 MST"
	}
	return i.Start.Format(layout) + " → " + i.End.Format(layout)
}

func backfillCount(s *backfill.Summary, status backfill.Status, delta int) {
	switch status {
	case backfill.Queued:
		s.Queued += delta
	case backfill.Running:
		s.Running += delta
	case backfill.Succeeded:
		s.Succeeded += delta
	case backfill.Failed:
		s.Failed += delta
	case backfill.Cancelled:
		s.Cancelled += delta
	}
}

func (d *backfillDisplay) observe(previous backfill.Status, r backfill.Record) {
	d.mu.Lock()
	defer d.mu.Unlock()
	backfillCount(&d.summary, previous, -1)
	backfillCount(&d.summary, r.Status, 1)
	// Keep a private attempt slice: the executor updates its record after this callback.
	r.Attempts = append([]backfill.Attempt(nil), r.Attempts...)
	if r.Status == backfill.Running {
		d.active[r.ID] = r
		i := r.Interval
		d.focus = &i
	} else {
		delete(d.active, r.ID)
		d.recent = append(d.recent, r)
		if len(d.recent) > 12 {
			d.recent = d.recent[1:]
		}
	}
	if d.writer != nil && d.err == nil {
		a := r.Attempts[len(r.Attempts)-1]
		duration := ""
		if a.FinishedAt != nil {
			duration = "  " + a.FinishedAt.Sub(a.StartedAt).Round(time.Millisecond).String()
		}
		_, d.err = fmt.Fprintf(d.writer, "%s  %s  attempt %d%s\n", backfillStatus(r.Status, d.color), backfillRange(r.Interval), len(r.Attempts), duration)
		if a.Error != "" && d.err == nil {
			_, d.err = fmt.Fprintf(d.writer, "  %s\n", a.Error)
		}
	}
}

type backfillFinished struct {
	summary backfill.Summary
	err     error
}
type backfillTick time.Time

type backfillModel struct {
	store         *backfill.Store
	anchor        *backfill.Interval
	manual        bool
	display       *backfillDisplay
	manifest      backfill.Manifest
	options       backfill.Options
	run           func() backfillFinished
	cancel        context.CancelFunc
	width, height int
	frame         int
	stopping      bool
	result        *backfillFinished
}

func backfillTickCmd() tea.Cmd {
	return tea.Tick(150*time.Millisecond, func(t time.Time) tea.Msg { return backfillTick(t) })
}

func (m backfillModel) Init() tea.Cmd {
	return tea.Batch(backfillTickCmd(), func() tea.Msg { return m.run() })
}

func (m backfillModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			m.scroll(-1)
		case "down", "j":
			m.scroll(1)
		case "pgup":
			m.scroll(-m.rowCount())
		case "pgdown":
			m.scroll(m.rowCount())
		case "home":
			m.anchor = nil
			m.manual = true
		case "f":
			m.manual = false
		}
		if msg.String() == "ctrl+c" || msg.String() == "q" {
			m.stopping = true
			m.cancel()
		}
	case backfillTick:
		m.frame++
		m.follow()
		return m, backfillTickCmd()
	case backfillFinished:
		m.result = &msg
		return m, tea.Quit
	}
	m.follow()
	return m, nil
}

// Generate only visible rows; even very large plans remain bounded in memory.
func (m backfillModel) rowCount() int { return max(1, m.height-15) }

func (m backfillModel) window() []backfill.Interval {
	m.clampAnchor()
	p := m.manifest.Plan
	if p.Partition == "" {
		return nil
	}
	if m.anchor != nil {
		if m.options.Reverse {
			p.End = m.anchor.End
		} else {
			p.Start = m.anchor.Start
		}
	}
	rows := make([]backfill.Interval, 0, m.rowCount())
	for i := range p.Intervals(m.options.Reverse) {
		rows = append(rows, i)
		if len(rows) == m.rowCount() {
			break
		}
	}
	return rows
}

// The furthest scroll position starts a full final viewport, not the final row.
func (m *backfillModel) clampAnchor() {
	if m.anchor == nil || m.manifest.Plan.Partition == "" {
		return
	}
	n := 0
	for i := range m.manifest.Plan.Intervals(!m.options.Reverse) {
		copy := i
		n++
		if n == m.rowCount() {
			if (!m.options.Reverse && m.anchor.Start.After(i.Start)) || (m.options.Reverse && m.anchor.Start.Before(i.Start)) {
				m.anchor = &copy
			}
			return
		}
	}
	// The entire plan fits on screen.
	m.anchor = nil
}

func (m *backfillModel) follow() {
	if m.manual {
		return
	}
	m.display.mu.Lock()
	defer m.display.mu.Unlock()
	focus := m.display.focus
	if focus == nil {
		return
	}
	for _, i := range m.window() {
		if i.ID == focus.ID {
			return
		}
	}
	copy := *focus
	m.anchor = &copy
	m.clampAnchor()
}

func (m *backfillModel) scroll(delta int) {
	rows := m.window()
	if len(rows) == 0 {
		return
	}
	p := m.manifest.Plan
	reverse := m.options.Reverse
	if delta < 0 {
		reverse = !reverse
		delta = -delta
	}
	if reverse {
		p.End = rows[0].End
	} else {
		p.Start = rows[0].Start
	}
	n := 0
	for i := range p.Intervals(reverse) {
		copy := i
		m.anchor = &copy
		if n == delta {
			break
		}
		n++
	}
	m.clampAnchor()
	if next := m.window(); len(next) > 0 && next[0].ID != rows[0].ID {
		m.manual = true
	}
}

func (m backfillModel) View() string {
	d := m.display
	d.mu.Lock()
	defer d.mu.Unlock()
	width := max(1, m.width-4)
	var b strings.Builder
	muted := func(s string) string { return backfillPaint(s, "#71717A", d.color) }
	accent := func(s string) string { return backfillPaint(s, "#FB923C", d.color) }
	fmt.Fprintf(&b, "%s  %s\n", accent("BACKFILL"), filepath.Base(m.manifest.Plan.Target))
	fmt.Fprintf(&b, "%s\n", muted(fmt.Sprintf("%s  /  %s  /  %s   ·   %d parallel × %d workers", m.manifest.Plan.Environment, m.manifest.Plan.Partition, m.manifest.Plan.Timezone, m.options.MaxParallel, m.options.Workers)))
	fmt.Fprintf(&b, "%s   %s\n\n", backfillTableRange(backfill.Interval{Start: m.manifest.Plan.Start, End: m.manifest.Plan.End}), muted("end exclusive"))
	s := d.summary
	completed := s.Succeeded + s.Failed + s.Cancelled
	percent := 0
	if s.Total > 0 {
		percent = completed * 100 / s.Total
	}
	barWidth := max(1, min(30, width-35))
	filled := barWidth * percent / 100
	fmt.Fprintf(&b, "%s%s  %s  %s\n", backfillPaint(strings.Repeat("▰", filled), "#34D399", d.color), muted(strings.Repeat("▱", barWidth-filled)), fmt.Sprintf("%d%%", percent), muted(fmt.Sprintf("%d / %d  ·  %s elapsed", completed, s.Total, time.Since(d.started).Round(time.Second))))
	stats := []string{backfillPaint(fmt.Sprintf("● %d running", s.Running), "#60A5FA", d.color), backfillPaint(fmt.Sprintf("✓ %d succeeded", s.Succeeded), "#34D399", d.color), muted(fmt.Sprintf("%d queued", s.Queued))}
	if s.Failed > 0 {
		stats = append(stats, backfillPaint(fmt.Sprintf("✗ %d failed", s.Failed), "#F87171", d.color))
	}
	if s.Cancelled > 0 {
		stats = append(stats, backfillPaint(fmt.Sprintf("%d cancelled", s.Cancelled), "#FBBF24", d.color))
	}
	if s.Skipped > 0 {
		stats = append(stats, muted(fmt.Sprintf("%d skipped", s.Skipped)))
	}
	fmt.Fprintf(&b, "%s\n\n", strings.Join(stats, "   "))
	tableWidth := max(36, width)
	rangeWidth := tableWidth - 32
	cell := func(value string, size int) string {
		value = terminalansi.Truncate(value, size, "…")
		return value + strings.Repeat(" ", max(0, size-terminalansi.StringWidth(value)))
	}
	row := func(rng, status, attempt, duration string) string {
		return muted("│") + " " + cell(rng, rangeWidth) + "  " + cell(status, 11) + "  " + cell(attempt, 3) + "  " + cell(duration, 8) + " " + muted("│")
	}
	border := func(left, right string) { fmt.Fprintln(&b, muted(left+strings.Repeat("─", tableWidth-2)+right)) }
	border("╭", "╮")
	fmt.Fprintln(&b, row(muted("PARTITION"), muted("STATUS"), muted("TRY"), muted("TIME")))
	border("├", "┤")
	for _, i := range m.window() {
		r := backfill.Record{Interval: i, Status: backfill.Queued}
		stateError := false
		if m.store != nil {
			saved, err := m.store.Read(i)
			if err != nil {
				stateError = true
			} else {
				r = saved
			}
		} else {
			for _, recent := range d.recent {
				if recent.ID == i.ID {
					r = recent
				}
			}
		}
		if active, ok := d.active[i.ID]; ok {
			r = active
		}
		symbol, attempt, duration := "○", "—", "—"
		switch r.Status {
		case backfill.Running:
			symbol = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}[m.frame%10]
		case backfill.Succeeded:
			symbol = "✓"
		case backfill.Failed:
			symbol = "✗"
		case backfill.Cancelled:
			symbol = "■"
		}
		if len(r.Attempts) > 0 {
			a := r.Attempts[len(r.Attempts)-1]
			elapsed := time.Since(a.StartedAt)
			if a.FinishedAt != nil {
				elapsed = a.FinishedAt.Sub(a.StartedAt)
			}
			attempt = fmt.Sprint(len(r.Attempts))
			duration = elapsed.Round(time.Second).String()
		}
		status := backfillStatus(r.Status, d.color)
		if stateError {
			status = "unavailable"
		}
		rng := backfillTableRange(i)
		if r.Status == backfill.Running {
			rng = backfillPaint(rng, "#93C5FD", d.color)
		}
		if r.Status == backfill.Queued {
			rng = muted(rng)
			attempt = muted(attempt)
			duration = muted(duration)
		}
		status = strings.TrimSpace(status)
		fmt.Fprintln(&b, row(rng, symbol+" "+status, attempt, duration))
	}
	border("╰", "╯")
	if m.stopping {
		fmt.Fprintln(&b, "\nStopping… waiting for active runs to exit.")
	} else {
		mode := "auto-follow"
		if m.manual {
			mode = "browsing"
		}
		fmt.Fprintf(&b, "\n%s  %s\n", accent("● "+mode), muted("↑↓ scroll   pgup/pgdn page   f follow   q stop"))
	}
	lines := strings.Split(strings.TrimRight(b.String(), "\n"), "\n")
	for i, line := range lines {
		lines[i] = "  " + terminalansi.Truncate(line, width, "…")
	}
	return strings.Join(lines[:min(len(lines), max(1, m.height-1))], "\n")
}

// Calendar partitions need dates rather than repeated midnight timestamps.
func backfillTableRange(i backfill.Interval) string {
	midnight := func(t time.Time) bool {
		return t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0
	}
	if midnight(i.Start) && midnight(i.End) {
		return i.Start.Format("02 Jan 2006") + " → " + i.End.Format("02 Jan 2006")
	}
	return backfillRange(i)
}

func executeBackfillWithDisplay(ctx context.Context, c *cli.Command, m backfill.Manifest, store *backfill.Store, options backfill.Options, page backfillPage, run backfill.Run) error {
	initial, err := backfill.Summarize(ctx, m.Plan, store, options.Rerun)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	color := !c.Bool("no-color") && os.Getenv("NO_COLOR") == "" && os.Getenv("TERM") != "dumb" && backfillTerminal(c.Writer)
	d := &backfillDisplay{summary: initial, active: make(map[string]backfill.Record), started: time.Now(), color: color}
	execute := func() backfillFinished {
		s, err := backfill.Execute(ctx, m, store, options, run, d.observe)
		return backfillFinished{s, err}
	}
	var result backfillFinished
	if c.String("output") == "json" {
		result = execute()
		return errors.Join(result.err, writeBackfillOutput(c.Writer, "json", m, store, options, result.summary, page))
	}
	interactive := backfillTerminal(c.Writer) && term.IsTerminal(int(os.Stdin.Fd())) && os.Getenv("TERM") != "dumb" && !c.Bool("no-progress") //nolint:gosec // File descriptors fit in int.
	if interactive {
		done := make(chan struct{})
		go func() { result = execute(); close(done) }()
		model := backfillModel{store: store, display: d, manifest: m, options: options, run: func() backfillFinished { <-done; return result }, cancel: cancel, width: 80, height: 24}
		// The model handles cancellation and waits for children before quitting.
		program := tea.NewProgram(model, tea.WithOutput(c.Writer), tea.WithInput(os.Stdin), tea.WithAltScreen(), tea.WithoutSignalHandler())
		final, uiErr := program.Run()
		if uiErr != nil {
			cancel()
			<-done
			return errors.Join(uiErr, result.err)
		}
		resultModel := final.(backfillModel)
		if resultModel.result == nil {
			return errors.New("backfill dashboard exited before execution completed")
		}
		result = *resultModel.result
	} else {
		d.writer = c.Writer
		if _, err = fmt.Fprintf(c.Writer, "Backfill · %s · %s · %s\n%s\n%d partitions · %d parallel · %d workers/run\n\n", filepath.Base(m.Plan.Target), m.Plan.Environment, m.Plan.Timezone, backfillRange(backfill.Interval{Start: m.Plan.Start, End: m.Plan.End}), initial.Total, options.MaxParallel, options.Workers); err != nil {
			return err
		}
		result = execute()
	}
	outputErr := writeBackfillSummary(c.Writer, m, store, result, time.Since(d.started), color)
	return errors.Join(result.err, d.err, outputErr)
}

func writeBackfillSummary(w io.Writer, m backfill.Manifest, store *backfill.Store, result backfillFinished, elapsed time.Duration, color bool) error {
	s := result.summary
	title, tint := "✓ Backfill complete", "#34D399"
	incomplete := result.err != nil || s.Succeeded != s.Total
	if incomplete {
		title, tint = "! Backfill incomplete", "#FBBF24"
		if s.Failed > 0 {
			title, tint = "✗ Backfill failed", "#F87171"
		}
		if errors.Is(result.err, context.Canceled) {
			title = "■ Backfill stopped"
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "\n%s  %s\n", backfillPaint(title, tint, color), elapsed.Round(time.Second))
	fmt.Fprintf(&b, "  %s · %s · %s\n", filepath.Base(m.Plan.Target), m.Plan.Environment, m.Plan.Partition)
	fmt.Fprintf(&b, "  %s\n\n", backfillRange(backfill.Interval{Start: m.Plan.Start, End: m.Plan.End}))
	counts := []string{backfillPaint(fmt.Sprintf("%d/%d succeeded", s.Succeeded, s.Total), "#34D399", color)}
	for _, entry := range []struct {
		count       int
		label, tint string
	}{
		{s.Failed, "failed", "#F87171"},
		{s.Cancelled, "cancelled", "#FBBF24"},
		{s.Queued, "queued", "#9CA3AF"},
		{s.Running, "running", "#60A5FA"},
		{s.Skipped, "skipped", "#9CA3AF"},
	} {
		if entry.count > 0 {
			counts = append(counts, backfillPaint(fmt.Sprintf("%d %s", entry.count, entry.label), entry.tint, color))
		}
	}
	fmt.Fprintf(&b, "  %s\n", strings.Join(counts, " · "))
	root := filepath.Dir(store.Dir)
	logs := filepath.Join(store.Dir, "children")
	if cwd, err := os.Getwd(); err == nil {
		if rel, err := filepath.Rel(cwd, logs); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			logs = rel
		}
		if rel, err := filepath.Rel(cwd, root); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			root = rel
		}
	}
	fmt.Fprintf(&b, "\n  Logs: %s\n", logs)
	if incomplete {
		fmt.Fprintf(&b, "\n  Resume: bruin backfill --state-dir %q --continue %s\n", root, m.ID)
	}
	_, err := io.WriteString(w, b.String())
	return err
}
