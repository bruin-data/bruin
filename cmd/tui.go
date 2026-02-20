package cmd

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/fatih/color"
	"golang.org/x/term"
)

// TUIRenderer manages a live-updating terminal progress display for pipeline runs.
type TUIRenderer struct {
	terminal     *os.File // real terminal fd (saved before logOutput replaces os.Stdout)
	pipelineName string
	startTime    time.Time

	mu        sync.Mutex
	assets    []*assetRow
	assetMap  map[string]*assetRow
	totalMain int // total main asset tasks

	ticker    *time.Ticker
	done      chan struct{}
	stopped   bool
	lastLines int // lines rendered in the last frame (for clearing)
}

type assetRow struct {
	name     string
	status   scheduler.TaskInstanceStatus
	startAt  time.Time
	duration time.Duration
	checks   []*checkRow
}

type checkRow struct {
	name   string
	status scheduler.TaskInstanceStatus
}

// NewTUIRenderer creates a TUI renderer. terminal should be the real terminal saved before logOutput().
func NewTUIRenderer(terminal *os.File, s *scheduler.Scheduler, pipelineName string) *TUIRenderer {
	t := &TUIRenderer{
		terminal:     terminal,
		pipelineName: pipelineName,
		startTime:    time.Now(),
		assetMap:     make(map[string]*assetRow),
		done:         make(chan struct{}),
	}
	t.initFromScheduler(s)
	return t
}

func (t *TUIRenderer) initFromScheduler(s *scheduler.Scheduler) {
	instances := s.GetTaskInstances()

	// Maintain insertion order by tracking which assets we've seen
	seen := make(map[string]bool)

	for _, inst := range instances {
		if inst.GetStatus() == scheduler.Skipped {
			continue
		}

		assetName := inst.GetAsset().Name
		row, exists := t.assetMap[assetName]
		if !exists {
			row = &assetRow{
				name:   assetName,
				status: scheduler.Pending,
			}
			t.assetMap[assetName] = row
			if !seen[assetName] {
				t.assets = append(t.assets, row)
				seen[assetName] = true
			}
		}

		switch inst.GetType() {
		case scheduler.TaskInstanceTypeMain:
			row.status = inst.GetStatus()
			t.totalMain++
		case scheduler.TaskInstanceTypeColumnCheck, scheduler.TaskInstanceTypeCustomCheck:
			row.checks = append(row.checks, &checkRow{
				name:   inst.GetHumanID(),
				status: inst.GetStatus(),
			})
		case scheduler.TaskInstanceTypeMetadataPush:
			// metadata push tasks are not displayed in the TUI
		}
	}
}

// OnStatusChange handles scheduler status change events.
func (t *TUIRenderer) OnStatusChange(event scheduler.StatusChangeEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()

	assetName := event.Instance.GetAsset().Name
	row, exists := t.assetMap[assetName]
	if !exists {
		return
	}

	switch event.Instance.GetType() {
	case scheduler.TaskInstanceTypeMain:
		row.status = event.NewStatus
	case scheduler.TaskInstanceTypeColumnCheck, scheduler.TaskInstanceTypeCustomCheck:
		humanID := event.Instance.GetHumanID()
		for _, c := range row.checks {
			if c.name == humanID {
				c.status = event.NewStatus
				break
			}
		}
	case scheduler.TaskInstanceTypeMetadataPush:
		// metadata push tasks are not displayed in the TUI
	}
}

// OnTaskStarted is called by the worker when it begins executing a task.
func (t *TUIRenderer) OnTaskStarted(inst scheduler.TaskInstance) {
	t.mu.Lock()
	defer t.mu.Unlock()

	assetName := inst.GetAsset().Name
	row, exists := t.assetMap[assetName]
	if !exists {
		return
	}

	if inst.GetType() == scheduler.TaskInstanceTypeMain {
		row.status = scheduler.Running
		row.startAt = time.Now()
	}
}

// OnTaskEnded is called by the worker when it finishes executing a task.
func (t *TUIRenderer) OnTaskEnded(inst scheduler.TaskInstance, err error, dur time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	assetName := inst.GetAsset().Name
	row, exists := t.assetMap[assetName]
	if !exists {
		return
	}

	if inst.GetType() == scheduler.TaskInstanceTypeMain {
		row.duration = dur
		if err != nil {
			row.status = scheduler.Failed
		} else {
			row.status = scheduler.Succeeded
		}
	}
}

// Start begins the TUI rendering loop.
func (t *TUIRenderer) Start() {
	// Hide cursor
	fmt.Fprint(t.terminal, "\033[?25l")

	t.ticker = time.NewTicker(100 * time.Millisecond)
	go t.renderLoop()
}

// Stop stops the TUI rendering loop and restores cursor.
func (t *TUIRenderer) Stop() {
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		return
	}
	t.stopped = true
	t.mu.Unlock()

	close(t.done)
	t.ticker.Stop()

	// Clear the last rendered frame under lock (lastLines is shared state)
	t.mu.Lock()
	t.clearLastRender()
	t.mu.Unlock()

	// Show cursor
	fmt.Fprint(t.terminal, "\033[?25h")
}

func (t *TUIRenderer) renderLoop() {
	t.render()
	for {
		select {
		case <-t.done:
			return
		case <-t.ticker.C:
			t.render()
		}
	}
}

func (t *TUIRenderer) render() {
	t.mu.Lock()
	defer t.mu.Unlock()

	width, height := t.getTerminalSize()
	output := t.buildOutput(width, height)

	t.clearLastRender()
	fmt.Fprint(t.terminal, output)

	t.lastLines = strings.Count(output, "\n")
}

func (t *TUIRenderer) clearLastRender() {
	if t.lastLines > 0 {
		// Clamp to terminal height so we don't move cursor past the top of screen
		// if the terminal was resized smaller between renders.
		_, h := t.getTerminalSize()
		lines := min(t.lastLines, h-1)
		for range lines {
			fmt.Fprint(t.terminal, "\033[A\033[2K")
		}
		fmt.Fprint(t.terminal, "\r")
	}
}

func (t *TUIRenderer) getTerminalSize() (int, int) {
	w, h, err := term.GetSize(int(t.terminal.Fd()))
	if err != nil {
		return 80, 24
	}
	return w, h
}

func (t *TUIRenderer) buildOutput(width, height int) string {
	var sb strings.Builder
	elapsed := time.Since(t.startTime).Truncate(time.Second)

	completed := t.countMainTerminal()
	total := t.totalMain
	running := t.countByStatus(scheduler.Running)

	// Header
	if width >= 80 {
		header := fmt.Sprintf("Pipeline: %s | %d/%d assets done | %s",
			t.pipelineName, completed, total, fmtDuration(elapsed))
		if running > 0 {
			header += fmt.Sprintf(" | %d running", running)
		}
		sb.WriteString(header + "\n\n")
	} else {
		fmt.Fprintf(&sb, "%d/%d done | %s\n\n", completed, total, fmtDuration(elapsed))
	}

	// Calculate available rows for assets
	// 2 header lines + 1 blank + 1 footer status line + 1 overflow line
	maxRows := max(height-5, 3)

	displayAssets := t.getDisplayOrder(maxRows)

	for _, row := range displayAssets {
		sb.WriteString(t.renderAssetRow(row, width))
		sb.WriteString("\n")
	}

	if len(t.assets) > len(displayAssets) {
		sb.WriteString(dimText(fmt.Sprintf("  ... and %d more\n", len(t.assets)-len(displayAssets))))
	}

	// Footer: status summary
	succeeded := t.countByStatus(scheduler.Succeeded)
	failed := t.countByStatus(scheduler.Failed)
	pending := t.countByStatus(scheduler.Pending) + t.countByStatus(scheduler.Queued)
	upFailed := t.countByStatus(scheduler.UpstreamFailed)

	var parts []string
	if succeeded > 0 {
		parts = append(parts, color.New(color.FgGreen).Sprintf("%d succeeded", succeeded))
	}
	if running > 0 {
		parts = append(parts, color.New(color.FgYellow).Sprintf("%d running", running))
	}
	if failed > 0 {
		parts = append(parts, color.New(color.FgRed).Sprintf("%d failed", failed))
	}
	if upFailed > 0 {
		parts = append(parts, color.New(color.FgYellow).Sprintf("%d upstream failed", upFailed))
	}
	if pending > 0 {
		parts = append(parts, color.New(color.Faint).Sprintf("%d waiting", pending))
	}

	if len(parts) > 0 {
		sb.WriteString("\n  " + strings.Join(parts, " · ") + "\n")
	} else {
		sb.WriteString("\n\n")
	}

	return sb.String()
}

func (t *TUIRenderer) renderAssetRow(row *assetRow, width int) string {
	icon := statusIcon(row.status)

	// Duration
	var durStr string
	switch {
	case row.status == scheduler.Running && !row.startAt.IsZero():
		durStr = fmtDuration(time.Since(row.startAt).Truncate(100 * time.Millisecond))
	case row.duration > 0:
		durStr = fmtDuration(row.duration.Truncate(time.Millisecond))
	default:
		durStr = "-"
	}

	// Check indicators
	checksStr := t.renderChecks(row.checks)

	// Name truncation
	// Layout: "  {icon} {name}  {dur}  {checks}"
	// Reserve: 2 (indent) + 2 (icon+space) + 2 (gap) + 8 (duration) + 2 (gap) + len(checks)
	overhead := 16 + len(stripAnsi(checksStr))
	maxNameLen := max(width-overhead, 10)

	name := row.name
	if len(name) > maxNameLen {
		name = name[:maxNameLen-3] + "..."
	}

	// Color the name based on status
	displayName := name
	switch row.status {
	case scheduler.Failed:
		displayName = color.New(color.FgRed).Sprint(name)
	case scheduler.Running:
		displayName = color.New(color.FgWhite).Sprint(name)
	case scheduler.UpstreamFailed:
		displayName = color.New(color.FgYellow).Sprint(name)
	case scheduler.Pending, scheduler.Queued:
		displayName = dimText(name)
	case scheduler.Succeeded, scheduler.Skipped:
	}

	// Pad name to align durations
	padding := max(maxNameLen-len(name), 0)

	return fmt.Sprintf("  %s %s%s  %8s %s", icon, displayName, strings.Repeat(" ", padding), durStr, checksStr)
}

func (t *TUIRenderer) renderChecks(checks []*checkRow) string {
	if len(checks) == 0 {
		return ""
	}
	var sb strings.Builder
	for _, c := range checks {
		switch c.status {
		case scheduler.Succeeded:
			sb.WriteString(color.New(color.FgGreen).Sprint("."))
		case scheduler.Failed:
			sb.WriteString(color.New(color.FgRed).Sprint("F"))
		case scheduler.Running:
			sb.WriteString(color.New(color.FgYellow).Sprint("~"))
		case scheduler.UpstreamFailed:
			sb.WriteString(color.New(color.FgYellow).Sprint("U"))
		case scheduler.Pending, scheduler.Queued, scheduler.Skipped:
			sb.WriteString(dimText("."))
		}
	}
	return sb.String()
}

func (t *TUIRenderer) countByStatus(status scheduler.TaskInstanceStatus) int {
	count := 0
	for _, row := range t.assets {
		if row.status == status {
			count++
		}
	}
	return count
}

func (t *TUIRenderer) countMainTerminal() int {
	count := 0
	for _, row := range t.assets {
		switch row.status {
		case scheduler.Succeeded, scheduler.Failed, scheduler.UpstreamFailed, scheduler.Skipped:
			count++
		case scheduler.Pending, scheduler.Queued, scheduler.Running:
			// not terminal
		}
	}
	return count
}

func (t *TUIRenderer) getDisplayOrder(maxRows int) []*assetRow {
	if len(t.assets) <= maxRows {
		return t.assets
	}

	// Priority: running > failed > queued > pending > upstream_failed > succeeded
	sorted := make([]*assetRow, len(t.assets))
	copy(sorted, t.assets)
	sort.SliceStable(sorted, func(i, j int) bool {
		return statusPriority(sorted[i].status) < statusPriority(sorted[j].status)
	})

	return sorted[:maxRows]
}

func statusPriority(s scheduler.TaskInstanceStatus) int {
	switch s {
	case scheduler.Running:
		return 0
	case scheduler.Failed:
		return 1
	case scheduler.Queued:
		return 2
	case scheduler.Pending:
		return 3
	case scheduler.UpstreamFailed:
		return 4
	case scheduler.Succeeded, scheduler.Skipped:
		return 5
	}
	return 6
}

func statusIcon(s scheduler.TaskInstanceStatus) string {
	switch s {
	case scheduler.Succeeded:
		return color.New(color.FgGreen).Sprint("✓")
	case scheduler.Failed:
		return color.New(color.FgRed).Sprint("✗")
	case scheduler.Running:
		return color.New(color.FgYellow).Sprint("⟳")
	case scheduler.UpstreamFailed:
		return color.New(color.FgYellow).Sprint("↑")
	case scheduler.Pending, scheduler.Queued:
		return dimText("○")
	case scheduler.Skipped:
		return dimText("-")
	}
	return " "
}

func dimText(s string) string {
	return color.New(color.Faint).Sprint(s)
}

func fmtDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	m := int(d.Minutes())
	s := int(d.Seconds()) % 60
	return fmt.Sprintf("%dm%02ds", m, s)
}

// stripAnsi removes ANSI escape codes for length calculation.
func stripAnsi(s string) string {
	var result strings.Builder
	inEscape := false
	for _, r := range s {
		if r == '\033' {
			inEscape = true
			continue
		}
		if inEscape {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				inEscape = false
			}
			continue
		}
		result.WriteRune(r)
	}
	return result.String()
}
