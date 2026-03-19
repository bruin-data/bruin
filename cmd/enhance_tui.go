package cmd

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	"golang.org/x/term"
)

type enhanceStatus int

const (
	enhancePending enhanceStatus = iota
	enhanceRunning
	enhanceSucceeded
	enhanceFailed
)

const maxLogsPerAsset = 5

type enhanceAssetRow struct {
	name     string
	status   enhanceStatus
	step     string // current step label, e.g. "filling columns", "enhancing..."
	startAt  time.Time
	duration time.Duration
	logs     []string // last N streaming lines from the agent (for TUI display)
	allLogs  []string // all streaming lines (for post-failure reporting)
}

// EnhanceTUI manages a live-updating terminal display for asset enhancement.
type EnhanceTUI struct {
	terminal *os.File

	mu       sync.Mutex
	assets   []*enhanceAssetRow
	assetMap map[string]*enhanceAssetRow

	frame     int
	ticker    *time.Ticker
	done      chan struct{}
	stopped   bool
	lastLines int
	startTime time.Time
}

// NewEnhanceTUI creates a new enhance TUI renderer.
// assetKeys are unique identifiers (e.g. relative paths) used for map lookups.
// displayNames are the human-readable names shown in the TUI (e.g. base filenames).
func NewEnhanceTUI(terminal *os.File, assetKeys, displayNames []string) *EnhanceTUI {
	t := &EnhanceTUI{
		terminal:  terminal,
		assetMap:  make(map[string]*enhanceAssetRow),
		done:      make(chan struct{}),
		startTime: time.Now(),
	}
	for i, key := range assetKeys {
		row := &enhanceAssetRow{name: displayNames[i], status: enhancePending}
		t.assets = append(t.assets, row)
		t.assetMap[key] = row
	}
	return t
}

// MarkRunning marks an asset as currently being enhanced.
func (t *EnhanceTUI) MarkRunning(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if row, ok := t.assetMap[name]; ok {
		row.status = enhanceRunning
		row.startAt = time.Now()
	}
}

// SetStep updates the current step label for an asset (e.g. "filling columns", "enhancing...").
func (t *EnhanceTUI) SetStep(name, step string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if row, ok := t.assetMap[name]; ok {
		row.step = step
	}
}

// MarkDone marks an asset as successfully enhanced.
func (t *EnhanceTUI) MarkDone(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if row, ok := t.assetMap[name]; ok {
		row.status = enhanceSucceeded
		row.step = "done"
		row.duration = time.Since(row.startAt)
		row.logs = nil
		row.allLogs = nil
	}
}

// MarkFailed marks an asset as failed.
func (t *EnhanceTUI) MarkFailed(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if row, ok := t.assetMap[name]; ok {
		row.status = enhanceFailed
		row.step = "failed"
		row.duration = time.Since(row.startAt)
	}
}

func (t *EnhanceTUI) addLog(asset, line string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	row, ok := t.assetMap[asset]
	if !ok {
		return
	}
	row.allLogs = append(row.allLogs, line)
	row.logs = append(row.logs, line)
	if len(row.logs) > maxLogsPerAsset {
		row.logs = row.logs[len(row.logs)-maxLogsPerAsset:]
	}
}

// GetLogs returns all collected log lines for the given asset key.
func (t *EnhanceTUI) GetLogs(name string) []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	if row, ok := t.assetMap[name]; ok {
		result := make([]string, len(row.allLogs))
		copy(result, row.allLogs)
		return result
	}
	return nil
}

// LogWriter returns an io.Writer that captures output lines for the named asset.
func (t *EnhanceTUI) LogWriter(assetName string) *enhanceLogWriter {
	return &enhanceLogWriter{tui: t, assetName: assetName}
}

// Start begins the TUI rendering loop.
func (t *EnhanceTUI) Start() {
	fmt.Fprint(t.terminal, "\033[?25l") // hide cursor
	t.ticker = time.NewTicker(100 * time.Millisecond)
	go t.renderLoop()
}

// Stop stops the TUI rendering loop and restores cursor.
func (t *EnhanceTUI) Stop() {
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		return
	}
	t.stopped = true
	t.mu.Unlock()

	close(t.done)
	t.ticker.Stop()

	// Final render to reflect completed state
	t.render()

	// Clear the rendered frame
	t.mu.Lock()
	t.clearLastRender()
	t.mu.Unlock()

	fmt.Fprint(t.terminal, "\033[?25h") // show cursor
}

func (t *EnhanceTUI) renderLoop() {
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

func (t *EnhanceTUI) render() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.frame++
	width, height := t.getTerminalSize()
	output := t.buildOutput(width, height)

	t.clearLastRender()
	fmt.Fprint(t.terminal, output)
	t.lastLines = strings.Count(output, "\n")
}

func (t *EnhanceTUI) clearLastRender() {
	if t.lastLines > 0 {
		_, h := t.getTerminalSize()
		lines := min(t.lastLines, h-1)
		for range lines {
			fmt.Fprint(t.terminal, "\033[A\033[2K")
		}
		fmt.Fprint(t.terminal, "\r")
	}
}

func (t *EnhanceTUI) getTerminalSize() (int, int) {
	w, h, err := term.GetSize(int(t.terminal.Fd())) //nolint:gosec // G115: fd is always safe to convert
	if err != nil {
		return 80, 24
	}
	return w, h
}

func (t *EnhanceTUI) buildOutput(width, height int) string {
	var sb strings.Builder
	elapsed := time.Since(t.startTime).Truncate(time.Second)

	total := len(t.assets)
	completed := t.countByStatus(enhanceSucceeded) + t.countByStatus(enhanceFailed)
	running := t.countByStatus(enhanceRunning)

	// Header
	header := fmt.Sprintf("  Enhancing assets | %d/%d done | %s",
		completed, total, fmtDuration(elapsed))
	if running > 0 {
		header += fmt.Sprintf(" | %d running", running)
	}
	sb.WriteString(header + "\n\n")

	// Calculate how many lines each running asset needs (1 row + up to maxLogsPerAsset log lines)
	// and fit within the terminal height: header(2) + footer(2) + overflow(1)
	budgetLines := max(height-5, 6)

	displayAssets := t.getDisplayOrder(budgetLines)

	// Find the longest asset name for alignment
	maxName := 0
	for _, row := range displayAssets {
		if len(row.name) > maxName {
			maxName = len(row.name)
		}
	}

	for _, row := range displayAssets {
		sb.WriteString(t.renderAssetRow(row, width, maxName))
		sb.WriteString("\n")

		// Show streaming log lines beneath running assets
		if row.status == enhanceRunning && len(row.logs) > 0 {
			for _, line := range row.logs {
				maxLine := max(width-8, 10)
				if len(line) > maxLine {
					line = line[:maxLine-3] + "..."
				}
				sb.WriteString(dimText("     │ "+line) + "\n")
			}
		}
	}

	if total > len(displayAssets) {
		sb.WriteString(dimText(fmt.Sprintf("  ... and %d more\n", total-len(displayAssets))))
	}

	// Footer: status summary
	succeeded := t.countByStatus(enhanceSucceeded)
	failed := t.countByStatus(enhanceFailed)
	pending := t.countByStatus(enhancePending)

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

func (t *EnhanceTUI) renderAssetRow(row *enhanceAssetRow, _ int, nameColWidth int) string {
	icon := enhanceStatusIcon(row.status)

	var durStr string
	switch {
	case row.status == enhanceRunning && !row.startAt.IsZero():
		durStr = fmtDuration(time.Since(row.startAt).Truncate(100 * time.Millisecond))
	case row.duration > 0:
		durStr = fmtDuration(row.duration.Truncate(time.Millisecond))
	default:
		durStr = "-"
	}

	name := row.name
	displayName := name
	switch row.status {
	case enhanceFailed:
		displayName = color.New(color.FgRed).Sprint(name)
	case enhanceRunning:
		displayName = shimmerText(name, t.frame)
	case enhancePending:
		displayName = dimText(name)
	case enhanceSucceeded:
		// default color
	}

	padding := max(nameColWidth-len(name), 0)

	// Format the step label
	displayStep := ""
	if row.step != "" {
		switch row.status {
		case enhanceRunning:
			displayStep = color.New(color.FgYellow).Sprint(row.step)
		case enhanceFailed:
			displayStep = color.New(color.FgRed).Sprint(row.step)
		case enhanceSucceeded:
			displayStep = color.New(color.FgGreen).Sprint(row.step)
		case enhancePending:
			displayStep = dimText(row.step)
		}
	}

	return fmt.Sprintf("  %s %s%s  %-16s %8s", icon, displayName, strings.Repeat(" ", padding), displayStep, durStr)
}

// getDisplayOrder returns assets to display, prioritizing running ones.
// budgetLines is the total line budget available for asset rows + their inline logs.
func (t *EnhanceTUI) getDisplayOrder(budgetLines int) []*enhanceAssetRow {
	if t.totalLines(t.assets) <= budgetLines {
		return t.assets
	}

	// Priority: running > failed > pending > succeeded
	sorted := make([]*enhanceAssetRow, len(t.assets))
	copy(sorted, t.assets)
	sort.SliceStable(sorted, func(i, j int) bool {
		return enhanceStatusPriority(sorted[i].status) < enhanceStatusPriority(sorted[j].status)
	})

	// Greedily pick assets that fit within the budget
	result := make([]*enhanceAssetRow, 0, len(sorted))
	used := 0
	for _, row := range sorted {
		needed := 1 // the asset row itself
		if row.status == enhanceRunning {
			needed += len(row.logs)
		}
		if used+needed > budgetLines {
			// Try to fit at least the row without logs
			if used+1 <= budgetLines {
				result = append(result, row)
				used++
			}
			continue
		}
		result = append(result, row)
		used += needed
	}
	return result
}

func (t *EnhanceTUI) totalLines(assets []*enhanceAssetRow) int {
	n := 0
	for _, row := range assets {
		n++ // the row itself
		if row.status == enhanceRunning {
			n += len(row.logs)
		}
	}
	return n
}

func enhanceStatusPriority(s enhanceStatus) int {
	switch s {
	case enhanceRunning:
		return 0
	case enhanceFailed:
		return 1
	case enhancePending:
		return 2
	case enhanceSucceeded:
		return 3
	}
	return 4
}

func enhanceStatusIcon(s enhanceStatus) string {
	switch s {
	case enhanceSucceeded:
		return color.New(color.FgGreen).Sprint("✓")
	case enhanceFailed:
		return color.New(color.FgRed).Sprint("✗")
	case enhanceRunning:
		return color.New(color.FgYellow).Sprint("⟳")
	case enhancePending:
		return dimText("○")
	}
	return " "
}

func (t *EnhanceTUI) countByStatus(status enhanceStatus) int {
	count := 0
	for _, row := range t.assets {
		if row.status == status {
			count++
		}
	}
	return count
}

// enhanceLogWriter implements io.Writer and captures lines for a specific asset in the TUI.
type enhanceLogWriter struct {
	tui       *EnhanceTUI
	assetName string
	mu        sync.Mutex
	buf       []byte
}

func (w *enhanceLogWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = append(w.buf, p...)
	for {
		idx := bytes.IndexByte(w.buf, '\n')
		if idx < 0 {
			break
		}
		line := strings.TrimRight(string(w.buf[:idx]), "\r")
		w.buf = w.buf[idx+1:]
		if strings.TrimSpace(line) != "" {
			w.tui.addLog(w.assetName, line)
		}
	}
	return len(p), nil
}

// Flush writes any remaining partial line to the TUI log.
func (w *enhanceLogWriter) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.buf) > 0 {
		line := strings.TrimRight(string(w.buf), "\r\n")
		if strings.TrimSpace(line) != "" {
			w.tui.addLog(w.assetName, line)
		}
		w.buf = nil
	}
}
