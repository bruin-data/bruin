package executor

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"golang.org/x/term"
)

type DisplayStatus int

const (
	StatusPending DisplayStatus = iota
	StatusRunning
	StatusSuccess
	StatusFailed
	StatusSkipped
	StatusUpstreamFailed
)

type AssetDisplayState struct {
	Name         string
	Group        string
	Status       DisplayStatus
	Duration     time.Duration
	Error        string
	StartedAt    time.Time
	ChecksPassed int
	ChecksFailed int
	ChecksTotal  int
}

var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

type LiveDisplay struct {
	mu         sync.Mutex
	assets     []AssetDisplayState
	assetIndex map[string]int
	linesDrawn int
	isTerminal bool
	noColor    bool
	spinFrame  int
	stopCh     chan struct{}
	stoppedCh  chan struct{}
}

func AssetGroup(asset *pipeline.Asset, pipelineDir string) string {
	assetPath := asset.ExecutableFile.Path
	if assetPath == "" {
		assetPath = asset.DefinitionFile.Path
	}
	if assetPath == "" {
		return ""
	}

	dir := filepath.Dir(assetPath)
	rel, err := filepath.Rel(pipelineDir, dir)
	if err != nil {
		return ""
	}

	parts := strings.Split(filepath.ToSlash(rel), "/")

	startIdx := 0
	if len(parts) > 0 && (parts[0] == "assets" || parts[0] == "tasks") {
		startIdx = 1
	}

	if startIdx >= len(parts) || (len(parts) == 1 && parts[0] == ".") {
		return ""
	}

	remaining := parts[startIdx:]
	if len(remaining) == 0 || (len(remaining) == 1 && remaining[0] == ".") {
		return ""
	}

	return strings.Join(remaining, "/")
}

type AssetInfo struct {
	Name        string
	Group       string
	ChecksTotal int
}

func NewLiveDisplay(assetInfos []AssetInfo, noColor bool) *LiveDisplay {
	isTTY := term.IsTerminal(int(os.Stdout.Fd()))

	assets := make([]AssetDisplayState, len(assetInfos))
	index := make(map[string]int, len(assetInfos))

	for i, info := range assetInfos {
		assets[i] = AssetDisplayState{
			Name:        info.Name,
			Group:       info.Group,
			Status:      StatusPending,
			ChecksTotal: info.ChecksTotal,
		}
		index[info.Name] = i
	}

	return &LiveDisplay{
		assets:     assets,
		assetIndex: index,
		isTerminal: isTTY,
		noColor:    noColor,
		stopCh:     make(chan struct{}),
		stoppedCh:  make(chan struct{}),
	}
}

func (d *LiveDisplay) Start() {
	d.mu.Lock()
	d.render()
	d.mu.Unlock()

	go d.spinLoop()
}

func (d *LiveDisplay) Stop() {
	close(d.stopCh)
	<-d.stoppedCh

	d.mu.Lock()
	d.render()
	d.mu.Unlock()
}

func (d *LiveDisplay) spinLoop() {
	defer close(d.stoppedCh)
	ticker := time.NewTicker(80 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.mu.Lock()
			hasRunning := false
			for _, a := range d.assets {
				if a.Status == StatusRunning {
					hasRunning = true
					break
				}
			}
			if hasRunning {
				d.spinFrame++
				d.render()
			}
			d.mu.Unlock()
		}
	}
}

func (d *LiveDisplay) MarkRunning(assetName string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	idx, ok := d.assetIndex[assetName]
	if !ok {
		return
	}

	if d.assets[idx].Status != StatusPending {
		return
	}

	d.assets[idx].Status = StatusRunning
	d.assets[idx].StartedAt = time.Now()
	d.render()
}

func (d *LiveDisplay) MarkSuccess(assetName string, duration time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()

	idx, ok := d.assetIndex[assetName]
	if !ok {
		return
	}

	d.assets[idx].Status = StatusSuccess
	d.assets[idx].Duration = duration
	d.render()
}

func (d *LiveDisplay) MarkFailed(assetName string, duration time.Duration, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	idx, ok := d.assetIndex[assetName]
	if !ok {
		return
	}

	d.assets[idx].Status = StatusFailed
	d.assets[idx].Duration = duration

	if err != nil {
		errMsg := err.Error()
		if nlPos := strings.Index(errMsg, "\n"); nlPos != -1 {
			errMsg = errMsg[:nlPos]
		}
		if len(errMsg) > 80 {
			errMsg = errMsg[:77] + "..."
		}
		d.assets[idx].Error = errMsg
	}

	d.render()
}

func (d *LiveDisplay) MarkSkipped(assetName string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	idx, ok := d.assetIndex[assetName]
	if !ok {
		return
	}

	d.assets[idx].Status = StatusSkipped
	d.render()
}

func (d *LiveDisplay) MarkUpstreamFailed(assetName string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	idx, ok := d.assetIndex[assetName]
	if !ok {
		return
	}

	d.assets[idx].Status = StatusUpstreamFailed
	d.render()
}

func (d *LiveDisplay) RecordCheckResult(assetName string, passed bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	idx, ok := d.assetIndex[assetName]
	if !ok {
		return
	}

	if passed {
		d.assets[idx].ChecksPassed++
	} else {
		d.assets[idx].ChecksFailed++
	}
	d.render()
}

func (d *LiveDisplay) render() {
	if !d.isTerminal {
		d.renderSimple()
		return
	}

	d.renderLive()
}

func (d *LiveDisplay) renderLive() {
	if d.linesDrawn > 0 {
		fmt.Printf("\033[%dA", d.linesDrawn)
	}

	output := d.buildTable()
	lines := strings.Count(output, "\n")

	fmt.Print(output)
	d.linesDrawn = lines
}

func (d *LiveDisplay) renderSimple() {
	output := d.buildTable()
	lines := strings.Count(output, "\n")

	if d.linesDrawn > 0 {
		return
	}

	allDone := true
	for _, a := range d.assets {
		if a.Status == StatusPending || a.Status == StatusRunning {
			allDone = false
			break
		}
	}

	if allDone {
		fmt.Print(output)
		d.linesDrawn = lines
	}
}

func (d *LiveDisplay) buildTable() string {
	t := table.NewWriter()
	t.SetStyle(table.StyleLight)

	t.Style().Options.SeparateRows = false
	t.Style().Options.SeparateColumns = true
	t.Style().Options.DrawBorder = true

	t.AppendHeader(table.Row{"", "Asset", "Status", "Checks", "Message"})

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, WidthMax: 3, Align: text.AlignCenter},
		{Number: 2, WidthMax: 40},
		{Number: 3, WidthMax: 20, Align: text.AlignLeft},
		{Number: 4, WidthMax: 10, Align: text.AlignCenter},
		{Number: 5, WidthMax: 80},
	})

	lastGroup := ""
	for _, asset := range d.assets {
		if asset.Group != "" && asset.Group != lastGroup {
			lastGroup = asset.Group
			if d.noColor {
				t.AppendSeparator()
				t.AppendRow(table.Row{"", asset.Group, "", "", ""})
			} else {
				t.AppendSeparator()
				t.AppendRow(table.Row{"", color.New(color.FgWhite, color.Bold).Sprint(asset.Group), "", "", ""})
			}
		}

		icon := d.statusIcon(asset.Status)
		displayName := d.assetDisplayName(asset)
		statusStr := d.statusText(asset)
		checksStr := d.checksText(asset)
		messageStr := d.messageText(asset)

		t.AppendRow(table.Row{icon, displayName, statusStr, checksStr, messageStr})
	}

	return t.Render() + "\n"
}

func (d *LiveDisplay) assetDisplayName(asset AssetDisplayState) string {
	name := asset.Name

	if asset.Group != "" {
		prefix := asset.Group + "."
		if strings.HasPrefix(name, prefix) {
			name = strings.TrimPrefix(name, prefix)
		}
	}

	switch asset.Status {
	case StatusPending, StatusSkipped:
		if !d.noColor {
			return color.New(color.Faint).Sprint(name)
		}
	}

	return name
}

func (d *LiveDisplay) statusIcon(status DisplayStatus) string {
	switch status {
	case StatusPending:
		if d.noColor {
			return "○"
		}
		return color.New(color.Faint).Sprint("○")
	case StatusRunning:
		frame := spinnerFrames[d.spinFrame%len(spinnerFrames)]
		if d.noColor {
			return frame
		}
		return color.New(color.FgYellow).Sprint(frame)
	case StatusSuccess:
		if d.noColor {
			return "✓"
		}
		return color.New(color.FgGreen).Sprint("✓")
	case StatusFailed:
		if d.noColor {
			return "✗"
		}
		return color.New(color.FgRed).Sprint("✗")
	case StatusSkipped:
		if d.noColor {
			return "↷"
		}
		return color.New(color.Faint).Sprint("↷")
	case StatusUpstreamFailed:
		if d.noColor {
			return "↷"
		}
		return color.New(color.FgYellow).Sprint("↷")
	}
	return " "
}

func (d *LiveDisplay) statusText(asset AssetDisplayState) string {
	switch asset.Status {
	case StatusPending:
		if d.noColor {
			return "pending"
		}
		return color.New(color.Faint).Sprint("pending")
	case StatusRunning:
		elapsed := time.Since(asset.StartedAt).Truncate(time.Millisecond)
		txt := fmt.Sprintf("running %s", formatDuration(elapsed))
		if d.noColor {
			return txt
		}
		return color.New(color.FgYellow).Sprint(txt)
	case StatusSuccess:
		txt := fmt.Sprintf("success %s", formatDuration(asset.Duration))
		if d.noColor {
			return txt
		}
		return color.New(color.FgGreen).Sprint(txt)
	case StatusFailed:
		txt := fmt.Sprintf("failed %s", formatDuration(asset.Duration))
		if d.noColor {
			return txt
		}
		return color.New(color.FgRed).Sprint(txt)
	case StatusSkipped:
		if d.noColor {
			return "skipped"
		}
		return color.New(color.Faint).Sprint("skipped")
	case StatusUpstreamFailed:
		if d.noColor {
			return "upstream failed"
		}
		return color.New(color.FgYellow).Sprint("upstream failed")
	}
	return ""
}

func (d *LiveDisplay) checksText(asset AssetDisplayState) string {
	if asset.ChecksTotal == 0 {
		return ""
	}

	ran := asset.ChecksPassed + asset.ChecksFailed

	switch {
	case asset.Status == StatusPending || asset.Status == StatusSkipped || asset.Status == StatusUpstreamFailed:
		txt := fmt.Sprintf("0/%d", asset.ChecksTotal)
		if d.noColor {
			return txt
		}
		return color.New(color.Faint).Sprint(txt)
	case asset.ChecksFailed > 0:
		txt := fmt.Sprintf("%d/%d", ran, asset.ChecksTotal)
		if d.noColor {
			return txt
		}
		return color.New(color.FgRed).Sprint(txt)
	case ran == asset.ChecksTotal:
		txt := fmt.Sprintf("%d/%d", ran, asset.ChecksTotal)
		if d.noColor {
			return txt
		}
		return color.New(color.FgGreen).Sprint(txt)
	default:
		return fmt.Sprintf("%d/%d", ran, asset.ChecksTotal)
	}
}

func (d *LiveDisplay) messageText(asset AssetDisplayState) string {
	if asset.Error != "" {
		if d.noColor {
			return asset.Error
		}
		return color.New(color.FgRed).Sprint(asset.Error)
	}

	return ""
}

func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
}
