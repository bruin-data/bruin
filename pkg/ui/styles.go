package ui

import (
	"fmt"

	"github.com/charmbracelet/lipgloss"
	"github.com/fatih/color"
)

// Color palette based on rgb(217, 95, 95) as the primary red
const (
	// Primary colors
	PrimaryRed    = lipgloss.Color("#D95F5F") // rgb(217, 95, 95)
	ErrorRed      = lipgloss.Color("#DC2626") // A bit more intense for critical errors
	WarningAmber  = lipgloss.Color("#F59E0B") // Amber for warnings
	SuccessGreen  = lipgloss.Color("#059669") // Professional green for success
	InfoBlue      = lipgloss.Color("#3B82F6") // Clear blue for info
	
	// Neutral tones
	TextPrimary   = lipgloss.Color("#374151") // Dark gray for primary text
	TextSecondary = lipgloss.Color("#6B7280") // Medium gray for secondary text
	TextFaint     = lipgloss.Color("#9CA3AF") // Light gray for faint text
	TextWhite     = lipgloss.Color("#FFFFFF") // Pure white
	
	// Background colors
	BackgroundLight = lipgloss.Color("#F9FAFB") // Very light gray background
	BackgroundDark  = lipgloss.Color("#1F2937") // Dark background for highlights
)

// Base styles
var (
	// Text styles
	ErrorStyle = lipgloss.NewStyle().
		Foreground(ErrorRed).
		Bold(true)
	
	WarningStyle = lipgloss.NewStyle().
		Foreground(WarningAmber).
		Bold(true)
	
	SuccessStyle = lipgloss.NewStyle().
		Foreground(SuccessGreen).
		Bold(true)
	
	InfoStyle = lipgloss.NewStyle().
		Foreground(InfoBlue).
		Bold(true)
	
	FaintStyle = lipgloss.NewStyle().
		Foreground(TextFaint)
	
	BoldStyle = lipgloss.NewStyle().
		Bold(true)
	
	// Status indicators with icons
	ErrorIcon = ErrorStyle.Render("✗")
	WarningIcon = WarningStyle.Render("⚠")
	SuccessIcon = SuccessStyle.Render("✓")
	InfoIcon = InfoStyle.Render("ℹ")
	
	// Message box styles
	ErrorBox = lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(ErrorRed).
		Padding(1, 2).
		Margin(1, 0)
	
	WarningBox = lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(WarningAmber).
		Padding(1, 2).
		Margin(1, 0)
	
	InfoBox = lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(InfoBlue).
		Padding(1, 2).
		Margin(1, 0)
	
	SuccessBox = lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(SuccessGreen).
		Padding(1, 2).
		Margin(1, 0)
	
	// Table and layout styles
	HeaderStyle = lipgloss.NewStyle().
		Foreground(TextWhite).
		Background(BackgroundDark).
		Padding(0, 1).
		Bold(true)
	
	CellStyle = lipgloss.NewStyle().
		Padding(0, 1)
	
	SeparatorStyle = lipgloss.NewStyle().
		Foreground(TextFaint)
)

// Formatters for common message types
func FormatError(message string) string {
	return fmt.Sprintf("%s %s", ErrorIcon, ErrorStyle.Render(message))
}

func FormatWarning(message string) string {
	return fmt.Sprintf("%s %s", WarningIcon, WarningStyle.Render(message))
}

func FormatSuccess(message string) string {
	return fmt.Sprintf("%s %s", SuccessIcon, SuccessStyle.Render(message))
}

func FormatInfo(message string) string {
	return fmt.Sprintf("%s %s", InfoIcon, InfoStyle.Render(message))
}

// Enhanced message formatting with context
func FormatErrorWithContext(title, message string) string {
	content := fmt.Sprintf("%s %s\n\n%s", ErrorIcon, ErrorStyle.Render(title), message)
	return ErrorBox.Render(content)
}

func FormatWarningWithContext(title, message string) string {
	content := fmt.Sprintf("%s %s\n\n%s", WarningIcon, WarningStyle.Render(title), message)
	return WarningBox.Render(content)
}

func FormatInfoWithContext(title, message string) string {
	content := fmt.Sprintf("%s %s\n\n%s", InfoIcon, InfoStyle.Render(title), message)
	return InfoBox.Render(content)
}

func FormatSuccessWithContext(title, message string) string {
	content := fmt.Sprintf("%s %s\n\n%s", SuccessIcon, SuccessStyle.Render(title), message)
	return SuccessBox.Render(content)
}

// Status formatting for execution results
type StatusType int

const (
	StatusSuccess StatusType = iota
	StatusError
	StatusWarning
	StatusSkip
	StatusRunning
	StatusUpstreamFailed
)

func FormatStatus(status StatusType, text string) string {
	switch status {
	case StatusSuccess:
		return lipgloss.NewStyle().Foreground(SuccessGreen).Render(text)
	case StatusError:
		return lipgloss.NewStyle().Foreground(ErrorRed).Render(text)
	case StatusWarning:
		return lipgloss.NewStyle().Foreground(WarningAmber).Render(text)
	case StatusSkip:
		return FaintStyle.Render(text)
	case StatusRunning:
		return InfoStyle.Render(text)
	case StatusUpstreamFailed:
		return lipgloss.NewStyle().Foreground(WarningAmber).Render(text)
	default:
		return text
	}
}

// Pipeline and asset name formatting
func FormatPipelineName(name, path string) string {
	return fmt.Sprintf("%s %s",
		lipgloss.NewStyle().Foreground(InfoBlue).Bold(true).Render(name),
		FaintStyle.Render(fmt.Sprintf("(%s)", path)))
}

func FormatAssetName(name, path string) string {
	return fmt.Sprintf("%s %s",
		lipgloss.NewStyle().Foreground(TextPrimary).Bold(true).Render(name),
		FaintStyle.Render(fmt.Sprintf("(%s)", path)))
}

// Tree-style connectors for hierarchical output
func TreeConnector(isLast bool) string {
	if isLast {
		return FaintStyle.Render("└──")
	}
	return FaintStyle.Render("├──")
}

func TreePipe(isLast bool) string {
	if isLast {
		return FaintStyle.Render(" ")
	}
	return FaintStyle.Render("│")
}

// Formatting for execution summaries
func FormatExecutionSummary(title string, hasFailures bool, duration string) string {
	var titleStyle lipgloss.Style
	var icon string
	
	if hasFailures {
		titleStyle = ErrorStyle
		icon = ErrorIcon
	} else {
		titleStyle = SuccessStyle
		icon = SuccessIcon
	}
	
	return fmt.Sprintf("\n%s %s in %s\n",
		icon,
		titleStyle.Render(title),
		FaintStyle.Render(duration))
}

// Quality check dots formatting
func FormatCheckResult(status StatusType) string {
	switch status {
	case StatusSuccess:
		return lipgloss.NewStyle().Foreground(SuccessGreen).Render(".")
	case StatusError:
		return lipgloss.NewStyle().Foreground(ErrorRed).Render("F")
	case StatusSkip:
		return FaintStyle.Render(".")
	case StatusUpstreamFailed:
		return lipgloss.NewStyle().Foreground(WarningAmber).Render("U")
	default:
		return FaintStyle.Render(".")
	}
}

// Progress and statistics formatting
func FormatProgressStats(succeeded, failed, skipped int) string {
	parts := []string{}
	
	if succeeded > 0 {
		parts = append(parts, lipgloss.NewStyle().
			Foreground(SuccessGreen).
			Render(fmt.Sprintf("%d succeeded", succeeded)))
	}
	
	if failed > 0 {
		parts = append(parts, lipgloss.NewStyle().
			Foreground(ErrorRed).
			Render(fmt.Sprintf("%d failed", failed)))
	}
	
	if skipped > 0 {
		parts = append(parts, FaintStyle.Render(fmt.Sprintf("%d skipped", skipped)))
	}
	
	if len(parts) == 0 {
		return "0"
	}
	
	result := parts[0]
	for i := 1; i < len(parts); i++ {
		result += FaintStyle.Render(" / ") + parts[i]
	}
	
	return result
}

// Printer interface compatibility helpers
type StylePrinter struct {
	style lipgloss.Style
}

func (p StylePrinter) Print(args ...interface{}) (int, error) {
	return fmt.Print(p.style.Render(fmt.Sprint(args...)))
}

func (p StylePrinter) Printf(format string, args ...interface{}) (int, error) {
	return fmt.Print(p.style.Render(fmt.Sprintf(format, args...)))
}

func (p StylePrinter) Println(args ...interface{}) (int, error) {
	return fmt.Print(p.style.Render(fmt.Sprint(args...)) + "\n")
}

func (p StylePrinter) Fprintf(w interface{}, format string, args ...interface{}) (int, error) {
	return fmt.Print(p.style.Render(fmt.Sprintf(format, args...)))
}

// Create printer-compatible wrappers
func NewErrorPrinter() StylePrinter   { return StylePrinter{ErrorStyle} }
func NewWarningPrinter() StylePrinter { return StylePrinter{WarningStyle} }
func NewSuccessPrinter() StylePrinter { return StylePrinter{SuccessStyle} }
func NewInfoPrinter() StylePrinter    { return StylePrinter{InfoStyle} }
func NewFaintPrinter() StylePrinter   { return StylePrinter{FaintStyle} }
func NewPlainPrinter() StylePrinter   { return StylePrinter{lipgloss.NewStyle()} }

// Legacy support - for cases that specifically need *color.Color
func GetLegacyErrorColor() *color.Color   { return color.New(color.FgRed) }
func GetLegacyWarningColor() *color.Color { return color.New(color.FgYellow) }
func GetLegacySuccessColor() *color.Color { return color.New(color.FgGreen) }
func GetLegacyInfoColor() *color.Color    { return color.New(color.Bold) }