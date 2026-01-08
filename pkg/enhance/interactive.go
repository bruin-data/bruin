package enhance

import (
	"fmt"
	"io"
	"strings"

	"github.com/fatih/color"
	"github.com/manifoldco/promptui"
)

var (
	bold    = color.New(color.Bold).SprintFunc()
	cyan    = color.New(color.FgCyan).SprintFunc()
	green   = color.New(color.FgGreen).SprintFunc()
	yellow  = color.New(color.FgYellow).SprintFunc()
	faintFn = color.New(color.Faint).SprintFunc()
)

// InteractiveConfirmer handles user confirmations for suggestions.
type InteractiveConfirmer struct {
	stdin  io.ReadCloser
	stdout io.Writer
}

// NewInteractiveConfirmer creates a new InteractiveConfirmer.
func NewInteractiveConfirmer(stdin io.ReadCloser, stdout io.Writer) *InteractiveConfirmer {
	return &InteractiveConfirmer{stdin: stdin, stdout: stdout}
}

// ConfirmSuggestions displays suggestions and asks for user confirmation.
// Returns only the approved suggestions.
func (c *InteractiveConfirmer) ConfirmSuggestions(suggestions *EnhancementSuggestions, assetName string) (*EnhancementSuggestions, error) {
	fmt.Fprintf(c.stdout, "\n%s\n", bold(fmt.Sprintf("Suggested Enhancements for '%s':", assetName)))
	fmt.Fprintln(c.stdout, strings.Repeat("-", 50))

	approved := &EnhancementSuggestions{
		ColumnDescriptions: make(map[string]string),
		ColumnChecks:       make(map[string][]CheckSuggestion),
	}

	// Asset description
	if suggestions.AssetDescription != "" {
		fmt.Fprintf(c.stdout, "\n%s\n", cyan("Asset Description:"))
		fmt.Fprintf(c.stdout, "  %s\n", suggestions.AssetDescription)
		if c.confirm("Apply this description?") {
			approved.AssetDescription = suggestions.AssetDescription
		}
	}

	// Column descriptions
	if len(suggestions.ColumnDescriptions) > 0 {
		fmt.Fprintf(c.stdout, "\n%s\n", cyan("Column Descriptions:"))
		for col, desc := range suggestions.ColumnDescriptions {
			fmt.Fprintf(c.stdout, "  %s: %s\n", green(col), desc)
		}
		if c.confirm("Apply all column descriptions?") {
			approved.ColumnDescriptions = suggestions.ColumnDescriptions
		}
	}

	// Column checks - ask for each check individually
	if len(suggestions.ColumnChecks) > 0 {
		fmt.Fprintf(c.stdout, "\n%s\n", cyan("Suggested Column Checks:"))
		for col, checks := range suggestions.ColumnChecks {
			for _, check := range checks {
				reasoning := ""
				if check.Reasoning != "" {
					reasoning = faintFn(fmt.Sprintf(" - %s", check.Reasoning))
				}
				valueStr := formatCheckValue(check)
				fmt.Fprintf(c.stdout, "  %s.%s%s%s\n", green(col), yellow(check.Name), valueStr, reasoning)

				if c.confirm(fmt.Sprintf("Add '%s' check to column '%s'?", check.Name, col)) {
					if approved.ColumnChecks[col] == nil {
						approved.ColumnChecks[col] = []CheckSuggestion{}
					}
					approved.ColumnChecks[col] = append(approved.ColumnChecks[col], check)
				}
			}
		}
	}

	// Tags
	if len(suggestions.SuggestedTags) > 0 {
		fmt.Fprintf(c.stdout, "\n%s %v\n", cyan("Suggested Tags:"), suggestions.SuggestedTags)
		if c.confirm("Apply these tags?") {
			approved.SuggestedTags = suggestions.SuggestedTags
		}
	}

	// Domains
	if len(suggestions.SuggestedDomains) > 0 {
		fmt.Fprintf(c.stdout, "\n%s %v\n", cyan("Suggested Domains:"), suggestions.SuggestedDomains)
		if c.confirm("Apply these domains?") {
			approved.SuggestedDomains = suggestions.SuggestedDomains
		}
	}

	// Owner
	if suggestions.SuggestedOwner != "" {
		fmt.Fprintf(c.stdout, "\n%s %s\n", cyan("Suggested Owner:"), suggestions.SuggestedOwner)
		if c.confirm("Apply this owner?") {
			approved.SuggestedOwner = suggestions.SuggestedOwner
		}
	}

	// Custom checks
	if len(suggestions.CustomChecks) > 0 {
		fmt.Fprintf(c.stdout, "\n%s\n", cyan("Suggested Custom Checks:"))
		for _, check := range suggestions.CustomChecks {
			fmt.Fprintf(c.stdout, "  %s: %s\n", yellow(check.Name), check.Description)
			if check.Reasoning != "" {
				fmt.Fprintf(c.stdout, "    %s\n", faintFn(check.Reasoning))
			}
			if c.confirm(fmt.Sprintf("Add custom check '%s'?", check.Name)) {
				approved.CustomChecks = append(approved.CustomChecks, check)
			}
		}
	}

	return approved, nil
}

// confirm asks for user confirmation and returns true if confirmed.
func (c *InteractiveConfirmer) confirm(label string) bool {
	prompt := promptui.Prompt{
		Label:     label,
		IsConfirm: true,
		Stdin:     c.stdin,
	}

	_, err := prompt.Run()
	return err == nil
}

// formatCheckValue formats the check value for display.
func formatCheckValue(check CheckSuggestion) string {
	if check.Value == nil {
		return ""
	}

	switch v := check.Value.(type) {
	case float64:
		return fmt.Sprintf("(%v)", v)
	case int, int64:
		return fmt.Sprintf("(%v)", v)
	case string:
		return fmt.Sprintf("(%q)", v)
	case []interface{}:
		return fmt.Sprintf("(%v)", v)
	case map[string]interface{}:
		if val, ok := v["value"]; ok {
			return fmt.Sprintf("(%v)", val)
		}
	}
	return ""
}

// DisplayAppliedChanges prints a summary of what changes were applied.
func DisplayAppliedChanges(stdout io.Writer, suggestions *EnhancementSuggestions, assetName string) {
	fmt.Fprintf(stdout, "\n%s\n", green(fmt.Sprintf("✓ Enhanced '%s'", assetName)))

	if suggestions.AssetDescription != "" {
		fmt.Fprintf(stdout, "  • Added asset description\n")
	}

	if len(suggestions.ColumnDescriptions) > 0 {
		fmt.Fprintf(stdout, "  • Added %d column description(s): %s\n",
			len(suggestions.ColumnDescriptions), joinKeys(suggestions.ColumnDescriptions))
	}

	if len(suggestions.ColumnChecks) > 0 {
		totalChecks := 0
		checkDetails := []string{}
		for col, checks := range suggestions.ColumnChecks {
			totalChecks += len(checks)
			for _, check := range checks {
				checkDetails = append(checkDetails, fmt.Sprintf("%s.%s", col, check.Name))
			}
		}
		fmt.Fprintf(stdout, "  • Added %d column check(s): %s\n", totalChecks, strings.Join(checkDetails, ", "))
	}

	if len(suggestions.SuggestedTags) > 0 {
		fmt.Fprintf(stdout, "  • Added tags: %v\n", suggestions.SuggestedTags)
	}

	if len(suggestions.SuggestedDomains) > 0 {
		fmt.Fprintf(stdout, "  • Added domains: %v\n", suggestions.SuggestedDomains)
	}

	if suggestions.SuggestedOwner != "" {
		fmt.Fprintf(stdout, "  • Set owner: %s\n", suggestions.SuggestedOwner)
	}

	if len(suggestions.CustomChecks) > 0 {
		checkNames := make([]string, len(suggestions.CustomChecks))
		for i, check := range suggestions.CustomChecks {
			checkNames[i] = check.Name
		}
		fmt.Fprintf(stdout, "  • Added %d custom check(s): %s\n", len(suggestions.CustomChecks), strings.Join(checkNames, ", "))
	}
}

// joinKeys returns a comma-separated list of map keys.
func joinKeys(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return strings.Join(keys, ", ")
}

// DisplaySuggestions prints suggestions without confirmation prompts (for dry-run).
func DisplaySuggestions(stdout io.Writer, suggestions *EnhancementSuggestions, assetName string) {
	fmt.Fprintf(stdout, "\n%s\n", bold(fmt.Sprintf("Suggestions for '%s' (dry-run):", assetName)))
	fmt.Fprintln(stdout, strings.Repeat("-", 50))

	if suggestions.AssetDescription != "" {
		fmt.Fprintf(stdout, "\n%s\n", cyan("Asset Description:"))
		fmt.Fprintf(stdout, "  %s\n", suggestions.AssetDescription)
	}

	if len(suggestions.ColumnDescriptions) > 0 {
		fmt.Fprintf(stdout, "\n%s\n", cyan("Column Descriptions:"))
		for col, desc := range suggestions.ColumnDescriptions {
			fmt.Fprintf(stdout, "  %s: %s\n", green(col), desc)
		}
	}

	if len(suggestions.ColumnChecks) > 0 {
		fmt.Fprintf(stdout, "\n%s\n", cyan("Column Checks:"))
		for col, checks := range suggestions.ColumnChecks {
			for _, check := range checks {
				reasoning := ""
				if check.Reasoning != "" {
					reasoning = faintFn(fmt.Sprintf(" - %s", check.Reasoning))
				}
				valueStr := formatCheckValue(check)
				fmt.Fprintf(stdout, "  %s.%s%s%s\n", green(col), yellow(check.Name), valueStr, reasoning)
			}
		}
	}

	if len(suggestions.SuggestedTags) > 0 {
		fmt.Fprintf(stdout, "\n%s %v\n", cyan("Tags:"), suggestions.SuggestedTags)
	}

	if len(suggestions.SuggestedDomains) > 0 {
		fmt.Fprintf(stdout, "\n%s %v\n", cyan("Domains:"), suggestions.SuggestedDomains)
	}

	if suggestions.SuggestedOwner != "" {
		fmt.Fprintf(stdout, "\n%s %s\n", cyan("Owner:"), suggestions.SuggestedOwner)
	}

	if len(suggestions.CustomChecks) > 0 {
		fmt.Fprintf(stdout, "\n%s\n", cyan("Custom Checks:"))
		for _, check := range suggestions.CustomChecks {
			fmt.Fprintf(stdout, "  %s: %s\n", yellow(check.Name), check.Description)
		}
	}
}
