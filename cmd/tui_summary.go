package cmd

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/fatih/color"
)

// printTUISummary writes the final execution summary to the given writer (usually the real terminal).
func printTUISummary(w io.Writer, results []*scheduler.TaskExecutionResult, s *scheduler.Scheduler, duration time.Duration, pipelineName string) {
	summary := analyzeResults(results, s)
	summary.Duration = duration

	separator := strings.Repeat("═", 50)
	fmt.Fprintf(w, "\n%s\n\n", dimText(separator))

	// Header with pipeline name and duration
	fmt.Fprintf(w, "  Pipeline: %s\n", color.New(color.Bold).Sprint(pipelineName))
	fmt.Fprintf(w, "  Duration: %s\n\n", fmtDuration(duration.Truncate(time.Millisecond)))

	hasFailures := summary.FailedTasks > 0

	// Assets
	if summary.Assets.HasAny() {
		if summary.Assets.Failed > 0 || summary.Assets.FailedDueToChecks > 0 || summary.Assets.Skipped > 0 {
			fmt.Fprintf(w, "  %s Assets executed      %s\n",
				color.New(color.FgRed).Sprint("✗"),
				formatCountWithSkipped(summary.Assets.Total, summary.Assets.Failed, summary.Assets.FailedDueToChecks, summary.Assets.Skipped))
		} else {
			fmt.Fprintf(w, "  %s Assets executed      %s\n",
				color.New(color.FgGreen).Sprint("✓"),
				color.New(color.FgGreen).Sprintf("%d succeeded", summary.Assets.Succeeded))
		}
	}

	// Quality checks
	totalChecks := summary.ColumnChecks.Total + summary.CustomChecks.Total
	totalCheckFailures := summary.ColumnChecks.Failed + summary.CustomChecks.Failed
	totalCheckSkipped := summary.ColumnChecks.Skipped + summary.CustomChecks.Skipped
	if totalChecks > 0 {
		if totalCheckFailures > 0 || totalCheckSkipped > 0 {
			fmt.Fprintf(w, "  %s Quality checks       %s\n",
				color.New(color.FgRed).Sprint("✗"),
				formatCountWithSkipped(totalChecks, totalCheckFailures, 0, totalCheckSkipped))
		} else {
			fmt.Fprintf(w, "  %s Quality checks       %s\n",
				color.New(color.FgGreen).Sprint("✓"),
				color.New(color.FgGreen).Sprintf("%d succeeded", summary.ColumnChecks.Succeeded+summary.CustomChecks.Succeeded))
		}
	}

	// Metadata push
	if summary.MetadataPush.HasAny() {
		metadataExecuted := summary.MetadataPush.Succeeded + summary.MetadataPush.Failed
		if summary.MetadataPush.Failed > 0 {
			fmt.Fprintf(w, "  %s Metadata pushed      %s\n",
				color.New(color.FgRed).Sprint("✗"),
				formatCount(metadataExecuted, summary.MetadataPush.Failed))
		} else {
			fmt.Fprintf(w, "  %s Metadata pushed      %d\n",
				color.New(color.FgGreen).Sprint("✓"), metadataExecuted)
		}
	}

	fmt.Fprintf(w, "\n%s\n", dimText(separator))

	// Overall status
	if hasFailures {
		fmt.Fprintf(w, "\n  %s\n\n",
			color.New(color.FgRed, color.Bold).Sprint("Run completed with failures"))
	} else {
		fmt.Fprintf(w, "\n  %s\n\n",
			color.New(color.FgGreen, color.Bold).Sprint("Run completed successfully"))
	}
}

// printTUIErrors writes error details to the given writer for failed tasks.
func printTUIErrors(w io.Writer, errorsInTaskResults []*scheduler.TaskExecutionResult, s *scheduler.Scheduler) {
	if len(errorsInTaskResults) == 0 {
		return
	}

	data := make(map[string][]*scheduler.TaskExecutionResult, len(errorsInTaskResults))
	for _, result := range errorsInTaskResults {
		assetName := result.Instance.GetAsset().Name
		data[assetName] = append(data[assetName], result)
	}

	fmt.Fprintf(w, "\n%s\n\n", color.New(color.FgRed, color.Bold).Sprint("Errors:"))

	for assetName, results := range data {
		fmt.Fprintf(w, "  %s %s\n",
			color.New(color.FgRed).Sprint("✗"),
			color.New(color.Bold).Sprint(assetName))

		for _, result := range results {
			if result.Error != nil {
				// Indent error message
				errLines := strings.Split(result.Error.Error(), "\n")
				for _, line := range errLines {
					if strings.TrimSpace(line) != "" {
						fmt.Fprintf(w, "    %s\n", line)
					}
				}
			}
		}
		fmt.Fprintln(w)
	}
}
