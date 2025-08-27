package lint

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/ui"
	"github.com/pkg/errors"
)

type Printer struct {
	RootCheckPath string
}

type (
	ruleIssue struct {
		rule  Rule
		issue *Issue
	}
)

// Removed global variables - using ui package styles directly

func (l *Printer) PrintIssues(analysis *PipelineAnalysisResult) {
	for _, pipelineIssues := range analysis.Pipelines {
		l.printPipelineSummary(pipelineIssues)
	}
}

func (l *Printer) PrintJSON(analysis *PipelineAnalysisResult) error {
	jsonRes, err := json.MarshalIndent(analysis, "", "  ")
	if err != nil {
		return errors.Wrap(err, "failed to convert lint result to JSON")
	}

	fmt.Println(string(jsonRes))
	return nil
}

func (l *Printer) printPipelineSummary(pipelineIssues *PipelineIssues) {
	fmt.Println() // Empty line before pipeline

	pipelineDirectory := l.relativePipelinePath(pipelineIssues.Pipeline)
	fmt.Printf("Pipeline: %s\n", ui.FormatPipelineName(pipelineIssues.Pipeline.Name, pipelineDirectory))

	if len(pipelineIssues.Issues) == 0 {
		fmt.Printf("  %s\n", ui.FormatSuccess("No issues found"))
		return
	}

	genericIssues := make(map[Rule][]*Issue, 0)
	taskIssueMap := make(map[*pipeline.Asset][]*ruleIssue, 0)

	for rule, issues := range pipelineIssues.Issues {
		for _, issue := range issues {
			if issue.Task == nil {
				if _, ok := genericIssues[rule]; !ok {
					genericIssues[rule] = make([]*Issue, 0)
				}

				genericIssues[rule] = append(genericIssues[rule], issue)
				continue
			}

			// create the defaults if there are no issues for this task yet
			if _, ok := taskIssueMap[issue.Task]; !ok {
				taskIssueMap[issue.Task] = make([]*ruleIssue, 0)
			}

			taskIssueMap[issue.Task] = append(taskIssueMap[issue.Task], &ruleIssue{rule, issue})
		}
	}

	printGenericIssues(genericIssues)
	if len(genericIssues) > 0 && len(taskIssueMap) > 0 {
		fmt.Println()
	}

	for task, summary := range taskIssueMap {
		relativeTaskPath := pipelineIssues.Pipeline.RelativeAssetPath(task)
		fmt.Printf("  %s\n", ui.FormatAssetName(task.Name, relativeTaskPath))
		printAssetIssues(summary)

		fmt.Println()
	}
}

func (l *Printer) relativePipelinePath(p *pipeline.Pipeline) string {
	absolutePipelineRoot := filepath.Dir(p.DefinitionFile.Path)

	absRootPath, err := filepath.Abs(l.RootCheckPath)
	if err != nil {
		return absolutePipelineRoot
	}

	pipelineDirectory, err := filepath.Rel(absRootPath, absolutePipelineRoot)
	if err != nil {
		return absolutePipelineRoot
	}

	return pipelineDirectory
}

func printGenericIssues(genericIssues map[Rule][]*Issue) {
	totalIssueCount := 0
	for _, issues := range genericIssues {
		totalIssueCount += len(issues)
	}

	printedIssueCount := 0
	for rule, issues := range genericIssues {
		for _, issue := range issues {
			printedIssueCount++

			connector := ui.TreeConnector(printedIssueCount == totalIssueCount)
			isLast := printedIssueCount == totalIssueCount
			
			var messageFormat string
			if rule.GetSeverity() == ValidatorSeverityWarning {
				messageFormat = fmt.Sprintf("    %s %s %s\n", 
					connector, 
					ui.WarningStyle.Render(issue.Description), 
					ui.FaintStyle.Render(fmt.Sprintf("(%s)", rule.Name())))
			} else {
				messageFormat = fmt.Sprintf("    %s %s %s\n", 
					connector, 
					ui.ErrorStyle.Render(issue.Description), 
					ui.FaintStyle.Render(fmt.Sprintf("(%s)", rule.Name())))
			}
			
			fmt.Print(messageFormat)
			printIssueContext(rule.GetSeverity(), issue.Context, isLast)
		}
	}
}

func printAssetIssues(assetIssues []*ruleIssue) {
	issueCount := len(assetIssues)
	for index, ruleIssue := range assetIssues {
		rule := ruleIssue.rule
		issue := ruleIssue.issue

		connector := ui.TreeConnector(index == issueCount-1)
		isLast := index == issueCount-1
		
		var messageFormat string
		if rule.GetSeverity() == ValidatorSeverityWarning {
			messageFormat = fmt.Sprintf("    %s %s %s\n", 
				connector, 
				ui.WarningStyle.Render(issue.Description), 
				ui.FaintStyle.Render(fmt.Sprintf("(%s)", rule.Name())))
		} else {
			messageFormat = fmt.Sprintf("    %s %s %s\n", 
				connector, 
				ui.ErrorStyle.Render(issue.Description), 
				ui.FaintStyle.Render(fmt.Sprintf("(%s)", rule.Name())))
		}
		
		fmt.Print(messageFormat)
		printIssueContext(rule.GetSeverity(), issue.Context, isLast)
	}
}

func printIssueContext(severity ValidatorSeverity, context []string, lastIssue bool) {
	issueCount := len(context)
	beginning := ui.TreePipe(lastIssue)

	var contextStyle = ui.FaintStyle
	if severity == ValidatorSeverityWarning {
		contextStyle = ui.WarningStyle
	} else if severity == ValidatorSeverityCritical {
		contextStyle = ui.ErrorStyle
	}

	for index, row := range context {
		connector := ui.TreeConnector(index == issueCount-1)
		
		fmt.Printf("    %s   %s %s\n", 
			beginning, 
			connector, 
			contextStyle.Render(padLinesIfMultiline(row, 11)))
	}
}

func padLinesIfMultiline(str string, padding int) string {
	lines := strings.Split(str, "\n")
	if len(lines) == 1 {
		return str
	}

	paddedLines := make([]string, 0, len(lines))
	for i, line := range lines {
		if i == 0 {
			paddedLines = append(paddedLines, line)
			continue
		}

		paddedLines = append(paddedLines, fmt.Sprintf("%s%s", strings.Repeat(" ", padding), line))
	}

	return strings.Join(paddedLines, "\n")
}
