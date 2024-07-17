package lint

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/fatih/color"
	"github.com/pkg/errors"
)

type Printer struct {
	RootCheckPath string
}

type (
	taskSummary map[Rule][]*Issue
	ruleIssue   struct {
		rule  Rule
		issue *Issue
	}
)

var (
	faint           = color.New(color.Faint).SprintFunc()
	successPrinter  = color.New(color.FgGreen)
	pipelinePrinter = color.New(color.FgBlue, color.Bold)
	taskNamePrinter = color.New(color.FgWhite, color.Bold)
	issuePrinter    = color.New(color.FgRed)
	warningPrinter  = color.New(color.FgYellow)
	contextPrinter  = color.New(color.FgRed)
)

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
	successPrinter.Println()

	pipelineDirectory := l.relativePipelinePath(pipelineIssues.Pipeline)
	pipelinePrinter.Printf("Pipeline: %s %s\n", pipelineIssues.Pipeline.Name, faint(fmt.Sprintf("(%s)", pipelineDirectory)))

	if len(pipelineIssues.Issues) == 0 {
		successPrinter.Println("  No issues found")
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
		issuePrinter.Println()
	}

	for task, summary := range taskIssueMap {
		relativeTaskPath := pipelineIssues.Pipeline.RelativeAssetPath(task)
		taskNamePrinter.Printf("  %s %s\n", task.Name, faint(fmt.Sprintf("(%s)", relativeTaskPath)))
		printAssetIssues(summary)

		issuePrinter.Println()
	}
}

func (l Printer) relativePipelinePath(p *pipeline.Pipeline) string {
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
		pp := issuePrinter
		if rule.GetSeverity() == ValidatorSeverityWarning {
			pp = warningPrinter
		}

		for _, issue := range issues {
			printedIssueCount++

			connector := "├──"
			if printedIssueCount == totalIssueCount {
				connector = "└──"
			}

			pp.Printf("    %s %s %s\n", connector, issue.Description, faint(fmt.Sprintf("(%s)", rule.Name())))
			printIssueContext(pp, issue.Context, printedIssueCount == totalIssueCount)
		}
	}
}

func printAssetIssues(assetIssues []*ruleIssue) {
	issueCount := len(assetIssues)
	for index, ruleIssue := range assetIssues {
		rule := ruleIssue.rule
		issue := ruleIssue.issue

		pp := issuePrinter
		if rule.GetSeverity() == ValidatorSeverityWarning {
			pp = warningPrinter
		}

		connector := "├──"
		if index == issueCount-1 {
			connector = "└──"
		}

		pp.Printf("    %s %s %s\n", connector, issue.Description, faint(fmt.Sprintf("(%s)", rule.Name())))
		printIssueContext(pp, issue.Context, index == issueCount-1)
	}
}

func printIssueContext(printer *color.Color, context []string, lastIssue bool) {
	issueCount := len(context)
	beginning := "│"
	if lastIssue {
		beginning = " "
	}

	for index, row := range context {
		connector := "├─"
		if index == issueCount-1 {
			connector = "└─"
		}

		printer.Printf("    %s   %s %s\n", beginning, connector, padLinesIfMultiline(row, 11))
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
