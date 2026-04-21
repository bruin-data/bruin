package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	path2 "path"
	"regexp"
	"sort"

	"github.com/bruin-data/bruin/pkg/bruincloud"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

var ansiEscapeRegex = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)

func Cloud(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:  "cloud",
		Usage: "Interact with Bruin Cloud API",
		Commands: []*cli.Command{
			CloudProjects(),
			CloudPipelines(),
			CloudRuns(),
			CloudAssets(),
			CloudInstances(),
			CloudGlossary(),
			CloudAgents(),
		},
	}
}

func apiKeyFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:    "api-key",
		Usage:   "Bruin Cloud API key",
		Sources: cli.EnvVars("BRUIN_CLOUD_API_KEY"),
	}
}

func outputFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:    "output",
		Aliases: []string{"o"},
		Usage:   "output format: plain or json",
		Value:   "plain",
	}
}

func projectFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:    "project-id",
		Aliases: []string{"p"},
		Usage:   "project ID (see 'bruin cloud projects list' for available IDs)",
	}
}

func pipelineFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:  "pipeline",
		Usage: "pipeline name",
	}
}

func runIDFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:  "run-id",
		Usage: "pipeline run ID",
	}
}

func limitFlag() *cli.IntFlag {
	return &cli.IntFlag{
		Name:  "limit",
		Usage: "maximum number of results",
		Value: 20,
	}
}

func offsetFlag() *cli.IntFlag {
	return &cli.IntFlag{
		Name:  "offset",
		Usage: "number of results to skip",
		Value: 0,
	}
}

func resolveAPIKey(c *cli.Command) (string, error) {
	key := c.String("api-key")
	if key != "" {
		return key, nil
	}

	key = os.Getenv("BRUIN_CLOUD_API_KEY")
	if key != "" {
		return key, nil
	}

	repoRoot, err := git.FindRepoFromPath(".")
	if err == nil {
		configFilePath := path2.Join(repoRoot.Path, ".bruin.yml")
		cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
		if err == nil {
			for _, env := range cm.Environments {
				if env.Connections != nil && len(env.Connections.BruinCloud) > 0 {
					token := env.Connections.BruinCloud[0].APIToken
					if token != "" {
						return token, nil
					}
				}
			}
		}
	}

	return "", errors.New("API key is required: use --api-key flag, BRUIN_CLOUD_API_KEY env var, or configure a bruin connection in .bruin.yml")
}

func latestFlag() *cli.BoolFlag {
	return &cli.BoolFlag{
		Name:  "latest",
		Usage: "use the latest pipeline run",
	}
}

func newCloudClient(c *cli.Command) (*bruincloud.APIClient, error) {
	key, err := resolveAPIKey(c)
	if err != nil {
		return nil, err
	}
	return bruincloud.NewAPIClient(key), nil
}

func resolveRunID(ctx context.Context, c *cli.Command, client *bruincloud.APIClient, project, pipeline string) (string, error) {
	runID := c.String("run-id")
	if runID != "" {
		return runID, nil
	}
	if c.Bool("latest") {
		run, err := client.GetLatestRun(ctx, project, pipeline)
		if err != nil {
			return "", fmt.Errorf("failed to get latest run: %w", err)
		}
		return run.RunID, nil
	}
	return "", errors.New("either --run-id or --latest is required")
}

func resolveProjectID(projectID string, listProjects func() ([]bruincloud.Project, error)) (string, error) {
	if projectID != "" {
		return projectID, nil
	}

	projects, err := listProjects()
	if err != nil {
		return "", fmt.Errorf("failed to list projects: %w", err)
	}

	switch len(projects) {
	case 0:
		return "", errors.New("no Bruin Cloud projects found for this account")
	case 1:
		return projects[0].ID, nil
	default:
		return "", errors.New("--project-id is required when your account has access to multiple projects")
	}
}

func printFormattedLogs(result json.RawMessage) {
	var logResp struct {
		Logs struct {
			Sections []struct {
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
				Rows []struct {
					Timestamp string `json:"timestamp"`
					Level     string `json:"level"`
					Message   string `json:"message"`
				} `json:"rows"`
			} `json:"sections"`
		} `json:"logs"`
	}
	if err := json.Unmarshal(result, &logResp); err != nil {
		fmt.Println(string(result))
		return
	}
	for _, section := range logResp.Logs.Sections {
		for _, row := range section.Rows {
			if row.Message != "" {
				msg := ansiEscapeRegex.ReplaceAllString(row.Message, "")
				fmt.Printf("  [%s] %s\n", row.Level, msg)
			}
		}
	}
}

// --- Projects ---

func CloudProjects() *cli.Command {
	return &cli.Command{
		Name:  "projects",
		Usage: "Manage Bruin Cloud projects",
		Commands: []*cli.Command{
			cloudProjectsList(),
		},
	}
}

func cloudProjectsList() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List all projects",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			projects, err := client.ListProjects(ctx)
			if err != nil {
				printError(err, output, "Failed to list projects")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(projects, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"ID", "Name", "Repo URL", "Branch"})
			for _, p := range projects {
				t.AppendRow(table.Row{p.ID, p.Name, p.Repo.URL, p.Repo.Branch})
			}
			t.Render()
			return nil
		},
	}
}

// --- Pipelines ---

func CloudPipelines() *cli.Command {
	return &cli.Command{
		Name:  "pipelines",
		Usage: "Manage Bruin Cloud pipelines",
		Commands: []*cli.Command{
			cloudPipelinesList(),
			cloudPipelinesGet(),
			cloudPipelinesErrors(),
			cloudPipelinesDelete(),
			cloudPipelinesEnable(),
			cloudPipelinesDisable(),
		},
	}
}

func cloudPipelinesList() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List all pipelines",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			projectFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			pipelines, err := client.ListPipelines(ctx)
			if err != nil {
				printError(err, output, "Failed to list pipelines")
				return cli.Exit("", 1)
			}

			if project := c.String("project-id"); project != "" {
				var filtered []bruincloud.Pipeline
				for _, p := range pipelines {
					if p.Project == project {
						filtered = append(filtered, p)
					}
				}
				if len(filtered) == 0 && len(pipelines) > 0 {
					errorPrinter.Printf("No pipelines found for project ID '%s'. Use 'bruin cloud projects list' to see valid project IDs.\n", project)
					return cli.Exit("", 1)
				}
				pipelines = filtered
			}

			if output == "json" {
				data, _ := json.MarshalIndent(pipelines, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"Project", "Name", "Schedule", "Start Date", "Status"})
			for _, p := range pipelines {
				schedule := ""
				if p.Schedule != nil {
					schedule = *p.Schedule
				}
				status := ""
				if p.Status != nil {
					status = *p.Status
				}
				t.AppendRow(table.Row{p.Project, p.Name, schedule, p.StartDate, status})
			}
			t.Render()
			return nil
		},
	}
}

func cloudPipelinesGet() *cli.Command {
	return &cli.Command{
		Name:  "get",
		Usage: "Get details of a specific pipeline",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			projectFlag(),
			&cli.StringFlag{
				Name:  "name",
				Usage: "pipeline name",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			project := c.String("project-id")
			name := c.String("name")
			if project == "" || name == "" {
				printError(errors.New("--project-id and --name are required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			p, err := client.GetPipeline(ctx, project, name)
			if err != nil {
				printError(err, output, "Failed to get pipeline")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(p, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			schedule := ""
			if p.Schedule != nil {
				schedule = *p.Schedule
			}
			desc := ""
			if p.Description != nil {
				desc = *p.Description
			}

			status := ""
			if p.Status != nil {
				status = *p.Status
			}

			infoPrinter.Printf("Pipeline: %s\n", p.Name)
			fmt.Printf("  Project:     %s\n", p.Project)
			fmt.Printf("  Description: %s\n", desc)
			fmt.Printf("  Schedule:    %s\n", schedule)
			fmt.Printf("  Start Date:  %s\n", p.StartDate)
			fmt.Printf("  Status:      %s\n", status)
			return nil
		},
	}
}

func cloudPipelinesErrors() *cli.Command {
	return &cli.Command{
		Name:  "errors",
		Usage: "List pipeline validation errors",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			errors, err := client.GetPipelineErrors(ctx)
			if err != nil {
				printError(err, output, "Failed to get pipeline errors")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(errors, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			if len(errors) == 0 {
				successPrinter.Println("No validation errors found.")
				return nil
			}

			for i, e := range errors {
				fmt.Printf("Error %d: %s\n", i+1, string(e))
			}
			return nil
		},
	}
}

func cloudPipelinesDelete() *cli.Command {
	return &cli.Command{
		Name:  "delete",
		Usage: "Delete a pipeline",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			projectFlag(),
			pipelineFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			project := c.String("project-id")
			pipeline := c.String("pipeline")
			if project == "" || pipeline == "" {
				printError(errors.New("--project-id and --pipeline are required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			err = client.DeletePipeline(ctx, project, pipeline)
			if err != nil {
				printError(err, output, "Failed to delete pipeline")
				return cli.Exit("", 1)
			}

			printSuccessForOutput(output, fmt.Sprintf("Successfully deleted pipeline '%s' in project '%s'", pipeline, project))
			return nil
		},
	}
}

func cloudPipelinesEnable() *cli.Command {
	return &cli.Command{
		Name:  "enable",
		Usage: "Enable a pipeline",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			projectFlag(),
			pipelineFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			project := c.String("project-id")
			pipeline := c.String("pipeline")
			if project == "" || pipeline == "" {
				printError(errors.New("--project-id and --pipeline are required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			err = client.EnablePipeline(ctx, project, pipeline)
			if err != nil {
				printError(err, output, "Failed to enable pipeline")
				return cli.Exit("", 1)
			}

			printSuccessForOutput(output, fmt.Sprintf("Successfully enabled pipeline '%s' in project '%s'", pipeline, project))
			return nil
		},
	}
}

func cloudPipelinesDisable() *cli.Command {
	return &cli.Command{
		Name:  "disable",
		Usage: "Disable a pipeline",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			projectFlag(),
			pipelineFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			project := c.String("project-id")
			pipeline := c.String("pipeline")
			if project == "" || pipeline == "" {
				printError(errors.New("--project-id and --pipeline are required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			err = client.DisablePipeline(ctx, project, pipeline)
			if err != nil {
				printError(err, output, "Failed to disable pipeline")
				return cli.Exit("", 1)
			}

			printSuccessForOutput(output, fmt.Sprintf("Successfully disabled pipeline '%s' in project '%s'", pipeline, project))
			return nil
		},
	}
}

// --- Runs ---

func CloudRuns() *cli.Command {
	return &cli.Command{
		Name:  "runs",
		Usage: "Manage pipeline runs",
		Commands: []*cli.Command{
			cloudRunsList(),
			cloudRunsGet(),
			cloudRunsTrigger(),
			cloudRunsRerun(),
			cloudRunsMarkStatus(),
			cloudRunsDiagnose(),
		},
	}
}

func cloudRunsList() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List pipeline runs",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			projectFlag(),
			pipelineFlag(),
			limitFlag(),
			offsetFlag(),
			&cli.StringFlag{
				Name:  "status",
				Usage: "filter by status (e.g. failed, success, running)",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			project := c.String("project-id")
			pipeline := c.String("pipeline")
			if project == "" || pipeline == "" {
				printError(errors.New("--project-id and --pipeline are required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			runs, err := client.ListRuns(ctx, project, pipeline, c.Int("limit"), c.Int("offset"))
			if err != nil {
				printError(err, output, "Failed to list runs")
				return cli.Exit("", 1)
			}

			if statusFilter := c.String("status"); statusFilter != "" {
				var filtered []bruincloud.PipelineRun
				for _, r := range runs {
					if r.Status == statusFilter {
						filtered = append(filtered, r)
					}
				}
				runs = filtered
			}

			if output == "json" {
				data, _ := json.MarshalIndent(runs, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"Run ID", "Pipeline", "Project", "Status", "Start Date", "Duration"})
			for _, r := range runs {
				duration := ""
				if r.WallTimeDurationHumanized != nil {
					duration = *r.WallTimeDurationHumanized
				}
				startDate := bruincloud.ExtractDateString(r.StartDate)
				t.AppendRow(table.Row{r.RunID, r.Pipeline, r.Project, r.Status, startDate, duration})
			}
			t.Render()
			return nil
		},
	}
}

func cloudRunsGet() *cli.Command {
	return &cli.Command{
		Name:  "get",
		Usage: "Get details of a specific run",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			projectFlag(),
			pipelineFlag(),
			runIDFlag(),
			latestFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			project := c.String("project-id")
			pipeline := c.String("pipeline")
			if project == "" || pipeline == "" {
				printError(errors.New("--project-id and --pipeline are required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			runID, err := resolveRunID(ctx, c, client, project, pipeline)
			if err != nil {
				printError(err, output, "Failed to resolve run ID")
				return cli.Exit("", 1)
			}

			run, err := client.GetRun(ctx, project, pipeline, runID)
			if err != nil {
				printError(err, output, "Failed to get run")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(run, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			duration := ""
			if run.WallTimeDurationHumanized != nil {
				duration = *run.WallTimeDurationHumanized
			}
			note := ""
			if run.Note != nil {
				note = *run.Note
			}
			startDate := bruincloud.ExtractDateString(run.StartDate)
			endDate := bruincloud.ExtractDateString(run.EndDate)
			totalExec := ""
			if run.TotalExecutionDuration != nil {
				totalExec = fmt.Sprintf("%.2fs", *run.TotalExecutionDuration)
			}

			infoPrinter.Printf("Run: %s\n", run.RunID)
			fmt.Printf("  Project:    %s\n", run.Project)
			fmt.Printf("  Pipeline:   %s\n", run.Pipeline)
			fmt.Printf("  Status:     %s\n", run.Status)
			fmt.Printf("  Start:      %s\n", startDate)
			fmt.Printf("  End:        %s\n", endDate)
			fmt.Printf("  Duration:   %s\n", duration)
			fmt.Printf("  Exec Time:  %s\n", totalExec)
			fmt.Printf("  Note:       %s\n", note)
			return nil
		},
	}
}

func cloudRunsTrigger() *cli.Command {
	return &cli.Command{
		Name:  "trigger",
		Usage: "Trigger a new pipeline run",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			projectFlag(),
			pipelineFlag(),
			&cli.StringFlag{
				Name:     "start-date",
				Usage:    "start date for the run (e.g. 2026-01-01T00:00:00Z)",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "end-date",
				Usage:    "end date for the run (e.g. 2026-01-02T00:00:00Z)",
				Required: true,
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			project := c.String("project-id")
			pipeline := c.String("pipeline")
			if project == "" || pipeline == "" {
				printError(errors.New("--project-id and --pipeline are required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			err = client.TriggerRun(ctx, project, pipeline, c.String("start-date"), c.String("end-date"))
			if err != nil {
				printError(err, output, "Failed to trigger run")
				return cli.Exit("", 1)
			}

			printSuccessForOutput(output, fmt.Sprintf("Successfully triggered run for pipeline '%s' in project '%s'", pipeline, project))
			return nil
		},
	}
}

func cloudRunsRerun() *cli.Command {
	return &cli.Command{
		Name:  "rerun",
		Usage: "Rerun a pipeline run",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			projectFlag(),
			pipelineFlag(),
			runIDFlag(),
			latestFlag(),
			&cli.BoolFlag{
				Name:  "only-failed",
				Usage: "rerun only failed assets",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			project := c.String("project-id")
			pipeline := c.String("pipeline")
			if project == "" || pipeline == "" {
				printError(errors.New("--project-id and --pipeline are required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			runID, err := resolveRunID(ctx, c, client, project, pipeline)
			if err != nil {
				printError(err, output, "Failed to resolve run ID")
				return cli.Exit("", 1)
			}

			err = client.RerunRun(ctx, project, pipeline, runID, c.Bool("only-failed"))
			if err != nil {
				printError(err, output, "Failed to rerun pipeline")
				return cli.Exit("", 1)
			}

			printSuccessForOutput(output, fmt.Sprintf("Successfully rerun pipeline '%s' run '%s'", pipeline, runID))
			return nil
		},
	}
}

func cloudRunsMarkStatus() *cli.Command {
	return &cli.Command{
		Name:  "mark-status",
		Usage: "Mark a pipeline run with a status",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			projectFlag(),
			pipelineFlag(),
			runIDFlag(),
			latestFlag(),
			&cli.StringFlag{
				Name:     "status",
				Usage:    "status to set (success or failed)",
				Required: true,
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			project := c.String("project-id")
			pipeline := c.String("pipeline")
			if project == "" || pipeline == "" {
				printError(errors.New("--project-id and --pipeline are required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			runID, err := resolveRunID(ctx, c, client, project, pipeline)
			if err != nil {
				printError(err, output, "Failed to resolve run ID")
				return cli.Exit("", 1)
			}

			err = client.MarkRunStatus(ctx, project, pipeline, runID, c.String("status"))
			if err != nil {
				printError(err, output, "Failed to mark run status")
				return cli.Exit("", 1)
			}

			printSuccessForOutput(output, fmt.Sprintf("Successfully marked run '%s' as '%s'", runID, c.String("status")))
			return nil
		},
	}
}

var errorLinePatterns = regexp.MustCompile(`(?i)^(error:|result:.*\(expected:)`)

func extractErrorLines(result json.RawMessage) []string {
	var logResp struct {
		Logs struct {
			Sections []struct {
				Rows []struct {
					Level   string `json:"level"`
					Message string `json:"message"`
				} `json:"rows"`
			} `json:"sections"`
		} `json:"logs"`
	}
	if err := json.Unmarshal(result, &logResp); err != nil {
		return nil
	}

	var errorLines []string
	var allLines []string
	for _, section := range logResp.Logs.Sections {
		for _, row := range section.Rows {
			if row.Message == "" {
				continue
			}
			msg := ansiEscapeRegex.ReplaceAllString(row.Message, "")
			allLines = append(allLines, msg)
			level := row.Level
			if level == "ERROR" || level == "CRITICAL" || level == "error" || level == "critical" {
				errorLines = append(errorLines, msg)
			} else if errorLinePatterns.MatchString(msg) {
				errorLines = append(errorLines, msg)
			}
		}
	}
	if len(errorLines) > 0 {
		return errorLines
	}
	// Fallback: return last few lines regardless of level
	if len(allLines) > 5 {
		allLines = allLines[len(allLines)-5:]
	}
	return allLines
}

func cloudRunsDiagnose() *cli.Command {
	return &cli.Command{
		Name:  "diagnose",
		Usage: "Show a consolidated diagnostic report for a pipeline run",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			projectFlag(),
			pipelineFlag(),
			runIDFlag(),
			latestFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			pipeline := c.String("pipeline")
			if pipeline == "" {
				printError(errors.New("--pipeline is required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			project, err := resolveProjectID(c.String("project-id"), func() ([]bruincloud.Project, error) {
				return client.ListProjects(ctx)
			})
			if err != nil {
				printError(err, output, "Failed to resolve project ID")
				return cli.Exit("", 1)
			}

			runID, err := resolveRunID(ctx, c, client, project, pipeline)
			if err != nil {
				printError(err, output, "Failed to resolve run ID")
				return cli.Exit("", 1)
			}

			// 1. Fetch run details
			run, err := client.GetRun(ctx, project, pipeline, runID)
			if err != nil {
				printError(err, output, "Failed to get run")
				return cli.Exit("", 1)
			}

			// 2. Fetch all asset instances
			instances, err := client.ListInstancesParsed(ctx, project, pipeline, runID)
			if err != nil {
				printError(err, output, "Failed to list instances")
				return cli.Exit("", 1)
			}

			// 3. Identify failed steps
			const stepStatusFailed = "failed"

			type failedStepInfo struct {
				asset     string
				stepName  string
				stepID    string
				tryNumber int
			}
			var failedSteps []failedStepInfo

			for _, inst := range instances.AssetInstances {
				if inst.Status != stepStatusFailed && inst.Status != "checks_failed" {
					continue
				}
				for _, step := range inst.Steps.Main {
					if step.Status == stepStatusFailed {
						failedSteps = append(failedSteps, failedStepInfo{
							asset: inst.Asset, stepName: step.Name,
							stepID: step.StepID, tryNumber: step.TryNumber,
						})
					}
				}
				for _, check := range inst.Steps.Checks.Custom {
					if check.Instance.Status == stepStatusFailed {
						failedSteps = append(failedSteps, failedStepInfo{
							asset: inst.Asset, stepName: "custom check: " + check.Name,
							stepID: check.Instance.StepID, tryNumber: check.Instance.TryNumber,
						})
					}
				}
				for _, check := range inst.Steps.Checks.Column {
					if check.Instance.Status == stepStatusFailed {
						failedSteps = append(failedSteps, failedStepInfo{
							asset: inst.Asset, stepName: "column check: " + check.Name,
							stepID: check.Instance.StepID, tryNumber: check.Instance.TryNumber,
						})
					}
				}
			}

			// 4. Fetch error logs for failed steps
			type failureDetail struct {
				Asset     string   `json:"asset"`
				Step      string   `json:"step"`
				StepID    string   `json:"step_id"`
				ErrorLogs []string `json:"error_logs"`
			}
			var failures []failureDetail

			for _, fs := range failedSteps {
				tryNum := fs.tryNumber
				if tryNum == 0 {
					tryNum = 1
				}
				logs, logErr := client.GetInstanceLogs(ctx, project, pipeline, runID, fs.stepID, tryNum)
				var errLines []string
				if logErr != nil {
					errLines = []string{fmt.Sprintf("(failed to fetch logs: %v)", logErr)}
				} else {
					errLines = extractErrorLines(logs)
				}
				failures = append(failures, failureDetail{
					Asset: fs.asset, Step: fs.stepName,
					StepID: fs.stepID, ErrorLogs: errLines,
				})
			}

			// 5. JSON output
			if output == "json" {
				type jsonReport struct {
					Run      *bruincloud.PipelineRun                 `json:"run"`
					Assets   map[string]bruincloud.AssetInstanceInfo `json:"assets"`
					Failures []failureDetail                         `json:"failures"`
				}
				report := jsonReport{
					Run:      run,
					Assets:   instances.AssetInstances,
					Failures: failures,
				}
				data, _ := json.MarshalIndent(report, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			// 6. Plain text output
			// --- Run metadata ---
			duration := ""
			if run.WallTimeDurationHumanized != nil {
				duration = *run.WallTimeDurationHumanized
			}
			startDate := bruincloud.ExtractDateString(run.StartDate)
			endDate := bruincloud.ExtractDateString(run.EndDate)

			infoPrinter.Println("=== Run Diagnosis ===")
			fmt.Printf("  Run ID:    %s\n", run.RunID)
			fmt.Printf("  Project:   %s\n", run.Project)
			fmt.Printf("  Pipeline:  %s\n", run.Pipeline)
			fmt.Printf("  Status:    %s\n", run.Status)
			fmt.Printf("  Start:     %s\n", startDate)
			fmt.Printf("  End:       %s\n", endDate)
			fmt.Printf("  Duration:  %s\n", duration)
			fmt.Println()

			// --- Asset summary table ---
			names := make([]string, 0, len(instances.AssetInstances))
			for name := range instances.AssetInstances {
				names = append(names, name)
			}
			sort.Strings(names)

			failedCount := 0
			for _, inst := range instances.AssetInstances {
				if inst.Status == stepStatusFailed || inst.Status == "checks_failed" {
					failedCount++
				}
			}

			infoPrinter.Printf("=== Assets (%d total, %d failed) ===\n", len(names), failedCount)
			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"Asset", "Type", "Status", "Duration"})
			for _, name := range names {
				inst := instances.AssetInstances[name]
				dur := fmt.Sprintf("%.1fs", inst.TotalExecutionDuration)
				t.AppendRow(table.Row{inst.Asset, inst.Type, inst.Status, dur})
			}
			t.Render()

			// --- Failure details ---
			if len(failures) > 0 {
				fmt.Println()
				infoPrinter.Println("=== Failure Details ===")
				for _, f := range failures {
					fmt.Println()
					errorPrinter.Printf("--- %s / %s ---\n", f.Asset, f.Step)
					if len(f.ErrorLogs) == 0 {
						fmt.Println("  (no error logs available)")
					}
					for _, line := range f.ErrorLogs {
						fmt.Printf("  %s\n", line)
					}
				}
			}

			return nil
		},
	}
}

// --- Assets ---

func CloudAssets() *cli.Command {
	return &cli.Command{
		Name:  "assets",
		Usage: "Manage pipeline assets",
		Commands: []*cli.Command{
			cloudAssetsList(),
			cloudAssetsGet(),
		},
	}
}

func cloudAssetsList() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List assets for a pipeline",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			projectFlag(),
			pipelineFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			project := c.String("project-id")
			pipeline := c.String("pipeline")
			if project == "" || pipeline == "" {
				printError(errors.New("--project-id and --pipeline are required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			assets, err := client.ListAssets(ctx, project, pipeline)
			if err != nil {
				printError(err, output, "Failed to list assets")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(assets, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"Name", "Type", "Pipeline", "Project"})
			for _, a := range assets {
				t.AppendRow(table.Row{a.Name, a.Type, a.Pipeline, a.Project})
			}
			t.Render()
			return nil
		},
	}
}

func cloudAssetsGet() *cli.Command {
	return &cli.Command{
		Name:  "get",
		Usage: "Get details of a specific asset",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			projectFlag(),
			pipelineFlag(),
			&cli.StringFlag{
				Name:  "asset",
				Usage: "asset name",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			project := c.String("project-id")
			pipeline := c.String("pipeline")
			asset := c.String("asset")
			if project == "" || pipeline == "" || asset == "" {
				printError(errors.New("--project-id, --pipeline, and --asset are required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			a, err := client.GetAsset(ctx, project, pipeline, asset)
			if err != nil {
				printError(err, output, "Failed to get asset")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(a, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			desc := ""
			if a.Description != nil {
				desc = *a.Description
			}

			infoPrinter.Printf("Asset: %s\n", a.Name)
			fmt.Printf("  Type:        %s\n", a.Type)
			fmt.Printf("  URI:         %s\n", a.URI)
			fmt.Printf("  Project:     %s\n", a.Project)
			fmt.Printf("  Pipeline:    %s\n", a.Pipeline)
			fmt.Printf("  Description: %s\n", desc)
			fmt.Printf("  Quality:     %d/%d (%d%%)\n", a.QualityScore, a.MaxPossibleQualityScore, a.QualityScorePercentage)
			return nil
		},
	}
}

// --- Instances ---

func CloudInstances() *cli.Command {
	return &cli.Command{
		Name:  "instances",
		Usage: "Manage asset instances",
		Commands: []*cli.Command{
			cloudInstancesList(),
			cloudInstancesGet(),
			cloudInstancesLogs(),
			cloudInstancesFailedLogs(),
		},
	}
}

func cloudInstancesList() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List asset instances for a run",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			projectFlag(),
			pipelineFlag(),
			runIDFlag(),
			latestFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			project := c.String("project-id")
			pipeline := c.String("pipeline")
			if project == "" || pipeline == "" {
				printError(errors.New("--project-id and --pipeline are required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			runID, err := resolveRunID(ctx, c, client, project, pipeline)
			if err != nil {
				printError(err, output, "Failed to resolve run ID")
				return cli.Exit("", 1)
			}

			result, err := client.ListInstances(ctx, project, pipeline, runID)
			if err != nil {
				printError(err, output, "Failed to list instances")
				return cli.Exit("", 1)
			}

			if output == "json" {
				var indented json.RawMessage
				if err := json.Unmarshal(result, &indented); err == nil {
					data, _ := json.MarshalIndent(indented, "", "  ")
					fmt.Println(string(data))
				} else {
					fmt.Println(string(result))
				}
				return nil
			}

			var parsed bruincloud.AssetInstanceResponse
			if err := json.Unmarshal(result, &parsed); err != nil {
				fmt.Println(string(result))
				return nil
			}

			// Sort asset names for stable output
			names := make([]string, 0, len(parsed.AssetInstances))
			for name := range parsed.AssetInstances {
				names = append(names, name)
			}
			sort.Strings(names)

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"Asset", "Type", "Status", "Duration", "Finished"})
			for _, name := range names {
				inst := parsed.AssetInstances[name]
				duration := fmt.Sprintf("%.1fs", inst.TotalExecutionDuration)
				finished := "no"
				if inst.IsFinished {
					finished = "yes"
				}
				t.AppendRow(table.Row{inst.Asset, inst.Type, inst.Status, duration, finished})
			}
			t.Render()
			return nil
		},
	}
}

func cloudInstancesGet() *cli.Command {
	return &cli.Command{
		Name:  "get",
		Usage: "Get details of a specific asset instance",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			projectFlag(),
			pipelineFlag(),
			runIDFlag(),
			latestFlag(),
			&cli.StringFlag{
				Name:  "asset",
				Usage: "asset name",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			project := c.String("project-id")
			pipeline := c.String("pipeline")
			asset := c.String("asset")
			if project == "" || pipeline == "" || asset == "" {
				printError(errors.New("--project-id, --pipeline, and --asset are required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			runID, err := resolveRunID(ctx, c, client, project, pipeline)
			if err != nil {
				printError(err, output, "Failed to resolve run ID")
				return cli.Exit("", 1)
			}

			result, err := client.GetInstance(ctx, project, pipeline, runID, asset)
			if err != nil {
				printError(err, output, "Failed to get instance")
				return cli.Exit("", 1)
			}

			if output == "json" {
				var indented json.RawMessage
				if err := json.Unmarshal(result, &indented); err == nil {
					data, _ := json.MarshalIndent(indented, "", "  ")
					fmt.Println(string(data))
				} else {
					fmt.Println(string(result))
				}
				return nil
			}

			// The detail endpoint returns snake_case fields (start_date, end_date, etc.)
			// while the list endpoint uses camelCase. Parse with a dedicated struct.
			var parsed struct {
				AssetInstance struct {
					Asset                  string                        `json:"asset"`
					Type                   string                        `json:"type"`
					StartDate              string                        `json:"start_date"`
					EndDate                string                        `json:"end_date"`
					WallTimeDuration       float64                       `json:"wall_time_duration"`
					TotalExecutionDuration float64                       `json:"total_execution_duration"`
					Status                 string                        `json:"status"`
					IsFinished             bool                          `json:"is_finished"`
					Steps                  bruincloud.AssetInstanceSteps `json:"steps"`
					StepIDs                []string                      `json:"step_ids"`
				} `json:"asset_instance"`
			}
			if err := json.Unmarshal(result, &parsed); err != nil {
				fmt.Println(string(result))
				return nil
			}

			inst := parsed.AssetInstance
			infoPrinter.Printf("Asset: %s\n", inst.Asset)
			fmt.Printf("  Type:       %s\n", inst.Type)
			fmt.Printf("  Status:     %s\n", inst.Status)
			fmt.Printf("  Start:      %s\n", inst.StartDate)
			fmt.Printf("  End:        %s\n", inst.EndDate)
			fmt.Printf("  Duration:   %.1fs\n", inst.TotalExecutionDuration)

			if len(inst.Steps.Main) > 0 || len(inst.Steps.Checks.Custom) > 0 || len(inst.Steps.Checks.Column) > 0 {
				fmt.Println()
				t := table.NewWriter()
				t.SetOutputMirror(os.Stdout)
				t.AppendHeader(table.Row{"Step", "Name", "Status", "Duration"})
				for _, step := range inst.Steps.Main {
					t.AppendRow(table.Row{"main", step.Name, step.Status, fmt.Sprintf("%.1fs", step.Duration)})
				}
				for _, check := range inst.Steps.Checks.Custom {
					t.AppendRow(table.Row{"custom check", check.Name, check.Instance.Status, fmt.Sprintf("%.1fs", check.Instance.Duration)})
				}
				for _, check := range inst.Steps.Checks.Column {
					t.AppendRow(table.Row{"column check", check.Name, check.Instance.Status, fmt.Sprintf("%.1fs", check.Instance.Duration)})
				}
				t.Render()
			}
			return nil
		},
	}
}

func cloudInstancesLogs() *cli.Command {
	return &cli.Command{
		Name:  "logs",
		Usage: "Get logs for an asset instance step",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			projectFlag(),
			pipelineFlag(),
			runIDFlag(),
			latestFlag(),
			&cli.StringFlag{
				Name:  "step-id",
				Usage: "step ID (auto-resolved if --asset is provided)",
			},
			&cli.StringFlag{
				Name:  "asset",
				Usage: "asset name (resolves step ID automatically, shows all steps unless --step-name is given)",
			},
			&cli.StringFlag{
				Name:  "step-name",
				Usage: "step or check name to filter logs when using --asset (e.g. 'query', 'this_check_will_fail')",
			},
			&cli.IntFlag{
				Name:  "try-number",
				Usage: "try number",
				Value: 1,
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			project := c.String("project-id")
			pipeline := c.String("pipeline")
			if project == "" || pipeline == "" {
				printError(errors.New("--project-id and --pipeline are required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			runID, err := resolveRunID(ctx, c, client, project, pipeline)
			if err != nil {
				printError(err, output, "Failed to resolve run ID")
				return cli.Exit("", 1)
			}

			stepID := c.String("step-id")
			assetName := c.String("asset")
			stepNameFilter := c.String("step-name")

			if stepID == "" && assetName == "" {
				printError(errors.New("either --step-id or --asset is required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			// When --step-id is given directly, fetch logs for that single step
			if stepID != "" {
				return fetchAndPrintLogs(ctx, client, project, pipeline, runID, stepID, "", c.Int("try-number"), output)
			}

			// When --asset is given, resolve steps from the instance details
			instanceResult, err := client.GetInstance(ctx, project, pipeline, runID, assetName)
			if err != nil {
				printError(err, output, "Failed to get instance for asset")
				return cli.Exit("", 1)
			}

			// Detail endpoint uses snake_case for top-level fields
			var parsed struct {
				AssetInstance struct {
					Steps   bruincloud.AssetInstanceSteps `json:"steps"`
					StepIDs []string                      `json:"step_ids"`
				} `json:"asset_instance"`
			}
			if err := json.Unmarshal(instanceResult, &parsed); err != nil {
				printError(fmt.Errorf("failed to parse instance response: %w", err), output, "Failed to resolve step ID")
				return cli.Exit("", 1)
			}

			type stepInfo struct {
				label     string
				stepID    string
				tryNumber int
			}
			var allSteps []stepInfo

			for _, step := range parsed.AssetInstance.Steps.Main {
				allSteps = append(allSteps, stepInfo{
					label:     "main: " + step.Name,
					stepID:    step.StepID,
					tryNumber: step.TryNumber,
				})
			}
			for _, check := range parsed.AssetInstance.Steps.Checks.Custom {
				allSteps = append(allSteps, stepInfo{
					label:     "custom check: " + check.Name,
					stepID:    check.Instance.StepID,
					tryNumber: check.Instance.TryNumber,
				})
			}
			for _, check := range parsed.AssetInstance.Steps.Checks.Column {
				allSteps = append(allSteps, stepInfo{
					label:     "column check: " + check.Name,
					stepID:    check.Instance.StepID,
					tryNumber: check.Instance.TryNumber,
				})
			}

			if len(allSteps) == 0 {
				printError(errors.New("no steps found for this asset"), output, "Failed to resolve step ID")
				return cli.Exit("", 1)
			}

			// Filter by step name if specified
			if stepNameFilter != "" {
				var filtered []stepInfo
				for _, s := range allSteps {
					// Match against the step/check name (not the label prefix)
					for _, step := range parsed.AssetInstance.Steps.Main {
						if step.Name == stepNameFilter && step.StepID == s.stepID {
							filtered = append(filtered, s)
						}
					}
					for _, check := range parsed.AssetInstance.Steps.Checks.Custom {
						if check.Name == stepNameFilter && check.Instance.StepID == s.stepID {
							filtered = append(filtered, s)
						}
					}
					for _, check := range parsed.AssetInstance.Steps.Checks.Column {
						if check.Name == stepNameFilter && check.Instance.StepID == s.stepID {
							filtered = append(filtered, s)
						}
					}
				}
				if len(filtered) == 0 {
					printError(fmt.Errorf("no step found with name '%s'", stepNameFilter), output, "Step not found")
					return cli.Exit("", 1)
				}
				allSteps = filtered
			}

			// Show logs for each step
			for i, s := range allSteps {
				if i > 0 {
					fmt.Println()
				}

				tryNum := s.tryNumber
				if tryNum == 0 {
					tryNum = 1
				}

				// Override try number if explicitly set by user
				if c.IsSet("try-number") {
					tryNum = c.Int("try-number")
				}

				if len(allSteps) > 1 && output != "json" {
					infoPrinter.Printf("=== %s ===\n", s.label)
				}
				if err := fetchAndPrintLogs(ctx, client, project, pipeline, runID, s.stepID, s.label, tryNum, output); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func fetchAndPrintLogs(ctx context.Context, client *bruincloud.APIClient, project, pipeline, runID, stepID, label string, tryNumber int, output string) error {
	result, err := client.GetInstanceLogs(ctx, project, pipeline, runID, stepID, tryNumber)
	if err != nil {
		if label != "" {
			errorPrinter.Printf("Failed to get logs for %s: %v\n", label, err)
			return nil
		}
		printError(err, output, "Failed to get instance logs")
		return cli.Exit("", 1)
	}

	if output == "json" {
		printJSON(result)
		return nil
	}

	printFormattedLogs(result)
	return nil
}

func printJSON(raw json.RawMessage) {
	var indented json.RawMessage
	if err := json.Unmarshal(raw, &indented); err == nil {
		data, err := json.MarshalIndent(indented, "", "  ")
		if err != nil {
			fmt.Println(string(raw))
			return
		}
		fmt.Println(string(data))
	} else {
		fmt.Println(string(raw))
	}
}

func cloudInstancesFailedLogs() *cli.Command {
	return &cli.Command{
		Name:  "failed-logs",
		Usage: "Get logs for all failed steps in a run",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			projectFlag(),
			pipelineFlag(),
			runIDFlag(),
			latestFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			project := c.String("project-id")
			pipeline := c.String("pipeline")
			if project == "" || pipeline == "" {
				printError(errors.New("--project-id and --pipeline are required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			runID, err := resolveRunID(ctx, c, client, project, pipeline)
			if err != nil {
				printError(err, output, "Failed to resolve run ID")
				return cli.Exit("", 1)
			}

			instances, err := client.ListInstancesParsed(ctx, project, pipeline, runID)
			if err != nil {
				printError(err, output, "Failed to list instances")
				return cli.Exit("", 1)
			}

			const stepStatusFailed = "failed"

			type failedStep struct {
				asset     string
				stepName  string
				stepID    string
				tryNumber int
			}
			var failedSteps []failedStep

			for _, inst := range instances.AssetInstances {
				for _, step := range inst.Steps.Main {
					if step.Status == stepStatusFailed {
						failedSteps = append(failedSteps, failedStep{
							asset:     inst.Asset,
							stepName:  step.Name,
							stepID:    step.StepID,
							tryNumber: step.TryNumber,
						})
					}
				}
				for _, check := range inst.Steps.Checks.Custom {
					if check.Instance.Status == stepStatusFailed {
						failedSteps = append(failedSteps, failedStep{
							asset:     inst.Asset,
							stepName:  "check: " + check.Name,
							stepID:    check.Instance.StepID,
							tryNumber: check.Instance.TryNumber,
						})
					}
				}
				for _, check := range inst.Steps.Checks.Column {
					if check.Instance.Status == stepStatusFailed {
						failedSteps = append(failedSteps, failedStep{
							asset:     inst.Asset,
							stepName:  "column check: " + check.Name,
							stepID:    check.Instance.StepID,
							tryNumber: check.Instance.TryNumber,
						})
					}
				}
			}

			if len(failedSteps) == 0 {
				successPrinter.Println("No failed steps found in this run.")
				return nil
			}

			for i, fs := range failedSteps {
				if i > 0 {
					fmt.Println()
				}

				tryNum := fs.tryNumber
				if tryNum == 0 {
					tryNum = 1
				}

				logs, err := client.GetInstanceLogs(ctx, project, pipeline, runID, fs.stepID, tryNum)
				if err != nil {
					errorPrinter.Printf("Failed to get logs for %s / %s: %v\n", fs.asset, fs.stepName, err)
					continue
				}

				if output == "json" {
					var indented json.RawMessage
					if err := json.Unmarshal(logs, &indented); err == nil {
						data, _ := json.MarshalIndent(indented, "", "  ")
						fmt.Println(string(data))
					} else {
						fmt.Println(string(logs))
					}
					continue
				}

				infoPrinter.Printf("=== %s / %s ===\n", fs.asset, fs.stepName)
				printFormattedLogs(logs)
			}

			return nil
		},
	}
}

// --- Glossary ---

func CloudGlossary() *cli.Command {
	return &cli.Command{
		Name:  "glossary",
		Usage: "Manage glossary entities",
		Commands: []*cli.Command{
			cloudGlossaryList(),
			cloudGlossaryGet(),
		},
	}
}

func cloudGlossaryList() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List all glossary entities",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			result, err := client.ListGlossaryEntities(ctx)
			if err != nil {
				printError(err, output, "Failed to list glossary entities")
				return cli.Exit("", 1)
			}

			if output == "json" {
				var indented json.RawMessage
				if err := json.Unmarshal(result, &indented); err == nil {
					data, _ := json.MarshalIndent(indented, "", "  ")
					fmt.Println(string(data))
				} else {
					fmt.Println(string(result))
				}
				return nil
			}

			// Plain output: print the entities
			var indented json.RawMessage
			if err := json.Unmarshal(result, &indented); err == nil {
				data, _ := json.MarshalIndent(indented, "", "  ")
				fmt.Println(string(data))
			} else {
				fmt.Println(string(result))
			}
			return nil
		},
	}
}

func cloudGlossaryGet() *cli.Command {
	return &cli.Command{
		Name:  "get",
		Usage: "Get details of a specific glossary entity",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			projectFlag(),
			&cli.StringFlag{
				Name:  "entity",
				Usage: "entity name",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			project := c.String("project-id")
			entity := c.String("entity")
			if project == "" || entity == "" {
				printError(errors.New("--project-id and --entity are required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			result, err := client.GetGlossaryEntity(ctx, project, entity)
			if err != nil {
				printError(err, output, "Failed to get glossary entity")
				return cli.Exit("", 1)
			}

			var indented json.RawMessage
			if err := json.Unmarshal(result, &indented); err == nil {
				data, _ := json.MarshalIndent(indented, "", "  ")
				fmt.Println(string(data))
			} else {
				fmt.Println(string(result))
			}
			return nil
		},
	}
}

// --- Agents ---

func CloudAgents() *cli.Command {
	return &cli.Command{
		Name:  "agents",
		Usage: "Manage Bruin Cloud agents",
		Commands: []*cli.Command{
			cloudAgentsList(),
			cloudAgentsSend(),
			cloudAgentsStatus(),
			cloudAgentsThreads(),
			cloudAgentsMessages(),
		},
	}
}

func cloudAgentsList() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List all agents",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			agents, err := client.ListAgents(ctx)
			if err != nil {
				printError(err, output, "Failed to list agents")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(agents, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"ID", "Name", "Description"})
			for _, a := range agents {
				desc := ""
				if a.Description != nil {
					desc = *a.Description
				}
				t.AppendRow(table.Row{a.ID, a.Name, desc})
			}
			t.Render()
			return nil
		},
	}
}

func cloudAgentsSend() *cli.Command {
	return &cli.Command{
		Name:  "send",
		Usage: "Send a message to an agent",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "agent-id",
				Usage:    "agent ID",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "message",
				Usage:    "message to send",
				Required: true,
			},
			&cli.IntFlag{
				Name:  "thread-id",
				Usage: "thread ID (optional, creates new thread if omitted)",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			var threadID *int
			if c.IsSet("thread-id") {
				tid := c.Int("thread-id")
				threadID = &tid
			}

			result, err := client.SendAgentMessage(ctx, c.Int("agent-id"), c.String("message"), threadID)
			if err != nil {
				printError(err, output, "Failed to send message")
				return cli.Exit("", 1)
			}

			if output == "json" {
				var indented json.RawMessage
				if err := json.Unmarshal(result, &indented); err == nil {
					data, _ := json.MarshalIndent(indented, "", "  ")
					fmt.Println(string(data))
				} else {
					fmt.Println(string(result))
				}
				return nil
			}

			printSuccessForOutput(output, "Message sent successfully")
			if result != nil {
				fmt.Println(string(result))
			}
			return nil
		},
	}
}

func cloudAgentsStatus() *cli.Command {
	return &cli.Command{
		Name:  "status",
		Usage: "Get the status of an agent message",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "agent-id",
				Usage:    "agent ID",
				Required: true,
			},
			&cli.IntFlag{
				Name:     "thread-id",
				Usage:    "thread ID",
				Required: true,
			},
			&cli.IntFlag{
				Name:     "message-id",
				Usage:    "message pair ID",
				Required: true,
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			msg, err := client.GetAgentMessageStatus(ctx, c.Int("agent-id"), c.Int("thread-id"), c.Int("message-id"))
			if err != nil {
				printError(err, output, "Failed to get message status")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(msg, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			outputMsg := ""
			if msg.OutputMessage != nil {
				outputMsg = *msg.OutputMessage
			}

			infoPrinter.Printf("Message %d\n", msg.ID)
			fmt.Printf("  Status:    %s\n", msg.Status)
			fmt.Printf("  Output:    %s\n", outputMsg)
			fmt.Printf("  Created:   %s\n", msg.CreatedAt)
			fmt.Printf("  Updated:   %s\n", msg.UpdatedAt)
			return nil
		},
	}
}

func cloudAgentsThreads() *cli.Command {
	return &cli.Command{
		Name:  "threads",
		Usage: "List threads for an agent",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "agent-id",
				Usage:    "agent ID",
				Required: true,
			},
			limitFlag(),
			offsetFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			threads, err := client.ListAgentThreads(ctx, c.Int("agent-id"), c.Int("limit"), c.Int("offset"))
			if err != nil {
				printError(err, output, "Failed to list threads")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(threads, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"ID", "Agent ID", "Created At", "Updated At"})
			for _, th := range threads {
				t.AppendRow(table.Row{th.ID, th.AgentID, th.CreatedAt, th.UpdatedAt})
			}
			t.Render()
			return nil
		},
	}
}

func cloudAgentsMessages() *cli.Command {
	return &cli.Command{
		Name:  "messages",
		Usage: "List messages in a thread",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "agent-id",
				Usage:    "agent ID",
				Required: true,
			},
			&cli.IntFlag{
				Name:     "thread-id",
				Usage:    "thread ID",
				Required: true,
			},
			limitFlag(),
			offsetFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			messages, err := client.ListAgentMessages(ctx, c.Int("agent-id"), c.Int("thread-id"), c.Int("limit"), c.Int("offset"))
			if err != nil {
				printError(err, output, "Failed to list messages")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(messages, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"ID", "Status", "Output", "Created At"})
			for _, m := range messages {
				outputMsg := ""
				if m.OutputMessage != nil {
					outputMsg = *m.OutputMessage
					if len(outputMsg) > 80 {
						outputMsg = outputMsg[:80] + "..."
					}
				}
				t.AppendRow(table.Row{m.ID, m.Status, outputMsg, m.CreatedAt})
			}
			t.Render()
			return nil
		},
	}
}
