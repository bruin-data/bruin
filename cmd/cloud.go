package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	path2 "path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/bruincloud"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
	"gopkg.in/yaml.v3"
)

var ansiEscapeRegex = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)

func Cloud(isDebug *bool) *cli.Command {
	cmd := &cli.Command{
		Name:  "cloud",
		Usage: "Interact with Bruin Cloud API",
		Commands: []*cli.Command{
			CloudTeams(),
			CloudProjects(),
			CloudPipelines(),
			CloudRuns(),
			CloudBackfills(),
			CloudAssets(),
			CloudInstances(),
			CloudGlossary(),
			CloudAgents(),
			CloudConnections(),
			CloudConnectionSets(),
			CloudDashboards(),
			CloudScheduledAgents(),
			CloudSkills(),
			CloudAuditLogs(),
			CloudCost(),
		},
	}
	addTeamFlag(cmd)
	return cmd
}

// addTeamFlag adds --team to every runnable cloud command, so any of them can
// target a team other than the token owner's current one. A command is runnable
// when it has an action of its own (leaves always, but also parents like
// "agents connections" that both list and hold subcommands); recurse into any
// parent so its children get the flag too.
func addTeamFlag(cmd *cli.Command) {
	for _, sub := range cmd.Commands {
		if sub.Action != nil || len(sub.Commands) == 0 {
			sub.Flags = append(sub.Flags, teamFlag())
		}
		if len(sub.Commands) > 0 {
			addTeamFlag(sub)
		}
	}
}

func teamFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:    "team",
		Usage:   "act on this team instead of your current one, given as its company prefix (see 'bruin cloud teams list' for available prefixes)",
		Sources: cli.EnvVars("BRUIN_CLOUD_TEAM"),
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
	client := bruincloud.NewAPIClient(key)
	if team := c.String("team"); team != "" {
		client.SetTeam(team)
	}
	return client, nil
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

func CloudTeams() *cli.Command {
	return &cli.Command{
		Name:  "teams",
		Usage: "Manage Bruin Cloud teams",
		Commands: []*cli.Command{
			cloudTeamsList(),
		},
	}
}

func cloudTeamsList() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List the teams your token can act on",
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

			teams, err := client.ListTeams(ctx)
			if err != nil {
				printError(err, output, "Failed to list teams")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(teams, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"ID", "Name", "Company Prefix"})
			for _, tm := range teams {
				t.AppendRow(table.Row{tm.ID, tm.Name, tm.CompanyPrefix})
			}
			t.Render()
			return nil
		},
	}
}

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

var validSplitUnits = map[string]bool{
	"minute": true, "hour": true, "day": true,
	"week": true, "month": true, "year": true,
}

// validateSplitFlags checks the --split / --chunk-size combination.
func validateSplitFlags(split string, chunkSizeSet bool, chunkSize int) error {
	if split == "" {
		if chunkSizeSet {
			return errors.New("--chunk-size requires --split")
		}
		return nil
	}
	if !validSplitUnits[split] {
		return fmt.Errorf("invalid --split %q (valid: minute, hour, day, week, month, year)", split)
	}
	if chunkSize < 1 {
		return errors.New("--chunk-size must be at least 1")
	}
	return nil
}

// parseRunVariables parses --var key=value pairs (JSON values) into a variables map.
func parseRunVariables(pairs []string) (map[string]any, error) {
	if len(pairs) == 0 {
		return nil, nil
	}
	vars := make(map[string]any, len(pairs))
	for _, variable := range pairs {
		parsed, err := parseVariable(variable)
		if err != nil {
			return nil, fmt.Errorf("invalid variable override %q: %w", variable, err)
		}
		for key, value := range parsed {
			vars[key] = value
		}
	}
	return vars, nil
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
			&cli.StringSliceFlag{
				Name:    "asset",
				Aliases: []string{"assets"},
				Usage:   "select specific assets to run by name; repeat or comma-separate for multiple",
			},
			&cli.BoolFlag{
				Name:  "downstream",
				Usage: "also run everything downstream of the selected --asset(s)",
			},
			&cli.StringSliceFlag{
				Name:    "tag",
				Aliases: []string{"t"},
				Usage:   "tag the run; repeat for multiple.",
			},
			&cli.BoolFlag{
				Name:    "full-refresh",
				Aliases: []string{"r"},
				Usage:   "full-refresh the assets in the run: the --asset selection if given, otherwise every asset",
			},
			&cli.StringSliceFlag{
				Name:  "var",
				Usage: "override pipeline variables with custom values (key=value)",
			},
			&cli.StringFlag{
				Name:  "note",
				Usage: "attach a note to the run.",
			},
			&cli.StringFlag{
				Name:  "split",
				Usage: "split the date range into batches by unit: minute, hour, day, week, month, year (one run per batch)",
			},
			&cli.IntFlag{
				Name:  "chunk-size",
				Usage: "number of split units per batch (used with --split)",
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

			// With --split the run becomes a backfill
			split := c.String("split")
			chunkSize := c.Int("chunk-size")
			if err := validateSplitFlags(split, c.IsSet("chunk-size"), chunkSize); err != nil {
				printError(err, output, "Invalid flags")
				return cli.Exit("", 1)
			}

			vars, err := parseRunVariables(c.StringSlice("var"))
			if err != nil {
				printError(err, output, "Invalid flag")
				return cli.Exit("", 1)
			}

			if c.Args().Len() > 0 {
				printError(fmt.Errorf("unexpected argument(s): %s", strings.Join(c.Args().Slice(), " ")), output, "Invalid arguments")
				return cli.Exit("", 1)
			}

			if c.Bool("downstream") && len(c.StringSlice("asset")) == 0 {
				printError(errors.New("--downstream requires --asset"), output, "Invalid flags")
				return cli.Exit("", 1)
			}

			opts := bruincloud.TriggerRunOptions{
				Assets:      c.StringSlice("asset"),
				Downstream:  c.Bool("downstream"),
				FullRefresh: c.Bool("full-refresh"),
				Split:       split,
				ChunkSize:   chunkSize,
				Variables:   vars,
				Note:        c.String("note"),
				Tags:        c.StringSlice("tag"),
			}

			startDate, endDate := c.String("start-date"), c.String("end-date")
			result, err := client.TriggerRun(ctx, project, pipeline, startDate, endDate, opts)
			if err != nil {
				printError(err, output, "Failed to trigger run")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(result, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			if split == "" {
				successPrinter.Printf("Successfully triggered run '%s' for pipeline '%s' in project '%s'\n", result.RunID, pipeline, project)
			} else {
				successPrinter.Printf("Successfully triggered backfill '%s' (split by %s, chunk size %d) for pipeline '%s' in project '%s'\n", result.MultipleActionID, split, chunkSize, pipeline, project)
				if result.URL != "" {
					fmt.Printf("Track this backfill at: %s\n", result.URL)
				}
			}
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

// --- Backfills ---

func CloudBackfills() *cli.Command {
	return &cli.Command{
		Name:  "backfills",
		Usage: "Inspect backfills (grouped runs created by 'runs trigger --split')",
		Commands: []*cli.Command{
			cloudBackfillsList(),
			cloudBackfillsRuns(),
		},
	}
}

func cloudBackfillsList() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List recent backfills",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			projectFlag(),
			pipelineFlag(),
			limitFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			backfills, err := client.ListBackfills(ctx, c.String("project-id"), c.String("pipeline"), c.Int("limit"))
			if err != nil {
				printError(err, output, "Failed to list backfills")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(backfills, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"Backfill ID", "Project", "Pipeline", "Interval Start", "Interval End", "Runs", "Created"})
			for _, b := range backfills {
				t.AppendRow(table.Row{b.ID, b.Project, b.Pipeline, b.IntervalStart, b.IntervalEnd, len(b.Runs), b.CreatedAt})
			}
			t.Render()
			return nil
		},
	}
}

func cloudBackfillsRuns() *cli.Command {
	return &cli.Command{
		Name:  "runs",
		Usage: "List the individual runs of a backfill",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.StringFlag{
				Name:     "id",
				Usage:    "backfill ID (the 'Backfill ID' from 'backfills list')",
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

			runs, err := client.GetBackfillRuns(ctx, c.String("id"), c.Int("limit"), c.Int("offset"))
			if err != nil {
				printError(err, output, "Failed to get backfill runs")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(runs, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"Run ID", "Interval Start", "Interval End", "Created", "Note"})
			for _, r := range runs {
				note := ""
				if r.Note != nil {
					note = *r.Note
				}
				t.AppendRow(table.Row{r.RunID, r.IntervalStart, r.IntervalEnd, r.CreatedAt, note})
			}
			t.Render()
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
			cloudAgentsCreate(),
			cloudAgentsGet(),
			cloudAgentsUpdate(),
			cloudAgentsDelete(),
			cloudAgentsSend(),
			cloudAgentsStatus(),
			cloudAgentsThreads(),
			cloudAgentsMessages(),
			cloudAgentsGetPrompt(),
			cloudAgentsSetPrompt(),
			cloudAgentsGetMemory(),
			cloudAgentsSetMemory(),
			cloudAgentsClearMemory(),
			cloudAgentsExportThread(),
			cloudAgentsConnections(),
			cloudAgentsMcp(),
		},
	}
}

func cloudAgentsConnections() *cli.Command {
	return &cli.Command{
		Name:  "connections",
		Usage: "List the connections available to an agent, or add one with the 'add' subcommand",
		// Backward compatible: the bare command still lists (its Action below); the
		// 'add' subcommand writes a connection. --agent-id is therefore not Required
		// on the parent (that would also demand it when invoking 'add'); the Action
		// validates it instead.
		Commands: []*cli.Command{
			cloudAgentsConnectionsAdd(),
		},
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:  "agent-id",
				Usage: "agent ID",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			// Fail fast on an invalid id rather than sending a bad request and
			// surfacing a generic remote error.
			if c.Int("agent-id") <= 0 {
				printError(fmt.Errorf("--agent-id must be a positive integer, got %d", c.Int("agent-id")), output, "Invalid --agent-id")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			connections, err := client.ListAgentConnections(ctx, c.Int("agent-id"))
			if err != nil {
				printError(err, output, "Failed to list agent connections")
				return cli.Exit("", 1)
			}
			// Normalize a nil slice so JSON output is an empty list, not null —
			// scripting consumers then handle one shape.
			if connections == nil {
				connections = []bruincloud.AgentConnection{}
			}

			if output == "json" {
				data, _ := json.MarshalIndent(connections, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			if len(connections) == 0 {
				infoPrinter.Println("No connections available to this agent.")
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"Name", "Type"})
			for _, conn := range connections {
				t.AppendRow(table.Row{conn.Name, conn.Type})
			}
			t.Render()
			return nil
		},
	}
}

func cloudAgentsConnectionsAdd() *cli.Command {
	return &cli.Command{
		Name:  "add",
		Usage: "Add a connection to an agent (from local .bruin.yml by default)",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:  "agent-id",
				Usage: "agent ID",
			},
			&cli.StringFlag{
				Name:  "name",
				Usage: "the name of the connection",
			},
			&cli.StringFlag{
				Name:  "environment",
				Usage: "the .bruin.yml environment to read the connection from (default: selected environment)",
			},
			&cli.StringFlag{
				Name:  "config-file",
				Usage: "path to the .bruin.yml file",
			},
			&cli.StringFlag{
				Name:  "type",
				Usage: "connection type; required only with --credentials (otherwise read from .bruin.yml)",
			},
			&cli.StringFlag{
				Name:  "credentials",
				Usage: "JSON credentials; if omitted, the connection is read from .bruin.yml",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			// Fail fast on an invalid id rather than sending a bad request and
			// surfacing a generic remote error.
			if c.Int("agent-id") <= 0 {
				printError(fmt.Errorf("--agent-id must be a positive integer, got %d", c.Int("agent-id")), output, "Invalid --agent-id")
				return cli.Exit("", 1)
			}

			name := c.String("name")
			if name == "" {
				printError(errors.New("--name is required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			// Two ways to supply the connection: explicit --credentials JSON (with
			// --type), or read it from the local .bruin.yml by name (the default).
			connType := c.String("type")
			creds := c.String("credentials")
			var credentials map[string]any

			if creds != "" {
				if connType == "" {
					printError(errors.New("--type is required when --credentials is provided"), output, "Missing required flags")
					return cli.Exit("", 1)
				}
				if err := json.Unmarshal([]byte(creds), &credentials); err != nil {
					printError(err, output, "Invalid --credentials JSON")
					return cli.Exit("", 1)
				}
			} else {
				t, cr, err := connectionFromConfig(ctx, name, c.String("environment"), c.String("config-file"))
				if err != nil {
					printError(err, output, "Failed to read connection")
					return cli.Exit("", 1)
				}
				connType, credentials = t, cr
			}

			// The cloud runner can't read local files, so resolve a local
			// service_account_file path into the service_account_json content here.
			if path, ok := credentials["service_account_file"].(string); ok && path != "" {
				data, err := os.ReadFile(path)
				if err != nil {
					printError(err, output, "Failed to read service_account_file")
					return cli.Exit("", 1)
				}
				credentials["service_account_json"] = string(data)
				delete(credentials, "service_account_file")
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			if _, err := client.AddAgentConnection(ctx, c.Int("agent-id"), connType, name, credentials); err != nil {
				printError(err, output, "Failed to add agent connection")
				return cli.Exit("", 1)
			}

			printSuccessForOutput(output, fmt.Sprintf("Added connection '%s' of type '%s' to agent %d", name, connType, c.Int("agent-id")))
			return nil
		},
	}
}

func cloudAgentsGet() *cli.Command {
	return &cli.Command{
		Name:  "get",
		Usage: "Get a single agent's details",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "agent-id",
				Usage:    "agent ID",
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

			agent, err := client.GetAgent(ctx, c.Int("agent-id"))
			if err != nil {
				printError(err, output, "Failed to get agent")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(agent, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			infoPrinter.Printf("Agent %d: %s (visibility: %s)\n", agent.ID, agent.Name, agent.Visibility)
			return nil
		},
	}
}

func cloudAgentsUpdate() *cli.Command {
	return &cli.Command{
		Name:  "update",
		Usage: "Update an agent's name, description, visibility or connection set",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "agent-id",
				Usage:    "agent ID",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "name",
				Usage: "the new agent name",
			},
			&cli.StringFlag{
				Name:  "description",
				Usage: "the new agent description",
			},
			&cli.StringFlag{
				Name:  "visibility",
				Usage: "agent visibility: team or private",
			},
			&cli.IntFlag{
				Name:  "connection-set-id",
				Usage: "assign a connection set to the agent (0 to detach); see 'bruin cloud connection-sets list'",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			// Build the patch from the flags the user actually set, so an explicit
			// empty value (e.g. --description "") is sent rather than dropped.
			fields := map[string]any{}
			if c.IsSet("name") {
				fields["name"] = c.String("name")
			}
			if c.IsSet("description") {
				fields["description"] = c.String("description")
			}
			if c.IsSet("visibility") {
				visibility := c.String("visibility")
				if visibility != "team" && visibility != "private" {
					printError(fmt.Errorf("visibility must be 'team' or 'private', got %q", visibility), output, "Invalid --visibility")
					return cli.Exit("", 1)
				}
				fields["visibility"] = visibility
			}
			if c.IsSet("connection-set-id") {
				// >0 assigns, 0 detaches (explicit null); a negative id is a typo,
				// not a detach — reject it rather than silently wiping the set.
				switch id := c.Int("connection-set-id"); {
				case id > 0:
					fields["connection_set_id"] = id
				case id == 0:
					fields["connection_set_id"] = nil
				default:
					printError(fmt.Errorf("--connection-set-id must be >= 0 (0 detaches), got %d", id), output, "Invalid --connection-set-id")
					return cli.Exit("", 1)
				}
			}
			if len(fields) == 0 {
				printError(errors.New("provide at least one of --name, --description, --visibility or --connection-set-id"), output, "Nothing to update")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			agent, err := client.UpdateAgent(ctx, c.Int("agent-id"), fields)
			if err != nil {
				printError(err, output, "Failed to update agent")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(agent, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			infoPrinter.Printf("Updated agent %d (%s)\n", agent.ID, agent.Name)
			return nil
		},
	}
}

func cloudAgentsDelete() *cli.Command {
	return &cli.Command{
		Name:  "delete",
		Usage: "Delete an agent (cascades to its scheduled agents, dashboards and threads)",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "agent-id",
				Usage:    "agent ID",
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

			if err := client.DeleteAgent(ctx, c.Int("agent-id")); err != nil {
				printError(err, output, "Failed to delete agent")
				return cli.Exit("", 1)
			}

			if output == "json" {
				fmt.Println(`{"success": true}`)
				return nil
			}

			successPrinter.Printf("Deleted agent %d.\n", c.Int("agent-id"))
			return nil
		},
	}
}

func cloudAgentsMcp() *cli.Command {
	return &cli.Command{
		Name:  "mcp",
		Usage: "Manage an agent's external MCP servers (Linear, GitHub, …)",
		Commands: []*cli.Command{
			cloudAgentsMcpList(),
			cloudAgentsMcpSet(),
			cloudAgentsMcpRemove(),
		},
	}
}

func cloudAgentsMcpList() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List an agent's MCP servers and the connections available for each kind",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "agent-id",
				Usage:    "agent ID",
				Required: true,
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			// Fail fast on an invalid id rather than sending a bad request and
			// surfacing a generic remote error.
			if c.Int("agent-id") <= 0 {
				printError(fmt.Errorf("--agent-id must be a positive integer, got %d", c.Int("agent-id")), output, "Invalid --agent-id")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			resp, err := client.ListAgentMcpServers(ctx, c.Int("agent-id"))
			if err != nil {
				printError(err, output, "Failed to list agent MCP servers")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(resp, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"Kind", "Connection", "Display Name"})
			for _, s := range resp.MCPIntegrations {
				display := ""
				if s.DisplayName != nil {
					display = *s.DisplayName
				}
				t.AppendRow(table.Row{s.Kind, s.ConnectionName, display})
			}
			t.Render()

			// Show what can be configured: every supported kind and the
			// connections eligible from the agent's dev-env set.
			kinds := make([]string, 0, len(resp.MCPKinds))
			for k := range resp.MCPKinds {
				kinds = append(kinds, k)
			}
			sort.Strings(kinds)
			infoPrinter.Println("\nAvailable kinds (eligible connections):")
			for _, k := range kinds {
				infoPrinter.Printf("  %s (%s): %s\n", k, resp.MCPKinds[k], strings.Join(resp.ConnectionsByMcpKind[k], ", "))
			}
			return nil
		},
	}
}

func cloudAgentsMcpSet() *cli.Command {
	return &cli.Command{
		Name:  "set",
		Usage: "Attach or update an MCP server on an agent",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "agent-id",
				Usage:    "agent ID",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "kind",
				Usage:    "MCP kind (e.g. linear, github, notion)",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "connection",
				Usage:    "bruin.yml connection name backing this MCP server",
				Required: true,
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")
			agentID := c.Int("agent-id")
			kind := c.String("kind")

			// Fail fast on an invalid id rather than sending a bad request and
			// surfacing a generic remote error.
			if agentID <= 0 {
				printError(fmt.Errorf("--agent-id must be a positive integer, got %d", agentID), output, "Invalid --agent-id")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			// Read-modify-write: the API replaces the whole set, so merge the new
			// pick into the current one to keep the other kinds intact.
			current, err := client.ListAgentMcpServers(ctx, agentID)
			if err != nil {
				printError(err, output, "Failed to load current MCP servers")
				return cli.Exit("", 1)
			}

			servers := upsertMcpServer(current.MCPIntegrations, kind, c.String("connection"))

			agent, err := client.SetAgentMcpServers(ctx, agentID, servers)
			if err != nil {
				printError(err, output, "Failed to set MCP server")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(agent, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			infoPrinter.Printf("Set MCP server %q on agent %d\n", kind, agentID)
			return nil
		},
	}
}

func cloudAgentsMcpRemove() *cli.Command {
	return &cli.Command{
		Name:  "remove",
		Usage: "Detach an MCP server from an agent",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "agent-id",
				Usage:    "agent ID",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "kind",
				Usage:    "MCP kind to remove (e.g. linear, github)",
				Required: true,
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")
			agentID := c.Int("agent-id")
			kind := c.String("kind")

			// Fail fast on an invalid id rather than sending a bad request and
			// surfacing a generic remote error.
			if agentID <= 0 {
				printError(fmt.Errorf("--agent-id must be a positive integer, got %d", agentID), output, "Invalid --agent-id")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			current, err := client.ListAgentMcpServers(ctx, agentID)
			if err != nil {
				printError(err, output, "Failed to load current MCP servers")
				return cli.Exit("", 1)
			}

			servers, removed := removeMcpServer(current.MCPIntegrations, kind)
			if !removed {
				printError(fmt.Errorf("agent %d has no MCP server of kind %q", agentID, kind), output, "Nothing to remove")
				return cli.Exit("", 1)
			}

			if _, err := client.SetAgentMcpServers(ctx, agentID, servers); err != nil {
				printError(err, output, "Failed to remove MCP server")
				return cli.Exit("", 1)
			}

			infoPrinter.Printf("Removed MCP server %q from agent %d\n", kind, agentID)
			return nil
		},
	}
}

// upsertMcpServer returns servers with kind set to connection — updating the
// existing entry in place or appending a new one.
func upsertMcpServer(servers []bruincloud.AgentMcpServer, kind, connection string) []bruincloud.AgentMcpServer {
	out := make([]bruincloud.AgentMcpServer, 0, len(servers)+1)
	found := false
	for _, s := range servers {
		if s.Kind == kind {
			s.ConnectionName = connection
			found = true
		}
		out = append(out, bruincloud.AgentMcpServer{Kind: s.Kind, ConnectionName: s.ConnectionName})
	}
	if !found {
		out = append(out, bruincloud.AgentMcpServer{Kind: kind, ConnectionName: connection})
	}
	return out
}

// removeMcpServer returns servers without kind, and whether it was present.
func removeMcpServer(servers []bruincloud.AgentMcpServer, kind string) ([]bruincloud.AgentMcpServer, bool) {
	out := make([]bruincloud.AgentMcpServer, 0, len(servers))
	removed := false
	for _, s := range servers {
		if s.Kind == kind {
			removed = true
			continue
		}
		out = append(out, bruincloud.AgentMcpServer{Kind: s.Kind, ConnectionName: s.ConnectionName})
	}
	return out, removed
}

func cloudAgentsCreate() *cli.Command {
	return &cli.Command{
		Name:  "create",
		Usage: "Create a new agent",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.StringFlag{
				Name:     "name",
				Usage:    "the agent name",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "description",
				Usage: "the agent description",
			},
			&cli.StringFlag{
				Name:  "prompt",
				Usage: "the agent's system prompt",
			},
			&cli.StringFlag{
				Name:  "visibility",
				Usage: "agent visibility: team or private (default: team)",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			visibility := c.String("visibility")
			if visibility != "" && visibility != "team" && visibility != "private" {
				printError(fmt.Errorf("visibility must be 'team' or 'private', got %q", visibility), output, "Invalid --visibility")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			agent, err := client.CreateAgent(ctx, c.String("name"), c.String("description"), c.String("prompt"), visibility)
			if err != nil {
				printError(err, output, "Failed to create agent")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(agent, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			infoPrinter.Printf("Created agent %d (%s)\n", agent.ID, agent.Name)
			return nil
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
		Usage: "List an agent's threads, or manage one with the 'rename'/'archive'/'unarchive'/'delete' subcommands",
		// Backward compatible: the bare command still lists (its Action below); the
		// subcommands manage a thread. --agent-id is therefore not Required on the
		// parent (that would also demand it for the subcommands); the Action validates it.
		Commands: []*cli.Command{
			cloudAgentsThreadsRename(),
			cloudAgentsThreadsArchive(),
			cloudAgentsThreadsUnarchive(),
			cloudAgentsThreadsDelete(),
		},
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:  "agent-id",
				Usage: "agent ID",
			},
			&cli.BoolFlag{
				Name:  "archived",
				Usage: "list archived threads instead of active ones",
			},
			limitFlag(),
			offsetFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			if c.Int("agent-id") <= 0 {
				printError(fmt.Errorf("--agent-id must be a positive integer, got %d", c.Int("agent-id")), output, "Invalid --agent-id")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			threads, err := client.ListAgentThreads(ctx, c.Int("agent-id"), c.Int("limit"), c.Int("offset"), c.Bool("archived"))
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
			t.AppendHeader(table.Row{"ID", "Agent ID", "Title", "Created At", "Updated At", "Archived At"})
			for _, th := range threads {
				t.AppendRow(table.Row{th.ID, th.AgentID, derefString(th.Title), th.CreatedAt, th.UpdatedAt, derefString(th.ArchivedAt)})
			}
			t.Render()
			return nil
		},
	}
}

// threadIDFlags are the required agent-id + thread-id flags shared by the thread
// management subcommands.
func threadIDFlags() []cli.Flag {
	return []cli.Flag{
		apiKeyFlag(),
		outputFlag(),
		&cli.IntFlag{Name: "agent-id", Usage: "agent ID", Required: true},
		&cli.IntFlag{Name: "thread-id", Usage: "thread ID", Required: true},
	}
}

func cloudAgentsThreadsRename() *cli.Command {
	return &cli.Command{
		Name:  "rename",
		Usage: "Rename a thread",
		Flags: append(threadIDFlags(), &cli.StringFlag{
			Name:     "title",
			Usage:    "the new thread title",
			Required: true,
		}),
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			thread, err := client.UpdateAgentThread(ctx, c.Int("agent-id"), c.Int("thread-id"), map[string]any{"title": c.String("title")})
			if err != nil {
				printError(err, output, "Failed to rename thread")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(thread, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			successPrinter.Printf("Renamed thread %d to %q.\n", thread.ID, derefString(thread.Title))
			return nil
		},
	}
}

func cloudAgentsThreadsArchive() *cli.Command {
	return &cli.Command{
		Name:  "archive",
		Usage: "Archive a thread",
		Flags: threadIDFlags(),
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			if _, err := client.UpdateAgentThread(ctx, c.Int("agent-id"), c.Int("thread-id"), map[string]any{"archived": true}); err != nil {
				printError(err, output, "Failed to archive thread")
				return cli.Exit("", 1)
			}

			if output == "json" {
				fmt.Println(`{"success": true}`)
				return nil
			}

			successPrinter.Printf("Archived thread %d.\n", c.Int("thread-id"))
			return nil
		},
	}
}

func cloudAgentsThreadsUnarchive() *cli.Command {
	return &cli.Command{
		Name:  "unarchive",
		Usage: "Restore an archived thread",
		Flags: threadIDFlags(),
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			if _, err := client.UpdateAgentThread(ctx, c.Int("agent-id"), c.Int("thread-id"), map[string]any{"archived": false}); err != nil {
				printError(err, output, "Failed to unarchive thread")
				return cli.Exit("", 1)
			}

			if output == "json" {
				fmt.Println(`{"success": true}`)
				return nil
			}

			successPrinter.Printf("Unarchived thread %d.\n", c.Int("thread-id"))
			return nil
		},
	}
}

func cloudAgentsThreadsDelete() *cli.Command {
	return &cli.Command{
		Name:  "delete",
		Usage: "Delete a thread",
		Flags: threadIDFlags(),
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			if err := client.DeleteAgentThread(ctx, c.Int("agent-id"), c.Int("thread-id")); err != nil {
				printError(err, output, "Failed to delete thread")
				return cli.Exit("", 1)
			}

			if output == "json" {
				fmt.Println(`{"success": true}`)
				return nil
			}

			successPrinter.Printf("Deleted thread %d.\n", c.Int("thread-id"))
			return nil
		},
	}
}

func cloudAgentsGetPrompt() *cli.Command {
	return &cli.Command{
		Name:  "get-prompt",
		Usage: "Get an agent's system prompt",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "agent-id",
				Usage:    "agent ID",
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

			prompt, err := client.GetAgentPrompt(ctx, c.Int("agent-id"))
			if err != nil {
				printError(err, output, "Failed to get agent prompt")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(prompt, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			if prompt.SystemPrompt == nil {
				fmt.Println("(none)")
				return nil
			}
			fmt.Println(*prompt.SystemPrompt)
			return nil
		},
	}
}

func cloudAgentsSetPrompt() *cli.Command {
	return &cli.Command{
		Name:  "set-prompt",
		Usage: "Set an agent's system prompt",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "agent-id",
				Usage:    "agent ID",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "prompt",
				Usage:    "the new system prompt",
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

			prompt, err := client.SetAgentPrompt(ctx, c.Int("agent-id"), c.String("prompt"))
			if err != nil {
				printError(err, output, "Failed to set agent prompt")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(prompt, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			infoPrinter.Printf("Updated system prompt for agent %d (%s)\n", prompt.ID, prompt.Name)
			return nil
		},
	}
}

func cloudAgentsGetMemory() *cli.Command {
	return &cli.Command{
		Name:  "get-memory",
		Usage: "Get an agent's long-term memory",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "agent-id",
				Usage:    "agent ID",
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

			memory, err := client.GetAgentMemory(ctx, c.Int("agent-id"))
			if err != nil {
				printError(err, output, "Failed to get agent memory")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(memory, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			if memory.Memory == nil {
				fmt.Println("(none)")
				return nil
			}
			fmt.Println(*memory.Memory)
			return nil
		},
	}
}

func cloudAgentsSetMemory() *cli.Command {
	return &cli.Command{
		Name:  "set-memory",
		Usage: "Replace an agent's long-term memory from --memory or --memory-file",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "agent-id",
				Usage:    "agent ID",
				Required: true,
			},
			&cli.StringFlag{Name: "memory", Usage: "the new memory content"},
			&cli.StringFlag{Name: "memory-file", Usage: "path to a file whose contents become the memory"},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			content, err := resolveMemoryContent(c)
			if err != nil {
				printError(err, output, "Invalid memory")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			memory, err := client.SetAgentMemory(ctx, c.Int("agent-id"), &content)
			if err != nil {
				printError(err, output, "Failed to set agent memory")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(memory, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			infoPrinter.Printf("Updated memory for agent %d (%s)\n", memory.ID, memory.Name)
			return nil
		},
	}
}

func cloudAgentsClearMemory() *cli.Command {
	return &cli.Command{
		Name:  "clear-memory",
		Usage: "Clear an agent's long-term memory",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "agent-id",
				Usage:    "agent ID",
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

			memory, err := client.SetAgentMemory(ctx, c.Int("agent-id"), nil)
			if err != nil {
				printError(err, output, "Failed to clear agent memory")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(memory, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			infoPrinter.Printf("Cleared memory for agent %d (%s)\n", memory.ID, memory.Name)
			return nil
		},
	}
}

// resolveMemoryContent reads the memory body from --memory or --memory-file.
func resolveMemoryContent(c *cli.Command) (string, error) {
	if c.IsSet("memory") && c.IsSet("memory-file") {
		return "", errors.New("pass only one of --memory or --memory-file")
	}
	if c.IsSet("memory-file") {
		data, err := os.ReadFile(c.String("memory-file"))
		if err != nil {
			return "", fmt.Errorf("failed to read --memory-file: %w", err)
		}
		return string(data), nil
	}
	if c.IsSet("memory") {
		return c.String("memory"), nil
	}
	return "", errors.New("provide the memory with --memory or --memory-file")
}

func cloudAgentsExportThread() *cli.Command {
	return &cli.Command{
		Name:  "export-thread",
		Usage: "Export a chat thread as JSON",
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
			&cli.StringFlag{
				Name:  "file",
				Usage: "write the export to this file instead of stdout",
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

			export, err := client.ExportThread(ctx, c.Int("agent-id"), c.Int("thread-id"))
			if err != nil {
				printError(err, output, "Failed to export thread")
				return cli.Exit("", 1)
			}

			// Indent the raw bytes rather than round-tripping through `any`, which
			// would coerce large integers through float64 and silently corrupt them.
			var buf bytes.Buffer
			if err := json.Indent(&buf, export, "", "  "); err != nil {
				printError(err, output, "Failed to format export")
				return cli.Exit("", 1)
			}
			pretty := buf.Bytes()

			if file := c.String("file"); file != "" {
				if err := os.WriteFile(file, pretty, 0o600); err != nil {
					printError(err, output, "Failed to write file")
					return cli.Exit("", 1)
				}
				infoPrinter.Printf("Exported thread %d to %s\n", c.Int("thread-id"), file)
				return nil
			}

			fmt.Println(string(pretty))
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

func CloudConnections() *cli.Command {
	return &cli.Command{
		Name:  "connections",
		Usage: "Manage Bruin Cloud connections",
		Commands: []*cli.Command{
			cloudConnectionsList(),
			cloudConnectionsAdd(),
			cloudConnectionsDuplicate(),
			cloudConnectionsDelete(),
		},
	}
}

func cloudConnectionsList() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List all connections",
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

			connections, err := client.ListConnections(ctx)
			if err != nil {
				printError(err, output, "Failed to list connections")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(connections, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"Name", "Type"})
			for _, conn := range connections {
				t.AppendRow(table.Row{conn.Name, conn.Type})
			}
			t.Render()
			return nil
		},
	}
}

// connectionFromConfig loads a named connection from the selected connection
// source and returns its type and credentials as a snake_case map (the cloud wire format).
// The struct's JSON tags already match the wire format, so a marshal round-trip
// avoids any per-type mapping.
func connectionFromConfig(ctx context.Context, name, environment, configFile string) (string, map[string]any, error) {
	configFilePath := configFile
	if configFilePath == "" {
		repoRoot, err := git.FindRepoFromPath(".")
		if err != nil {
			return "", nil, errors.New("could not locate .bruin.yml (not in a git repo); pass --config-file or --credentials")
		}
		configFilePath = path2.Join(repoRoot.Path, ".bruin.yml")
	}

	cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
	if err != nil {
		return "", nil, fmt.Errorf("failed to load %s: %w", configFilePath, err)
	}

	if environment != "" {
		if err := cm.SelectEnvironment(environment); err != nil {
			return "", nil, err
		}
	}

	if secretsBackendFromContext(ctx) != "" {
		ctx = context.WithValue(ctx, config.ConfigFilePathContextKey, configFilePath)
		ctx = context.WithValue(ctx, config.EnvironmentNameContextKey, cm.SelectedEnvironmentName)
		manager, errs := connectionManagerFromConfig(ctx, cm, makeLogger(false))
		if len(errs) > 0 {
			return "", nil, fmt.Errorf("failed to create connection manager: %w", errs[0])
		}

		connType := manager.GetConnectionType(name)
		details := manager.GetConnectionDetails(name)
		if connType == "" || details == nil {
			return "", nil, config.NewConnectionNotFoundError(ctx, "", name)
		}

		credentials, err := connectionCredentialsMap(details)
		if err != nil {
			return "", nil, err
		}
		return connType, credentials, nil
	}

	env := cm.SelectedEnvironment
	if env == nil || env.Connections == nil || !env.Connections.Exists(name) {
		return "", nil, fmt.Errorf("connection '%s' not found in config", name)
	}

	connType := env.Connections.ConnectionsSummaryList()[name]
	credentials, err := connectionCredentialsMap(env.Connections.GetConnection(name))
	if err != nil {
		return "", nil, err
	}

	// A relative service_account_file in .bruin.yml is relative to the config
	// file, not the CWD the command runs from. Resolve it against the config dir.
	if p, ok := credentials["service_account_file"].(string); ok && p != "" && !filepath.IsAbs(p) {
		credentials["service_account_file"] = filepath.Join(filepath.Dir(configFilePath), p)
	}

	return connType, credentials, nil
}

func connectionCredentialsMap(connectionDetails any) (map[string]any, error) {
	data, err := json.Marshal(connectionDetails)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize connection: %w", err)
	}
	var credentials map[string]any
	if err := json.Unmarshal(data, &credentials); err != nil {
		return nil, fmt.Errorf("failed to serialize connection: %w", err)
	}
	delete(credentials, "name")
	return credentials, nil
}

func cloudConnectionsAdd() *cli.Command {
	return &cli.Command{
		Name:  "add",
		Usage: "Add a connection to Bruin Cloud (from local .bruin.yml by default)",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.StringFlag{
				Name:  "name",
				Usage: "the name of the connection",
			},
			&cli.StringFlag{
				Name:  "environment",
				Usage: "the .bruin.yml environment to read the connection from (default: selected environment)",
			},
			&cli.StringFlag{
				Name:  "config-file",
				Usage: "path to the .bruin.yml file",
			},
			&cli.StringFlag{
				Name:  "type",
				Usage: "connection type; required only with --credentials (otherwise read from .bruin.yml)",
			},
			&cli.StringFlag{
				Name:  "credentials",
				Usage: "JSON credentials; if omitted, the connection is read from .bruin.yml",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			name := c.String("name")
			if name == "" {
				printError(errors.New("--name is required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			connType := c.String("type")
			creds := c.String("credentials")
			var credentials map[string]any

			if creds != "" {
				if connType == "" {
					printError(errors.New("--type is required when --credentials is provided"), output, "Missing required flags")
					return cli.Exit("", 1)
				}
				if err := json.Unmarshal([]byte(creds), &credentials); err != nil {
					printError(err, output, "Invalid --credentials JSON")
					return cli.Exit("", 1)
				}
			} else {
				t, cr, err := connectionFromConfig(ctx, name, c.String("environment"), c.String("config-file"))
				if err != nil {
					printError(err, output, "Failed to read connection")
					return cli.Exit("", 1)
				}
				connType, credentials = t, cr
			}

			// The cloud runner can't read local files, so resolve a local
			// service_account_file path into the service_account_json content here.
			if path, ok := credentials["service_account_file"].(string); ok && path != "" {
				data, err := os.ReadFile(path)
				if err != nil {
					printError(err, output, "Failed to read service_account_file")
					return cli.Exit("", 1)
				}
				credentials["service_account_json"] = string(data)
				delete(credentials, "service_account_file")
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			if err := client.CreateConnection(ctx, name, connType, credentials); err != nil {
				printError(err, output, "Failed to create connection")
				return cli.Exit("", 1)
			}

			printSuccessForOutput(output, fmt.Sprintf("Successfully created connection '%s' of type '%s'", name, connType))
			return nil
		},
	}
}

func cloudConnectionsDelete() *cli.Command {
	return &cli.Command{
		Name:  "delete",
		Usage: "Delete a connection",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.StringFlag{
				Name:  "name",
				Usage: "the name of the connection to delete",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			name := c.String("name")
			if name == "" {
				printError(errors.New("--name is required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			if err := client.DeleteConnection(ctx, name); err != nil {
				printError(err, output, "Failed to delete connection")
				return cli.Exit("", 1)
			}

			printSuccessForOutput(output, fmt.Sprintf("Successfully deleted connection '%s'", name))
			return nil
		},
	}
}

func cloudConnectionsDuplicate() *cli.Command {
	return &cli.Command{
		Name:  "duplicate",
		Usage: "Duplicate a connection under a new name (credentials are copied server-side)",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.StringFlag{
				Name:  "name",
				Usage: "the name of the connection to duplicate",
			},
			&cli.StringFlag{
				Name:  "as",
				Usage: "the name for the duplicated connection",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			name := c.String("name")
			target := c.String("as")
			if name == "" || target == "" {
				printError(errors.New("--name and --as are required"), output, "Missing required flags")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			if err := client.DuplicateConnection(ctx, name, target); err != nil {
				printError(err, output, "Failed to duplicate connection")
				return cli.Exit("", 1)
			}

			printSuccessForOutput(output, fmt.Sprintf("Successfully duplicated connection '%s' to '%s'", name, target))
			return nil
		},
	}
}

func CloudConnectionSets() *cli.Command {
	return &cli.Command{
		Name:  "connection-sets",
		Usage: "Manage Bruin Cloud connection sets (named bundles of connections an agent runs against)",
		Commands: []*cli.Command{
			cloudConnectionSetsList(),
			cloudConnectionSetsGet(),
			cloudConnectionSetsCreate(),
			cloudConnectionSetsUpdate(),
			cloudConnectionSetsDelete(),
		},
	}
}

func cloudConnectionSetsList() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List connection sets",
		Flags: []cli.Flag{apiKeyFlag(), outputFlag()},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			sets, err := client.ListConnectionSets(ctx)
			if err != nil {
				printError(err, output, "Failed to list connection sets")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(sets, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			if len(sets) == 0 {
				infoPrinter.Println("No connection sets found.")
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"ID", "Name", "Created", "Updated"})
			for _, s := range sets {
				t.AppendRow(table.Row{s.ID, s.Name, s.CreatedAt, s.UpdatedAt})
			}
			t.Render()
			return nil
		},
	}
}

func cloudConnectionSetsGet() *cli.Command {
	return &cli.Command{
		Name:  "get",
		Usage: "List the connections in a connection set (names and types only)",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{Name: "set-id", Usage: "connection set ID", Required: true},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			if c.Int("set-id") <= 0 {
				printError(fmt.Errorf("--set-id must be a positive integer, got %d", c.Int("set-id")), output, "Invalid --set-id")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			conns, err := client.ListConnectionSetConnections(ctx, c.Int("set-id"))
			if err != nil {
				printError(err, output, "Failed to get connection set")
				return cli.Exit("", 1)
			}
			if conns == nil {
				conns = []bruincloud.Connection{}
			}

			if output == "json" {
				data, _ := json.MarshalIndent(conns, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			if len(conns) == 0 {
				infoPrinter.Println("This connection set has no connections.")
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"Name", "Type"})
			for _, conn := range conns {
				t.AppendRow(table.Row{conn.Name, conn.Type})
			}
			t.Render()
			return nil
		},
	}
}

func cloudConnectionSetsCreate() *cli.Command {
	return &cli.Command{
		Name:  "create",
		Usage: "Create a connection set from connections in the local .bruin.yml",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.StringFlag{Name: "name", Usage: "the connection set name", Required: true},
			&cli.StringSliceFlag{Name: "connection", Usage: "a connection name to include, read from the local .bruin.yml; repeat for multiple", Required: true},
			&cli.StringFlag{Name: "environment", Usage: "the .bruin.yml environment to read from (default: selected environment)"},
			&cli.StringFlag{Name: "config-file", Usage: "path to the .bruin.yml file"},
			&cli.BoolFlag{Name: "skip-validation", Usage: "skip the live connection test"},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			inputs, err := connectionSetInputsFromConfig(ctx, c.StringSlice("connection"), c.String("environment"), c.String("config-file"))
			if err != nil {
				printError(err, output, "Failed to read connections")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			set, err := client.CreateConnectionSet(ctx, c.String("name"), inputs, c.Bool("skip-validation"))
			if err != nil {
				printError(err, output, "Failed to create connection set")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(set, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			printSuccessForOutput(output, fmt.Sprintf("Created connection set '%s' (ID %d) with %d connection(s)", set.Name, set.ID, len(inputs)))
			return nil
		},
	}
}

func cloudConnectionSetsUpdate() *cli.Command {
	return &cli.Command{
		Name:  "update",
		Usage: "Replace a connection set's connections with ones from the local .bruin.yml",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{Name: "set-id", Usage: "connection set ID", Required: true},
			&cli.StringSliceFlag{Name: "connection", Usage: "a connection name to include, read from the local .bruin.yml; repeat for multiple. The set becomes exactly these connections.", Required: true},
			&cli.StringFlag{Name: "environment", Usage: "the .bruin.yml environment to read from (default: selected environment)"},
			&cli.StringFlag{Name: "config-file", Usage: "path to the .bruin.yml file"},
			&cli.BoolFlag{Name: "skip-validation", Usage: "skip the live connection test"},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			if c.Int("set-id") <= 0 {
				printError(fmt.Errorf("--set-id must be a positive integer, got %d", c.Int("set-id")), output, "Invalid --set-id")
				return cli.Exit("", 1)
			}

			inputs, err := connectionSetInputsFromConfig(ctx, c.StringSlice("connection"), c.String("environment"), c.String("config-file"))
			if err != nil {
				printError(err, output, "Failed to read connections")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			if err := client.UpdateConnectionSet(ctx, c.Int("set-id"), inputs, c.Bool("skip-validation")); err != nil {
				printError(err, output, "Failed to update connection set")
				return cli.Exit("", 1)
			}

			printSuccessForOutput(output, fmt.Sprintf("Updated connection set %d (%d connection(s))", c.Int("set-id"), len(inputs)))
			return nil
		},
	}
}

func cloudConnectionSetsDelete() *cli.Command {
	return &cli.Command{
		Name:  "delete",
		Usage: "Delete a connection set",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{Name: "set-id", Usage: "connection set ID", Required: true},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			if c.Int("set-id") <= 0 {
				printError(fmt.Errorf("--set-id must be a positive integer, got %d", c.Int("set-id")), output, "Invalid --set-id")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			if err := client.DeleteConnectionSet(ctx, c.Int("set-id")); err != nil {
				printError(err, output, "Failed to delete connection set")
				return cli.Exit("", 1)
			}

			printSuccessForOutput(output, fmt.Sprintf("Deleted connection set %d", c.Int("set-id")))
			return nil
		},
	}
}

// connectionSetInputsFromConfig reads each named connection from the local
// .bruin.yml and returns them as {type, name, config} inputs, inlining any
// service_account_file into service_account_json (the cloud runner can't read
// local files). Every connection is sent with a full config — the API takes no
// partial edits.
func connectionSetInputsFromConfig(ctx context.Context, names []string, environment, configFile string) ([]bruincloud.ConnectionSetInput, error) {
	inputs := make([]bruincloud.ConnectionSetInput, 0, len(names))
	for _, name := range names {
		connType, credentials, err := connectionFromConfig(ctx, name, environment, configFile)
		if err != nil {
			return nil, fmt.Errorf("connection %q: %w", name, err)
		}

		if path, ok := credentials["service_account_file"].(string); ok && path != "" {
			data, err := os.ReadFile(path)
			if err != nil {
				return nil, fmt.Errorf("connection %q: failed to read service_account_file: %w", name, err)
			}
			credentials["service_account_json"] = string(data)
			delete(credentials, "service_account_file")
		}

		inputs = append(inputs, bruincloud.ConnectionSetInput{Type: connType, Name: name, Config: credentials})
	}
	return inputs, nil
}

func CloudSkills() *cli.Command {
	return &cli.Command{
		Name:  "skills",
		Usage: "Manage Bruin Cloud team skills (instruction snippets attached to agents)",
		Commands: []*cli.Command{
			cloudSkillsList(),
			cloudSkillsCreate(),
			cloudSkillsUpdate(),
			cloudSkillsDelete(),
			cloudSkillsSetAgents(),
		},
	}
}

// skillContentFlags are the create/update flags. The server requires name,
// description and a body on both, so name/description are required here and the
// body (via --body or --body-file) is validated in the action.
func skillContentFlags() []cli.Flag {
	return []cli.Flag{
		apiKeyFlag(),
		outputFlag(),
		&cli.StringFlag{Name: "name", Usage: "skill name (letters, numbers, '_' and '-')", Required: true},
		&cli.StringFlag{Name: "description", Usage: "short description", Required: true},
		&cli.StringFlag{Name: "body", Usage: "the skill instructions"},
		&cli.StringFlag{Name: "body-file", Usage: "path to a file with the skill instructions"},
		&cli.BoolFlag{Name: "all-agents", Usage: "apply to every agent on the team"},
	}
}

// resolveSkillBody reads the body from --body or --body-file (exactly one required).
func resolveSkillBody(c *cli.Command) (string, error) {
	body := c.String("body")
	file := c.String("body-file")
	if body != "" && file != "" {
		return "", errors.New("pass only one of --body or --body-file")
	}
	if file != "" {
		data, err := os.ReadFile(file)
		if err != nil {
			return "", fmt.Errorf("failed to read --body-file: %w", err)
		}
		return string(data), nil
	}
	if body == "" {
		return "", errors.New("provide the skill body via --body or --body-file")
	}
	return body, nil
}

func skillFieldsFromFlags(c *cli.Command) (map[string]any, error) {
	body, err := resolveSkillBody(c)
	if err != nil {
		return nil, err
	}
	fields := map[string]any{
		"name":        c.String("name"),
		"description": c.String("description"),
		"body":        body,
	}
	if c.IsSet("all-agents") {
		fields["all_agents"] = c.Bool("all-agents")
	}
	return fields, nil
}

// skillUpdateFields builds a partial update from only the flags that were set, so a
// caller can change one field without resending the whole skill.
func skillUpdateFields(c *cli.Command) (map[string]any, error) {
	fields := map[string]any{}
	if c.IsSet("name") {
		fields["name"] = c.String("name")
	}
	if c.IsSet("description") {
		fields["description"] = c.String("description")
	}
	if c.IsSet("body") || c.IsSet("body-file") {
		body, err := resolveSkillBody(c)
		if err != nil {
			return nil, err
		}
		fields["body"] = body
	}
	if c.IsSet("all-agents") {
		fields["all_agents"] = c.Bool("all-agents")
	}
	if len(fields) == 0 {
		return nil, errors.New("provide at least one field to update (--name, --description, --body/--body-file or --all-agents)")
	}
	return fields, nil
}

func cloudSkillsList() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List the team's skills",
		Flags: []cli.Flag{apiKeyFlag(), outputFlag()},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			skills, err := client.ListSkills(ctx)
			if err != nil {
				printError(err, output, "Failed to list skills")
				return cli.Exit("", 1)
			}
			if skills == nil {
				skills = []bruincloud.Skill{}
			}

			if output == "json" {
				data, _ := json.MarshalIndent(skills, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			if len(skills) == 0 {
				infoPrinter.Println("No skills yet.")
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"ID", "Name", "Description", "All Agents", "Agents"})
			for _, s := range skills {
				t.AppendRow(table.Row{s.ID, s.Name, s.Description, s.AllAgents, len(s.AgentIDs)})
			}
			t.Render()
			return nil
		},
	}
}

func cloudSkillsCreate() *cli.Command {
	return &cli.Command{
		Name:  "create",
		Usage: "Create a team skill",
		Flags: skillContentFlags(),
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			fields, err := skillFieldsFromFlags(c)
			if err != nil {
				printError(err, output, "Invalid skill")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			skill, err := client.CreateSkill(ctx, fields)
			if err != nil {
				printError(err, output, "Failed to create skill")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(skill, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			successPrinter.Printf("Created skill %d (%s).\n", skill.ID, skill.Name)
			return nil
		},
	}
}

func cloudSkillsUpdate() *cli.Command {
	return &cli.Command{
		Name:  "update",
		Usage: "Update a skill (only the fields you pass change)",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{Name: "skill-id", Usage: "skill ID", Required: true},
			&cli.StringFlag{Name: "name", Usage: "new skill name"},
			&cli.StringFlag{Name: "description", Usage: "new description"},
			&cli.StringFlag{Name: "body", Usage: "new skill instructions"},
			&cli.StringFlag{Name: "body-file", Usage: "path to a file with the new skill instructions"},
			&cli.BoolFlag{Name: "all-agents", Usage: "apply to every agent on the team (--all-agents=false to unset)"},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			fields, err := skillUpdateFields(c)
			if err != nil {
				printError(err, output, "Invalid update")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			skill, err := client.UpdateSkill(ctx, c.Int("skill-id"), fields)
			if err != nil {
				printError(err, output, "Failed to update skill")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(skill, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			successPrinter.Printf("Updated skill %d (%s).\n", skill.ID, skill.Name)
			return nil
		},
	}
}

func cloudSkillsDelete() *cli.Command {
	return &cli.Command{
		Name:  "delete",
		Usage: "Delete a skill",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{Name: "skill-id", Usage: "skill ID", Required: true},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			if err := client.DeleteSkill(ctx, c.Int("skill-id")); err != nil {
				printError(err, output, "Failed to delete skill")
				return cli.Exit("", 1)
			}

			if output == "json" {
				fmt.Println(`{"deleted": true}`)
				return nil
			}

			successPrinter.Printf("Deleted skill %d.\n", c.Int("skill-id"))
			return nil
		},
	}
}

func cloudSkillsSetAgents() *cli.Command {
	return &cli.Command{
		Name:  "set-agents",
		Usage: "Set which agents a skill is attached to (replaces the set; omit --agent-id to detach all)",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{Name: "skill-id", Usage: "skill ID", Required: true},
			&cli.StringSliceFlag{Name: "agent-id", Usage: "agent ID to attach (repeatable)"},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			agentIDs := make([]int, 0, len(c.StringSlice("agent-id")))
			for _, raw := range c.StringSlice("agent-id") {
				id, err := strconv.Atoi(raw)
				if err != nil {
					printError(fmt.Errorf("invalid --agent-id %q: must be an integer", raw), output, "Invalid --agent-id")
					return cli.Exit("", 1)
				}
				agentIDs = append(agentIDs, id)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			attached, err := client.SetSkillAgents(ctx, c.Int("skill-id"), agentIDs)
			if err != nil {
				printError(err, output, "Failed to set skill agents")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(map[string]any{"agent_ids": attached}, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			successPrinter.Printf("Skill %d is now attached to %d agent(s): %v\n", c.Int("skill-id"), len(attached), attached)
			return nil
		},
	}
}

func CloudDashboards() *cli.Command {
	return &cli.Command{
		Name:  "dashboards",
		Usage: "Manage Bruin Cloud dashboards",
		Commands: []*cli.Command{
			cloudDashboardsList(),
			cloudDashboardsGet(),
			cloudDashboardsCreate(),
			cloudDashboardsUpdate(),
			cloudDashboardsDelete(),
		},
	}
}

func cloudDashboardsList() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List dashboards",
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

			dashboards, err := client.ListDashboards(ctx)
			if err != nil {
				printError(err, output, "Failed to list dashboards")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(dashboards, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"ID", "Title", "Visibility", "Updated At"})
			for _, d := range dashboards {
				title := ""
				if d.Title != nil {
					title = *d.Title
				}
				updated := ""
				if d.UpdatedAt != nil {
					updated = *d.UpdatedAt
				}
				t.AppendRow(table.Row{d.ID, title, d.Visibility, updated})
			}
			t.Render()
			return nil
		},
	}
}

func cloudDashboardsGet() *cli.Command {
	return &cli.Command{
		Name:  "get",
		Usage: "Get a dashboard including its published definition",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "dashboard-id",
				Usage:    "dashboard ID",
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

			dashboard, err := client.GetDashboard(ctx, c.Int("dashboard-id"))
			if err != nil {
				printError(err, output, "Failed to get dashboard")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(dashboard, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			title := ""
			if dashboard.Title != nil {
				title = *dashboard.Title
			}
			infoPrinter.Printf("Dashboard %d: %s (visibility: %s)\n", dashboard.ID, title, dashboard.Visibility)

			// Print the definition (state) as pretty JSON so it can be inspected or saved.
			if len(dashboard.State) > 0 {
				var obj any
				if err := json.Unmarshal(dashboard.State, &obj); err == nil {
					pretty, _ := json.MarshalIndent(obj, "", "  ")
					fmt.Println(string(pretty))
				} else {
					fmt.Println(string(dashboard.State))
				}
			}
			return nil
		},
	}
}

// parseJSONOrYAMLObject decodes a JSON or YAML object (a dashboard definition, a
// scheduled-agent plan, ...) into a map. It walks the YAML node tree rather than
// decoding straight into a map so
// that timestamp-like scalars (e.g. an unquoted `2024-01-01`) are preserved as
// their original string — yaml.v3 would otherwise resolve them to time.Time,
// which JSON-encodes as an RFC3339 timestamp and changes the value. Returns a
// nil map (no error) when the document is empty or not a mapping; the caller
// rejects that as "must be an object".
func parseJSONOrYAMLObject(raw []byte) (map[string]any, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	if len(doc.Content) == 0 {
		return nil, nil
	}
	v, err := yamlNodeToValue(doc.Content[0])
	if err != nil {
		return nil, err
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, nil
	}
	return m, nil
}

func yamlNodeToValue(n *yaml.Node) (any, error) {
	switch n.Kind {
	case yaml.DocumentNode:
		if len(n.Content) == 0 {
			return nil, nil
		}
		return yamlNodeToValue(n.Content[0])
	case yaml.AliasNode:
		return yamlNodeToValue(n.Alias)
	case yaml.MappingNode:
		m := make(map[string]any, len(n.Content)/2)
		for i := 0; i+1 < len(n.Content); i += 2 {
			val, err := yamlNodeToValue(n.Content[i+1])
			if err != nil {
				return nil, err
			}
			m[n.Content[i].Value] = val
		}
		return m, nil
	case yaml.SequenceNode:
		s := make([]any, 0, len(n.Content))
		for _, c := range n.Content {
			val, err := yamlNodeToValue(c)
			if err != nil {
				return nil, err
			}
			s = append(s, val)
		}
		return s, nil
	default: // ScalarNode
		// Keep timestamp-like scalars as their raw string so the value shape is
		// preserved through the JSON request to the API.
		if n.Tag == "!!timestamp" {
			return n.Value, nil
		}
		var v any
		if err := n.Decode(&v); err != nil {
			return nil, err
		}
		return v, nil
	}
}

func cloudDashboardsCreate() *cli.Command {
	return &cli.Command{
		Name:  "create",
		Usage: "Create a dashboard from a definition (written to draft; publish stays in the UI)",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.StringFlag{
				Name:     "title",
				Usage:    "the dashboard title",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "visibility",
				Usage: "team or private (default: team)",
			},
			&cli.IntFlag{
				Name:  "agent-id",
				Usage: "the agent to bind for canvas chat and refresh (defaults to the token's agent)",
			},
			&cli.StringFlag{
				Name:  "state",
				Usage: "the dashboard definition as a JSON or YAML string",
			},
			&cli.StringFlag{
				Name:  "state-file",
				Usage: "path to a file containing the dashboard definition as JSON or YAML",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			visibility := c.String("visibility")
			if visibility != "" && visibility != "team" && visibility != "private" {
				printError(fmt.Errorf("visibility must be 'team' or 'private', got %q", visibility), output, "Invalid --visibility")
				return cli.Exit("", 1)
			}

			// Reject an explicit non-positive --agent-id rather than silently
			// dropping it (which would fall back to the token agent, or none, and
			// create a dashboard with no chat against the caller's intent).
			if c.IsSet("agent-id") && c.Int("agent-id") <= 0 {
				printError(fmt.Errorf("--agent-id must be a positive integer, got %d", c.Int("agent-id")), output, "Invalid --agent-id")
				return cli.Exit("", 1)
			}

			// The definition can come inline (--state) or from a file (--state-file),
			// but not both — otherwise a stale file could silently override the flag.
			// Check whether each flag was provided (not its value) so an explicit
			// empty --state alongside --state-file is still rejected as ambiguous.
			inline := c.String("state")
			file := c.String("state-file")
			if c.IsSet("state") && c.IsSet("state-file") {
				printError(errors.New("pass only one of --state or --state-file"), output, "Ambiguous state")
				return cli.Exit("", 1)
			}
			raw := inline
			if file != "" {
				data, err := os.ReadFile(file)
				if err != nil {
					printError(fmt.Errorf("failed to read --state-file: %w", err), output, "Invalid state file")
					return cli.Exit("", 1)
				}
				raw = string(data)
			}

			// Validate whenever a state flag was provided (even if empty), so an
			// explicit --state '' or empty --state-file is rejected rather than
			// silently creating an empty draft. Omitting both is title-only, which is fine.
			// Accept JSON or YAML — dashboards are YAML-native and JSON is valid YAML.
			var state map[string]any
			if c.IsSet("state") || c.IsSet("state-file") {
				parsed, err := parseJSONOrYAMLObject([]byte(raw))
				if err != nil {
					printError(fmt.Errorf("invalid dashboard definition (expected a JSON or YAML object): %w", err), output, "Invalid state")
					return cli.Exit("", 1)
				}
				state = parsed
				// A mapping is required; null/scalars/arrays are not a definition.
				if state == nil {
					printError(errors.New("dashboard definition must be a JSON or YAML object"), output, "Invalid state")
					return cli.Exit("", 1)
				}
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			dashboard, err := client.CreateDashboard(ctx, c.String("title"), visibility, c.Int("agent-id"), state)
			if err != nil {
				printError(err, output, "Failed to create dashboard")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(dashboard, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			title := ""
			if dashboard.Title != nil {
				title = *dashboard.Title
			}
			if dashboard.URL != "" {
				infoPrinter.Printf("Created dashboard %d (%s) as a draft — open it to review and publish: %s\n", dashboard.ID, title, dashboard.URL)
			} else {
				infoPrinter.Printf("Created dashboard %d (%s) as a draft — publish it from the Bruin Cloud UI.\n", dashboard.ID, title)
			}
			// No agent resolved (neither an explicit --agent-id nor the token's
			// agent), so the dashboard opens without a chat panel. Warn and point
			// at the fix instead of leaving it a silent surprise.
			if dashboard.AgentID == nil || *dashboard.AgentID <= 0 {
				errorPrinter.Println("Warning: no agent bound — this dashboard opens without a chat panel. Pass --agent-id <id> (see 'bruin cloud agents list').")
			}
			return nil
		},
	}
}

func cloudDashboardsUpdate() *cli.Command {
	return &cli.Command{
		Name:  "update",
		Usage: "Update a dashboard's title, visibility or definition (definition written to draft; publish stays in the UI)",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "dashboard-id",
				Usage:    "dashboard ID",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "title",
				Usage: "the new dashboard title",
			},
			&cli.StringFlag{
				Name:  "visibility",
				Usage: "team or private",
			},
			&cli.StringFlag{
				Name:  "state",
				Usage: "the dashboard definition as a JSON or YAML string",
			},
			&cli.StringFlag{
				Name:  "state-file",
				Usage: "path to a file containing the dashboard definition as JSON or YAML",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			// Build the patch from the flags actually set, so an explicit empty value
			// is sent rather than dropped.
			fields := map[string]any{}
			if c.IsSet("title") {
				fields["title"] = c.String("title")
			}
			if c.IsSet("visibility") {
				visibility := c.String("visibility")
				if visibility != "team" && visibility != "private" {
					printError(fmt.Errorf("visibility must be 'team' or 'private', got %q", visibility), output, "Invalid --visibility")
					return cli.Exit("", 1)
				}
				fields["visibility"] = visibility
			}

			// The definition can come inline (--state) or from a file (--state-file),
			// but not both — otherwise a stale file could silently override the flag.
			if c.IsSet("state") && c.IsSet("state-file") {
				printError(errors.New("pass only one of --state or --state-file"), output, "Ambiguous state")
				return cli.Exit("", 1)
			}
			if c.IsSet("state") || c.IsSet("state-file") {
				raw := c.String("state")
				if file := c.String("state-file"); file != "" {
					data, err := os.ReadFile(file)
					if err != nil {
						printError(fmt.Errorf("failed to read --state-file: %w", err), output, "Invalid state file")
						return cli.Exit("", 1)
					}
					raw = string(data)
				}
				// Accept JSON or YAML — dashboards are YAML-native and JSON is valid YAML.
				state, err := parseJSONOrYAMLObject([]byte(raw))
				if err != nil {
					printError(fmt.Errorf("invalid dashboard definition (expected a JSON or YAML object): %w", err), output, "Invalid state")
					return cli.Exit("", 1)
				}
				// A mapping is required; null/scalars/arrays are not a definition.
				if state == nil {
					printError(errors.New("dashboard definition must be a JSON or YAML object"), output, "Invalid state")
					return cli.Exit("", 1)
				}
				fields["state"] = state
			}

			if len(fields) == 0 {
				printError(errors.New("provide at least one of --title, --visibility, --state or --state-file"), output, "Nothing to update")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			dashboard, err := client.UpdateDashboard(ctx, c.Int("dashboard-id"), fields)
			if err != nil {
				printError(err, output, "Failed to update dashboard")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(dashboard, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			title := ""
			if dashboard.Title != nil {
				title = *dashboard.Title
			}
			if dashboard.URL != "" {
				infoPrinter.Printf("Updated dashboard %d (%s) — open it to review and publish: %s\n", dashboard.ID, title, dashboard.URL)
			} else {
				infoPrinter.Printf("Updated dashboard %d (%s).\n", dashboard.ID, title)
			}
			return nil
		},
	}
}

func cloudDashboardsDelete() *cli.Command {
	return &cli.Command{
		Name:  "delete",
		Usage: "Delete a dashboard so it stops appearing",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "dashboard-id",
				Usage:    "dashboard ID",
				Required: true,
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			if c.Int("dashboard-id") <= 0 {
				printError(fmt.Errorf("--dashboard-id must be a positive integer, got %d", c.Int("dashboard-id")), output, "Invalid --dashboard-id")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			if err := client.DeleteDashboard(ctx, c.Int("dashboard-id")); err != nil {
				printError(err, output, "Failed to delete dashboard")
				return cli.Exit("", 1)
			}

			if output == "json" {
				fmt.Println(`{"success": true}`)
				return nil
			}

			successPrinter.Printf("Deleted dashboard %d.\n", c.Int("dashboard-id"))
			return nil
		},
	}
}

func CloudScheduledAgents() *cli.Command {
	return &cli.Command{
		Name:  "scheduled-agents",
		Usage: "Manage Bruin Cloud scheduled agents",
		Commands: []*cli.Command{
			cloudScheduledAgentsList(),
			cloudScheduledAgentsGet(),
			cloudScheduledAgentsCreate(),
			cloudScheduledAgentsUpdate(),
			cloudScheduledAgentsTrigger(),
			cloudScheduledAgentsDelete(),
			cloudScheduledAgentsRunStates(),
		},
	}
}

// cloudScheduledAgentsRunStates groups the CRUD commands for a scheduled agent's
// run-state ("memory") files — the markdown the agent persists across runs.
func cloudScheduledAgentsRunStates() *cli.Command {
	return &cli.Command{
		Name:  "run-states",
		Usage: "Manage a scheduled agent's run-state (\"memory\") files",
		Commands: []*cli.Command{
			cloudRunStatesList(),
			cloudRunStatesGet(),
			cloudRunStatesSet(),
			cloudRunStatesDelete(),
		},
	}
}

// scheduledAgentIDFlag is the required parent-scope flag shared by the run-state
// commands (a run-state file only exists in the context of a scheduled agent).
func scheduledAgentIDFlag() *cli.IntFlag {
	return &cli.IntFlag{
		Name:     "scheduled-agent-id",
		Usage:    "scheduled agent the run-state file belongs to",
		Required: true,
	}
}

func runStateNameFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:     "name",
		Usage:    "run-state file name",
		Required: true,
	}
}

func cloudRunStatesList() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List a scheduled agent's run-state files",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			scheduledAgentIDFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			states, err := client.ListRunStates(ctx, c.Int("scheduled-agent-id"))
			if err != nil {
				printError(err, output, "Failed to list run states")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(states, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"Name", "Size (bytes)", "Updated"})
			for _, s := range states {
				t.AppendRow(table.Row{s.Name, len(s.Content), derefString(s.UpdatedAt)})
			}
			t.Render()
			return nil
		},
	}
}

func cloudRunStatesGet() *cli.Command {
	return &cli.Command{
		Name:  "get",
		Usage: "Get a run-state file (prints its content; use --output json for the full record)",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			scheduledAgentIDFlag(),
			runStateNameFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			state, err := client.GetRunState(ctx, c.Int("scheduled-agent-id"), c.String("name"))
			if err != nil {
				printError(err, output, "Failed to get run state")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(state, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			// Print the raw content so it can be redirected to a file.
			fmt.Print(state.Content)
			return nil
		},
	}
}

func cloudRunStatesSet() *cli.Command {
	return &cli.Command{
		Name:  "set",
		Usage: "Create or replace a run-state file from --content or --content-file",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			scheduledAgentIDFlag(),
			runStateNameFlag(),
			&cli.StringFlag{Name: "content", Usage: "the file content"},
			&cli.StringFlag{Name: "content-file", Usage: "path to a file whose contents become the run-state content"},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			content, err := resolveRunStateContent(c)
			if err != nil {
				printError(err, output, "Invalid content")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			state, err := client.SetRunState(ctx, c.Int("scheduled-agent-id"), c.String("name"), content)
			if err != nil {
				printError(err, output, "Failed to set run state")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(state, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			successPrinter.Printf("Saved run-state file '%s' on scheduled agent %d.\n", state.Name, c.Int("scheduled-agent-id"))
			return nil
		},
	}
}

func cloudRunStatesDelete() *cli.Command {
	return &cli.Command{
		Name:  "delete",
		Usage: "Delete a run-state file",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			scheduledAgentIDFlag(),
			runStateNameFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			if err := client.DeleteRunState(ctx, c.Int("scheduled-agent-id"), c.String("name")); err != nil {
				printError(err, output, "Failed to delete run state")
				return cli.Exit("", 1)
			}

			if output == "json" {
				fmt.Println(`{"success": true}`)
				return nil
			}

			successPrinter.Printf("Deleted run-state file '%s' from scheduled agent %d.\n", c.String("name"), c.Int("scheduled-agent-id"))
			return nil
		},
	}
}

// resolveRunStateContent reads the run-state content from exactly one of
// --content or --content-file.
func resolveRunStateContent(c *cli.Command) (string, error) {
	if c.IsSet("content") && c.IsSet("content-file") {
		return "", errors.New("pass only one of --content or --content-file")
	}
	if c.IsSet("content-file") {
		data, err := os.ReadFile(c.String("content-file"))
		if err != nil {
			return "", fmt.Errorf("failed to read --content-file: %w", err)
		}
		return string(data), nil
	}
	if c.IsSet("content") {
		return c.String("content"), nil
	}
	return "", errors.New("provide the content with --content or --content-file")
}

func cloudScheduledAgentsList() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List scheduled agents",
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

			runs, err := client.ListScheduledAgents(ctx)
			if err != nil {
				printError(err, output, "Failed to list scheduled agents")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(runs, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"ID", "Title", "Active", "Cron", "Next Run"})
			for _, r := range runs {
				t.AppendRow(table.Row{r.ID, derefString(r.Title), r.IsActive, derefString(r.ScheduleCron), derefString(r.NextRunAt)})
			}
			t.Render()
			return nil
		},
	}
}

func cloudScheduledAgentsGet() *cli.Command {
	return &cli.Command{
		Name:  "get",
		Usage: "Get a scheduled agent including its plan",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "scheduled-agent-id",
				Usage:    "scheduled agent ID",
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

			run, err := client.GetScheduledAgent(ctx, c.Int("scheduled-agent-id"))
			if err != nil {
				printError(err, output, "Failed to get scheduled agent")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(run, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			printScheduledAgent(run)
			return nil
		},
	}
}

func cloudScheduledAgentsCreate() *cli.Command {
	return &cli.Command{
		Name:  "create",
		Usage: "Create a scheduled agent from a plan (active when it includes a schedule; a plan with no cron stays a draft)",
		Flags: append(
			scheduledAgentPlanFlags(),
			&cli.IntFlag{
				Name:     "agent-id",
				Usage:    "the agent that runs the scheduled task",
				Required: true,
			},
		),
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			fields, err := buildScheduledAgentFields(c)
			if err != nil {
				printError(err, output, "Invalid plan")
				return cli.Exit("", 1)
			}
			fields["agent_id"] = c.Int("agent-id")

			if id, ok := messagePairIDFromEnv(); ok {
				fields["message_pair_id"] = id
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			run, err := client.CreateScheduledAgent(ctx, fields)
			if err != nil {
				printError(err, output, "Failed to create scheduled agent")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(run, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			switch {
			case run.IsActive:
				infoPrinter.Printf("Created scheduled agent %d (%s) — it's active and the next run is scheduled. Manage it in the Bruin Cloud UI.\n", run.ID, derefString(run.Title))
			case run.ScheduleCron != nil && *run.ScheduleCron != "":
				infoPrinter.Printf("Created scheduled agent %d (%s) as a draft — activate it from the Bruin Cloud UI.\n", run.ID, derefString(run.Title))
			default:
				infoPrinter.Printf("Created scheduled agent %d (%s) as a draft — add a schedule, then activate it from the Bruin Cloud UI.\n", run.ID, derefString(run.Title))
			}
			return nil
		},
	}
}

func cloudScheduledAgentsUpdate() *cli.Command {
	return &cli.Command{
		Name:  "update",
		Usage: "Update a scheduled agent's title, plan, or active state (--active true|false)",
		Flags: append(
			scheduledAgentPlanFlags(),
			&cli.IntFlag{
				Name:     "scheduled-agent-id",
				Usage:    "scheduled agent ID",
				Required: true,
			},
			&cli.BoolFlag{
				Name:  "active",
				Usage: "activate (true) or pause (false) the scheduled agent; activating needs a schedule",
			},
		),
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			fields, err := buildScheduledAgentFields(c)
			if err != nil {
				printError(err, output, "Invalid plan")
				return cli.Exit("", 1)
			}
			if c.IsSet("active") {
				fields["is_active"] = c.Bool("active")
			}
			if len(fields) == 0 {
				printError(errors.New("provide at least one field to update (e.g. --title, --cron, --instructions, --active or --state-file)"), output, "Nothing to update")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			run, err := client.UpdateScheduledAgent(ctx, c.Int("scheduled-agent-id"), fields)
			if err != nil {
				printError(err, output, "Failed to update scheduled agent")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(run, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			infoPrinter.Printf("Updated scheduled agent %d (%s).\n", run.ID, derefString(run.Title))
			return nil
		},
	}
}

func cloudScheduledAgentsTrigger() *cli.Command {
	return &cli.Command{
		Name:  "trigger",
		Usage: "Run a scheduled agent now, off its schedule (the schedule is untouched)",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "scheduled-agent-id",
				Usage:    "scheduled agent ID",
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

			execution, err := client.TriggerScheduledAgent(ctx, c.Int("scheduled-agent-id"))
			if err != nil {
				printError(err, output, "Failed to trigger scheduled agent")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(execution, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			successPrinter.Printf("Triggered scheduled agent %d — execution %d is running in thread %d.\n", c.Int("scheduled-agent-id"), execution.ExecutionID, execution.ThreadID)
			return nil
		},
	}
}

func cloudScheduledAgentsDelete() *cli.Command {
	return &cli.Command{
		Name:  "delete",
		Usage: "Delete a scheduled agent so it stops firing",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.IntFlag{
				Name:     "scheduled-agent-id",
				Usage:    "scheduled agent ID",
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

			if err := client.DeleteScheduledAgent(ctx, c.Int("scheduled-agent-id")); err != nil {
				printError(err, output, "Failed to delete scheduled agent")
				return cli.Exit("", 1)
			}

			if output == "json" {
				fmt.Println(`{"success": true}`)
				return nil
			}

			successPrinter.Printf("Deleted scheduled agent %d.\n", c.Int("scheduled-agent-id"))
			return nil
		},
	}
}

// scheduledAgentPlanFlags are the plan flags shared by create and update.
func scheduledAgentPlanFlags() []cli.Flag {
	return []cli.Flag{
		apiKeyFlag(),
		outputFlag(),
		&cli.StringFlag{Name: "title", Usage: "the run title"},
		&cli.StringFlag{Name: "cron", Usage: "cron schedule, e.g. '0 9 * * *'"},
		&cli.StringFlag{Name: "timezone", Usage: "schedule timezone, e.g. 'UTC'"},
		&cli.StringFlag{Name: "instructions", Usage: "what the run should do"},
		&cli.StringFlag{Name: "output-formatting", Usage: "how the result should be formatted"},
		&cli.StringFlag{Name: "connection", Usage: "the data connection to query"},
		&cli.StringFlag{Name: "state", Usage: "the full plan as a JSON or YAML object string"},
		&cli.StringFlag{Name: "state-file", Usage: "path to a file with the full plan as JSON or YAML"},
	}
}

// messagePairIDFromEnv reads BRUIN_MESSAGE_PAIR_ID; false when unset, non-numeric or non-positive.
func messagePairIDFromEnv() (int, bool) {
	v := strings.TrimSpace(os.Getenv("BRUIN_MESSAGE_PAIR_ID"))
	if v == "" {
		return 0, false
	}
	id, err := strconv.Atoi(v)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

// buildScheduledAgentFields assembles the request body from an optional full plan
// (--state / --state-file) overlaid with the convenience flags. Shared by create
// and update so the two build the body identically.
func buildScheduledAgentFields(c *cli.Command) (map[string]any, error) {
	fields := map[string]any{}

	// The plan can come inline (--state) or from a file (--state-file), not both.
	if c.IsSet("state") && c.IsSet("state-file") {
		return nil, errors.New("pass only one of --state or --state-file")
	}
	if c.IsSet("state") || c.IsSet("state-file") {
		raw := c.String("state")
		if file := c.String("state-file"); file != "" {
			data, err := os.ReadFile(file)
			if err != nil {
				return nil, fmt.Errorf("failed to read --state-file: %w", err)
			}
			raw = string(data)
		}
		parsed, err := parseJSONOrYAMLObject([]byte(raw))
		if err != nil {
			return nil, fmt.Errorf("invalid plan (expected a JSON or YAML object): %w", err)
		}
		if parsed == nil {
			return nil, errors.New("plan must be a JSON or YAML object")
		}
		fields = parsed
	}

	if c.IsSet("title") {
		fields["title"] = c.String("title")
	}
	if c.IsSet("instructions") {
		fields["instructions"] = c.String("instructions")
	}
	if c.IsSet("output-formatting") {
		fields["output_formatting"] = c.String("output-formatting")
	}
	if c.IsSet("connection") {
		fields["connection"] = c.String("connection")
	}
	// --cron / --timezone are shorthand for the nested schedule object.
	if c.IsSet("cron") || c.IsSet("timezone") {
		schedule, _ := fields["schedule"].(map[string]any)
		if schedule == nil {
			schedule = map[string]any{}
		}
		if c.IsSet("cron") {
			schedule["cron"] = c.String("cron")
		}
		if c.IsSet("timezone") {
			schedule["timezone"] = c.String("timezone")
		}
		fields["schedule"] = schedule
	}

	return fields, nil
}

func printScheduledAgent(run *bruincloud.ScheduledAgent) {
	infoPrinter.Printf("Scheduled agent %d: %s\n", run.ID, derefString(run.Title))
	fmt.Printf("  Active:    %v\n", run.IsActive)
	fmt.Printf("  Cron:      %s\n", derefString(run.ScheduleCron))
	fmt.Printf("  Timezone:  %s\n", derefString(run.ScheduleTimezone))
	fmt.Printf("  Next run:  %s\n", derefString(run.NextRunAt))
	fmt.Printf("  Last run:  %s\n", derefString(run.LastRunAt))
	if run.Instructions != nil && *run.Instructions != "" {
		fmt.Printf("  Instructions:\n%s\n", *run.Instructions)
	}
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func CloudAuditLogs() *cli.Command {
	return &cli.Command{
		Name:  "audit-logs",
		Usage: "Read the Bruin Cloud team audit log",
		Commands: []*cli.Command{
			cloudAuditLogsList(),
		},
	}
}

func cloudAuditLogsList() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List audit log entries (requires a personal API token)",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.StringSliceFlag{
				Name:  "type",
				Usage: "filter by event type (repeatable), e.g. --type login --type new_conn",
			},
			&cli.StringSliceFlag{
				Name:  "user-id",
				Usage: "filter by acting user ID (repeatable)",
			},
			&cli.StringFlag{
				Name:  "start-date",
				Usage: "only entries at or after this time (ISO 8601)",
			},
			&cli.StringFlag{
				Name:  "end-date",
				Usage: "only entries at or before this time (ISO 8601)",
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

			logs, err := client.ListAuditLogs(ctx, bruincloud.AuditLogListOptions{
				Types:     c.StringSlice("type"),
				UserIDs:   c.StringSlice("user-id"),
				StartDate: c.String("start-date"),
				EndDate:   c.String("end-date"),
				Limit:     c.Int("limit"),
				Offset:    c.Int("offset"),
			})
			if err != nil {
				printError(err, output, "Failed to list audit logs")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(logs, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"Time", "Type", "User", "Source", "IP"})
			for _, l := range logs {
				t.AppendRow(table.Row{l.CreatedAt, l.Type, l.UserIdentifier, derefString(l.Source), derefString(l.IPAddress)})
			}
			t.Render()
			return nil
		},
	}
}

func CloudCost() *cli.Command {
	return &cli.Command{
		Name:  "cost",
		Usage: "Explore Bruin Cloud warehouse costs",
		Commands: []*cli.Command{
			cloudCostSchema(),
			cloudCostExplorer(),
		},
	}
}

func cloudCostSchema() *cli.Command {
	return &cli.Command{
		Name:  "schema",
		Usage: "List the dimensions, filters, and time buckets the cost explorer supports",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			&cli.StringFlag{
				Name:  "platform",
				Usage: "warehouse platform: bigquery (default) or databricks",
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

			schema, err := client.GetCostExplorerSchema(ctx, c.String("platform"))
			if err != nil {
				printError(err, output, "Failed to get cost explorer schema")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(schema, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			fmt.Printf("Platform: %s\n", schema.Platform)
			fmt.Printf("Available platforms: %s\n", strings.Join(schema.AvailablePlatforms, ", "))
			fmt.Printf("Time buckets: %s\n\n", strings.Join(schema.TimeDimensions, ", "))

			dt := table.NewWriter()
			dt.SetOutputMirror(os.Stdout)
			dt.SetTitle("Dimensions")
			dt.AppendHeader(table.Row{"Key", "Label"})
			for _, d := range schema.Dimensions {
				dt.AppendRow(table.Row{d.Key, d.Label})
			}
			dt.Render()

			ft := table.NewWriter()
			ft.SetOutputMirror(os.Stdout)
			ft.SetTitle("Filters")
			ft.AppendHeader(table.Row{"Field", "Op", "Multiple"})
			for _, f := range schema.Filters {
				ft.AppendRow(table.Row{f.Field, f.Op, f.Multiple})
			}
			ft.Render()
			return nil
		},
	}
}

func cloudCostExplorer() *cli.Command {
	return &cli.Command{
		Name:  "explorer",
		Usage: "Show warehouse cost breakdowns over a date range",
		Flags: []cli.Flag{
			apiKeyFlag(),
			outputFlag(),
			limitFlag(),
			offsetFlag(),
			&cli.StringFlag{
				Name:     "start-date",
				Usage:    "start of the range, inclusive (e.g. 2026-07-01)",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "end-date",
				Usage:    "end of the range, inclusive (e.g. 2026-07-31)",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "platform",
				Usage: "warehouse platform: bigquery (default) or databricks",
			},
			&cli.StringFlag{
				Name:  "dimension",
				Usage: "group costs by this dimension (see 'bruin cloud cost schema')",
			},
			&cli.StringFlag{
				Name:  "time-dimension",
				Usage: "bucket costs over time: day, week, or month",
			},
			&cli.StringSliceFlag{
				Name:  "filter",
				Usage: "filter as field:op:value; repeat --filter per value for op 'in' (e.g. --filter pipeline_id:in:a --filter pipeline_id:in:b)",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")

			filters, err := parseCostFilters(c.StringSlice("filter"))
			if err != nil {
				printError(err, output, "Invalid --filter")
				return cli.Exit("", 1)
			}

			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}

			resp, err := client.GetCostExplorer(ctx, bruincloud.CostExplorerRequest{
				StartDate:     c.String("start-date"),
				EndDate:       c.String("end-date"),
				Platform:      c.String("platform"),
				Dimension:     c.String("dimension"),
				TimeDimension: c.String("time-dimension"),
				Filters:       filters,
				Limit:         c.Int("limit"),
				Offset:        c.Int("offset"),
			})
			if err != nil {
				printError(err, output, "Failed to get cost explorer data")
				return cli.Exit("", 1)
			}

			if output == "json" {
				data, _ := json.MarshalIndent(resp, "", "  ")
				fmt.Println(string(data))
				return nil
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			header := table.Row{}
			if resp.TimeDimension != nil {
				header = append(header, "Period")
			}
			if resp.Dimension != nil {
				header = append(header, *resp.Dimension)
			}
			header = append(header, "Queries", "Cost (USD)", "TB Billed")
			t.AppendHeader(header)
			for _, row := range resp.Rows {
				r := table.Row{}
				if resp.TimeDimension != nil {
					r = append(r, formatCostCell(row["time_period"]))
				}
				if resp.Dimension != nil {
					r = append(r, formatCostCell(row[*resp.Dimension]))
				}
				r = append(r, formatCostCell(row["query_count"]), formatCostCell(row["total_cost_usd"]), formatCostCell(row["total_terabytes_billed"]))
				t.AppendRow(r)
			}
			t.Render()

			if resp.NextOffset != nil {
				fmt.Printf("\nShowing rows %d-%d of %d. Next page: --offset=%d\n", resp.Offset, resp.Offset+resp.ReturnedRows-1, resp.TotalRows, *resp.NextOffset)
			}
			return nil
		},
	}
}

func parseCostFilters(raw []string) ([]bruincloud.CostFilter, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	// urfave's StringSliceFlag splits values on commas, so multiple `in` values are passed as
	// repeated --filter flags (e.g. --filter pipeline_id:in:a --filter pipeline_id:in:b) and
	// merged here by field. Every token must be a complete field:op:value.
	filters := make([]bruincloud.CostFilter, 0, len(raw))
	inIndex := make(map[string]int)
	for _, entry := range raw {
		parts := strings.SplitN(entry, ":", 3)
		if len(parts) != 3 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("filter %q must be field:op:value", entry)
		}
		field, op, value := parts[0], parts[1], parts[2]
		if op == "in" {
			if idx, ok := inIndex[field]; ok {
				vals := filters[idx].Value.([]string)
				vals = append(vals, value)
				filters[idx].Value = vals
				continue
			}
			inIndex[field] = len(filters)
			filters = append(filters, bruincloud.CostFilter{Field: field, Op: op, Value: []string{value}})
			continue
		}
		filters = append(filters, bruincloud.CostFilter{Field: field, Op: op, Value: value})
	}
	return filters, nil
}

func formatCostCell(v any) string {
	switch val := v.(type) {
	case nil:
		return ""
	case string:
		return val
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(val)
	default:
		data, err := json.Marshal(val)
		if err != nil {
			return fmt.Sprintf("%v", val)
		}
		return string(data)
	}
}
