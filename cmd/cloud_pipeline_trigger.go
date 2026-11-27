package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/bruin-data/bruin/pkg/bruincloud"
	"github.com/urfave/cli/v3"
)

func cloudScheduledAgentsPipelineTrigger() *cli.Command {
	return &cli.Command{
		Name:  "pipeline-trigger",
		Usage: "Manage the pipeline trigger of a scheduled agent (runs it after a pipeline run succeeds or fails)",
		Commands: []*cli.Command{
			cloudPipelineTriggerCommand("get", "Get the scheduled agent's pipeline trigger"),
			cloudPipelineTriggerCommand("set", "Set the pipeline trigger without changing the plan or activating the agent"),
			cloudPipelineTriggerCommand("delete", "Remove the pipeline trigger; pause first if it is the active agent's only trigger"),
		},
	}
}

func cloudPipelineTriggerCommand(name, usage string) *cli.Command {
	flags := []cli.Flag{
		apiKeyFlag(),
		outputFlag(),
		&cli.IntFlag{Name: "scheduled-agent-id", Usage: "scheduled agent ID", Required: true},
	}
	if name == "set" {
		flags = append(
			flags,
			&cli.StringFlag{Name: "project-id", Usage: "project containing the pipelines"},
			&cli.StringSliceFlag{Name: "pipeline", Usage: "pipeline name; repeat for multiple"},
			&cli.StringFlag{Name: "subscriptions-file", Usage: "JSON file with custom notification subscriptions (event types and selector) instead of --pipeline"},
		)
	}
	return &cli.Command{
		Name:  name,
		Usage: usage,
		Flags: flags,
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()
			output := c.String("output")
			id := c.Int("scheduled-agent-id")
			if id <= 0 {
				printError(errors.New("scheduled-agent-id must be a positive integer"), output, "Invalid scheduled agent")
				return cli.Exit("", 1)
			}
			var selection bruincloud.ScheduledAgentPipelineTrigger
			if name == "set" {
				var err error
				selection, err = pipelineTriggerSelection(c)
				if err != nil {
					printError(err, output, "Invalid pipeline trigger")
					return cli.Exit("", 1)
				}
			}
			client, err := newCloudClient(c)
			if err != nil {
				printError(err, output, "Failed to create API client")
				return cli.Exit("", 1)
			}
			var result *bruincloud.ScheduledAgentPipelineTriggerResponse
			switch name {
			case "get":
				result, err = client.GetScheduledAgentPipelineTrigger(ctx, id)
			case "set":
				result, err = client.SetScheduledAgentPipelineTrigger(ctx, id, selection)
			case "delete":
				result, err = client.DeleteScheduledAgentPipelineTrigger(ctx, id)
			}
			if err != nil {
				printError(err, output, "Failed to "+name+" pipeline trigger")
				return cli.Exit("", 1)
			}
			if output == "json" {
				data, _ := json.MarshalIndent(result, "", "  ")
				fmt.Println(string(data))
				return nil
			}
			switch {
			case name == "delete":
				successPrinter.Printf("Removed pipeline trigger from scheduled agent %d.\n", id)
			case result.PipelineTrigger == nil:
				infoPrinter.Printf("Scheduled agent %d has no pipeline trigger.\n", id)
			default:
				infoPrinter.Printf("Scheduled agent %d pipeline trigger: %s\n", id, describePipelineTrigger(result.PipelineTrigger))
			}
			return nil
		},
	}
}

func pipelineTriggerSelection(c *cli.Command) (bruincloud.ScheduledAgentPipelineTrigger, error) {
	projectID := strings.TrimSpace(c.String("project-id"))
	var pipelines []string
	for _, pipeline := range c.StringSlice("pipeline") {
		pipelines = append(pipelines, strings.TrimSpace(pipeline))
	}
	if path := c.String("subscriptions-file"); path != "" {
		if projectID != "" || len(pipelines) > 0 {
			return bruincloud.ScheduledAgentPipelineTrigger{}, errors.New("use either --subscriptions-file or --project-id with --pipeline, not both")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return bruincloud.ScheduledAgentPipelineTrigger{}, fmt.Errorf("failed to read subscriptions file: %w", err)
		}
		var subscriptions []json.RawMessage
		if err := json.Unmarshal(data, &subscriptions); err != nil || len(subscriptions) == 0 {
			return bruincloud.ScheduledAgentPipelineTrigger{}, errors.New("subscriptions file must contain a non-empty JSON array")
		}
		return bruincloud.ScheduledAgentPipelineTrigger{Subscriptions: data}, nil
	}
	if projectID == "" || len(pipelines) == 0 {
		return bruincloud.ScheduledAgentPipelineTrigger{}, errors.New("pass --project-id with at least one --pipeline, or --subscriptions-file")
	}
	for _, pipeline := range pipelines {
		if pipeline == "" {
			return bruincloud.ScheduledAgentPipelineTrigger{}, errors.New("pipeline must not be empty")
		}
	}
	if len(pipelines) == 1 {
		return bruincloud.ScheduledAgentPipelineTrigger{ID: pipelines[0], ProjectID: projectID}, nil
	}
	selection := bruincloud.ScheduledAgentPipelineTrigger{}
	for _, pipeline := range pipelines {
		selection.Pipelines = append(selection.Pipelines, bruincloud.ScheduledAgentTriggerPipeline{ID: pipeline, ProjectID: projectID})
	}
	return selection, nil
}

func describePipelineTrigger(trigger *bruincloud.ScheduledAgentPipelineTrigger) string {
	if len(trigger.Subscriptions) > 0 {
		return "custom conditions (see --output json)"
	}
	if len(trigger.Pipelines) > 0 {
		names := make([]string, 0, len(trigger.Pipelines))
		for _, pipeline := range trigger.Pipelines {
			names = append(names, pipeline.ProjectID+"/"+pipeline.ID)
		}
		return strings.Join(names, ", ")
	}
	return trigger.ProjectID + "/" + trigger.ID
}
