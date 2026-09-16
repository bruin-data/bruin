package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/bruincloud"
	"github.com/urfave/cli/v3"
)

func cloudScheduledAgentsPipelineTrigger() *cli.Command {
	return &cli.Command{
		Name:  "pipeline-trigger",
		Usage: "Manage the pipeline success trigger of a scheduled agent",
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
			&cli.StringFlag{Name: "project-id", Usage: "project containing the pipeline", Required: true},
			&cli.StringFlag{Name: "pipeline", Usage: "pipeline name", Required: true},
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
			selection := bruincloud.ScheduledAgentPipelineTrigger{
				ID:        strings.TrimSpace(c.String("pipeline")),
				ProjectID: strings.TrimSpace(c.String("project-id")),
			}
			if name == "set" && (selection.ID == "" || selection.ProjectID == "") {
				printError(errors.New("pipeline and project-id must not be empty"), output, "Invalid pipeline trigger")
				return cli.Exit("", 1)
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
			if name == "delete" {
				successPrinter.Printf("Removed pipeline trigger from scheduled agent %d.\n", id)
			} else if result.PipelineTrigger == nil {
				infoPrinter.Printf("Scheduled agent %d has no pipeline trigger.\n", id)
			} else {
				infoPrinter.Printf("Scheduled agent %d pipeline trigger: %s/%s\n", id, result.PipelineTrigger.ProjectID, result.PipelineTrigger.ID)
			}
			return nil
		},
	}
}
