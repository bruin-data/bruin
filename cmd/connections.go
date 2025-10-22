package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	path2 "path"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/jedib0t/go-pretty/v6/table"
	errors2 "github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

func Connections() *cli.Command {
	return &cli.Command{
		Name:   "connections",
		Hidden: true,
		Commands: []*cli.Command{
			ListConnections(),
			AddConnection(),
			DeleteConnection(),
			PingConnection(),
		},
	}
}

func ListConnections() *cli.Command {
	return &cli.Command{
		Name:      "list",
		Usage:     "list connections defined in a Bruin project",
		ArgsUsage: "[path to project root, defaults to current folder]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "the output type, possible values are: plain, json",
			},
			&cli.StringFlag{
				Name:    "environment",
				Aliases: []string{"env"},
				Usage:   "",
			},
			&cli.StringFlag{
				Name:    "config-file",
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			r := ConnectionsCommand{}

			path := "."
			if c.Args().Present() {
				path = c.Args().First()
			}

			configFilePath := c.String("config-file")
			if configFilePath == "" {
				repoRoot, err := git.FindRepoFromPath(path)
				if err != nil {
					printErrorForOutput(c.String("output"), errors2.Wrap(err, "failed to find the git repository root"))
					return cli.Exit("", 1)
				}

				configFilePath = path2.Join(repoRoot.Path, ".bruin.yml")
			}

			return r.ListConnections(path, c.String("output"), c.String("environment"), configFilePath)
		},
	}
}

func AddConnection() *cli.Command {
	return &cli.Command{
		Name:  "add",
		Usage: "add a new connection to a Bruin project",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "environment",
				Aliases:  []string{"e", "env"},
				Usage:    "the name of the environment to add the connection to",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "name",
				Usage:    "the name of the connection",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "type",
				Usage:    "the type of the connection",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "credentials",
				Usage:    "the JSON object containing the credentials",
				Required: true,
			},
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "the output type, possible values are: plain, json",
			},
			&cli.StringFlag{
				Name:    "config-file",
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			path := "."
			if c.Args().Present() {
				path = c.Args().First()
			}

			environment := c.String("environment")
			name := c.String("name")
			connType := c.String("type")
			credentials := c.String("credentials")
			output := c.String("output")

			defer RecoverFromPanic()

			configFilePath := c.String("config-file")
			if configFilePath == "" {
				repoRoot, err := git.FindRepoFromPath(path)
				if err != nil {
					printErrorForOutput(output, errors2.Wrap(err, "failed to find the git repository root"))
					return cli.Exit("", 1)
				}

				configFilePath = path2.Join(repoRoot.Path, ".bruin.yml")
			}

			cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
			if err != nil {
				printErrorForOutput(output, errors2.Wrap(err, "failed to load or create config"))
				return cli.Exit("", 1)
			}

			// Check if the environment exists
			if _, exists := cm.Environments[environment]; !exists {
				printErrorForOutput(output, fmt.Errorf("environment '%s' does not exist", environment))
				return cli.Exit("", 1)
			}

			// Check if a connection with the same name already exists in the environment
			if cm.Environments[environment].Connections.Exists(name) {
				printErrorForOutput(output, fmt.Errorf("a connection named '%s' already exists in the '%s' environment", name, environment))
				return cli.Exit("", 1)
			}

			var creds map[string]interface{}
			if err := json.Unmarshal([]byte(credentials), &creds); err != nil {
				printErrorForOutput(output, errors2.Wrap(err, "failed to parse credentials JSON"))
				return cli.Exit("", 1)
			}

			if err := cm.AddConnection(environment, name, connType, creds); err != nil {
				printErrorForOutput(output, errors2.Wrap(err, "failed to add connection"))
				return cli.Exit("", 1)
			}

			err = cm.Persist()
			if err != nil {
				printErrorForOutput(output, errors2.Wrap(err, "failed to persist config"))
				return cli.Exit("", 1)
			}

			if output == "json" {
				return nil
			}

			infoPrinter.Printf("Successfully added connection: %s\n", name)
			return nil
		},
	}
}

func DeleteConnection() *cli.Command {
	return &cli.Command{
		Name:  "delete",
		Usage: "Delete a connection from an environment",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "environment",
				Aliases:  []string{"e", "env"},
				Usage:    "the name of the environment to delete the connection from",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "name",
				Aliases:  []string{"n"},
				Usage:    "the name of the connection to delete",
				Required: true,
			},
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				DefaultText: "plain",
				Usage:       "the output type, possible values are: plain, json",
			},
			&cli.StringFlag{
				Name:    "config-file",
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			environment := c.String("environment")
			name := c.String("name")
			output := c.String("output")

			configFilePath := c.String("config-file")
			if configFilePath == "" {
				repoRoot, err := git.FindRepoFromPath(".")
				if err != nil {
					printErrorForOutput(output, errors2.Wrap(err, "failed to find the git repository root"))
					return cli.Exit("", 1)
				}

				configFilePath = path2.Join(repoRoot.Path, ".bruin.yml")
			}

			cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
			if err != nil {
				printErrorForOutput(output, errors2.Wrap(err, "failed to load or create config"))
				return cli.Exit("", 1)
			}

			err = cm.DeleteConnection(environment, name)
			if err != nil {
				printErrorForOutput(output, errors2.Wrap(err, "failed to delete connection"))
				return cli.Exit("", 1)
			}

			err = cm.Persist()
			if err != nil {
				printErrorForOutput(output, errors2.Wrap(err, "failed to persist config"))
				return cli.Exit("", 1)
			}

			if output == "json" {
				jsonOutput := map[string]string{
					"status":  "success",
					"message": fmt.Sprintf("Successfully deleted connection: %s from environment: %s", name, environment),
				}
				jsonBytes, err := json.Marshal(jsonOutput)
				if err != nil {
					printErrorForOutput(output, errors2.Wrap(err, "failed to marshal JSON"))
					return cli.Exit("", 1)
				}
				fmt.Println(string(jsonBytes))
				return nil
			}

			infoPrinter.Printf("Successfully deleted connection: %s from environment: %s\n", name, environment)
			return nil
		},
	}
}

type ConnectionsCommand struct{}

func (r *ConnectionsCommand) ListConnections(pathToProject, output, environment, configFilePath string) error {
	defer RecoverFromPanic()

	cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
	if err != nil {
		errorPrinter.Printf("Failed to load or create the config file: %v\n", err)
		return cli.Exit("", 1)
	}

	if output == "json" {
		if environment != "" {
			// Check if the specified environment exists
			env, exists := cm.Environments[environment]
			if !exists {
				errorPrinter.Printf("Environment '%s' not found.\n", environment)
				return cli.Exit("", 1)
			}

			// Construct the output structure to include the environment name
			envOutput := map[string]interface{}{
				environment: map[string]interface{}{
					"connections": env.Connections,
				},
			}

			// Marshal the structured output to JSON
			js, err := json.Marshal(envOutput)
			if err != nil {
				printErrorJSON(err)
				return cli.Exit("", 1)
			}

			fmt.Println(string(js))
			return nil
		}

		// Marshal the entire configuration if no specific environment is specified
		js, err := json.Marshal(cm)
		if err != nil {
			printErrorJSON(err)
			return cli.Exit("", 1)
		}

		fmt.Println(string(js))
		return nil
	}

	// Check if a specific environment is requested
	if environment != "" {
		env, exists := cm.Environments[environment]
		if !exists {
			errorPrinter.Printf("Environment '%s' not found.\n", environment)
			return cli.Exit("", 1)
		}

		fmt.Println()
		infoPrinter.Printf("Environment: %s\n", environment)

		t := table.NewWriter()
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"Type", "Name"})

		rows := env.Connections.ConnectionsSummaryList()

		for row, connType := range rows {
			t.AppendRow(table.Row{connType, row})
		}
		t.Render()
		fmt.Println()

		return nil
	}
	// If no specific environment is requested, iterate through all environments
	for envName, env := range cm.Environments {
		fmt.Println()
		infoPrinter.Printf("Environment: %s\n", envName)

		t := table.NewWriter()
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"Type", "Name"})

		rows := env.Connections.ConnectionsSummaryList()

		if len(rows) == 0 {
			t.Render()
			fmt.Println()
			continue
		}

		for row, connType := range rows {
			t.AppendRow(table.Row{connType, row})
		}
		t.Render()
		fmt.Println()
	}

	return err
}

func printErrorForOutput(output string, err error) {
	if output == "json" {
		printErrorJSON(err)
	} else {
		errorPrinter.Println(err.Error())
	}
}

func PingConnection() *cli.Command {
	return &cli.Command{
		Name:  "test",
		Usage: "Test the validity of a connection in an environment",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "environment",
				Aliases: []string{"e", "env"},
				Usage:   "the name of the environment (e.g., dev, prod). If not specified, the default environment will be used",
			},
			&cli.StringFlag{
				Name:   "type",
				Usage:  "deprecated: connection type is now automatically detected",
				Hidden: true,
			},
			&cli.StringFlag{
				Name:     "name",
				Aliases:  []string{"n"},
				Usage:    "the name of the connection to test",
				Required: true,
			},
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				DefaultText: "plain",
				Usage:       "the output type, possible values are: plain, json",
			},
			&cli.StringFlag{
				Name:    "config-file",
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			// Extract command-line arguments
			environment := c.String("environment")
			name := c.String("name")
			output := c.String("output")

			repoRoot, err := git.FindRepoFromPath(".")
			if err != nil {
				printErrorForOutput(output, errors2.Wrap(err, "failed to find the git repository root"))
				return cli.Exit("", 1)
			}

			configFilePath := c.String("config-file")
			if configFilePath == "" {
				configFilePath = path2.Join(repoRoot.Path, ".bruin.yml")
			}

			cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
			if err != nil {
				printErrorForOutput(output, errors2.Wrap(err, "failed to load or create config"))
				return cli.Exit("", 1)
			}
			if environment == "" {
				environment = cm.DefaultEnvironmentName
			}
			err = cm.SelectEnvironment(environment)
			if err != nil {
				printErrorForOutput(output, errors2.Wrap(err, "failed to select the environment"))
				return cli.Exit("", 1)
			}
			manager, errs := connection.NewManagerFromConfigWithContext(ctx, cm)
			if len(errs) > 0 {
				// Handle each error in the errs slice
				for _, err := range errs {
					printErrorForOutput(output, errors2.Wrap(err, "failed to create connection manager"))
				}
				return cli.Exit("", 1)
			}

			conn := manager.GetConnection(name)
			if conn == nil {
				printErrorForOutput(output, errors2.Errorf("failed to get connection '%s'", name))
				return cli.Exit("", 1)
			}

			if tester, ok := conn.(interface {
				Ping(ctx context.Context) error
			}); ok {
				testErr := tester.Ping(ctx)
				if testErr != nil {
					printErrorForOutput(output, errors2.Wrap(testErr, fmt.Sprintf("failed to test connection '%s'", name)))
					return cli.Exit("", 1)
				}
			} else {
				infoPrinter.Printf("Connection '%s' does not support testing yet.\n", name)
				return nil
			}

			if output == "json" {
				jsonOutput := map[string]string{
					"status": "success",
				}
				jsonBytes, err := json.Marshal(jsonOutput)
				if err != nil {
					printErrorForOutput(output, errors2.Wrap(err, "failed to marshal JSON"))
					return cli.Exit("", 1)
				}
				fmt.Println(string(jsonBytes))
			} else {
				infoPrinter.Printf("Successfully tested connection '%s' in environment: %s\n", name, environment)
			}

			return nil
		},
	}
}
