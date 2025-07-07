package cmd

import (
	"encoding/json"
	"fmt"
	path2 "path"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

func Environments(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:  "environments",
		Usage: "manage environments defined in .bruin.yml",
		Subcommands: []*cli.Command{
			ListEnvironments(isDebug),
			CreateEnvironment(isDebug),
		},
	}
}

func ListEnvironments(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "list environments found in the current repo",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "the output type, possible values are: plain, json",
			},
			&cli.StringFlag{
				Name:    "config-file",
				EnvVars: []string{"BRUIN_CONFIG_FILE"},
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(c *cli.Context) error {
			r := EnvironmentListCommand{}
			logger := makeLogger(*isDebug)

			configFilePath := c.String("config-file")
			if configFilePath == "" {
				repoRoot, err := git.FindRepoFromPath(path2.Clean("."))
				if err != nil {
					printError(errors.Wrap(err, "Failed to find the git repository root"), strings.ToLower(c.String("output")), "")
					return cli.Exit("", 1)
				}
				logger.Debugf("found repo root '%s'", repoRoot.Path)
				configFilePath = path2.Join(repoRoot.Path, ".bruin.yml")
			}

			return r.Run(strings.ToLower(c.String("output")), configFilePath)
		},
	}
}

type EnvironmentListCommand struct{}

func (r *EnvironmentListCommand) Run(output, configFilePath string) error {
	defer RecoverFromPanic()

	cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
	if err != nil {
		printError(err, output, "Failed to load the config file at "+configFilePath)
		return cli.Exit("", 1)
	}

	envs := cm.GetEnvironmentNames()
	if output == "json" {
		type environ struct {
			Name string `json:"name"`
		}

		type envResponse struct {
			SelectedEnvironment string    `json:"selected_environment"`
			Environments        []environ `json:"environments"`
		}
		resp := envResponse{
			SelectedEnvironment: cm.SelectedEnvironmentName,
			Environments:        make([]environ, len(envs)),
		}

		i := 0
		for _, env := range envs {
			resp.Environments[i] = environ{
				Name: env,
			}
			i++
		}

		js, err := json.Marshal(resp)
		if err != nil {
			printErrorJSON(err)
			return err
		}

		fmt.Println(string(js))
		return nil
	}

	fmt.Println()
	infoPrinter.Println("Selected environment: " + cm.SelectedEnvironmentName)
	infoPrinter.Println("Available environments:")
	for _, env := range envs {
		infoPrinter.Println("- " + env)
	}

	return nil
}

func CreateEnvironment(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:  "create",
		Usage: "create a new environment",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "name",
				Aliases:  []string{"n"},
				Usage:    "the name of the environment",
				Required: true,
			},
			&cli.StringFlag{
				Name:    "schema-prefix",
				Aliases: []string{"s"},
				Usage:   "schema prefix for schema-based environments",
			},
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "the output type, possible values are: plain, json",
			},
			&cli.StringFlag{
				Name:    "config-file",
				EnvVars: []string{"BRUIN_CONFIG_FILE"},
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(c *cli.Context) error {
			envName := c.String("name")
			schemaPrefix := c.String("schema-prefix")
			output := strings.ToLower(c.String("output"))

			configFilePath := c.String("config-file")
			if configFilePath == "" {
				repoRoot, err := git.FindRepoFromPath(path2.Clean("."))
				if err != nil {
					printError(errors.Wrap(err, "Failed to find the git repository root"), output, "")
					return cli.Exit("", 1)
				}
				configFilePath = path2.Join(repoRoot.Path, ".bruin.yml")
			}

			cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
			if err != nil {
				printError(err, output, "Failed to load the config file at "+configFilePath)
				return cli.Exit("", 1)
			}

			if _, exists := cm.Environments[envName]; exists {
				printError(fmt.Errorf("environment '%s' already exists", envName), output, "")
				return cli.Exit("", 1)
			}

			if err := cm.AddEnvironment(envName, schemaPrefix); err != nil {
				printError(err, output, "failed to add environment")
				return cli.Exit("", 1)
			}

			if err := cm.Persist(); err != nil {
				printError(err, output, "failed to persist config")
				return cli.Exit("", 1)
			}

			printSuccessForOutput(output, fmt.Sprintf("Successfully created environment: %s", envName))
			return nil
		},
	}
}
