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
		},
		Action: func(c *cli.Context) error {
			r := EnvironmentListCommand{}

			return r.Run(isDebug, strings.ToLower(c.String("output")))
		},
	}
}

type EnvironmentListCommand struct{}

func (r *EnvironmentListCommand) Run(isDebug *bool, output string) error {
	defer RecoverFromPanic()

	logger := makeLogger(*isDebug)

	repoRoot, err := git.FindRepoFromPath(path2.Clean("."))
	if err != nil {
		printError(errors.Wrap(err, "Failed to find the git repository root"), output, "")
		return cli.Exit("", 1)
	}
	logger.Debugf("found repo root '%s'", repoRoot.Path)

	configFilePath := path2.Join(repoRoot.Path, ".bruin.yml")
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
