package cmd

import (
	"encoding/json"
	"fmt"
	path2 "path"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

func Environments(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:   "environments",
		Hidden: true,
		Subcommands: []*cli.Command{
			ListEnvironments(isDebug),
		},
	}
}

func ListEnvironments(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "list environments for the current path",
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
		errorPrinter.Printf("Failed to find the git repository root: %v\n", err)
		return cli.Exit("", 1)
	}
	logger.Debugf("found repo root '%s'", repoRoot.Path)

	configFilePath := path2.Join(repoRoot.Path, ".bruin.yml")
	cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
	if err != nil {
		errorPrinter.Printf("Failed to load the config file at '%s': %v\n", configFilePath, err)
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
