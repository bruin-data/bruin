package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	path2 "path"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

func Connections() *cli.Command {
	return &cli.Command{
		Name:   "connections",
		Hidden: true,
		Subcommands: []*cli.Command{
			ListConnections(),
		},
	}
}

func ListConnections() *cli.Command {
	return &cli.Command{
		Name:      "list",
		Usage:     "list connections defined in a Bruin project",
		Args:      true,
		ArgsUsage: "[path to project root, defaults to current folder]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "the output type, possible values are: plain, json",
			},
		},
		Action: func(c *cli.Context) error {
			r := ConnectionsCommand{}

			path := "."
			if c.Args().Present() {
				path = c.Args().First()
			}

			return r.ListConnections(path, c.String("output"))
		},
	}
}

type ConnectionsCommand struct{}

func (r *ConnectionsCommand) ListConnections(pathToProject, output string) error {
	defer RecoverFromPanic()

	repoRoot, err := git.FindRepoFromPath(pathToProject)
	if err != nil {
		errorPrinter.Printf("Failed to find the git repository root: %v\n", err)
		return cli.Exit("", 1)
	}

	configFilePath := path2.Join(repoRoot.Path, ".bruin.yml")
	cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)

	if output == "json" {
		js, err := json.Marshal(cm)
		if err != nil {
			printErrorJSON(err)
			return cli.Exit("", 1)
		}

		fmt.Println(string(js))
		return nil
	}

	for envName, env := range cm.Environments {
		fmt.Println()
		infoPrinter.Printf("Environment: %s\n", envName)

		t := table.NewWriter()
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"Type", "Name"})

		rows := env.Connections.ConnectionsSummaryList()

		for row, connType := range rows {
			t.AppendRow(table.Row{connType, row})
		}
		t.Render()
		fmt.Println()
	}

	return err
}
