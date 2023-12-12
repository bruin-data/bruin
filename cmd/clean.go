package cmd

import (
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/user"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

func CleanCmd() *cli.Command {
	return &cli.Command{
		Name:      "clean",
		Usage:     "clean the temporary artifacts such as logs",
		ArgsUsage: "[path to project root]",
		Action: func(c *cli.Context) error {
			inputPath := c.Args().Get(0)
			if inputPath == "" {
				inputPath = "."
			}

			r := CleanCommand{
				infoPrinter:  infoPrinter,
				errorPrinter: errorPrinter,
			}

			return r.Run(inputPath)
		},
	}
}

type CleanCommand struct {
	infoPrinter  printer
	errorPrinter printer
}

func (r *CleanCommand) Run(inputPath string) error {
	cm := user.NewConfigManager(afero.NewOsFs())
	err := cm.RecreateHomeDir()
	if err != nil {
		return errors.Wrap(err, "failed to recreate the home directory")
	}

	repoRoot, err := git.FindRepoFromPath(inputPath)
	if err != nil {
		errorPrinter.Printf("Failed to find the git repository root: %v\n", err)
		return cli.Exit("", 1)
	}

	logsFolder := path.Join(repoRoot.Path, LogsFolder)

	contents, err := filepath.Glob(fmt.Sprintf("%s/*.log", logsFolder))
	if err != nil {
		return errors.Wrap(err, "failed to find the logs folder")
	}

	if len(contents) == 0 {
		infoPrinter.Println("No log files found, nothing to clean up...")
		return nil
	}

	infoPrinter.Printf("Found %d log files, cleaning them up...\n", len(contents))

	for _, f := range contents {
		err := os.Remove(f)
		if err != nil {
			return errors.Wrapf(err, "failed to remove file: %s", f)
		}
	}

	infoPrinter.Printf("Successfully removed %d log files.\n", len(contents))

	return nil
}
