package cmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/bruin-data/bruin/pkg/user"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

func CleanCmd() *cli.Command {
	return &cli.Command{
		Name:      "clean",
		Usage:     "clean the temporary artifacts such as logs and uv caches",
		ArgsUsage: "[path to project root]",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "uv-cache",
				Aliases: []string{"uv"},
				Usage:   "clean uv caches",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			inputPath := c.Args().Get(0)
			if inputPath == "" {
				inputPath = "."
			}

			r := CleanCommand{
				infoPrinter:  infoPrinter,
				errorPrinter: errorPrinter,
			}

			return r.Run(inputPath, c.Bool("uv-cache"))
		},
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
	}
}

type CleanCommand struct {
	infoPrinter  printer
	errorPrinter printer
}

func (r *CleanCommand) Run(inputPath string, cleanUvCache bool) error {
	cm := user.NewConfigManager(afero.NewOsFs())
	bruinHomeDirAbsPath, err := cm.EnsureAndGetBruinHomeDir()
	if err != nil {
		return errors.Wrap(err, "failed to get bruin home directory")
	}

	// Clean uv caches if requested
	if cleanUvCache {
		if err := r.cleanUvCache(bruinHomeDirAbsPath); err != nil {
			return err
		}
	}

	err = cm.RecreateHomeDir()
	if err != nil {
		return errors.Wrap(err, "failed to recreate the home directory")
	}

	repoRoot, err := git.FindRepoFromPath(inputPath)
	if err != nil {
		errorPrinter.Printf("Failed to find the git repository root: %v\n", err)
		return cli.Exit("", 1)
	}

	logsFolder := path.Join(repoRoot.Path, LogsFolder)

	contents, err := filepath.Glob(logsFolder + "/*.log")
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

func (r *CleanCommand) cleanUvCache(bruinHomeDirAbsPath string) error {
	var binaryName string
	if runtime.GOOS == "windows" {
		binaryName = "uv.exe"
	} else {
		binaryName = "uv"
	}
	uvBinaryPath := filepath.Join(bruinHomeDirAbsPath, binaryName)

	// Check if uv binary exists
	if _, err := os.Stat(uvBinaryPath); os.IsNotExist(err) {
		infoPrinter.Println("UV is not installed yet. Nothing to clean.")
		return nil
	}

	// Check if uv is available and working
	cmd := exec.Command(uvBinaryPath, "version")
	if err := cmd.Run(); err != nil {
		return errors.Wrap(err, "uv binary exists but is not working properly")
	}

	// Prompt user for confirmation
	if !r.confirmUvCacheClean() {
		infoPrinter.Println("UV cache cleaning cancelled by user.")
		return nil
	}

	infoPrinter.Println("Cleaning uv caches...")

	cleanCmd := exec.Command(uvBinaryPath, "cache", "clean")
	output, err := cleanCmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "failed to clean uv cache: %s", string(output))
	}

	infoPrinter.Println("Successfully cleaned uv caches.")
	return nil
}

func (r *CleanCommand) confirmUvCacheClean() bool {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Are you sure you want to clean uv cache? (y/N): ")

	response, err := reader.ReadString('\n')
	if err != nil {
		errorPrinter.Printf("Error reading input: %v\n", err)
		return false
	}

	response = strings.TrimSpace(strings.ToLower(response))
	return response == "y" || response == "yes"
}
