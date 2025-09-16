package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/bruin-data/bruin/pkg/user"
	"github.com/fatih/color"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

type colorPrinter struct {
	c *color.Color
}

func (p *colorPrinter) Printf(format string, args ...interface{}) {
	p.c.Printf(format, args...) // ignore return values
}

func (p *colorPrinter) Println(args ...interface{}) {
	p.c.Println(args...)
}

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

			r := NewCleanCommand(
				user.NewConfigManager(afero.NewOsFs()),     // cm
				&git.RepoFinder{},                          // gitFinder
				afero.NewOsFs(),                            // fs
				&colorPrinter{c: color.New(color.FgGreen)}, // infoPrinter
				&colorPrinter{c: color.New(color.FgRed)},   // errorPrinter
			)

			err := r.Run(inputPath, c.Bool("uv-cache"))
			if err != nil {
				return cli.Exit("", 1)
			}
			return nil
		},
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
	}
}

type ConfigManager interface {
	EnsureAndGetBruinHomeDir() (string, error)
	RecreateHomeDir() error
}

type GitFinder interface {
	Repo(path string) (*git.Repo, error)
}

type Printer interface {
	Printf(format string, a ...interface{})
	Println(a ...interface{})
}

type CleanCommand struct {
	cm           ConfigManager
	gitFinder    GitFinder
	fs           afero.Fs
	infoPrinter  Printer
	errorPrinter Printer
}

func NewCleanCommand(cm ConfigManager, gitFinder GitFinder, fs afero.Fs, info Printer, errPrinter Printer) *CleanCommand {
	return &CleanCommand{
		cm:           cm,
		gitFinder:    gitFinder,
		fs:           fs,
		infoPrinter:  info,
		errorPrinter: errPrinter,
	}
}

func (r *CleanCommand) Run(inputPath string, cleanUvCache bool) error {
	bruinHomeDir, err := r.cm.EnsureAndGetBruinHomeDir()
	if err != nil {
		return errors.Wrap(err, "failed to get bruin home directory")
	}

	if cleanUvCache {
		if err := r.cleanUvCache(bruinHomeDir); err != nil {
			return err
		}
	}

	if err := r.cm.RecreateHomeDir(); err != nil {
		return errors.Wrap(err, "failed to recreate the home directory")
	}

	repoRoot, err := r.gitFinder.Repo(inputPath)
	if err != nil {
		r.errorPrinter.Printf("Failed to find the git repository root: %v\n", err)
		return errors.Wrap(err, "failed to find the git repository root")
	}

	logsFolder := path.Join(repoRoot.Path, LogsFolder)

	// Check if logs folder exists
	exists, err := afero.Exists(r.fs, logsFolder)
	if err != nil {
		return errors.Wrap(err, "failed to check logs folder")
	}

	if !exists {
		r.infoPrinter.Println("No log files found, nothing to clean up...")
		return nil
	}

	// Read directory contents
	entries, err := afero.ReadDir(r.fs, logsFolder)
	if err != nil {
		return errors.Wrap(err, "failed to read logs folder")
	}

	// Filter for .log files
	var logFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
			logFiles = append(logFiles, path.Join(logsFolder, entry.Name()))
		}
	}

	if len(logFiles) == 0 {
		r.infoPrinter.Println("No log files found, nothing to clean up...")
		return nil
	}

	r.infoPrinter.Printf("Found %d log files, cleaning them up...\n", len(logFiles))
	for _, f := range logFiles {
		if err := r.fs.Remove(f); err != nil {
			return errors.Wrapf(err, "failed to remove file: %s", f)
		}
	}
	r.infoPrinter.Printf("Successfully removed %d log files.\n", len(logFiles))
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
		r.infoPrinter.Println("UV is not installed yet. Nothing to clean.")
		return nil
	}

	// Check if uv is available and working
	cmd := exec.Command(uvBinaryPath, "version")
	if err := cmd.Run(); err != nil {
		return errors.Wrap(err, "uv binary exists but is not working properly")
	}

	// Prompt user for confirmation
	if !r.confirmUvCacheClean(os.Stdin) {
		r.infoPrinter.Println("UV cache cleaning cancelled by user.")
		return nil
	}

	r.infoPrinter.Println("Cleaning uv caches...")

	cleanCmd := exec.Command(uvBinaryPath, "cache", "clean")
	output, err := cleanCmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "failed to clean uv cache: %s", string(output))
	}

	r.infoPrinter.Println("Successfully cleaned uv caches.")
	return nil
}

func (r *CleanCommand) confirmUvCacheClean(reader io.Reader) bool {
	bufReader := bufio.NewReader(reader)
	fmt.Print("Are you sure you want to clean uv cache? (y/N): ")

	response, err := bufReader.ReadString('\n')
	if err != nil {
		r.errorPrinter.Printf("Error reading input: %v\n", err)
		return false
	}

	response = strings.TrimSpace(strings.ToLower(response))
	return response == "y" || response == "yes"
}
