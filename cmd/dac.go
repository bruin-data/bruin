package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"

	"github.com/bruin-data/bruin/pkg/dac"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/bruin-data/bruin/pkg/user"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

// dacBinaryOverrideEnv points `bruin dac` at a local dac build instead of the
// managed release, for testing dac changes.
const dacBinaryOverrideEnv = "BRUIN_DAC_BINARY"

// Dac runs the pinned dac release, installing it into Bruin's home directory on
// first use. Every argument goes to dac unparsed; dac runs as a separate process
// and sends its queries back through this Bruin binary.
func Dac() *cli.Command {
	return &cli.Command{
		Name:            "dac",
		Usage:           "build and serve dashboards as code with dac (installed on first use)",
		ArgsUsage:       "[dac command and arguments]",
		Description:     "Runs the dac release pinned by this Bruin version, downloading it on first use. Run `bruin dac --help` for dac's commands.",
		SkipFlagParsing: true,
		// dac prints its own help as `bruin dac`; `bruin dac help` and
		// `bruin dac --help` must reach it.
		HideHelp: true,
		Action: func(ctx context.Context, c *cli.Command) error {
			root := c.Root()

			installCtx, stopInstall := signal.NotifyContext(ctx, dac.Signals...)
			binary, err := resolveDacBinary(installCtx, root.ErrWriter)
			stopInstall()
			if err != nil {
				return failDacCommand(root, err)
			}

			bruinPath, err := os.Executable()
			if err != nil {
				return failDacCommand(root, errors.Wrap(err, "failed to locate the running bruin binary"))
			}

			// Catch signals for as long as dac runs, so Bruin outlives it and
			// can report its exit code.
			signals := make(chan os.Signal, 1)
			signal.Notify(signals, dac.Signals...)
			defer signal.Stop(signals)

			env := dac.Environment(os.Environ(), bruinPath, telemetry.OptOut)
			err = dac.Run(binary, c.Args().Slice(), env, root.Reader, root.Writer, root.ErrWriter, signals)
			if err != nil {
				var exitError *exec.ExitError
				if errors.As(err, &exitError) {
					return cli.Exit("", subprocessExitCode(exitError))
				}
				return failDacCommand(root, errors.Wrapf(err, "failed to run dac at %s", binary))
			}
			return nil
		},
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
	}
}

// InstallDac installs the pinned dac release, so images can include it instead
// of downloading it on first use.
func InstallDac() *cli.Command {
	return &cli.Command{
		Name:  "install-dac",
		Usage: "install the dac release used by `bruin dac`",
		Action: func(ctx context.Context, c *cli.Command) error {
			root := c.Root()
			binary, err := ensureManagedDac(ctx, root.ErrWriter)
			if err != nil {
				return failDacCommand(root, err)
			}
			_, _ = fmt.Fprintln(root.Writer, binary)
			return nil
		},
	}
}

func resolveDacBinary(ctx context.Context, progress io.Writer) (string, error) {
	if override := os.Getenv(dacBinaryOverrideEnv); override != "" {
		return override, nil
	}
	return ensureManagedDac(ctx, progress)
}

func ensureManagedDac(ctx context.Context, progress io.Writer) (string, error) {
	homeDir, err := user.NewConfigManager(afero.NewOsFs()).EnsureAndGetBruinHomeDir()
	if err != nil {
		return "", errors.Wrap(err, "failed to get the bruin home directory")
	}
	installer, err := dac.NewInstaller(homeDir, progress)
	if err != nil {
		return "", err
	}
	return installer.EnsureInstalled(ctx)
}

// failDacCommand reports err on stderr and exits with 1. dac owns stdout.
func failDacCommand(root *cli.Command, err error) error {
	_, _ = fmt.Fprintf(root.ErrWriter, "%v\n", err)
	return cli.Exit("", 1)
}
