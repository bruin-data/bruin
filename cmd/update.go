package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"

	"github.com/pkg/errors"
	"github.com/urfave/cli/v3"

	"github.com/bruin-data/bruin/pkg/telemetry"
)

func Update() *cli.Command {
	return &cli.Command{
		Name:  "update",
		Usage: "Update Bruin CLI to the latest version",
		Action: func(ctx context.Context, c *cli.Command) error {
			// Show current version before updating
			currentVersion := c.Root().Version
			infoPrinter.Printf("Current Bruin version: %s\n", currentVersion)

			// Check if we're on Windows and warn about using the right terminal
			if runtime.GOOS == "windows" {
				infoPrinter.Println("Note: If you're on Windows, make sure to run this command in Git Bash or WSL terminal.")
			}

			// Inform user what we're doing
			infoPrinter.Println("Downloading and running the latest Bruin installation script...")

			// Determine which command to use for downloading
			var cmd *exec.Cmd
			switch {
			case isCommandAvailable("curl"):
				cmd = exec.Command("curl", "-LsSf", "https://getbruin.com/install/cli")
			case isCommandAvailable("wget"):
				cmd = exec.Command("wget", "-qO-", "https://getbruin.com/install/cli")
			default:
				return errors.New("neither curl nor wget is available - please install one of them to update Bruin")
			}

			// Create a pipe to sh
			shCmd := exec.Command("sh")

			// Connect the download command output to sh input
			downloadStdout, err := cmd.StdoutPipe()
			if err != nil {
				return errors.Wrap(err, "failed to create pipe for download command")
			}

			shCmd.Stdin = downloadStdout

			// Create pipes for sh command to stream output
			shStdout, err := shCmd.StdoutPipe()
			if err != nil {
				return errors.Wrap(err, "failed to create stdout pipe")
			}

			shStderr, err := shCmd.StderrPipe()
			if err != nil {
				return errors.Wrap(err, "failed to create stderr pipe")
			}

			// Start both commands
			if err := cmd.Start(); err != nil {
				return errors.Wrap(err, "failed to start download command")
			}

			if err := shCmd.Start(); err != nil {
				return errors.Wrap(err, "failed to start installation script")
			}

			// Stream output from both stdout and stderr
			go streamOutput(shStdout, os.Stdout)
			go streamOutput(shStderr, os.Stderr)

			// Wait for download command to finish
			if err := cmd.Wait(); err != nil {
				return errors.Wrap(err, "download command failed")
			}

			// Wait for installation script to finish
			if err := shCmd.Wait(); err != nil {
				return errors.Wrap(err, "installation script failed")
			}

			successPrinter.Println("\nBruin CLI update completed successfully!")
			infoPrinter.Println("You may need to restart your shell or run 'source ~/.bashrc' (or equivalent) to use the updated version.")

			return nil
		},
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
	}
}

// isCommandAvailable checks if a command is available in the system PATH.
func isCommandAvailable(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}

// streamOutput streams data from reader to writer line by line.
func streamOutput(reader io.Reader, writer io.Writer) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		fmt.Fprintln(writer, scanner.Text())
	}
}
