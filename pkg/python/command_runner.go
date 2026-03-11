package python

import (
	"bufio"
	"context"
	stderrors "errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type cmd interface {
	Run(ctx context.Context, repo *git.Repo, command *CommandInstance) error
}

type CommandInstance struct {
	Name    string
	Args    []string
	EnvVars map[string]string
}

type CommandRunner struct{}

func (l *CommandRunner) Run(ctx context.Context, repo *git.Repo, command *CommandInstance) error {
	log := ctx.Value(executor.ContextLogger).(logger.Logger)
	log.Debugf(
		"%s %s",
		command.Name,
		strings.Join(command.Args, " "),
	)

	cmd := exec.Command(command.Name, command.Args...) //nolint:gosec
	cmd.Dir = repo.Path

	// Build environment: start with parent environment to inherit PATH, CC, CFLAGS, etc.
	envMap := make(map[string]string)

	// First, parse parent environment into a map
	for _, envVar := range os.Environ() {
		parts := strings.SplitN(envVar, "=", 2)
		if len(parts) == 2 {
			envMap[parts[0]] = parts[1]
		}
	}

	// Ensure path-related env vars are set (preserving backward compatibility)
	// These will only be set if not already present in parent environment.
	if _, exists := envMap["USERPROFILE"]; !exists {
		if val := os.Getenv("USERPROFILE"); val != "" {
			envMap["USERPROFILE"] = val
		}
	}
	if _, exists := envMap["HOMEPATH"]; !exists {
		if val := os.Getenv("HOMEPATH"); val != "" {
			envMap["HOMEPATH"] = val
		}
	}
	if _, exists := envMap["HOME"]; !exists {
		if val := os.Getenv("HOME"); val != "" {
			envMap["HOME"] = val
		}
	}

	// Override with command-specific env vars if provided
	for k, v := range command.EnvVars {
		envMap[k] = v
	}

	// Rebuild env slice
	cmd.Env = make([]string, 0, len(envMap))
	for k, v := range envMap {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	return l.RunAnyCommand(ctx, cmd)
}

func (l *CommandRunner) RunAnyCommand(ctx context.Context, cmd *exec.Cmd) error {
	var output io.Writer = os.Stdout
	if ctx.Value(executor.KeyPrinter) != nil {
		output = ctx.Value(executor.KeyPrinter).(io.Writer)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "failed to get stdout")
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return errors.Wrap(err, "failed to get stdout")
	}

	// Start reading from pipes in goroutines before starting the command
	// This prevents deadlock if the command generates a lot of output.
	wg := new(errgroup.Group)
	wg.Go(func() error { return consumePipe(stdout, output) })
	wg.Go(func() error { return consumePipe(stderr, output) })

	err = cmd.Start()
	if err != nil {
		return errors.Wrap(err, "failed to start CommandInstance")
	}

	// Wait for pipe consumption to complete FIRST
	// This prevents deadlock since cmd.Wait() will close the pipes after the command exits.
	pipeErr := wg.Wait()

	// Now wait for the command to finish
	cmdErr := cmd.Wait()

	// Return command error first if both exist
	if cmdErr != nil {
		return cmdErr
	}

	// Return pipe error if it exists
	if pipeErr != nil {
		return errors.Wrap(pipeErr, "failed to consume pipe")
	}

	return nil
}

func consumePipe(pipe io.Reader, output io.Writer) error {
	// Use bufio.Reader.ReadLine() instead of Scanner to handle unlimited line lengths.
	reader := bufio.NewReaderSize(pipe, 64*1024) // 64KB buffer for efficient reading

	for {
		line, isPrefix, err := reader.ReadLine()
		if err != nil {
			if stderrors.Is(err, io.EOF) {
				break
			}
			return err
		}

		// Start building the line with prefix.
		lineBuf := append([]byte(">> "), line...)
		for isPrefix {
			line, isPrefix, err = reader.ReadLine()
			if err != nil {
				if stderrors.Is(err, io.EOF) {
					if len(lineBuf) > 0 {
						_, _ = output.Write(lineBuf)
					}
					return nil
				}
				return err
			}
			lineBuf = append(lineBuf, line...)
		}

		lineBuf = append(lineBuf, '\n')
		if _, err := output.Write(lineBuf); err != nil {
			return err
		}
	}

	return nil
}
