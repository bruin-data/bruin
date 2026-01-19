package python

import (
	"bufio"
	"context"
	stderrors "errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const WINDOWS = "windows"

type cmd interface {
	Run(ctx context.Context, repo *git.Repo, command *CommandInstance) error
}

type requirementsInstaller interface {
	EnsureVirtualEnvExists(ctx context.Context, repo *git.Repo, requirementsTxt string) (string, error)
}

type localPythonRunner struct {
	cmd                   cmd
	requirementsInstaller requirementsInstaller
	pathToPython          string
}

func log(ctx context.Context, message string) {
	if ctx.Value(executor.KeyPrinter) == nil {
		return
	}

	if !strings.HasSuffix(message, "\n") {
		message += "\n"
	}

	writer := ctx.Value(executor.KeyPrinter).(io.Writer)
	_, _ = writer.Write([]byte(message))
}

func (l *localPythonRunner) Run(ctx context.Context, execCtx *executionContext) error {
	pythonCommandForScript := fmt.Sprintf("%s -u -m %s", l.pathToPython, execCtx.module)
	noDependencyCommand := &CommandInstance{
		Name:    Shell,
		Args:    []string{ShellSubcommandFlag, pythonCommandForScript},
		EnvVars: execCtx.envVariables,
	}
	if execCtx.requirementsTxt == "" {
		return l.cmd.Run(ctx, execCtx.repo, noDependencyCommand)
	}

	log(ctx, "requirements.txt found, setting up the isolated environment...")
	log(ctx, "requirements.txt path: "+execCtx.requirementsTxt)
	depsPath, err := l.requirementsInstaller.EnsureVirtualEnvExists(ctx, execCtx.repo, execCtx.requirementsTxt)
	if err != nil {
		return err
	}

	if depsPath == "" {
		log(ctx, "requirements.txt is empty, executing the script right away...")
		return l.cmd.Run(ctx, execCtx.repo, noDependencyCommand)
	}

	// if there's a virtualenv, use the Python there explicitly, otherwise aliases change the runtime used
	pythonCommandForScript = fmt.Sprintf("%s/%s/%s -u -m %s", depsPath, VirtualEnvBinaryFolder, DefaultPythonExecutable, execCtx.module)
	fullCommand := fmt.Sprintf("%s/%s/activate && echo 'activated virtualenv' && %s", depsPath, VirtualEnvBinaryFolder, pythonCommandForScript)
	if runtime.GOOS != WINDOWS {
		fullCommand = ". " + fullCommand
	}

	return l.cmd.Run(ctx, execCtx.repo, &CommandInstance{
		Name:    Shell,
		Args:    []string{ShellSubcommandFlag, fullCommand},
		EnvVars: execCtx.envVariables,
	})
}

type CommandRunner struct{}

type CommandInstance struct {
	Name    string
	Args    []string
	EnvVars map[string]string
}

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
	// These will only be set if not already present in parent environment
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
	// This prevents deadlock if the command generates a lot of output
	wg := new(errgroup.Group)
	wg.Go(func() error { return consumePipe(stdout, output) })
	wg.Go(func() error { return consumePipe(stderr, output) })

	err = cmd.Start()
	if err != nil {
		return errors.Wrap(err, "failed to start CommandInstance")
	}

	// Wait for pipe consumption to complete FIRST
	// This is critical: we must finish reading from pipes before calling cmd.Wait()
	// because cmd.Wait() will close the pipes after the command exits
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
	// Scanner has a maximum token size limit, but ReadLine handles lines of any length
	// by returning isPrefix=true when a line exceeds the buffer size.
	// This is critical for ML model outputs, large JSON, and other big data workloads.
	reader := bufio.NewReaderSize(pipe, 64*1024) // 64KB buffer for efficient reading

	// Buffer to assemble full line before writing. This prevents interleaving
	// when multiple goroutines (stdout/stderr) write to the same output.
	// By writing the complete line in a single Write call, we ensure atomicity.
	var lineBuf []byte

	for {
		line, isPrefix, err := reader.ReadLine()
		if err != nil {
			if stderrors.Is(err, io.EOF) {
				break
			}
			return err
		}

		// Start building the line with prefix
		lineBuf = append(lineBuf[:0], ">> "...) // Reset and add prefix
		lineBuf = append(lineBuf, line...)

		// If the line was longer than the buffer, keep reading until complete.
		// isPrefix=true means there's more data for this line (no \n found yet).
		for isPrefix {
			line, isPrefix, err = reader.ReadLine()
			if err != nil {
				// EOF mid-line: write what we have without newline and exit
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

		// Add newline and write the complete line in a single atomic write
		lineBuf = append(lineBuf, '\n')
		if _, err := output.Write(lineBuf); err != nil {
			return err
		}
	}

	return nil
}
