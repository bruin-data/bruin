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
	"github.com/sourcegraph/conc/pool"
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
	p := pool.New().WithMaxGoroutines(2).WithErrors()
	p.Go(func() error { return consumePipe(stdout, output) })
	p.Go(func() error { return consumePipe(stderr, output) })

	err = cmd.Start()
	if err != nil {
		return errors.Wrap(err, "failed to start CommandInstance")
	}

	// Wait for pipe consumption to complete FIRST
	// This is critical: we must finish reading from pipes before calling cmd.Wait()
	// because cmd.Wait() will close the pipes after the command exits
	pipeErr := p.Wait()

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
	scanner := bufio.NewScanner(pipe)

	// Use a smaller buffer (4KB) for more responsive output instead of the default 64KB
	// This reduces latency when streaming subprocess output in real-time
	buf := make([]byte, 4096)
	scanner.Buffer(buf, 4096)

	for scanner.Scan() {
		// the size of the slice here is important, the added 4 at the end includes the 3 bytes for the prefix and the 1 byte for the newline
		msg := make([]byte, len(scanner.Bytes())+4)
		copy(msg, ">> ")
		copy(msg[3:], scanner.Bytes())
		msg[len(msg)-1] = '\n'

		_, err := output.Write(msg)
		if err != nil {
			return err
		}
	}

	// scanner.Err() returns nil if the scanner stopped due to EOF or a closed pipe,
	// which is the expected behavior when a subprocess finishes.
	// We only return actual errors here.
	if err := scanner.Err(); err != nil && !stderrors.Is(err, io.EOF) {
		return err
	}

	return nil
}
