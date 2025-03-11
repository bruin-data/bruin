package python

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/pkg/errors"
	"go.uber.org/zap"
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
	log := ctx.Value(executor.ContextLogger).(*zap.SugaredLogger)
	log.Debugf(
		"%s %s",
		command.Name,
		strings.Join(command.Args, " "),
	)

	cmd := exec.Command(command.Name, command.Args...) //nolint:gosec
	cmd.Dir = repo.Path

	// pass the path-related env vars by default
	cmd.Env = []string{"USERPROFILE=" + os.Getenv("USERPROFILE"), "HOMEPATH=" + os.Getenv("HOMEPATH"), "HOME=" + os.Getenv("HOME")}
	for k, v := range command.EnvVars {
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

	wg := new(errgroup.Group)
	wg.Go(func() error { return consumePipe(stdout, output) })
	wg.Go(func() error { return consumePipe(stderr, output) })

	err = cmd.Start()
	if err != nil {
		return errors.Wrap(err, "failed to start CommandInstance")
	}

	res := cmd.Wait()
	if res != nil {
		return res
	}

	err = wg.Wait()
	if err != nil {
		return errors.Wrap(err, "failed to consume pipe")
	}

	return nil
}

func consumePipe(pipe io.Reader, output io.Writer) error {
	scanner := bufio.NewScanner(pipe)
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

	return nil
}
