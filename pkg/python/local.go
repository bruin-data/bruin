package python

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type cmd interface {
	Run(ctx context.Context, repo *git.Repo, command *command) error
}

type requirementsInstaller interface {
	EnsureVirtualEnvExists(ctx context.Context, repo *git.Repo, requirementsTxt string) (string, error)
}

type localPythonRunner struct {
	cmd                   cmd
	requirementsInstaller requirementsInstaller
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
	noDependencyCommand := &command{
		Name:    "python3",
		Args:    []string{"-u", "-m", execCtx.module},
		EnvVars: execCtx.envVariables,
	}
	if execCtx.requirementsTxt == "" {
		return l.cmd.Run(ctx, execCtx.repo, noDependencyCommand)
	}

	log(ctx, "requirements.txt found, setting up the isolated environment...")
	depsPath, err := l.requirementsInstaller.EnsureVirtualEnvExists(ctx, execCtx.repo, execCtx.requirementsTxt)
	if err != nil {
		return err
	}

	if depsPath == "" {
		log(ctx, "requirements.txt is empty, executing the script right away...")
		return l.cmd.Run(ctx, execCtx.repo, noDependencyCommand)
	}
	fullCommand := fmt.Sprintf("source %s/bin/activate && echo 'activated virtualenv' && python3 -u -m %s", depsPath, execCtx.module)
	return l.cmd.Run(ctx, execCtx.repo, &command{
		Name:    Shell,
		Args:    []string{"-c", fullCommand},
		EnvVars: execCtx.envVariables,
	})
}

type commandRunner struct{}

type command struct {
	Name    string
	Args    []string
	EnvVars map[string]string
}

func (l *commandRunner) Run(ctx context.Context, repo *git.Repo, command *command) error {
	cmd := exec.Command(command.Name, command.Args...) //nolint:gosec
	cmd.Dir = repo.Path
	cmd.Env = make([]string, len(command.EnvVars))
	for k, v := range command.EnvVars {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

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
		return errors.Wrap(err, "failed to start command")
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
