package r

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
	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type cmd interface {
	Run(ctx context.Context, repo *git.Repo, command *CommandInstance) error
}

type renvInstaller interface {
	EnsureRenvExists(ctx context.Context, repo *git.Repo, renvLock string) error
}

type localRRunner struct {
	cmd           cmd
	renvInstaller renvInstaller
	pathToRscript string
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

func (l *localRRunner) Run(ctx context.Context, execCtx *executionContext) error {
	scriptPath := execCtx.asset.ExecutableFile.Path

	// If there's no renv.lock, just run the R script directly
	if execCtx.renvLock == "" {
		log(ctx, "No renv.lock found, executing R script directly...")
		return l.cmd.Run(ctx, execCtx.repo, &CommandInstance{
			Name:    l.pathToRscript,
			Args:    []string{scriptPath},
			EnvVars: execCtx.envVariables,
		})
	}

	// If renv.lock exists, ensure dependencies are installed
	log(ctx, "renv.lock found, setting up the isolated R environment...")
	log(ctx, "renv.lock path: "+execCtx.renvLock)

	err := l.renvInstaller.EnsureRenvExists(ctx, execCtx.repo, execCtx.renvLock)
	if err != nil {
		return err
	}

	log(ctx, "renv dependencies ready, executing R script...")
	return l.cmd.Run(ctx, execCtx.repo, &CommandInstance{
		Name:    l.pathToRscript,
		Args:    []string{scriptPath},
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
		return errors.Wrap(err, "failed to get stderr")
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
